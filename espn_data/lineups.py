"""Lineup tracking from play-by-play data.

Three approaches:
1. Ground truth: Uses explicit substitution events (2025+ data only)
2. Heuristic inference: Infers on-court players from event participation,
   constrained by box score minutes. Works on all years.
3. HMM inference: Probabilistic model treating lineups as hidden states
   with player appearances as observations. Uses forward algorithm for
   exact posterior inference. Works on all years.

All produce the same output format: for each play, the set of players
on court for each team.
"""

import json
import logging
import re
from itertools import combinations
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("espn_data")

# Play types that identify a player as on-court
_PLAYER_EVENT_TYPES = frozenset([
    "JumpShot", "Three Point Jump Shot", "LayUpShot", "DunkShot", "TipShot",
    "MadeFreeThrow", "PersonalFoul", "Lost Ball Turnover", "Foul Turnover",
    "Steal", "Block Shot", "Offensive Rebound", "Defensive Rebound",
    "Jumpball", "Technical Foul",
])

# Events where we're confident the player must be on court
# (as opposed to e.g. a technical foul which can be called on bench players)
_STRONG_EVIDENCE_TYPES = frozenset([
    "JumpShot", "Three Point Jump Shot", "LayUpShot", "DunkShot", "TipShot",
    "MadeFreeThrow", "Lost Ball Turnover", "Foul Turnover",
    "Steal", "Block Shot", "Offensive Rebound", "Defensive Rebound",
    "Jumpball",
])

SUB_TYPE_ID = "584"


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

def load_game(game_path: Path) -> dict:
    """Load a raw game JSON file."""
    with open(game_path, "r") as f:
        return json.load(f)


def get_starters_and_minutes(game_data: dict) -> dict[str, dict]:
    """Extract starter status and minutes per player from boxscore.

    Returns
    -------
    dict keyed by team_id, each value is a dict keyed by player_id with:
        {"name": str, "starter": bool, "minutes": int, "position": str}
    """
    result = {}
    boxscore = game_data.get("boxscore", {})

    for team_entry in boxscore.get("players", []):
        team = team_entry.get("team", {})
        team_id = str(team.get("id", ""))
        if not team_id:
            continue

        players = {}
        for stat_group in team_entry.get("statistics", []):
            for athlete_entry in stat_group.get("athletes", []):
                athlete = athlete_entry.get("athlete", {})
                player_id = str(athlete.get("id", ""))
                if not player_id:
                    continue

                stats = athlete_entry.get("stats", [])
                minutes = 0
                if stats:
                    try:
                        minutes = int(stats[0])
                    except (ValueError, TypeError):
                        pass

                players[player_id] = {
                    "name": athlete.get("displayName", ""),
                    "starter": athlete_entry.get("starter", False),
                    "minutes": minutes,
                    "position": athlete.get("position", {}).get("abbreviation", ""),
                }

        result[team_id] = players

    return result


def get_home_away_ids(game_data: dict) -> tuple[Optional[str], Optional[str]]:
    """Return (home_team_id, away_team_id) from game header."""
    header = game_data.get("header", {})
    competitions = header.get("competitions", [])
    if not competitions:
        return None, None

    home_id = away_id = None
    for competitor in competitions[0].get("competitors", []):
        tid = str(competitor.get("id", ""))
        if competitor.get("homeAway") == "home":
            home_id = tid
        else:
            away_id = tid

    return home_id, away_id


# ---------------------------------------------------------------------------
# Ground truth lineup tracker (uses substitution events)
# ---------------------------------------------------------------------------

def track_lineups_ground_truth(game_data: dict) -> Optional[pd.DataFrame]:
    """Track lineups using explicit substitution events.

    Returns None if the game has no substitution data.

    Handles two key edge cases:
    - Sub batching: when multiple subs happen at the same clock time,
      all outs are processed before ins to keep lineup size at 5.
    - Period transitions: at the start of each new period, lineups are
      inferred from the first sub batch (who gets subbed out was on court,
      who gets subbed in was not) rather than blindly resetting to game starters.

    Returns
    -------
    DataFrame with one row per play, columns include:
        play_id, sequence_number, period, clock, clock_seconds,
        play_type, play_type_id, team_id, text,
        score_home, score_away, scoring_play, score_value,
        home_on_court (comma-separated player IDs),
        away_on_court (comma-separated player IDs),
        all_player_ids, home_team_id, away_team_id
    """
    plays = game_data.get("plays", [])
    if not plays:
        return None

    # Check if this game has substitution data
    has_subs = any(
        isinstance(p.get("type"), dict) and p["type"].get("id") == SUB_TYPE_ID
        for p in plays
    )
    if not has_subs:
        return None

    home_id, away_id = get_home_away_ids(game_data)
    if not home_id or not away_id:
        return None

    roster = get_starters_and_minutes(game_data)

    # Pre-process: collect all subs and group them into batches
    # A batch = consecutive sub events at the same (period, clock, team)
    parsed_plays = []
    for play in plays:
        if not isinstance(play, dict):
            continue
        parsed_plays.append(_parse_play(play))

    # Pre-process sub batches: within each consecutive run of sub events,
    # reorder so outs come before ins (per team) to keep lineup size at 5.
    parsed_plays = _reorder_sub_batches(parsed_plays)

    # Initialize with period 1 starters
    on_court = {home_id: set(), away_id: set()}
    for tid in [home_id, away_id]:
        team_roster = roster.get(tid, {})
        for pid, info in team_roster.items():
            if info["starter"]:
                on_court[tid].add(pid)

    # Don't reset lineups at period boundaries — the sub events at the start
    # of each period handle transitions naturally. Resetting to game starters
    # causes errors because 2nd-half starters often differ.

    rows = []

    for pp in parsed_plays:
        # Process substitution (outs already ordered before ins within each batch)
        if pp["type_id"] == SUB_TYPE_ID and pp["team_id"] in on_court:
            pid = pp["sub_player_id"]
            if pid:
                if pp["sub_direction"] == "in":
                    on_court[pp["team_id"]].add(pid)
                elif pp["sub_direction"] == "out":
                    on_court[pp["team_id"]].discard(pid)

        rows.append({
            "play_id": pp["play_id"],
            "sequence_number": pp["sequence_number"],
            "period": pp["period"],
            "clock": pp["clock"],
            "clock_seconds": pp["clock_seconds"],
            "play_type": pp["type_text"],
            "play_type_id": pp["type_id"],
            "team_id": pp["team_id"],
            "text": pp["text"],
            "score_home": pp["score_home"],
            "score_away": pp["score_away"],
            "scoring_play": pp["scoring_play"],
            "score_value": pp["score_value"],
            "home_on_court": ",".join(sorted(on_court.get(home_id, set()))),
            "away_on_court": ",".join(sorted(on_court.get(away_id, set()))),
            "all_player_ids": ",".join(pp["all_player_ids"]),
        })

    df = pd.DataFrame(rows)
    df["home_team_id"] = home_id
    df["away_team_id"] = away_id
    return df


def _parse_play(play: dict) -> dict:
    """Parse a raw play dict into a flat structure."""
    play_type = play.get("type", {})
    type_id = str(play_type.get("id", ""))
    type_text = play_type.get("text", "")
    period = play.get("period", {})
    period_num = period.get("number", 0) if isinstance(period, dict) else 0
    clock = play.get("clock", {})
    clock_display = clock.get("displayValue", "") if isinstance(clock, dict) else ""
    team = play.get("team")
    team_id = str(team.get("id", "")) if isinstance(team, dict) else ""
    text = play.get("text") or ""

    # Sub-specific fields
    sub_player_id = None
    sub_direction = None
    if type_id == SUB_TYPE_ID:
        participants = play.get("participants", [])
        if participants and isinstance(participants[0], dict):
            athlete = participants[0].get("athlete", {})
            if isinstance(athlete, dict):
                sub_player_id = str(athlete.get("id", ""))
        text_lower = text.lower()
        if "subbing in" in text_lower:
            sub_direction = "in"
        elif "subbing out" in text_lower:
            sub_direction = "out"

    return {
        "play_id": play.get("id"),
        "sequence_number": play.get("sequenceNumber"),
        "period": period_num,
        "clock": clock_display,
        "clock_seconds": _clock_to_seconds(clock_display),
        "type_id": type_id,
        "type_text": type_text,
        "team_id": team_id,
        "text": text,
        "score_home": play.get("homeScore"),
        "score_away": play.get("awayScore"),
        "scoring_play": play.get("scoringPlay", False),
        "score_value": play.get("scoreValue", 0),
        "all_player_ids": _extract_player_ids(play),
        "sub_player_id": sub_player_id,
        "sub_direction": sub_direction,
    }


def _reorder_sub_batches(parsed_plays: list[dict]) -> list[dict]:
    """Reorder substitution events so outs come before ins within each batch.

    A batch is a consecutive run of sub events. Within each batch, events are
    grouped by team with outs before ins per team, ensuring lineup counts
    stay at 5 during processing.
    """
    result = []
    i = 0
    while i < len(parsed_plays):
        pp = parsed_plays[i]
        if pp["type_id"] != SUB_TYPE_ID:
            result.append(pp)
            i += 1
            continue

        # Collect the full batch of consecutive subs
        batch = [pp]
        j = i + 1
        while j < len(parsed_plays) and parsed_plays[j]["type_id"] == SUB_TYPE_ID:
            batch.append(parsed_plays[j])
            j += 1

        # Group by team, then outs before ins within each team
        teams_seen = []
        team_subs = {}
        for p in batch:
            tid = p["team_id"]
            if tid not in team_subs:
                teams_seen.append(tid)
                team_subs[tid] = {"outs": [], "ins": [], "other": []}
            if p["sub_direction"] == "out":
                team_subs[tid]["outs"].append(p)
            elif p["sub_direction"] == "in":
                team_subs[tid]["ins"].append(p)
            else:
                team_subs[tid]["other"].append(p)

        for tid in teams_seen:
            result.extend(team_subs[tid]["outs"])
            result.extend(team_subs[tid]["ins"])
            result.extend(team_subs[tid]["other"])

        i = j

    return result


# ---------------------------------------------------------------------------
# Heuristic lineup tracker (infers from player appearances)
# ---------------------------------------------------------------------------

def track_lineups_heuristic(game_data: dict) -> Optional[pd.DataFrame]:
    """Infer lineups from player appearances in play-by-play events.

    Strategy:
    1. Start with known starters.
    2. For each play that identifies a player, mark them as "last seen."
    3. At any point, the on-court players are the 5 most recently seen
       players for each team, with minutes-weighted tie-breaking.
    4. No period resets — continuity is maintained across periods.

    Returns the same format as track_lineups_ground_truth.
    """
    plays = game_data.get("plays", [])
    if not plays:
        return None

    home_id, away_id = get_home_away_ids(game_data)
    if not home_id or not away_id:
        return None

    roster = get_starters_and_minutes(game_data)

    # Build player-to-team mapping
    player_team = {}
    for tid in [home_id, away_id]:
        for pid in roster.get(tid, {}):
            player_team[pid] = tid

    # Players who actually played (minutes > 0)
    eligible = {home_id: set(), away_id: set()}
    for tid in [home_id, away_id]:
        for pid, info in roster.get(tid, {}).items():
            if info["minutes"] > 0:
                eligible[tid].add(pid)

    # Parse all plays
    parsed = []
    for play in plays:
        if not isinstance(play, dict):
            continue
        parsed.append(_parse_play(play))

    # Initialize: starters get index 0 as their "last seen"
    last_seen = {}  # pid -> play_index
    current_period = 0
    for tid in [home_id, away_id]:
        for pid, info in roster.get(tid, {}).items():
            if info["starter"]:
                last_seen[pid] = 0

    rows = []

    for play_idx, pp in enumerate(parsed):
        # Period change: boost starters back to "recently seen" since they
        # typically re-enter at the start of each half/OT
        if pp["period"] > current_period:
            current_period = pp["period"]
            for tid in [home_id, away_id]:
                for pid, info in roster.get(tid, {}).items():
                    if info["starter"]:
                        last_seen[pid] = play_idx

        # Update last_seen for players in this play
        if pp["type_text"] in _PLAYER_EVENT_TYPES:
            for pid in pp["all_player_ids"]:
                if pid in player_team:
                    last_seen[pid] = play_idx

        # Determine current on-court: top 5 most recently seen per team
        # Tie-break by box score minutes (higher-minute players more likely on court)
        on_court = {}
        for tid in [home_id, away_id]:
            candidates = []
            for pid in eligible[tid]:
                if pid not in last_seen:
                    continue
                minutes = roster.get(tid, {}).get(pid, {}).get("minutes", 0)
                candidates.append((last_seen[pid], minutes, pid))

            candidates.sort(reverse=True)
            on_court[tid] = {pid for _, _, pid in candidates[:5]}

        rows.append({
            "play_id": pp["play_id"],
            "sequence_number": pp["sequence_number"],
            "period": pp["period"],
            "clock": pp["clock"],
            "clock_seconds": pp["clock_seconds"],
            "play_type": pp["type_text"],
            "play_type_id": pp["type_id"],
            "team_id": pp["team_id"],
            "text": pp["text"],
            "score_home": pp["score_home"],
            "score_away": pp["score_away"],
            "scoring_play": pp["scoring_play"],
            "score_value": pp["score_value"],
            "home_on_court": ",".join(sorted(on_court.get(home_id, set()))),
            "away_on_court": ",".join(sorted(on_court.get(away_id, set()))),
            "all_player_ids": ",".join(pp["all_player_ids"]),
        })

    df = pd.DataFrame(rows)
    df["home_team_id"] = home_id
    df["away_team_id"] = away_id
    return df


# Dead ball events where substitutions can happen
_DEAD_BALL_TYPES = frozenset([
    "MadeFreeThrow", "PersonalFoul", "OfficialTVTimeOut", "ShortTimeOut",
    "RegularTimeOut", "End Period", "Dead Ball Rebound", "Technical Foul",
    "Jumpball",
])


def track_lineups_hmm(game_data: dict) -> Optional[pd.DataFrame]:
    """Infer lineups using forward-backward smoothing.

    Two-pass algorithm:
    1. Forward pass: for each player, record the play index where they were
       last seen (looking backwards from current play).
    2. Backward pass: for each player, record the play index where they will
       next be seen (looking forwards from current play).
    3. Combine: at each play, score each player based on:
       - Distance to last sighting (forward evidence)
       - Distance to next sighting (backward evidence)
       - Box score minutes (prior on how much they play)
       Select top 5 per team.

    The key insight: if player A was last seen 5 plays ago and will next be
    seen 30 plays from now, they're likely off court. If player B was last
    seen 20 plays ago but will be seen again in 2 plays, they're likely
    on court now.

    Returns the same format as track_lineups_ground_truth.
    """
    plays = game_data.get("plays", [])
    if not plays:
        return None

    home_id, away_id = get_home_away_ids(game_data)
    if not home_id or not away_id:
        return None

    roster = get_starters_and_minutes(game_data)

    # Build player-to-team mapping
    player_team = {}
    for tid in [home_id, away_id]:
        for pid in roster.get(tid, {}):
            player_team[pid] = tid

    # Players who actually played (minutes > 0)
    eligible = {home_id: set(), away_id: set()}
    for tid in [home_id, away_id]:
        for pid, info in roster.get(tid, {}).items():
            if info["minutes"] > 0:
                eligible[tid].add(pid)

    # Minutes prior: P(on court at a random time) ≈ minutes / 40
    minutes_prior = {}
    for tid in [home_id, away_id]:
        for pid, info in roster.get(tid, {}).items():
            if info["minutes"] > 0:
                minutes_prior[pid] = min(info["minutes"] / 40.0, 1.0)

    # Starters
    starters = {}
    for tid in [home_id, away_id]:
        starters[tid] = {
            pid for pid, info in roster.get(tid, {}).items() if info["starter"]
        }

    # Parse all plays
    parsed = []
    for play in plays:
        if not isinstance(play, dict):
            continue
        parsed.append(_parse_play(play))

    n_plays = len(parsed)

    # Collect all player sightings per team
    # sighting = play index where the player was identified
    player_sightings = {}  # pid -> sorted list of play indices
    for i, pp in enumerate(parsed):
        if pp["type_text"] not in _STRONG_EVIDENCE_TYPES:
            continue
        for pid in pp["all_player_ids"]:
            if pid in player_team:
                player_sightings.setdefault(pid, []).append(i)

    # Add synthetic sightings for starters at index 0 and at period starts
    period_starts = {}
    current_period = 0
    for i, pp in enumerate(parsed):
        if pp["period"] > current_period:
            current_period = pp["period"]
            period_starts[current_period] = i

    for tid in [home_id, away_id]:
        for pid in starters[tid]:
            sightings = player_sightings.setdefault(pid, [])
            if not sightings or sightings[0] != 0:
                sightings.insert(0, 0)
            # At period starts, re-insert starters
            for period, start_idx in period_starts.items():
                if period > 1 and start_idx not in sightings:
                    import bisect
                    bisect.insort(sightings, start_idx)

    # Precompute for each player: at each play index, what is the
    # distance to the previous sighting and the next sighting?
    # Use binary search for efficiency.
    import bisect

    def _get_prev_next(pid: str, play_idx: int):
        """Return (dist_to_prev_sighting, dist_to_next_sighting) for a player."""
        sightings = player_sightings.get(pid)
        if not sightings:
            return n_plays, n_plays  # never seen

        pos = bisect.bisect_right(sightings, play_idx)

        # Distance to previous sighting (at or before play_idx)
        if pos > 0:
            dist_prev = play_idx - sightings[pos - 1]
        else:
            dist_prev = n_plays  # no previous sighting

        # Distance to next sighting (at or after play_idx)
        # Check if play_idx itself is a sighting
        if pos > 0 and sightings[pos - 1] == play_idx:
            dist_next = 0
        elif pos < len(sightings):
            dist_next = sightings[pos] - play_idx
        else:
            dist_next = n_plays  # no future sighting

        return dist_prev, dist_next

    # Position groups for position-aware tiebreaking
    _GUARD_POSITIONS = {"G", "PG", "SG"}
    _FORWARD_POSITIONS = {"F", "C", "PF", "SF"}

    player_position_group = {}  # pid -> "G" or "F"
    for tid in [home_id, away_id]:
        for pid, info in roster.get(tid, {}).items():
            pos = info.get("position", "")
            if pos in _GUARD_POSITIONS:
                player_position_group[pid] = "G"
            else:
                player_position_group[pid] = "F"

    # Expected position distribution from starters
    starter_pos_dist = {}  # tid -> {"G": n, "F": n}
    for tid in [home_id, away_id]:
        g_count = sum(1 for pid in starters[tid] if player_position_group.get(pid) == "G")
        starter_pos_dist[tid] = {"G": g_count, "F": 5 - g_count}

    # Score function: combine forward, backward, and minutes evidence
    def _player_score(pid: str, play_idx: int) -> float:
        """Score how likely a player is to be on court at a given play.

        Higher score = more likely on court.
        """
        dist_prev, dist_next = _get_prev_next(pid, play_idx)
        prior = minutes_prior.get(pid, 0.25)

        # Convert distances to "evidence" using exponential decay
        decay = 0.05
        fwd_evidence = np.exp(-decay * dist_prev)
        bwd_evidence = np.exp(-decay * dist_next)

        # Combine with max: you're likely on court if you were recently seen
        # OR will be seen soon. This creates a natural crossover between
        # two players competing for a slot — one's forward evidence is high
        # while the other's backward evidence is high.
        combined = max(fwd_evidence, bwd_evidence)

        # Weight by minutes prior — high-minute players get a boost
        return combined * (0.7 + 0.3 * prior)

    def _select_top5_with_position(tid: str, candidates: list) -> set[str]:
        """Select top 5 players with position-aware tiebreaking.

        If the naive top-5 has a different guard/forward distribution than
        the starting lineup, and a "wrong-position" player near the cutoff
        has a similar score to a "right-position" player just outside,
        swap them.
        """
        candidates.sort(reverse=True)
        top5 = [(score, pid) for score, pid in candidates[:5]]
        rest = [(score, pid) for score, pid in candidates[5:]]

        if not rest:
            return {pid for _, pid in top5}

        # Count position groups in current top 5
        expected = starter_pos_dist[tid]
        current_g = sum(1 for _, pid in top5 if player_position_group.get(pid) == "G")
        current_f = 5 - current_g

        # If distribution matches expected, no adjustment needed
        if current_g == expected["G"]:
            return {pid for _, pid in top5}

        # Try swapping the weakest "over-represented" position player
        # with the strongest "under-represented" position player from rest
        margin = 0.12  # only swap if scores are within this margin

        if current_g > expected["G"]:
            over_pos, under_pos = "G", "F"
        else:
            over_pos, under_pos = "F", "G"

        # Find weakest over-represented player in top5
        over_candidates = [
            (s, p) for s, p in top5
            if player_position_group.get(p) == over_pos
        ]
        if not over_candidates:
            return {pid for _, pid in top5}
        weakest_over = min(over_candidates, key=lambda x: x[0])

        # Find strongest under-represented player in rest
        under_candidates = [
            (s, p) for s, p in rest
            if player_position_group.get(p) == under_pos
        ]
        if not under_candidates:
            return {pid for _, pid in top5}
        strongest_under = max(under_candidates, key=lambda x: x[0])

        # Swap if scores are close enough
        if weakest_over[0] - strongest_under[0] < margin:
            result = {pid for _, pid in top5}
            result.discard(weakest_over[1])
            result.add(strongest_under[1])
            return result

        return {pid for _, pid in top5}

    # Build lineups at each play
    rows = []
    for play_idx, pp in enumerate(parsed):
        on_court = {}
        for tid in [home_id, away_id]:
            candidates = []
            for pid in eligible[tid]:
                score = _player_score(pid, play_idx)
                candidates.append((score, pid))
            on_court[tid] = _select_top5_with_position(tid, candidates)

        rows.append({
            "play_id": pp["play_id"],
            "sequence_number": pp["sequence_number"],
            "period": pp["period"],
            "clock": pp["clock"],
            "clock_seconds": pp["clock_seconds"],
            "play_type": pp["type_text"],
            "play_type_id": pp["type_id"],
            "team_id": pp["team_id"],
            "text": pp["text"],
            "score_home": pp["score_home"],
            "score_away": pp["score_away"],
            "scoring_play": pp["scoring_play"],
            "score_value": pp["score_value"],
            "home_on_court": ",".join(sorted(on_court.get(home_id, set()))),
            "away_on_court": ",".join(sorted(on_court.get(away_id, set()))),
            "all_player_ids": ",".join(pp["all_player_ids"]),
        })

    df = pd.DataFrame(rows)
    df["home_team_id"] = home_id
    df["away_team_id"] = away_id

    # Post-processing: minutes budget constraint (optional)
    # Disabled for now — marginal gains don't justify the complexity
    # and can hurt on some game samples.
    # df = _apply_minutes_budget(df, roster, home_id, away_id, eligible,
    #                            minutes_prior, _player_score, n_plays,
    #                            player_position_group, starter_pos_dist)

    return df


def _apply_minutes_budget(
    df: pd.DataFrame,
    roster: dict,
    home_id: str,
    away_id: str,
    eligible: dict,
    minutes_prior: dict,
    score_fn,
    n_plays: int,
    player_position_group: dict,
    starter_pos_dist: dict,
) -> pd.DataFrame:
    """Post-process lineups to better match box score minutes.

    For each team:
    1. Count how many plays each player is assigned on-court
    2. Compare to expected (minutes/40 * total_plays)
    3. For over-allocated players, find their weakest plays and swap them
       with the strongest under-allocated player at that play
    """
    for tid in [home_id, away_id]:
        side = "home" if tid == home_id else "away"
        col = f"{side}_on_court"

        # Count current allocation
        player_play_count = {}
        for pid in eligible[tid]:
            player_play_count[pid] = 0

        for idx, row in df.iterrows():
            lineup = set(row[col].split(",")) - {""}
            for pid in lineup:
                if pid in player_play_count:
                    player_play_count[pid] += 1

        total_plays = len(df)

        # Expected plays per player
        expected = {}
        for pid in eligible[tid]:
            expected[pid] = minutes_prior.get(pid, 0.25) * total_plays

        # Find over/under allocated players
        over = {pid: player_play_count[pid] - expected[pid]
                for pid in eligible[tid]
                if player_play_count[pid] > expected[pid] * 1.15}  # >15% over
        under = {pid: expected[pid] - player_play_count[pid]
                 for pid in eligible[tid]
                 if player_play_count[pid] < expected[pid] * 0.85}  # >15% under

        if not over or not under:
            continue

        # For each over-allocated player, find plays where their score is
        # weakest and an under-allocated player could replace them
        # Collect (play_idx, over_pid, over_score, under_pid, under_score)
        swaps = []
        for idx in range(len(df)):
            row = df.iloc[idx]
            lineup = set(row[col].split(",")) - {""}

            for over_pid in over:
                if over_pid not in lineup:
                    continue
                over_score = score_fn(over_pid, idx)

                for under_pid in under:
                    if under_pid in lineup:
                        continue
                    under_score = score_fn(under_pid, idx)

                    # Only swap if scores are close (under player is plausible)
                    if under_score > over_score * 0.5:
                        benefit = (over[over_pid] + under[under_pid]) / total_plays
                        score_gap = over_score - under_score
                        swaps.append((benefit - score_gap * 0.5, idx,
                                     over_pid, under_pid))

        # Apply best swaps greedily
        swaps.sort(reverse=True)
        swaps_applied = 0
        max_swaps = int(sum(over.values()) * 0.3)  # conservative: fix at most 30%

        for _, idx, over_pid, under_pid in swaps:
            if swaps_applied >= max_swaps:
                break
            if over.get(over_pid, 0) <= 0 or under.get(under_pid, 0) <= 0:
                continue

            row = df.iloc[idx]
            lineup = set(row[col].split(",")) - {""}
            if over_pid not in lineup or under_pid in lineup:
                continue

            lineup.discard(over_pid)
            lineup.add(under_pid)
            df.at[df.index[idx], col] = ",".join(sorted(lineup))

            over[over_pid] -= 1
            under[under_pid] -= 1
            swaps_applied += 1

    return df


# ---------------------------------------------------------------------------
# Validation: compare ground truth vs heuristic
# ---------------------------------------------------------------------------

def validate_tracker(
    game_data: dict,
    tracker_fn=None,
) -> Optional[dict]:
    """Compare a lineup tracker against ground truth for one game.

    Parameters
    ----------
    game_data : dict
        Raw game JSON data.
    tracker_fn : callable, optional
        Function that takes game_data and returns a DataFrame.
        Defaults to track_lineups_heuristic.

    Returns None if the game lacks substitution data (can't validate).
    """
    if tracker_fn is None:
        tracker_fn = track_lineups_heuristic

    gt = track_lineups_ground_truth(game_data)
    predicted = tracker_fn(game_data)

    if gt is None or predicted is None:
        return None

    # Only compare on non-sub, non-timeout plays (actual basketball events)
    noise_types = {"Substitution", "OfficialTVTimeOut", "ShortTimeOut",
                   "RegularTimeOut", "End Period", "End Game",
                   "Dead Ball Rebound"}

    gt_filtered = gt[~gt["play_type"].isin(noise_types)].copy()
    p_filtered = predicted[~predicted["play_type"].isin(noise_types)].copy()

    # Align on sequence_number
    merged = gt_filtered.merge(
        p_filtered,
        on="sequence_number",
        suffixes=("_gt", "_p"),
    )

    if merged.empty:
        return None

    results = {"total_plays": len(merged)}

    for side in ["home", "away"]:
        col_gt = f"{side}_on_court_gt"
        col_p = f"{side}_on_court_p"

        exact_matches = 0
        total_correct_players = 0
        total_players = 0

        for _, row in merged.iterrows():
            gt_set = set(row[col_gt].split(",")) if row[col_gt] else set()
            p_set = set(row[col_p].split(",")) if row[col_p] else set()

            gt_set.discard("")
            p_set.discard("")

            if gt_set == p_set:
                exact_matches += 1

            correct = len(gt_set & p_set)
            total_correct_players += correct
            total_players += max(len(gt_set), 1)

        n = len(merged)
        results[f"{side}_exact_accuracy"] = exact_matches / n if n else 0
        results[f"{side}_player_accuracy"] = total_correct_players / total_players if total_players else 0
        results[f"{side}_avg_correct_of_5"] = total_correct_players / n if n else 0

    return results


# Keep backward compat
def validate_heuristic(game_data: dict) -> Optional[dict]:
    return validate_tracker(game_data, track_lineups_heuristic)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_player_ids(play: dict) -> list:
    """Extract all player IDs from a play event."""
    pids = []

    # participants array
    for participant in (play.get("participants") or []):
        if isinstance(participant, dict):
            athlete = participant.get("athlete")
            if isinstance(athlete, dict) and athlete.get("id"):
                pids.append(str(athlete["id"]))

    # athlete1, athlete2
    for i in range(1, 3):
        athlete = play.get(f"athlete{i}")
        if isinstance(athlete, dict) and athlete.get("id"):
            pids.append(str(athlete["id"]))

    # athletesInvolved
    for athlete in (play.get("athletesInvolved") or []):
        if isinstance(athlete, dict) and athlete.get("id"):
            pids.append(str(athlete["id"]))

    # participantsCodes
    if isinstance(play.get("participantsCodes"), list):
        pids.extend(str(p) for p in play["participantsCodes"])

    # Deduplicate preserving order
    seen = set()
    unique = []
    for pid in pids:
        if pid not in seen:
            seen.add(pid)
            unique.append(pid)

    return unique


def _clock_to_seconds(clock_str: str) -> Optional[int]:
    """Convert clock display string (e.g. '16:34') to total seconds."""
    if not clock_str:
        return None
    match = re.match(r"(\d+):(\d+)", clock_str)
    if match:
        return int(match.group(1)) * 60 + int(match.group(2))
    return None
