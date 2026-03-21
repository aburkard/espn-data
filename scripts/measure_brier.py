"""Measure Brier score of current model's raw scores as probabilities.

For each player at each play, compare P(on court) from our model
against the ground truth binary from substitution data.

Also diagnose: what do the score distributions look like for
on-court vs off-court players?
"""

import bisect
import random
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))

from espn_data.lineups import (
    load_game, track_lineups_ground_truth,
    get_home_away_ids, get_starters_and_minutes,
    _parse_play, _STRONG_EVIDENCE_TYPES, SUB_TYPE_ID,
)


def find_games_with_subs(games_dir, n=100, seed=42):
    all_files = sorted(games_dir.glob("*.json"))
    random.seed(seed)
    random.shuffle(all_files)
    selected = []
    for fpath in all_files:
        if len(selected) >= n:
            break
        with open(fpath) as f:
            content = f.read(500_000)
        if '"id": "584"' in content:
            selected.append(fpath)
    return selected


def compute_scores_for_game(game_data):
    """Compute raw player scores at each play (same logic as track_lineups_hmm)."""
    plays = game_data.get("plays", [])
    if not plays:
        return None

    home_id, away_id = get_home_away_ids(game_data)
    if not home_id or not away_id:
        return None

    roster = get_starters_and_minutes(game_data)

    player_team = {}
    for tid in [home_id, away_id]:
        for pid in roster.get(tid, {}):
            player_team[pid] = tid

    eligible = {home_id: set(), away_id: set()}
    minutes_prior = {}
    for tid in [home_id, away_id]:
        for pid, info in roster.get(tid, {}).items():
            if info["minutes"] > 0:
                eligible[tid].add(pid)
                minutes_prior[pid] = min(info["minutes"] / 40.0, 1.0)

    starters = {}
    for tid in [home_id, away_id]:
        starters[tid] = {
            pid for pid, info in roster.get(tid, {}).items() if info["starter"]
        }

    parsed = [_parse_play(p) for p in plays if isinstance(p, dict)]
    n_plays = len(parsed)

    # Collect sightings
    player_sightings = {}
    for i, pp in enumerate(parsed):
        if pp["type_text"] not in _STRONG_EVIDENCE_TYPES:
            continue
        for pid in pp["all_player_ids"]:
            if pid in player_team:
                player_sightings.setdefault(pid, []).append(i)

    # Starter sightings
    period_starts = {}
    cur_period = 0
    for i, pp in enumerate(parsed):
        if pp["period"] > cur_period:
            cur_period = pp["period"]
            period_starts[cur_period] = i

    for tid in [home_id, away_id]:
        for pid in starters[tid]:
            sightings = player_sightings.setdefault(pid, [])
            if not sightings or sightings[0] != 0:
                sightings.insert(0, 0)
            for period, idx in period_starts.items():
                if period > 1 and idx not in sightings:
                    bisect.insort(sightings, idx)

    def get_score(pid, play_idx):
        sightings = player_sightings.get(pid)
        if not sightings:
            return 0.0

        pos = bisect.bisect_right(sightings, play_idx)
        dist_prev = (play_idx - sightings[pos - 1]) if pos > 0 else n_plays
        if pos > 0 and sightings[pos - 1] == play_idx:
            dist_next = 0
        elif pos < len(sightings):
            dist_next = sightings[pos] - play_idx
        else:
            dist_next = n_plays

        decay = 0.05
        fwd = np.exp(-decay * dist_prev)
        bwd = np.exp(-decay * dist_next)
        combined = max(fwd, bwd)
        prior = minutes_prior.get(pid, 0.25)
        return combined * (0.7 + 0.3 * prior)

    return {
        "home_id": home_id,
        "away_id": away_id,
        "eligible": eligible,
        "parsed": parsed,
        "get_score": get_score,
        "n_plays": n_plays,
    }


def main():
    games_dir = Path("data/raw/mens/2026/games")
    game_files = find_games_with_subs(games_dir, n=100)
    print(f"Analyzing {len(game_files)} games...\n")

    all_scores_on = []   # raw scores when player IS on court
    all_scores_off = []  # raw scores when player is NOT on court
    brier_sum = 0.0
    brier_count = 0
    log_loss_sum = 0.0

    noise_types = {"Substitution", "OfficialTVTimeOut", "ShortTimeOut",
                   "RegularTimeOut", "End Period", "End Game",
                   "Dead Ball Rebound"}

    for fpath in game_files:
        game_data = load_game(fpath)
        gt = track_lineups_ground_truth(game_data)
        info = compute_scores_for_game(game_data)
        if gt is None or info is None:
            continue

        home_id = info["home_id"]
        away_id = info["away_id"]
        parsed = info["parsed"]
        get_score = info["get_score"]

        # Build ground truth lookup: for each play, which players are on court
        gt_dict = {}
        for _, row in gt.iterrows():
            seq = row["sequence_number"]
            for side, tid in [("home", home_id), ("away", away_id)]:
                lineup = set(row[f"{side}_on_court"].split(",")) - {""}
                gt_dict[(seq, tid)] = lineup

        for play_idx, pp in enumerate(parsed):
            if pp["type_text"] in noise_types:
                continue

            seq = pp["sequence_number"]

            for tid in [home_id, away_id]:
                gt_lineup = gt_dict.get((seq, tid))
                if not gt_lineup:
                    continue

                for pid in info["eligible"][tid]:
                    score = get_score(pid, play_idx)
                    on_court = pid in gt_lineup

                    if on_court:
                        all_scores_on.append(score)
                    else:
                        all_scores_off.append(score)

                    # Brier score: (prediction - actual)^2
                    actual = 1.0 if on_court else 0.0
                    brier_sum += (score - actual) ** 2
                    brier_count += 1

                    # Log loss
                    p = np.clip(score, 1e-7, 1 - 1e-7)
                    log_loss_sum += -(actual * np.log(p) + (1 - actual) * np.log(1 - p))

    print(f"Total player-play observations: {brier_count:,}")
    print(f"  On court: {len(all_scores_on):,}")
    print(f"  Off court: {len(all_scores_off):,}")

    print(f"\n=== RAW SCORE AS PROBABILITY ===")
    print(f"  Brier score: {brier_sum / brier_count:.4f}")
    print(f"  Log loss: {log_loss_sum / brier_count:.4f}")

    # Score distribution
    on = np.array(all_scores_on)
    off = np.array(all_scores_off)

    print(f"\n=== SCORE DISTRIBUTION ===")
    print(f"  On court:  mean={on.mean():.3f}  median={np.median(on):.3f}  "
          f"std={on.std():.3f}  min={on.min():.3f}  max={on.max():.3f}")
    print(f"  Off court: mean={off.mean():.3f}  median={np.median(off):.3f}  "
          f"std={off.std():.3f}  min={off.min():.3f}  max={off.max():.3f}")

    # Score percentiles
    print(f"\n  On court percentiles:  10%={np.percentile(on, 10):.3f}  "
          f"25%={np.percentile(on, 25):.3f}  50%={np.percentile(on, 50):.3f}  "
          f"75%={np.percentile(on, 75):.3f}  90%={np.percentile(on, 90):.3f}")
    print(f"  Off court percentiles: 10%={np.percentile(off, 10):.3f}  "
          f"25%={np.percentile(off, 25):.3f}  50%={np.percentile(off, 50):.3f}  "
          f"75%={np.percentile(off, 75):.3f}  90%={np.percentile(off, 90):.3f}")

    # What fraction of on-court players have score > threshold?
    print(f"\n=== SEPARATION BY THRESHOLD ===")
    for t in [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]:
        on_above = (on >= t).mean()
        off_above = (off >= t).mean()
        print(f"  score >= {t:.1f}: on_court={on_above:.3f}  off_court={off_above:.3f}  "
              f"ratio={on_above/max(off_above, 0.001):.1f}x")

    # AUC-like metric: how often does an on-court player score higher
    # than an off-court player?
    n_comparisons = min(100000, len(all_scores_on))
    rng = np.random.RandomState(42)
    on_sample = rng.choice(on, n_comparisons)
    off_sample = rng.choice(off, n_comparisons)
    auc_approx = (on_sample > off_sample).mean()
    print(f"\n  Approximate AUC: {auc_approx:.3f}")


if __name__ == "__main__":
    main()
