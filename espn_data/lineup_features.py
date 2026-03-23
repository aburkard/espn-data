"""Feature extraction for lineup probability model.

Extracts 101 features per (player, play) observation for the trained
LightGBM model. Used by both training scripts and inference.
"""

import bisect
from collections import defaultdict
from typing import Optional

import numpy as np

from espn_data.lineups import (
    get_home_away_ids, get_starters_and_minutes,
    _parse_play, _STRONG_EVIDENCE_TYPES, _DEAD_BALL_TYPES,
    track_lineups_ground_truth,
)

NOISE_TYPES = frozenset({
    "Substitution", "OfficialTVTimeOut", "ShortTimeOut",
    "RegularTimeOut", "End Period", "End Game", "Dead Ball Rebound",
})

_TIMEOUT_TYPES = frozenset({"OfficialTVTimeOut", "ShortTimeOut", "RegularTimeOut"})
_FOUL_TYPES = frozenset({"PersonalFoul", "Technical Foul"})

DECAY_RATES = [0.02, 0.03, 0.05, 0.07, 0.10, 0.15, 0.20]

# Canonical feature order — must match the trained model exactly.
FEATURE_NAMES = []
for _d in DECAY_RATES:
    FEATURE_NAMES += [f"fwd_{_d}", f"bwd_{_d}", f"max_{_d}", f"min_{_d}"]
FEATURE_NAMES += [
    "minutes_prior", "is_starter",
    "norm_dist_prev", "norm_dist_next",
    "db_prev", "db_next",
    "sights_in_window", "total_sights",
    "fwd_05_x_min", "bwd_05_x_min",
    "dist_ratio", "log_dist_prev", "log_dist_next",
    "period", "norm_game_time", "score_diff",
    "foul_count",
    "teammates_10", "teammates_20", "teammates_50",
    "in_stint", "stint_length",
    "sighting_density",
    "team_fresh_slots", "team_stale_slots", "team_avg_dist_prev",
    "n_displacers", "displacement_recency",
    "top5_streak", "top5_entries", "rank_in_team",
    "minutes_remaining_est", "minutes_used_fraction",
    "last_db_is_timeout", "last_db_is_foul", "plays_since_last_db",
    "buddy_avg_score", "buddy_min_score",
    "fwd_bwd_ratio", "score_vs_team_mean", "score_vs_5th",
    "roster_depth", "roster_depth_inv",
    "team_top4_score_sum", "team_top4_score_min",
    "team_prob_mass", "implied_5th_slot",
    "n_high_conf_teammates", "n_low_conf_teammates",
    "is_period_start", "is_second_half_start", "starter_at_period_start",
    "minutes_pace_ratio", "minutes_budget_remaining",
    "n_guards_in_top5", "n_forwards_in_top5",
    "is_guard", "is_forward",
    "gap_to_5th", "gap_to_6th", "gap_5th_6th",
    "rank_variance_20", "rank_min_20", "rank_max_20", "time_in_top5_20",
    "score_diff_delta_20", "score_diff_abs",
    "db_density_10", "db_density_30",
    "top_cooccur_score", "top_cooccur_in_top5",
    "est_subs_so_far", "plays_since_period_start",
]
N_FEATURES = len(FEATURE_NAMES)


def _precompute_game_state(game_data: dict) -> Optional[dict]:
    """Build all intermediate data structures for feature extraction.

    Returns None if the game doesn't have enough data.
    """
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

    team_players = {}
    player_info = {}
    for tid in [home_id, away_id]:
        pids = []
        for pid, info in roster.get(tid, {}).items():
            if info["minutes"] > 0:
                pids.append(pid)
                pos = info.get("position", "")
                player_info[pid] = {
                    "minutes_prior": min(info["minutes"] / 40.0, 1.0),
                    "is_starter": 1.0 if info.get("starter", False) else 0.0,
                    "minutes": info["minutes"],
                    "is_guard": 1.0 if pos in ("PG", "SG", "G") else 0.0,
                    "is_forward": 1.0 if pos in ("SF", "PF", "F", "C") else 0.0,
                }
        team_players[tid] = sorted(pids)

    starters = {}
    for tid in [home_id, away_id]:
        starters[tid] = {
            pid for pid, info in roster.get(tid, {}).items() if info["starter"]
        }

    parsed = [_parse_play(p) for p in plays if isinstance(p, dict)]
    n_plays = len(parsed)
    if n_plays == 0:
        return None

    # --- Sightings ---
    player_sightings = {}
    for i, pp in enumerate(parsed):
        if pp["type_text"] not in _STRONG_EVIDENCE_TYPES:
            continue
        for pid in pp["all_player_ids"]:
            if pid in player_team:
                player_sightings.setdefault(pid, []).append(i)

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

    # --- Dead ball indices + types ---
    db_indices = []
    db_types = []
    for i, pp in enumerate(parsed):
        if pp["type_text"] in _DEAD_BALL_TYPES:
            db_indices.append(i)
            db_types.append(pp["type_text"])
    db_indices_arr = np.array(db_indices, dtype=np.int64) if db_indices else np.array([], dtype=np.int64)

    # --- Fouls ---
    player_fouls = {}
    for i, pp in enumerate(parsed):
        if pp["type_text"] == "PersonalFoul":
            for pid in pp["all_player_ids"]:
                if pid in player_team:
                    player_fouls.setdefault(pid, []).append(i)

    # --- Team sightings for teammate activity ---
    team_sighting_indices = {}
    team_sighting_pids = {}
    for tid in [home_id, away_id]:
        indices = []
        pids_at = []
        for i, pp in enumerate(parsed):
            if pp["type_text"] not in _STRONG_EVIDENCE_TYPES:
                continue
            tpids = [pid for pid in pp["all_player_ids"] if player_team.get(pid) == tid]
            if tpids:
                indices.append(i)
                pids_at.append(tpids)
        team_sighting_indices[tid] = np.array(indices, dtype=np.int64) if indices else np.array([], dtype=np.int64)
        team_sighting_pids[tid] = pids_at

    # --- Game state arrays ---
    periods = np.array([pp["period"] for pp in parsed], dtype=np.float32)
    clock_seconds = np.array([pp["clock_seconds"] or 0 for pp in parsed], dtype=np.float32)
    score_home = np.array([pp["score_home"] or 0 for pp in parsed], dtype=np.float32)
    score_away = np.array([pp["score_away"] or 0 for pp in parsed], dtype=np.float32)
    norm_game_time = np.clip(((periods - 1) * 1200 + (1200 - clock_seconds)) / 2400.0, 0, 1)
    game_progress = np.arange(n_plays, dtype=np.float32) / max(n_plays - 1, 1)

    # --- Dist prev/next ---
    player_dist_prev = {}
    player_dist_next = {}
    for tid in [home_id, away_id]:
        for pid in team_players[tid]:
            sightings = player_sightings.get(pid, [])
            sights_arr = np.array(sightings) if sightings else np.array([], dtype=int)
            n_sights = len(sights_arr)
            dp = np.full(n_plays, n_plays, dtype=np.float64)
            dn = np.full(n_plays, n_plays, dtype=np.float64)
            if n_sights > 0:
                for play_idx in range(n_plays):
                    pos = bisect.bisect_right(sights_arr, play_idx)
                    if pos > 0:
                        dp[play_idx] = play_idx - sights_arr[pos - 1]
                    if pos > 0 and sights_arr[pos - 1] == play_idx:
                        dn[play_idx] = 0
                    elif pos < n_sights:
                        dn[play_idx] = sights_arr[pos] - play_idx
            player_dist_prev[pid] = dp
            player_dist_next[pid] = dn

    # --- Raw scores ---
    player_raw_score = {}
    for tid in [home_id, away_id]:
        for pid in team_players[tid]:
            fwd = np.exp(-0.05 * player_dist_prev[pid])
            bwd = np.exp(-0.05 * player_dist_next[pid])
            mp = player_info[pid]["minutes_prior"]
            player_raw_score[pid] = np.maximum(fwd, bwd) * (0.7 + 0.3 * mp)

    # --- Team rankings ---
    team_pid_list = {}
    team_pid_to_idx = {}
    team_scores = {}
    team_ranks = {}
    team_sorted_scores = {}
    for tid in [home_id, away_id]:
        pids = team_players[tid]
        team_pid_list[tid] = pids
        pid_to_idx = {pid: i for i, pid in enumerate(pids)}
        team_pid_to_idx[tid] = pid_to_idx
        n_tp = len(pids)
        scores = np.zeros((n_tp, n_plays), dtype=np.float32)
        for i, pid in enumerate(pids):
            scores[i] = player_raw_score[pid]
        team_scores[tid] = scores
        scores_t = scores.T
        order = np.argsort(-scores_t, axis=1)
        ranks = np.empty_like(order)
        rows = np.arange(n_plays)[:, None]
        ranks[rows, order] = np.arange(n_tp)[None, :]
        team_ranks[tid] = ranks
        team_sorted_scores[tid] = np.take_along_axis(scores_t, order, axis=1)

    # --- Streaks ---
    player_streak = {}
    player_entries_30 = {}
    for tid in [home_id, away_id]:
        pids = team_pid_list[tid]
        ranks = team_ranks[tid]
        for pi, pid in enumerate(pids):
            in_top5 = ranks[:, pi] < 5
            streak = np.zeros(n_plays, dtype=np.int32)
            if n_plays > 0:
                streak[0] = int(in_top5[0])
                for j in range(1, n_plays):
                    streak[j] = (streak[j-1] + 1) if in_top5[j] else 0
            player_streak[pid] = streak
            entry_points = np.zeros(n_plays, dtype=np.int32)
            if n_plays > 0:
                entry_points[0] = int(in_top5[0])
                for j in range(1, n_plays):
                    if in_top5[j] and not in_top5[j-1]:
                        entry_points[j] = 1
                cs = np.cumsum(entry_points)
                entries = np.zeros(n_plays, dtype=np.int32)
                for j in range(n_plays):
                    lo = max(0, j - 29)
                    entries[j] = cs[j] - (cs[lo-1] if lo > 0 else 0)
            else:
                entries = np.zeros(n_plays, dtype=np.int32)
            player_entries_30[pid] = entries

    # --- Co-occurrence ---
    coappear_count = defaultdict(lambda: defaultdict(int))
    for i, pp in enumerate(parsed):
        if pp["type_text"] not in _STRONG_EVIDENCE_TYPES:
            continue
        by_team = defaultdict(list)
        for pid in pp["all_player_ids"]:
            if pid in player_team:
                by_team[player_team[pid]].append(pid)
        for tid, tpids in by_team.items():
            for a in range(len(tpids)):
                for b in range(a+1, len(tpids)):
                    coappear_count[tpids[a]][tpids[b]] += 1
                    coappear_count[tpids[b]][tpids[a]] += 1

    player_buddies = {}
    player_top_cooccur = {}
    for tid in [home_id, away_id]:
        for pid in team_players[tid]:
            counts = coappear_count.get(pid, {})
            same_team = [(cnt, bpid) for bpid, cnt in counts.items()
                         if bpid in team_pid_to_idx.get(tid, {})]
            same_team.sort(reverse=True)
            player_buddies[pid] = [bpid for _, bpid in same_team[:3]]
            player_top_cooccur[pid] = [bpid for _, bpid in same_team[:3]]

    # --- Stint detection ---
    player_stint_map = {}
    for tid in [home_id, away_id]:
        for pid in team_players[tid]:
            sightings = player_sightings.get(pid, [])
            sights_arr = np.array(sightings) if sightings else np.array([], dtype=int)
            n_sights = len(sights_arr)
            stint_map = {}
            if n_sights > 1:
                stint_start = 0
                for si in range(1, n_sights):
                    if sights_arr[si] - sights_arr[si - 1] >= 30:
                        stint_len = si - stint_start
                        for sj in range(stint_start, si):
                            stint_map[int(sights_arr[sj])] = (1.0, float(stint_len))
                        stint_start = si
                stint_len = n_sights - stint_start
                for sj in range(stint_start, n_sights):
                    stint_map[int(sights_arr[sj])] = (1.0, float(stint_len))
            elif n_sights == 1:
                stint_map[int(sights_arr[0])] = (1.0, 1.0)
            player_stint_map[pid] = stint_map

    # --- Event context ---
    last_db_type_is_timeout = np.zeros(n_plays, dtype=np.float32)
    last_db_type_is_foul = np.zeros(n_plays, dtype=np.float32)
    plays_since_db = np.full(n_plays, n_plays, dtype=np.float32)
    if len(db_indices) > 0:
        for play_idx in range(n_plays):
            pos = bisect.bisect_right(db_indices_arr, play_idx) - 1
            if pos >= 0:
                plays_since_db[play_idx] = play_idx - db_indices[pos]
                last_db_type_is_timeout[play_idx] = 1.0 if db_types[pos] in _TIMEOUT_TYPES else 0.0
                last_db_type_is_foul[play_idx] = 1.0 if db_types[pos] in _FOUL_TYPES else 0.0

    # --- Period start masks ---
    is_period_start_arr = np.zeros(n_plays, dtype=np.float32)
    is_second_half_start_arr = np.zeros(n_plays, dtype=np.float32)
    for period, start_idx in period_starts.items():
        for j in range(start_idx, min(start_idx + 3, n_plays)):
            is_period_start_arr[j] = 1.0
            if period == 2:
                is_second_half_start_arr[j] = 1.0

    period_start_indices = sorted(period_starts.values())

    return {
        "home_id": home_id, "away_id": away_id,
        "parsed": parsed, "n_plays": n_plays,
        "player_team": player_team, "team_players": team_players,
        "player_info": player_info, "starters": starters,
        "player_sightings": player_sightings,
        "db_indices_arr": db_indices_arr, "db_indices": db_indices,
        "db_types": db_types,
        "player_fouls": player_fouls,
        "team_sighting_indices": team_sighting_indices,
        "team_sighting_pids": team_sighting_pids,
        "periods": periods, "norm_game_time": norm_game_time,
        "score_home": score_home, "score_away": score_away,
        "game_progress": game_progress,
        "player_dist_prev": player_dist_prev,
        "player_dist_next": player_dist_next,
        "player_raw_score": player_raw_score,
        "team_pid_list": team_pid_list, "team_pid_to_idx": team_pid_to_idx,
        "team_scores": team_scores, "team_ranks": team_ranks,
        "team_sorted_scores": team_sorted_scores,
        "player_streak": player_streak, "player_entries_30": player_entries_30,
        "player_buddies": player_buddies, "player_top_cooccur": player_top_cooccur,
        "player_stint_map": player_stint_map,
        "last_db_type_is_timeout": last_db_type_is_timeout,
        "last_db_type_is_foul": last_db_type_is_foul,
        "plays_since_db": plays_since_db,
        "is_period_start_arr": is_period_start_arr,
        "is_second_half_start_arr": is_second_half_start_arr,
        "period_start_indices": period_start_indices,
        "coappear_count": coappear_count,
    }


def _count_teammates_in_window(gs, tid, pid, play_idx, window_size):
    ts_idx = gs["team_sighting_indices"][tid]
    if len(ts_idx) == 0:
        return 0
    lo = np.searchsorted(ts_idx, play_idx - window_size)
    hi = np.searchsorted(ts_idx, play_idx, side='right')
    seen = set()
    pids_list = gs["team_sighting_pids"][tid]
    for j in range(lo, hi):
        for p in pids_list[j]:
            if p != pid:
                seen.add(p)
    return len(seen)


def extract_features(game_data: dict, include_labels: bool = True):
    """Extract all 101 features for a game.

    Args:
        game_data: Raw game JSON dict.
        include_labels: If True, requires ground truth and returns (X, y).
            If False, returns (X, metadata) where metadata is a list of
            (sequence_number, team_id, player_id) tuples.

    Returns:
        (X, y) if include_labels=True, (X, metadata) if include_labels=False.
        Returns (None, None) if the game can't be processed.
    """
    gs = _precompute_game_state(game_data)
    if gs is None:
        return None, None

    home_id, away_id = gs["home_id"], gs["away_id"]
    parsed = gs["parsed"]
    n_plays = gs["n_plays"]

    # Ground truth (only needed for training)
    gt_dict = None
    if include_labels:
        gt = track_lineups_ground_truth(game_data)
        if gt is None:
            return None, None
        gt_dict = {}
        for _, row in gt.iterrows():
            seq = row["sequence_number"]
            for side, tid in [("home", home_id), ("away", away_id)]:
                lineup = set(row[f"{side}_on_court"].split(",")) - {""}
                gt_dict[(seq, tid)] = lineup

    valid_plays = [(i, pp) for i, pp in enumerate(parsed) if pp["type_text"] not in NOISE_TYPES]

    features = []
    labels = []
    metadata = []

    for tid in [home_id, away_id]:
        team_sign = 1.0 if tid == home_id else -1.0
        pids = gs["team_pid_list"][tid]
        n_tp = len(pids)
        pid_to_idx = gs["team_pid_to_idx"][tid]
        scores_matrix = gs["team_scores"][tid]
        ranks_matrix = gs["team_ranks"][tid]
        sorted_scores = gs["team_sorted_scores"][tid]
        depth = len(pids)

        for pid in pids:
            info = gs["player_info"][pid]
            pi = pid_to_idx[pid]
            sightings = gs["player_sightings"].get(pid, [])
            sights_arr = np.array(sightings) if sightings else np.array([], dtype=int)
            n_sights = len(sights_arr)
            total_sights_norm = n_sights / max(n_plays, 1)
            sighting_density = n_sights / max(n_plays, 1) * 100.0

            dp_all = gs["player_dist_prev"][pid]
            dn_all = gs["player_dist_next"][pid]
            raw_scores = gs["player_raw_score"][pid]

            foul_arr = np.array(gs["player_fouls"].get(pid, []), dtype=np.int64)
            stint_map = gs["player_stint_map"][pid]

            buddies = gs["player_buddies"].get(pid, [])
            buddy_raw = [gs["player_raw_score"][bpid] for bpid in buddies
                         if bpid in gs["player_raw_score"]]

            cooccur_pids = gs["player_top_cooccur"].get(pid, [])
            valid_cooccur = [cpid for cpid in cooccur_pids if cpid in gs["player_raw_score"]]

            player_minutes = info["minutes"]
            potential_displacers = [
                tpid for tpid in pids
                if tpid != pid and gs["player_info"][tpid]["minutes"] < player_minutes
            ]

            for play_idx, pp in valid_plays:
                seq = pp["sequence_number"]

                if include_labels:
                    gt_lineup = gt_dict.get((seq, tid))
                    if not gt_lineup:
                        continue

                dp = dp_all[play_idx]
                dn = dn_all[play_idx]
                mp = info["minutes_prior"]

                feat = []

                # --- Combined features (69) ---

                # 1) Multi-rate decay
                for d in DECAY_RATES:
                    fwd = np.exp(-d * dp)
                    bwd = np.exp(-d * dn)
                    feat.extend([fwd, bwd, max(fwd, bwd), min(fwd, bwd)])

                # 2) Player-level
                fwd_05 = np.exp(-0.05 * dp)
                bwd_05 = np.exp(-0.05 * dn)
                feat.extend([mp, info["is_starter"], dp / n_plays, dn / n_plays])

                if dp < n_plays and len(gs["db_indices_arr"]) > 0:
                    lo = np.searchsorted(gs["db_indices_arr"], play_idx - dp)
                    hi = np.searchsorted(gs["db_indices_arr"], play_idx, side='right')
                    db_prev = hi - lo
                else:
                    db_prev = 0
                if dn < n_plays and len(gs["db_indices_arr"]) > 0:
                    lo = np.searchsorted(gs["db_indices_arr"], play_idx)
                    hi = np.searchsorted(gs["db_indices_arr"], play_idx + dn, side='right')
                    db_next = hi - lo
                else:
                    db_next = 0
                feat.extend([db_prev, db_next])

                if n_sights > 0:
                    lo = np.searchsorted(sights_arr, play_idx - 30)
                    hi = np.searchsorted(sights_arr, play_idx + 30, side='right')
                    siw = hi - lo
                else:
                    siw = 0
                feat.extend([siw, total_sights_norm, fwd_05 * mp, bwd_05 * mp])

                # 3) Ratio/log
                dist_sum = dp + dn
                feat.extend([
                    dp / dist_sum if dist_sum > 0 else 0.5,
                    np.log1p(dp), np.log1p(dn),
                ])

                # 4) Game state
                score_diff = (gs["score_home"][play_idx] - gs["score_away"][play_idx]) * team_sign
                feat.extend([gs["periods"][play_idx], gs["norm_game_time"][play_idx], score_diff])

                # 5) Foul count
                fc = int(np.searchsorted(foul_arr, play_idx, side='right')) if len(foul_arr) > 0 else 0
                feat.append(float(fc))

                # 6) Teammates
                feat.extend([
                    _count_teammates_in_window(gs, tid, pid, play_idx, 10),
                    _count_teammates_in_window(gs, tid, pid, play_idx, 20),
                    _count_teammates_in_window(gs, tid, pid, play_idx, 50),
                ])

                # 7) Stint
                if n_sights > 0:
                    si_pos = bisect.bisect_right(sights_arr, play_idx)
                    nearest_prev = sights_arr[si_pos - 1] if si_pos > 0 else -999
                    if play_idx - nearest_prev < 30:
                        stint_info = stint_map.get(int(nearest_prev), (0.0, 0.0))
                        feat.extend([stint_info[0], stint_info[1]])
                    else:
                        feat.extend([0.0, 0.0])
                else:
                    feat.extend([0.0, 0.0])

                # 8) Sighting density
                feat.append(sighting_density)

                # 9) Sub pressure
                top5_mask = ranks_matrix[play_idx] < 5
                fresh_count = stale_count = 0
                team_dp_sum = 0.0
                for tpi in range(n_tp):
                    if top5_mask[tpi]:
                        tdp = gs["player_dist_prev"][pids[tpi]][play_idx]
                        team_dp_sum += tdp
                        if tdp <= 3:
                            fresh_count += 1
                        if tdp >= 15:
                            stale_count += 1
                feat.extend([fresh_count, stale_count, team_dp_sum / min(5, n_tp) / n_plays if n_tp > 0 else 0])

                # 10) Displacement
                n_displacers = 0
                max_displacer_recency = 0.0
                if dp < n_plays:
                    last_seen_idx = play_idx - int(dp)
                    for dpid in potential_displacers:
                        d_sights = gs["player_sightings"].get(dpid, [])
                        if d_sights:
                            dpos = bisect.bisect_right(d_sights, last_seen_idx)
                            if dpos < len(d_sights) and d_sights[dpos] <= play_idx:
                                n_displacers += 1
                                d_recency = np.exp(-0.05 * (play_idx - d_sights[dpos]))
                                if d_recency > max_displacer_recency:
                                    max_displacer_recency = d_recency
                feat.extend([n_displacers, max_displacer_recency])

                # 11) Streaks
                feat.extend([
                    gs["player_streak"][pid][play_idx] / max(n_plays, 1),
                    gs["player_entries_30"][pid][play_idx] / 10.0,
                    ranks_matrix[play_idx, pi] / max(n_tp - 1, 1),
                ])

                # 12) Clock-aware minutes
                progress = gs["game_progress"][play_idx]
                minutes_used_est = progress * 40.0
                feat.extend([
                    max(0, info["minutes"] - minutes_used_est) / 40.0,
                    min(minutes_used_est / max(info["minutes"], 1.0), 3.0) / 3.0,
                ])

                # 13) Event context
                feat.extend([
                    gs["last_db_type_is_timeout"][play_idx],
                    gs["last_db_type_is_foul"][play_idx],
                    gs["plays_since_db"][play_idx] / max(n_plays, 1),
                ])

                # 14) Buddy
                if buddy_raw:
                    buddy_scores = [br[play_idx] for br in buddy_raw]
                    feat.extend([sum(buddy_scores) / len(buddy_scores), min(buddy_scores)])
                else:
                    feat.extend([0.5, 0.5])

                # 15) Score-relative
                raw_s = raw_scores[play_idx]
                fwd_bwd_sum = fwd_05 + bwd_05
                feat.extend([
                    fwd_05 / fwd_bwd_sum if fwd_bwd_sum > 1e-8 else 0.5,
                    raw_s - scores_matrix[:, play_idx].mean(),
                    raw_s - sorted_scores[play_idx, min(4, n_tp - 1)],
                ])

                # --- Constraint features (17) ---

                teammate_scores = sorted(
                    [gs["player_raw_score"][tpid][play_idx] for tpid in pids if tpid != pid],
                    reverse=True,
                )
                top4 = teammate_scores[:4] if len(teammate_scores) >= 4 else teammate_scores
                all_team_scores_here = [gs["player_raw_score"][tpid][play_idx] for tpid in pids]

                feat.extend([
                    depth,
                    1.0 / max(depth, 1),
                    sum(top4) if top4 else 0.0,
                    min(top4) if top4 else 0.0,
                    sum(all_team_scores_here),
                    max(0.0, min(1.0, 5.0 - (sum(top4) if top4 else 0.0))),
                    sum(1 for s in teammate_scores if s > 0.8),
                    sum(1 for s in teammate_scores if s < 0.3),
                    gs["is_period_start_arr"][play_idx],
                    gs["is_second_half_start_arr"][play_idx],
                    info["is_starter"] * gs["is_period_start_arr"][play_idx],
                    min(progress * 40.0 / max(info["minutes"], 1.0), 3.0) / 3.0,
                    max(0, info["minutes"] - progress * 40.0) / 40.0,
                ])

                # Position mix
                all_team_score_pid = sorted(
                    [(gs["player_raw_score"][tpid][play_idx], tpid) for tpid in pids],
                    reverse=True,
                )
                top5_pids = [tpid for _, tpid in all_team_score_pid[:5]]
                feat.extend([
                    sum(1 for tpid in top5_pids if gs["player_info"][tpid]["is_guard"] > 0.5),
                    sum(1 for tpid in top5_pids if gs["player_info"][tpid]["is_forward"] > 0.5),
                    info["is_guard"],
                    info["is_forward"],
                ])

                # --- V2 features (15) ---

                my_score = scores_matrix[pi, play_idx]
                my_rank = ranks_matrix[play_idx, pi]
                team_scores_here = scores_matrix[:, play_idx]
                sorted_sc = np.sort(team_scores_here)[::-1]
                score_5th = sorted_sc[min(4, n_tp-1)]
                score_6th = sorted_sc[min(5, n_tp-1)] if n_tp > 5 else 0.0

                feat.extend([
                    my_score - score_5th,
                    my_score - score_6th,
                    score_5th - score_6th,
                ])

                window_start = max(0, play_idx - 20)
                ranks_window = ranks_matrix[window_start:play_idx+1, pi]
                if len(ranks_window) > 1:
                    feat.extend([
                        np.var(ranks_window) / max(n_tp, 1),
                        np.min(ranks_window) / max(n_tp - 1, 1),
                        np.max(ranks_window) / max(n_tp - 1, 1),
                        np.mean(ranks_window < 5),
                    ])
                else:
                    feat.extend([0.0, float(my_rank) / max(n_tp-1, 1),
                                 float(my_rank) / max(n_tp-1, 1),
                                 1.0 if my_rank < 5 else 0.0])

                score_diff_now = (gs["score_home"][play_idx] - gs["score_away"][play_idx]) * team_sign
                if play_idx >= 20:
                    score_diff_20ago = (gs["score_home"][play_idx-20] - gs["score_away"][play_idx-20]) * team_sign
                    feat.append(score_diff_now - score_diff_20ago)
                else:
                    feat.append(0.0)
                feat.append(abs(gs["score_home"][play_idx] - gs["score_away"][play_idx]))

                if len(gs["db_indices_arr"]) > 0:
                    lo10 = np.searchsorted(gs["db_indices_arr"], play_idx - 10)
                    hi10 = np.searchsorted(gs["db_indices_arr"], play_idx + 10, side='right')
                    lo30 = np.searchsorted(gs["db_indices_arr"], play_idx - 30)
                    hi30 = np.searchsorted(gs["db_indices_arr"], play_idx + 30, side='right')
                    feat.extend([hi10 - lo10, hi30 - lo30])
                else:
                    feat.extend([0, 0])

                if valid_cooccur:
                    cooccur_scores = [gs["player_raw_score"][cpid][play_idx] for cpid in valid_cooccur]
                    feat.append(sum(cooccur_scores) / len(cooccur_scores))
                    cooccur_ranks = [ranks_matrix[play_idx, pid_to_idx[cpid]]
                                     for cpid in valid_cooccur if cpid in pid_to_idx]
                    feat.append(sum(1 for r in cooccur_ranks if r < 5))
                else:
                    feat.extend([0.5, 0])

                if len(gs["db_indices_arr"]) > 0:
                    feat.append(np.searchsorted(gs["db_indices_arr"], play_idx, side='right') / 3.0 / 20.0)
                else:
                    feat.append(0.0)

                ps_before = [idx for idx in gs["period_start_indices"] if idx <= play_idx]
                feat.append((play_idx - ps_before[-1]) / max(n_plays, 1) if ps_before else play_idx / max(n_plays, 1))

                features.append(feat)
                metadata.append((seq, tid, pid))
                if include_labels:
                    labels.append(1.0 if pid in gt_lineup else 0.0)

    if not features:
        return None, None

    X = np.array(features, dtype=np.float32)
    if include_labels:
        return X, np.array(labels, dtype=np.float32)
    else:
        return X, metadata
