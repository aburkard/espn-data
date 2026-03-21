"""Fast vectorized training for lineup probability model."""

import bisect
import json
import pickle
import random
import sys
from pathlib import Path

import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_predict, KFold
from sklearn.metrics import brier_score_loss, log_loss

sys.path.insert(0, str(Path(__file__).parent.parent))

from espn_data.lineups import (
    load_game, track_lineups_ground_truth,
    get_home_away_ids, get_starters_and_minutes,
    _parse_play, _STRONG_EVIDENCE_TYPES, _DEAD_BALL_TYPES, SUB_TYPE_ID,
)

NOISE_TYPES = {"Substitution", "OfficialTVTimeOut", "ShortTimeOut",
               "RegularTimeOut", "End Period", "End Game", "Dead Ball Rebound"}

FEATURE_NAMES = [
    "fwd_05", "bwd_05", "max_05", "min_05",
    "fwd_12", "bwd_12", "max_12",
    "minutes_prior", "is_starter",
    "norm_dist_prev", "norm_dist_next",
    "db_prev", "db_next",
    "sights_in_window", "total_sights",
    "fwd_x_min", "bwd_x_min",
]


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


def extract_game_features_fast(game_data):
    """Vectorized feature extraction for one game."""
    gt = track_lineups_ground_truth(game_data)
    if gt is None:
        return None, None

    plays = game_data.get("plays", [])
    if not plays:
        return None, None

    home_id, away_id = get_home_away_ids(game_data)
    if not home_id or not away_id:
        return None, None

    roster = get_starters_and_minutes(game_data)

    player_team = {}
    for tid in [home_id, away_id]:
        for pid in roster.get(tid, {}):
            player_team[pid] = tid

    # Eligible players per team
    team_players = {}  # tid -> list of pids (sorted for consistency)
    player_info = {}
    for tid in [home_id, away_id]:
        pids = []
        for pid, info in roster.get(tid, {}).items():
            if info["minutes"] > 0:
                pids.append(pid)
                player_info[pid] = {
                    "minutes_prior": min(info["minutes"] / 40.0, 1.0),
                    "is_starter": 1.0 if info.get("starter", False) else 0.0,
                }
        team_players[tid] = sorted(pids)

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

    # Dead ball indices
    db_indices = np.array(sorted(
        i for i, pp in enumerate(parsed) if pp["type_text"] in _DEAD_BALL_TYPES
    ))

    # GT lookup
    gt_dict = {}
    for _, row in gt.iterrows():
        seq = row["sequence_number"]
        for side, tid in [("home", home_id), ("away", away_id)]:
            lineup = set(row[f"{side}_on_court"].split(",")) - {""}
            gt_dict[(seq, tid)] = lineup

    # Identify non-noise play indices
    valid_plays = [(i, pp) for i, pp in enumerate(parsed) if pp["type_text"] not in NOISE_TYPES]

    features = []
    labels = []

    # For each player, precompute dist_prev and dist_next arrays for ALL plays
    for tid in [home_id, away_id]:
        for pid in team_players[tid]:
            info = player_info[pid]
            sightings = player_sightings.get(pid, [])
            sights_arr = np.array(sightings) if sightings else np.array([], dtype=int)
            n_sights = len(sights_arr)
            total_sights_norm = n_sights / max(n_plays, 1)

            # Precompute dist_prev and dist_next for all play indices at once
            dist_prev_all = np.full(n_plays, n_plays, dtype=float)
            dist_next_all = np.full(n_plays, n_plays, dtype=float)

            if n_sights > 0:
                for play_idx in range(n_plays):
                    pos = bisect.bisect_right(sights_arr, play_idx)
                    if pos > 0:
                        dist_prev_all[play_idx] = play_idx - sights_arr[pos - 1]
                    if pos > 0 and sights_arr[pos - 1] == play_idx:
                        dist_next_all[play_idx] = 0
                    elif pos < n_sights:
                        dist_next_all[play_idx] = sights_arr[pos] - play_idx

            # Now extract features only for valid plays
            for play_idx, pp in valid_plays:
                seq = pp["sequence_number"]
                gt_lineup = gt_dict.get((seq, tid))
                if not gt_lineup:
                    continue

                dp = dist_prev_all[play_idx]
                dn = dist_next_all[play_idx]

                fwd_05 = np.exp(-0.05 * dp)
                bwd_05 = np.exp(-0.05 * dn)
                fwd_12 = np.exp(-0.12 * dp)
                bwd_12 = np.exp(-0.12 * dn)

                # Dead balls (use searchsorted)
                if dp < n_plays and len(db_indices) > 0:
                    lo = np.searchsorted(db_indices, play_idx - dp)
                    hi = np.searchsorted(db_indices, play_idx, side='right')
                    db_prev = hi - lo
                else:
                    db_prev = 0
                if dn < n_plays and len(db_indices) > 0:
                    lo = np.searchsorted(db_indices, play_idx)
                    hi = np.searchsorted(db_indices, play_idx + dn, side='right')
                    db_next = hi - lo
                else:
                    db_next = 0

                # Sightings in window
                if n_sights > 0:
                    lo = np.searchsorted(sights_arr, play_idx - 30)
                    hi = np.searchsorted(sights_arr, play_idx + 30, side='right')
                    siw = hi - lo
                else:
                    siw = 0

                mp = info["minutes_prior"]

                features.append([
                    fwd_05, bwd_05,
                    max(fwd_05, bwd_05), min(fwd_05, bwd_05),
                    fwd_12, bwd_12, max(fwd_12, bwd_12),
                    mp, info["is_starter"],
                    dp / n_plays, dn / n_plays,
                    db_prev, db_next,
                    siw, total_sights_norm,
                    fwd_05 * mp, bwd_05 * mp,
                ])
                labels.append(1.0 if pid in gt_lineup else 0.0)

    return np.array(features) if features else None, np.array(labels) if labels else None


def main():
    games_dir = Path("data/raw/mens/2026/games")
    game_files = find_games_with_subs(games_dir, n=100)

    print(f"Extracting features from {len(game_files)} games...", flush=True)
    all_X = []
    all_y = []
    for i, fpath in enumerate(game_files):
        game_data = load_game(fpath)
        X, y = extract_game_features_fast(game_data)
        if X is not None:
            all_X.append(X)
            all_y.append(y)
        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(game_files)} games...", flush=True)

    X = np.vstack(all_X)
    y = np.concatenate(all_y)
    print(f"Dataset: {len(y):,} obs, {X.shape[1]} features", flush=True)
    print(f"On-court rate: {y.mean():.3f}\n", flush=True)

    cv = KFold(5, shuffle=True, random_state=42)

    # === Logistic Regression ===
    print("LOGISTIC REGRESSION", flush=True)
    lr = LogisticRegression(max_iter=5000, C=1.0, solver='lbfgs')
    lr_probs = cross_val_predict(lr, X, y, cv=cv, method='predict_proba')[:, 1]
    print(f"  Brier: {brier_score_loss(y, lr_probs):.4f}", flush=True)
    print(f"  LogLoss: {log_loss(y, lr_probs):.4f}", flush=True)

    # === GBT configs ===
    print("\nGRADIENT BOOSTED TREES", flush=True)
    configs = [
        (200, 4, 0.1, 0.8, 10),
        (300, 5, 0.05, 0.8, 10),
        (500, 5, 0.05, 0.8, 20),
        (500, 6, 0.03, 0.8, 20),
        (800, 5, 0.03, 0.8, 20),
    ]

    best_brier = 1.0
    best_cfg = None

    for n_est, depth, lr_rate, ss, msl in configs:
        gbt = GradientBoostingClassifier(
            n_estimators=n_est, max_depth=depth, learning_rate=lr_rate,
            subsample=ss, min_samples_leaf=msl, random_state=42,
        )
        probs = cross_val_predict(gbt, X, y, cv=cv, method='predict_proba')[:, 1]
        bs = brier_score_loss(y, probs)
        ll = log_loss(y, probs)
        label = f"n={n_est} d={depth} lr={lr_rate} msl={msl}"
        print(f"  {label:>35s}: Brier={bs:.4f} LogLoss={ll:.4f}", flush=True)
        if bs < best_brier:
            best_brier = bs
            best_cfg = (n_est, depth, lr_rate, ss, msl)

    print(f"\nBest: {best_cfg} -> Brier={best_brier:.4f}", flush=True)

    # === Baseline comparison ===
    baseline_raw = X[:, 2] * (0.7 + 0.3 * X[:, 7])  # max_05 * (0.7 + 0.3*min_prior)
    baseline_brier = brier_score_loss(y, np.clip(baseline_raw, 0, 1))
    print(f"\nBaseline raw: Brier={baseline_brier:.4f}", flush=True)
    print(f"Best GBT improvement: {(1-best_brier/baseline_brier)*100:.1f}%", flush=True)

    # === Train final model ===
    print("\nTraining final model on all data...", flush=True)
    n_est, depth, lr_rate, ss, msl = best_cfg
    final = GradientBoostingClassifier(
        n_estimators=n_est, max_depth=depth, learning_rate=lr_rate,
        subsample=ss, min_samples_leaf=msl, random_state=42,
    )
    final.fit(X, y)

    print("Feature importance:", flush=True)
    for name, imp in sorted(zip(FEATURE_NAMES, final.feature_importances_),
                             key=lambda x: -x[1]):
        bar = "█" * int(imp * 100)
        print(f"  {name:20s}: {imp:.3f} {bar}", flush=True)

    # Save
    model_path = Path("espn_data/lineup_model.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(final, f)

    meta = {
        "features": FEATURE_NAMES,
        "config": dict(zip(
            ["n_estimators", "max_depth", "learning_rate", "subsample", "min_samples_leaf"],
            best_cfg
        )),
        "cv_brier": best_brier,
        "baseline_brier": float(baseline_brier),
        "n_obs": len(y),
        "n_games": len(all_X),
    }
    with open(Path("espn_data/lineup_model_meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\nSaved model to {model_path}", flush=True)


if __name__ == "__main__":
    main()
