"""Optimize lineup probabilities to minimize Brier score.

Three levels of optimization:
1. Platt scaling: fit sigmoid(a*score + b) on current scores
2. Parameter tuning: optimize decay, minutes weight, combination function
3. Feature-based: use logistic regression on multiple features
"""

import bisect
import random
import sys
from pathlib import Path

import numpy as np
from scipy.optimize import minimize_scalar, minimize

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


NOISE_TYPES = {"Substitution", "OfficialTVTimeOut", "ShortTimeOut",
               "RegularTimeOut", "End Period", "End Game", "Dead Ball Rebound"}


def build_dataset(game_files):
    """Build arrays of (features, label) for all player-play observations."""
    all_features = []  # each row: [dist_prev, dist_next, minutes_prior, is_starter]
    all_labels = []    # 1 if on court, 0 if not

    for fpath in game_files:
        game_data = load_game(fpath)
        gt = track_lineups_ground_truth(game_data)
        if gt is None:
            continue

        plays = game_data.get("plays", [])
        if not plays:
            continue

        home_id, away_id = get_home_away_ids(game_data)
        if not home_id or not away_id:
            continue

        roster = get_starters_and_minutes(game_data)

        player_team = {}
        for tid in [home_id, away_id]:
            for pid in roster.get(tid, {}):
                player_team[pid] = tid

        eligible = {home_id: set(), away_id: set()}
        minutes_prior = {}
        is_starter = {}
        for tid in [home_id, away_id]:
            for pid, info in roster.get(tid, {}).items():
                if info["minutes"] > 0:
                    eligible[tid].add(pid)
                    minutes_prior[pid] = min(info["minutes"] / 40.0, 1.0)
                    is_starter[pid] = 1.0 if info.get("starter", False) else 0.0

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

        # Build GT lookup
        gt_dict = {}
        for _, row in gt.iterrows():
            seq = row["sequence_number"]
            for side, tid in [("home", home_id), ("away", away_id)]:
                lineup = set(row[f"{side}_on_court"].split(",")) - {""}
                gt_dict[(seq, tid)] = lineup

        # Collect features
        for play_idx, pp in enumerate(parsed):
            if pp["type_text"] in NOISE_TYPES:
                continue
            seq = pp["sequence_number"]

            for tid in [home_id, away_id]:
                gt_lineup = gt_dict.get((seq, tid))
                if not gt_lineup:
                    continue

                for pid in eligible[tid]:
                    sightings = player_sightings.get(pid)
                    if not sightings:
                        dist_prev = n_plays
                        dist_next = n_plays
                    else:
                        pos = bisect.bisect_right(sightings, play_idx)
                        dist_prev = (play_idx - sightings[pos - 1]) if pos > 0 else n_plays
                        if pos > 0 and sightings[pos - 1] == play_idx:
                            dist_next = 0
                        elif pos < len(sightings):
                            dist_next = sightings[pos] - play_idx
                        else:
                            dist_next = n_plays

                    all_features.append([
                        dist_prev,
                        dist_next,
                        minutes_prior.get(pid, 0.25),
                        is_starter.get(pid, 0.0),
                    ])
                    all_labels.append(1.0 if pid in gt_lineup else 0.0)

    return np.array(all_features), np.array(all_labels)


def brier_score(probs, labels):
    return np.mean((probs - labels) ** 2)


def log_loss(probs, labels):
    p = np.clip(probs, 1e-7, 1 - 1e-7)
    return -np.mean(labels * np.log(p) + (1 - labels) * np.log(1 - p))


def sigmoid(x):
    return 1.0 / (1.0 + np.exp(-np.clip(x, -500, 500)))


def main():
    games_dir = Path("data/raw/mens/2026/games")

    # Use 70 games for training, 30 for test
    game_files = find_games_with_subs(games_dir, n=100)
    train_files = game_files[:70]
    test_files = game_files[70:]

    print(f"Building dataset from {len(train_files)} train + {len(test_files)} test games...")
    train_X, train_y = build_dataset(train_files)
    test_X, test_y = build_dataset(test_files)
    print(f"Train: {len(train_y):,} observations ({train_y.mean():.3f} on-court rate)")
    print(f"Test:  {len(test_y):,} observations ({test_y.mean():.3f} on-court rate)")

    # === 1. Current model baseline ===
    def current_score(X, decay=0.05):
        fwd = np.exp(-decay * X[:, 0])
        bwd = np.exp(-decay * X[:, 1])
        combined = np.maximum(fwd, bwd)
        return combined * (0.7 + 0.3 * X[:, 2])

    baseline = current_score(test_X)
    print(f"\n{'='*60}")
    print(f"BASELINE (current model, raw scores)")
    print(f"  Brier: {brier_score(baseline, test_y):.4f}")
    print(f"  Log loss: {log_loss(baseline, test_y):.4f}")

    # === 2. Platt scaling: sigmoid(a*score + b) ===
    print(f"\n{'='*60}")
    print("PLATT SCALING")
    train_scores = current_score(train_X)
    test_scores = current_score(test_X)

    def platt_brier(params):
        a, b = params
        p = sigmoid(a * train_scores + b)
        return brier_score(p, train_y)

    result = minimize(platt_brier, [5.0, -2.0], method='Nelder-Mead')
    a_opt, b_opt = result.x
    platt_probs = sigmoid(a_opt * test_scores + b_opt)
    print(f"  Optimal: a={a_opt:.3f}, b={b_opt:.3f}")
    print(f"  Brier: {brier_score(platt_probs, test_y):.4f}")
    print(f"  Log loss: {log_loss(platt_probs, test_y):.4f}")

    # === 3. Optimize decay + minutes weight + Platt ===
    print(f"\n{'='*60}")
    print("OPTIMIZED DECAY + MINUTES + PLATT")

    def full_brier(params):
        decay, mw, a, b = params
        fwd = np.exp(-decay * train_X[:, 0])
        bwd = np.exp(-decay * train_X[:, 1])
        combined = np.maximum(fwd, bwd)
        raw = combined * ((1 - mw) + mw * train_X[:, 2])
        p = sigmoid(a * raw + b)
        return brier_score(p, train_y)

    result = minimize(full_brier, [0.05, 0.3, 5.0, -2.0],
                      method='Nelder-Mead',
                      options={'maxiter': 5000, 'xatol': 1e-5})
    d_opt, mw_opt, a2_opt, b2_opt = result.x

    fwd_test = np.exp(-d_opt * test_X[:, 0])
    bwd_test = np.exp(-d_opt * test_X[:, 1])
    raw_test = np.maximum(fwd_test, bwd_test) * ((1 - mw_opt) + mw_opt * test_X[:, 2])
    opt_probs = sigmoid(a2_opt * raw_test + b2_opt)
    print(f"  Optimal: decay={d_opt:.4f}, minutes_w={mw_opt:.3f}, a={a2_opt:.3f}, b={b2_opt:.3f}")
    print(f"  Brier: {brier_score(opt_probs, test_y):.4f}")
    print(f"  Log loss: {log_loss(opt_probs, test_y):.4f}")

    # === 4. Logistic regression on features directly ===
    print(f"\n{'='*60}")
    print("LOGISTIC REGRESSION ON FEATURES")

    # Features: fwd, bwd, max(fwd,bwd), min(fwd,bwd), minutes, starter,
    #           fwd*minutes, bwd*minutes
    def make_lr_features(X, decay):
        fwd = np.exp(-decay * X[:, 0])
        bwd = np.exp(-decay * X[:, 1])
        return np.column_stack([
            fwd,
            bwd,
            np.maximum(fwd, bwd),
            np.minimum(fwd, bwd),
            X[:, 2],  # minutes prior
            X[:, 3],  # is_starter
            fwd * X[:, 2],  # fwd * minutes
            bwd * X[:, 2],  # bwd * minutes
        ])

    for decay in [0.03, 0.05, 0.08, 0.12]:
        lr_train = make_lr_features(train_X, decay)
        lr_test = make_lr_features(test_X, decay)

        # Fit logistic regression (minimize Brier)
        n_features = lr_train.shape[1]

        def lr_brier(params):
            w = params[:n_features]
            b = params[n_features]
            logits = lr_train @ w + b
            p = sigmoid(logits)
            return brier_score(p, train_y)

        init = np.zeros(n_features + 1)
        result = minimize(lr_brier, init, method='L-BFGS-B',
                         options={'maxiter': 1000})
        w_opt = result.x[:n_features]
        b_opt_lr = result.x[n_features]

        test_logits = lr_test @ w_opt + b_opt_lr
        test_probs = sigmoid(test_logits)
        bs = brier_score(test_probs, test_y)
        ll = log_loss(test_probs, test_y)
        print(f"  decay={decay}: Brier={bs:.4f}, LogLoss={ll:.4f}")
        if decay == 0.05:
            print(f"    Weights: {dict(zip(['fwd','bwd','max','min','min_prior','starter','fwd*min','bwd*min'], w_opt.round(3)))}")
            print(f"    Bias: {b_opt_lr:.3f}")

    # === 5. Try different combination functions ===
    print(f"\n{'='*60}")
    print("COMBINATION FUNCTIONS (with Platt scaling)")

    def try_combo(name, combo_fn, X_train, y_train, X_test, y_test):
        def combo_brier(params):
            decay, a, b = params
            fwd = np.exp(-decay * X_train[:, 0])
            bwd = np.exp(-decay * X_train[:, 1])
            combined = combo_fn(fwd, bwd)
            raw = combined * (0.7 + 0.3 * X_train[:, 2])
            p = sigmoid(a * raw + b)
            return brier_score(p, y_train)

        result = minimize(combo_brier, [0.05, 5.0, -2.0], method='Nelder-Mead')
        d, a, b = result.x
        fwd = np.exp(-d * X_test[:, 0])
        bwd = np.exp(-d * X_test[:, 1])
        raw = combo_fn(fwd, bwd) * (0.7 + 0.3 * X_test[:, 2])
        p = sigmoid(a * raw + b)
        print(f"  {name:20s}: Brier={brier_score(p, y_test):.4f}  decay={d:.4f}")

    try_combo("max(fwd,bwd)", np.maximum, train_X, train_y, test_X, test_y)
    try_combo("fwd+bwd", lambda f, b: f + b, train_X, train_y, test_X, test_y)
    try_combo("fwd+0.5*bwd", lambda f, b: f + 0.5*b, train_X, train_y, test_X, test_y)
    try_combo("sqrt(fwd*bwd)", lambda f, b: np.sqrt(f * b), train_X, train_y, test_X, test_y)
    try_combo("fwd*bwd", lambda f, b: f * b, train_X, train_y, test_X, test_y)
    try_combo("p_or", lambda f, b: f + b - f*b, train_X, train_y, test_X, test_y)
    try_combo("mean(fwd,bwd)", lambda f, b: (f + b) / 2, train_X, train_y, test_X, test_y)


if __name__ == "__main__":
    main()
