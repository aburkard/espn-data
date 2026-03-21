"""Train lineup probability model with LightGBM (fast)."""

import bisect
import json
import pickle
import random
import sys
from pathlib import Path

import numpy as np
import lightgbm as lgb
from sklearn.model_selection import KFold
from sklearn.metrics import brier_score_loss, log_loss

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.train_fast import (
    find_games_with_subs, extract_game_features_fast, FEATURE_NAMES,
)
from espn_data.lineups import load_game


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

    # Baseline
    baseline_raw = X[:, 2] * (0.7 + 0.3 * X[:, 7])
    baseline_brier = brier_score_loss(y, np.clip(baseline_raw, 0, 1))
    print(f"Baseline raw: Brier={baseline_brier:.4f}\n", flush=True)

    cv = KFold(5, shuffle=True, random_state=42)

    # LightGBM configs
    configs = [
        {"n_estimators": 200, "max_depth": 4, "learning_rate": 0.1, "num_leaves": 31,
         "subsample": 0.8, "min_child_samples": 20, "reg_alpha": 0, "reg_lambda": 0},
        {"n_estimators": 500, "max_depth": 5, "learning_rate": 0.05, "num_leaves": 31,
         "subsample": 0.8, "min_child_samples": 20, "reg_alpha": 0, "reg_lambda": 0},
        {"n_estimators": 500, "max_depth": 6, "learning_rate": 0.05, "num_leaves": 63,
         "subsample": 0.8, "min_child_samples": 20, "reg_alpha": 0.1, "reg_lambda": 0.1},
        {"n_estimators": 800, "max_depth": 5, "learning_rate": 0.03, "num_leaves": 31,
         "subsample": 0.8, "min_child_samples": 20, "reg_alpha": 0, "reg_lambda": 0},
        {"n_estimators": 1000, "max_depth": 6, "learning_rate": 0.03, "num_leaves": 63,
         "subsample": 0.8, "min_child_samples": 50, "reg_alpha": 0.1, "reg_lambda": 0.1},
        {"n_estimators": 1500, "max_depth": 7, "learning_rate": 0.02, "num_leaves": 127,
         "subsample": 0.7, "min_child_samples": 50, "reg_alpha": 0.1, "reg_lambda": 1.0},
        {"n_estimators": 2000, "max_depth": -1, "learning_rate": 0.02, "num_leaves": 63,
         "subsample": 0.7, "min_child_samples": 100, "reg_alpha": 0.5, "reg_lambda": 1.0},
    ]

    print(f"{'Config':>55s} | {'Brier':>7} {'LogLoss':>8}", flush=True)
    print("-" * 80, flush=True)

    best_brier = 1.0
    best_cfg = None

    for cfg in configs:
        model = lgb.LGBMClassifier(
            objective="binary", metric="binary_logloss",
            verbose=-1, random_state=42, **cfg,
        )

        # Manual CV to get probabilities
        probs = np.zeros(len(y))
        for train_idx, test_idx in cv.split(X):
            model.fit(X[train_idx], y[train_idx])
            probs[test_idx] = model.predict_proba(X[test_idx])[:, 1]

        bs = brier_score_loss(y, probs)
        ll = log_loss(y, probs)
        label = f"n={cfg['n_estimators']} d={cfg['max_depth']} lr={cfg['learning_rate']} nl={cfg['num_leaves']}"
        print(f"{label:>55s} | {bs:>7.4f} {ll:>8.4f}", flush=True)
        if bs < best_brier:
            best_brier = bs
            best_cfg = cfg

    print(f"\nBest: Brier={best_brier:.4f} ({(1-best_brier/baseline_brier)*100:.1f}% improvement)", flush=True)
    print(f"Config: {best_cfg}", flush=True)

    # Train final model
    print("\nTraining final model on all data...", flush=True)
    final = lgb.LGBMClassifier(
        objective="binary", metric="binary_logloss",
        verbose=-1, random_state=42, **best_cfg,
    )
    final.fit(X, y)

    print("Feature importance:", flush=True)
    for name, imp in sorted(zip(FEATURE_NAMES, final.feature_importances_),
                             key=lambda x: -x[1]):
        bar = "█" * int(imp // 5)
        print(f"  {name:20s}: {imp:>5d} {bar}", flush=True)

    # Save
    model_path = Path("espn_data/lineup_model.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(final, f)

    meta = {
        "features": FEATURE_NAMES,
        "config": best_cfg,
        "cv_brier": best_brier,
        "baseline_brier": float(baseline_brier),
        "n_obs": len(y),
        "n_games": len(all_X),
        "model_type": "lightgbm",
    }
    with open(Path("espn_data/lineup_model_meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    print(f"\nSaved model to {model_path}", flush=True)


if __name__ == "__main__":
    main()
