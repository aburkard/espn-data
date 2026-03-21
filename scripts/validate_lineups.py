"""Validate lineup inference approaches against ground truth on 2026 games.

Compares:
1. Heuristic (last-seen top-5)
2. HMM (probabilistic forward inference)

Both are measured against ground truth from explicit substitution events.
"""

import json
import os
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from espn_data.lineups import (
    load_game,
    track_lineups_ground_truth,
    track_lineups_heuristic,
    track_lineups_hmm,
    validate_tracker,
    SUB_TYPE_ID,
)


def find_games_with_subs(games_dir: Path, n: int = 50, seed: int = 42) -> list[Path]:
    """Find N random games that have substitution data."""
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


def print_results(results, label):
    n = len(results)
    print(f"\n{'='*60}")
    print(f"{label} ({n} games)")
    print(f"{'='*60}\n")

    for metric in ["exact_accuracy", "player_accuracy", "avg_correct_of_5"]:
        for side in ["home", "away"]:
            key = f"{side}_{metric}"
            values = [r[key] for r in results]
            mean = sum(values) / len(values)
            print(f"  {side}_{metric}: {mean:.3f}")
        print()

    print("Exact lineup match distribution (home):")
    home_exact = [r["home_exact_accuracy"] for r in results]
    for threshold in [1.0, 0.9, 0.8, 0.7, 0.5]:
        count = sum(1 for v in home_exact if v >= threshold)
        print(f"  >= {threshold:.0%}: {count}/{n} games ({count/n:.0%})")


def main():
    games_dir = Path("data/raw/mens/2026/games")
    if not games_dir.exists():
        print(f"Games directory not found: {games_dir}")
        return

    n_games = 100
    print(f"Finding {n_games} games with substitution data...")
    game_files = find_games_with_subs(games_dir, n=n_games)
    print(f"Found {len(game_files)} games\n")

    heuristic_results = []
    hmm_results = []

    for i, fpath in enumerate(game_files):
        game_data = load_game(fpath)
        game_id = fpath.stem

        # Heuristic
        h_metrics = validate_tracker(game_data, track_lineups_heuristic)
        if h_metrics:
            h_metrics["game_id"] = game_id
            heuristic_results.append(h_metrics)

        # HMM
        t0 = time.time()
        hmm_metrics = validate_tracker(game_data, track_lineups_hmm)
        hmm_time = time.time() - t0
        if hmm_metrics:
            hmm_metrics["game_id"] = game_id
            hmm_results.append(hmm_metrics)

        if (i + 1) % 10 == 0:
            print(f"  Processed {i + 1}/{len(game_files)} games "
                  f"(last HMM: {hmm_time:.1f}s)")

    print_results(heuristic_results, "HEURISTIC")
    print_results(hmm_results, "HMM")

    # Head-to-head comparison
    if hmm_results and heuristic_results:
        print(f"\n{'='*60}")
        print("HEAD-TO-HEAD (per-game)")
        print(f"{'='*60}\n")

        hmm_map = {r["game_id"]: r for r in hmm_results}
        h_map = {r["game_id"]: r for r in heuristic_results}

        hmm_wins = 0
        h_wins = 0
        ties = 0
        for gid in hmm_map:
            if gid in h_map:
                hmm_avg = (hmm_map[gid]["home_exact_accuracy"] +
                          hmm_map[gid]["away_exact_accuracy"]) / 2
                h_avg = (h_map[gid]["home_exact_accuracy"] +
                        h_map[gid]["away_exact_accuracy"]) / 2
                if hmm_avg > h_avg + 0.01:
                    hmm_wins += 1
                elif h_avg > hmm_avg + 0.01:
                    h_wins += 1
                else:
                    ties += 1

        print(f"  HMM wins: {hmm_wins}")
        print(f"  Heuristic wins: {h_wins}")
        print(f"  Ties (within 1%): {ties}")


if __name__ == "__main__":
    main()
