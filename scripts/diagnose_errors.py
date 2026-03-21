"""Diagnose where the forward-backward model makes errors.

Categorizes errors by type to guide improvements:
- Was the wrong player recently seen? (sub cooldown issue)
- Was the right player a low-minutes bench player? (minutes issue)
- Did the error happen right after a dead ball? (transition timing)
- Were the swapped players the same position? (position issue)
"""

import bisect
import random
import sys
from pathlib import Path
from collections import Counter, defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

from espn_data.lineups import (
    load_game, track_lineups_ground_truth, track_lineups_hmm,
    get_home_away_ids, get_starters_and_minutes, _DEAD_BALL_TYPES,
    _STRONG_EVIDENCE_TYPES, SUB_TYPE_ID,
)


def find_games_with_subs(games_dir, n=50, seed=42):
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


def diagnose_game(game_data):
    gt = track_lineups_ground_truth(game_data)
    pred = track_lineups_hmm(game_data)
    if gt is None or pred is None:
        return None

    home_id, away_id = get_home_away_ids(game_data)
    roster = get_starters_and_minutes(game_data)

    # Build lookups
    player_info = {}
    for tid in [home_id, away_id]:
        for pid, info in roster.get(tid, {}).items():
            player_info[pid] = {**info, "team_id": tid}

    noise_types = {"Substitution", "OfficialTVTimeOut", "ShortTimeOut",
                   "RegularTimeOut", "End Period", "End Game",
                   "Dead Ball Rebound"}

    gt_f = gt[~gt["play_type"].isin(noise_types)].copy()
    pred_f = pred[~pred["play_type"].isin(noise_types)].copy()
    merged = gt_f.merge(pred_f, on="sequence_number", suffixes=("_gt", "_p"))

    errors = []

    for _, row in merged.iterrows():
        for side in ["home", "away"]:
            gt_set = set(row[f"{side}_on_court_gt"].split(",")) - {""}
            pred_set = set(row[f"{side}_on_court_p"].split(",")) - {""}

            if gt_set == pred_set:
                continue

            missing = gt_set - pred_set   # should be on court but aren't
            extra = pred_set - gt_set     # predicted on court but shouldn't be

            for pid in missing:
                info = player_info.get(pid, {})
                errors.append({
                    "type": "missing",  # player should be on court but model says no
                    "player_id": pid,
                    "player_name": info.get("name", "?"),
                    "minutes": info.get("minutes", 0),
                    "starter": info.get("starter", False),
                    "position": info.get("position", "?"),
                    "play_type": row["play_type_gt"],
                    "period": row["period_gt"],
                    "clock": row["clock_gt"],
                    "side": side,
                })

            for pid in extra:
                info = player_info.get(pid, {})
                errors.append({
                    "type": "extra",  # model says on court but shouldn't be
                    "player_id": pid,
                    "player_name": info.get("name", "?"),
                    "minutes": info.get("minutes", 0),
                    "starter": info.get("starter", False),
                    "position": info.get("position", "?"),
                    "play_type": row["play_type_gt"],
                    "period": row["period_gt"],
                    "clock": row["clock_gt"],
                    "side": side,
                })

    return errors


def main():
    games_dir = Path("data/raw/mens/2026/games")
    game_files = find_games_with_subs(games_dir, n=100)
    print(f"Analyzing {len(game_files)} games...\n")

    all_errors = []
    for fpath in game_files:
        game_data = load_game(fpath)
        errors = diagnose_game(game_data)
        if errors:
            all_errors.extend(errors)

    print(f"Total error instances: {len(all_errors)}")
    print(f"  Missing (should be on court): {sum(1 for e in all_errors if e['type'] == 'missing')}")
    print(f"  Extra (shouldn't be on court): {sum(1 for e in all_errors if e['type'] == 'extra')}")

    # --- Error by player minutes ---
    print("\n=== ERRORS BY PLAYER MINUTES ===")
    buckets = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 25), (25, 30), (30, 40)]
    for lo, hi in buckets:
        missing = [e for e in all_errors if e["type"] == "missing" and lo <= e["minutes"] < hi]
        extra = [e for e in all_errors if e["type"] == "extra" and lo <= e["minutes"] < hi]
        print(f"  {lo}-{hi} min: {len(missing)} missing, {len(extra)} extra")

    # --- Error by starter status ---
    print("\n=== ERRORS BY STARTER STATUS ===")
    for starter in [True, False]:
        label = "Starters" if starter else "Bench"
        missing = [e for e in all_errors if e["type"] == "missing" and e["starter"] == starter]
        extra = [e for e in all_errors if e["type"] == "extra" and e["starter"] == starter]
        print(f"  {label}: {len(missing)} missing, {len(extra)} extra")

    # --- Error by position ---
    print("\n=== ERRORS BY POSITION ===")
    for pos in sorted(set(e["position"] for e in all_errors)):
        missing = [e for e in all_errors if e["type"] == "missing" and e["position"] == pos]
        extra = [e for e in all_errors if e["type"] == "extra" and e["position"] == pos]
        print(f"  {pos}: {len(missing)} missing, {len(extra)} extra")

    # --- Swapped pairs: which player was "extra" when another was "missing"? ---
    print("\n=== COMMON SWAP PATTERNS ===")
    # Group errors by (game play) to find swap pairs
    missing_by_play = defaultdict(list)
    extra_by_play = defaultdict(list)
    for e in all_errors:
        key = (e["period"], e["clock"], e["side"])
        if e["type"] == "missing":
            missing_by_play[key].append(e)
        else:
            extra_by_play[key].append(e)

    swap_minutes_diff = []  # minutes(extra) - minutes(missing)
    swap_position_match = 0
    swap_total = 0
    starter_bench_swaps = 0

    for key in missing_by_play:
        if key in extra_by_play:
            for m in missing_by_play[key]:
                for x in extra_by_play[key]:
                    swap_total += 1
                    swap_minutes_diff.append(x["minutes"] - m["minutes"])
                    if m["position"] == x["position"]:
                        swap_position_match += 1
                    if x["starter"] and not m["starter"]:
                        starter_bench_swaps += 1

    if swap_total > 0:
        import numpy as np
        diffs = np.array(swap_minutes_diff)
        print(f"  Total swap pairs: {swap_total}")
        print(f"  Same position: {swap_position_match}/{swap_total} ({swap_position_match/swap_total*100:.1f}%)")
        print(f"  Extra player has MORE minutes: {(diffs > 0).sum()}/{swap_total} ({(diffs > 0).mean()*100:.1f}%)")
        print(f"  Extra player has FEWER minutes: {(diffs < 0).sum()}/{swap_total} ({(diffs < 0).mean()*100:.1f}%)")
        print(f"  Avg minutes difference (extra - missing): {diffs.mean():.1f}")
        print(f"  Starter wrongly kept over bench: {starter_bench_swaps}/{swap_total} ({starter_bench_swaps/swap_total*100:.1f}%)")

    # --- Error by play type (at what events do we get it wrong?) ---
    print("\n=== ERRORS BY PLAY TYPE ===")
    type_counts = Counter(e["play_type"] for e in all_errors)
    for pt, count in type_counts.most_common(10):
        print(f"  {pt}: {count}")

    # --- Error by period ---
    print("\n=== ERRORS BY PERIOD ===")
    for period in sorted(set(e["period"] for e in all_errors)):
        count = sum(1 for e in all_errors if e["period"] == period)
        print(f"  Period {period}: {count}")

    # --- Clock analysis: do errors cluster at certain times? ---
    print("\n=== ERRORS BY GAME CLOCK (5-min buckets) ===")
    from espn_data.lineups import _clock_to_seconds
    for lo, hi in [(15, 20), (10, 15), (5, 10), (0, 5)]:
        count = 0
        for e in all_errors:
            secs = _clock_to_seconds(e["clock"])
            if secs is not None and lo * 60 <= secs < hi * 60:
                count += 1
        print(f"  {lo}-{hi} min: {count}")


if __name__ == "__main__":
    main()
