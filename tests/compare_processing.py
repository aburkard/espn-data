"""Compare processing output against existing data to verify refactoring didn't change results.

Processes one season to a temp directory using --output-dir, then diffs against existing parquets.

Usage:
    python tests/compare_processing.py --season 2023 --gender mens
"""

import argparse
import os
import sys
import shutil
import tempfile
import logging
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_TYPES = ['game_info', 'teams_info', 'player_stats', 'team_stats',
              'play_by_play', 'officials', 'broadcasts', 'schedules']


def compare_parquets(old_path: Path, new_path: Path, name: str) -> dict:
    """Compare two parquet files and return differences."""
    result = {'name': name, 'match': False, 'details': ''}

    if not old_path.exists():
        result['details'] = 'OLD file missing'
        return result
    if not new_path.exists():
        result['details'] = 'NEW file missing'
        return result

    old_df = pd.read_parquet(old_path)
    new_df = pd.read_parquet(new_path)

    # Compare row counts
    if len(old_df) != len(new_df):
        result['details'] = f'Row count mismatch: old={len(old_df)}, new={len(new_df)}'
        return result

    # Align columns (order doesn't matter)
    col_note = ''
    only_old = set(old_df.columns) - set(new_df.columns)
    only_new = set(new_df.columns) - set(old_df.columns)
    if only_old or only_new:
        parts = []
        if only_old:
            parts.append(f'removed: {sorted(only_old)}')
        if only_new:
            parts.append(f'added: {sorted(only_new)}')
        col_note = f' ({", ".join(parts)})'
        # Compare only shared columns
        shared = [c for c in old_df.columns if c in set(new_df.columns)]
        old_df = old_df[shared]
        new_df = new_df[shared]
    elif list(old_df.columns) != list(new_df.columns):
        col_note = ' (column order differs)'
        new_df = new_df[old_df.columns]

    # Sort both DataFrames for stable comparison (processing order may vary)
    sort_cols = [c for c in old_df.columns
                 if str(old_df[c].dtype) in ('int64', 'float64', 'object', 'string', 'Int64')]
    if sort_cols:
        old_sorted = old_df.sort_values(sort_cols, ignore_index=True)
        new_sorted = new_df.sort_values(sort_cols, ignore_index=True)
    else:
        old_sorted = old_df.reset_index(drop=True)
        new_sorted = new_df.reset_index(drop=True)

    # Compare values (treating NaN == NaN)
    try:
        matches = (old_sorted == new_sorted) | (old_sorted.isna() & new_sorted.isna())
        if matches.all().all():
            result['match'] = True
            result['details'] = f'MATCH ({old_df.shape[0]} rows, {old_df.shape[1]} cols){col_note}'
            return result

        mismatch_cols = [c for c in matches.columns if not matches[c].all()]
        n_mismatches = (~matches).sum().sum()
        result['details'] = (f'{n_mismatches} value mismatches across columns: {mismatch_cols[:5]}'
                             f'{"..." if len(mismatch_cols) > 5 else ""}')

        for col in mismatch_cols[:3]:
            mask = ~matches[col]
            idx = mask[mask].index[:3]
            for i in idx:
                result['details'] += f'\n    {col}[{i}]: old={old_sorted[col].iloc[i]!r}, new={new_sorted[col].iloc[i]!r}'

    except Exception as e:
        result['details'] = f'Comparison error: {e}'

    return result


def main():
    parser = argparse.ArgumentParser(description='Compare processing output against existing data')
    parser.add_argument('--season', '-s', type=int, required=True, help='Season to compare')
    parser.add_argument('--gender', '-g', type=str, default='mens', choices=['mens', 'womens'])
    parser.add_argument('--keep-tmp', action='store_true', help='Keep temp directory after comparison')
    args = parser.parse_args()

    from espn_data.utils import configure, get_config, ensure_dirs

    # Existing data location (default data dir)
    configure(gender=args.gender)
    original_data_dir = get_config().data_dir
    old_parquet_dir = original_data_dir / "processed" / args.gender / "parquet" / str(args.season)
    old_raw_dir = original_data_dir / "raw" / args.gender

    logger.info(f'Existing processed data: {old_parquet_dir}')
    if not old_parquet_dir.exists():
        logger.error(f'No existing processed data at {old_parquet_dir}')
        return

    # Create temp directory, but symlink raw data so we don't re-download
    tmp_dir = Path(tempfile.mkdtemp(prefix='espn_compare_'))
    tmp_raw_dir = tmp_dir / "raw"
    os.makedirs(tmp_raw_dir, exist_ok=True)

    # Symlink the raw data directories (read-only, no re-download needed)
    for gender_dir in ("mens", "womens"):
        src = old_raw_dir.parent / gender_dir
        dst = tmp_raw_dir / gender_dir
        if src.exists():
            os.symlink(src, dst)

    logger.info(f'Temp output dir: {tmp_dir}')
    logger.info(f'Raw data symlinked from: {old_raw_dir.parent}')

    # Reconfigure to use temp dir
    configure(gender=args.gender, data_dir=tmp_dir)
    ensure_dirs()

    try:
        from espn_data.processor import process_season_data, process_teams_data

        logger.info(f'\nProcessing season {args.season} ({args.gender})...')
        process_teams_data(force=True)
        result = process_season_data(args.season, max_workers=4, force=True, verbose=False)
        logger.info(f'Processing result: {result}')

        new_parquet_dir = tmp_dir / "processed" / args.gender / "parquet" / str(args.season)

        logger.info(f'\n{"="*60}')
        logger.info(f'COMPARISON: {args.gender} season {args.season}')
        logger.info(f'{"="*60}')

        all_match = True
        for dt in DATA_TYPES:
            old_file = old_parquet_dir / f'{dt}.parquet'
            new_file = new_parquet_dir / f'{dt}.parquet'
            cmp_result = compare_parquets(old_file, new_file, dt)

            status = 'OK' if cmp_result['match'] else 'DIFF'
            if not cmp_result['match']:
                all_match = False
            logger.info(f'  [{status}] {dt}: {cmp_result["details"]}')

        logger.info(f'{"="*60}')
        if all_match:
            logger.info('ALL DATA TYPES MATCH')
        else:
            logger.warning('DIFFERENCES FOUND - see details above')

    finally:
        # Restore config to default
        configure(gender=args.gender, data_dir=original_data_dir)

        if args.keep_tmp:
            logger.info(f'Temp dir preserved at: {tmp_dir}')
        else:
            shutil.rmtree(tmp_dir)
            logger.info('Temp dir cleaned up')


if __name__ == '__main__':
    main()
