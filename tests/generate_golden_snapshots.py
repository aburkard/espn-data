"""Generate golden snapshot data by processing example game files through the current processor.

This script processes each example game JSON through the processor pipeline and saves
the resulting DataFrames as CSV files. These serve as ground truth for regression testing.

Usage:
    python tests/generate_golden_snapshots.py
"""

import sys
import os
import json
import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from espn_data.processor import (
    get_game_details,
    process_game_data,
    optimize_dataframe_dtypes,
    remove_redundant_columns,
    get_broadcasts,
    get_primary_broadcast,
    convert_clock_to_seconds,
    process_teams_data,
)
from espn_data.utils import load_json, get_games_dir, set_gender

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXAMPLE_DIR = os.path.join(os.path.dirname(__file__), '..', 'example_data')
SNAPSHOT_DIR = os.path.join(os.path.dirname(__file__), 'golden_snapshots')

# Example game files and their metadata
GAME_FILES = {
    'mens_game': {'file': 'mens_game.json', 'gender': 'mens'},
    'mens_game_2': {'file': 'mens_game_2.json', 'gender': 'mens'},
    'mens_game_3': {'file': 'mens_game_3.json', 'gender': 'mens'},
    'mens_6ot_game': {'file': 'mens_6ot_game.json', 'gender': 'mens'},
    'womens_game': {'file': 'womens_game.json', 'gender': 'womens'},
}

DATA_TYPES = ['game_info', 'teams_info', 'player_stats', 'team_stats', 'play_by_play', 'officials', 'broadcasts']


def process_game_from_json(game_data: dict, game_name: str) -> dict:
    """Process a game directly from JSON data, bypassing the file-loading in process_game_data.

    This replicates the core logic of process_game_data but takes raw data directly
    instead of loading from disk.
    """
    from espn_data.processor import process_game_data as _process_game_data
    import numpy as np

    # Extract game_id from the data
    game_id = 'unknown'
    if 'gameId' in game_data:
        game_id = str(game_data['gameId'])
    elif 'header' in game_data and 'id' in game_data['header']:
        game_id = str(game_data['header']['id'])
    elif ('header' in game_data and 'competitions' in game_data['header']
          and game_data['header']['competitions']):
        game_id = str(game_data['header']['competitions'][0].get('id', 'unknown'))

    # Extract season
    season = 2023  # default
    if 'season' in game_data and isinstance(game_data['season'], dict):
        season = game_data['season'].get('year', 2023)
    elif 'header' in game_data and isinstance(game_data['header'], dict):
        season = game_data['header'].get('season', {}).get('year', 2023)

    # Write the game data to the expected location temporarily so process_game_data can find it
    games_dir = get_games_dir(season)
    os.makedirs(games_dir, exist_ok=True)
    temp_path = games_dir / f"{game_id}.json"

    # Check if file already exists - don't overwrite real data
    existed = temp_path.exists()
    if not existed:
        with open(temp_path, 'w') as f:
            json.dump(game_data, f)

    try:
        result = _process_game_data(game_id, season, verbose=True)
    finally:
        # Clean up temp file if we created it
        if not existed and temp_path.exists():
            os.remove(temp_path)

    return result


def save_snapshots():
    """Process all example games and save golden snapshots."""
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)

    for game_name, meta in GAME_FILES.items():
        filepath = os.path.join(EXAMPLE_DIR, meta['file'])
        if not os.path.exists(filepath):
            logger.warning(f"Example file not found: {filepath}")
            continue

        logger.info(f"Processing {game_name} from {meta['file']}...")
        set_gender(meta['gender'])

        with open(filepath) as f:
            game_data = json.load(f)

        result = process_game_from_json(game_data, game_name)

        if not result.get('processed'):
            logger.error(f"Failed to process {game_name}: {result.get('error')}")
            continue

        # Save each DataFrame as CSV
        game_snapshot_dir = os.path.join(SNAPSHOT_DIR, game_name)
        os.makedirs(game_snapshot_dir, exist_ok=True)

        for data_type in DATA_TYPES:
            df = result['data'].get(data_type, pd.DataFrame())
            if not df.empty:
                parquet_path = os.path.join(game_snapshot_dir, f'{data_type}.parquet')
                df.to_parquet(parquet_path, index=False)
                logger.info(f"  {data_type}: {len(df)} rows, {len(df.columns)} columns -> {parquet_path}")
            else:
                logger.info(f"  {data_type}: empty")

        # Also save column lists for quick structural checks
        columns_info = {}
        for data_type in DATA_TYPES:
            df = result['data'].get(data_type, pd.DataFrame())
            columns_info[data_type] = {
                'columns': list(df.columns) if not df.empty else [],
                'row_count': len(df),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()} if not df.empty else {},
            }

        with open(os.path.join(game_snapshot_dir, 'columns_info.json'), 'w') as f:
            json.dump(columns_info, f, indent=2)

    logger.info(f"\nGolden snapshots saved to {SNAPSHOT_DIR}")


if __name__ == '__main__':
    save_snapshots()
