"""Regression tests that compare processor output against golden snapshots.

These tests ensure that refactoring doesn't silently change the output data.
If a test fails after a code change, either:
  1. The change introduced a bug (fix the code), or
  2. The change fixed a bug (update the snapshot and document the change)

To regenerate snapshots after an intentional change:
    python tests/generate_golden_snapshots.py
"""

import os
import sys
import json
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import numpy as np
from espn_data.processor import (
    get_game_details,
    convert_clock_to_seconds,
    get_broadcasts,
    get_primary_broadcast,
    optimize_dataframe_dtypes,
    remove_redundant_columns,
)
from espn_data.utils import set_gender, get_games_dir

EXAMPLE_DIR = os.path.join(os.path.dirname(__file__), '..', 'example_data')
SNAPSHOT_DIR = os.path.join(os.path.dirname(__file__), 'golden_snapshots')

GAME_FILES = {
    'mens_game': {'file': 'mens_game.json', 'gender': 'mens'},
    'mens_game_2': {'file': 'mens_game_2.json', 'gender': 'mens'},
    'mens_game_3': {'file': 'mens_game_3.json', 'gender': 'mens'},
    'mens_6ot_game': {'file': 'mens_6ot_game.json', 'gender': 'mens'},
    'womens_game': {'file': 'womens_game.json', 'gender': 'womens'},
}

DATA_TYPES = ['game_info', 'teams_info', 'player_stats', 'team_stats', 'play_by_play', 'officials', 'broadcasts']


def _load_game_json(game_name):
    """Load an example game JSON file."""
    filepath = os.path.join(EXAMPLE_DIR, GAME_FILES[game_name]['file'])
    with open(filepath) as f:
        return json.load(f)


def _process_game(game_name):
    """Process a game through the full pipeline and return result dict."""
    from espn_data.processor import process_game_data

    game_data = _load_game_json(game_name)
    meta = GAME_FILES[game_name]
    set_gender(meta['gender'])

    # Extract game_id and season from the data
    game_id = 'unknown'
    if 'gameId' in game_data:
        game_id = str(game_data['gameId'])
    elif 'header' in game_data and 'id' in game_data['header']:
        game_id = str(game_data['header']['id'])
    elif ('header' in game_data and 'competitions' in game_data['header']
          and game_data['header']['competitions']):
        game_id = str(game_data['header']['competitions'][0].get('id', 'unknown'))

    season = 2023
    if 'season' in game_data and isinstance(game_data['season'], dict):
        season = game_data['season'].get('year', 2023)
    elif 'header' in game_data and isinstance(game_data['header'], dict):
        season = game_data['header'].get('season', {}).get('year', 2023)

    # Write temp file for process_game_data to find
    games_dir = get_games_dir(season)
    os.makedirs(games_dir, exist_ok=True)
    temp_path = games_dir / f"{game_id}.json"
    existed = temp_path.exists()

    if not existed:
        with open(temp_path, 'w') as f:
            json.dump(game_data, f)

    try:
        result = process_game_data(game_id, season, verbose=False)
    finally:
        if not existed and temp_path.exists():
            os.remove(temp_path)

    return result


def _load_snapshot(game_name, data_type):
    """Load a golden snapshot (parquet preferred, CSV fallback)."""
    parquet_path = os.path.join(SNAPSHOT_DIR, game_name, f'{data_type}.parquet')
    if os.path.exists(parquet_path):
        return pd.read_parquet(parquet_path)
    csv_path = os.path.join(SNAPSHOT_DIR, game_name, f'{data_type}.csv')
    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)
    return None


def _compare_dataframes(actual, expected, data_type, game_name):
    """Compare two DataFrames, handling type differences from CSV round-tripping."""
    # Check shape
    assert actual.shape == expected.shape, (
        f"{game_name}/{data_type}: shape mismatch - "
        f"got {actual.shape}, expected {expected.shape}"
    )

    # Check columns match
    assert list(actual.columns) == list(expected.columns), (
        f"{game_name}/{data_type}: column mismatch - "
        f"got {list(actual.columns)}, expected {list(expected.columns)}"
    )

    # Compare values column by column
    for col in actual.columns:
        actual_col = actual[col]
        expected_col = expected[col]

        # Both NaN → equal
        both_nan = actual_col.isna() & expected_col.isna()
        # Compare non-NaN values as strings to avoid type issues from CSV round-trip
        non_nan = ~actual_col.isna() & ~expected_col.isna()

        if non_nan.any():
            actual_vals = actual_col[non_nan].astype(str).values
            expected_vals = expected_col[non_nan].astype(str).values

            # For float-like values, compare numerically to handle precision
            mismatches = []
            for i, (a, e) in enumerate(zip(actual_vals, expected_vals)):
                if a != e:
                    # Try numeric comparison
                    try:
                        if abs(float(a) - float(e)) < 1e-6:
                            continue
                    except (ValueError, TypeError):
                        pass
                    mismatches.append((i, a, e))

            assert not mismatches, (
                f"{game_name}/{data_type} column '{col}': "
                f"{len(mismatches)} value mismatches. First 3: {mismatches[:3]}"
            )

        # Check NaN pattern matches
        actual_nan = actual_col.isna()
        expected_nan = expected_col.isna()
        nan_mismatch = (actual_nan != expected_nan).sum()
        assert nan_mismatch == 0, (
            f"{game_name}/{data_type} column '{col}': "
            f"{nan_mismatch} NaN pattern mismatches"
        )


# ============================================================
# Regression tests: compare processor output to golden snapshots
# ============================================================

@pytest.mark.parametrize("game_name", list(GAME_FILES.keys()))
@pytest.mark.parametrize("data_type", DATA_TYPES)
def test_snapshot_regression(game_name, data_type):
    """Verify processor output matches golden snapshot for each game and data type."""
    expected = _load_snapshot(game_name, data_type)
    if expected is None:
        pytest.skip(f"No snapshot for {game_name}/{data_type}")

    result = _process_game(game_name)
    assert result.get('processed'), f"Processing failed: {result.get('error')}"

    actual = result['data'].get(data_type, pd.DataFrame())
    assert not actual.empty, f"{game_name}/{data_type}: got empty DataFrame but snapshot exists"

    _compare_dataframes(actual, expected, data_type, game_name)


# ============================================================
# Unit tests for utility functions
# ============================================================

class TestConvertClockToSeconds:
    def test_normal(self):
        assert convert_clock_to_seconds("10:00") == 600

    def test_zero(self):
        assert convert_clock_to_seconds("0:00") == 0

    def test_partial(self):
        assert convert_clock_to_seconds("5:30") == 330

    def test_single_digit_seconds(self):
        assert convert_clock_to_seconds("1:05") == 65

    def test_none(self):
        assert convert_clock_to_seconds(None) is None

    def test_empty_string(self):
        assert convert_clock_to_seconds("") is None

    def test_non_string(self):
        assert convert_clock_to_seconds(123) is None

    def test_invalid_format(self):
        assert convert_clock_to_seconds("abc") is None

    def test_single_number(self):
        assert convert_clock_to_seconds("30") is None


class TestGetBroadcasts:
    def test_top_level_broadcasts(self):
        data = {'broadcasts': [{'media': {'shortName': 'ESPN'}}]}
        result = get_broadcasts(data)
        assert len(result) == 1
        assert result[0]['media']['shortName'] == 'ESPN'

    def test_header_broadcasts(self):
        data = {'header': {'competitions': [{'broadcasts': [{'media': {'shortName': 'ABC'}}]}]}}
        result = get_broadcasts(data)
        assert len(result) == 1

    def test_game_info_broadcasts(self):
        data = {'gameInfo': {'broadcasts': [{'media': {'shortName': 'CBS'}}]}}
        result = get_broadcasts(data)
        assert len(result) == 1

    def test_no_broadcasts(self):
        data = {}
        result = get_broadcasts(data)
        assert result == [] or result is None or len(result) == 0

    def test_empty_broadcasts(self):
        data = {'broadcasts': []}
        result = get_broadcasts(data)
        assert len(result) == 0


class TestGetPrimaryBroadcast:
    def test_prefers_national_tv(self):
        data = {'broadcasts': [
            {'type': {'shortName': 'Web'}, 'market': {'type': 'National'}, 'media': {'shortName': 'ESPN+'}},
            {'type': {'shortName': 'TV'}, 'market': {'type': 'National'}, 'media': {'shortName': 'ESPN'}},
            {'type': {'shortName': 'TV'}, 'market': {'type': 'Local'}, 'media': {'shortName': 'NESN'}},
        ]}
        result = get_primary_broadcast(data)
        assert result['media']['shortName'] == 'ESPN'

    def test_falls_back_to_tv(self):
        data = {'broadcasts': [
            {'type': {'shortName': 'Web'}, 'market': {'type': 'National'}, 'media': {'shortName': 'ESPN+'}},
            {'type': {'shortName': 'TV'}, 'market': {'type': 'Local'}, 'media': {'shortName': 'NESN'}},
        ]}
        result = get_primary_broadcast(data)
        assert result['media']['shortName'] == 'NESN'

    def test_falls_back_to_national(self):
        data = {'broadcasts': [
            {'type': {'shortName': 'Web'}, 'market': {'type': 'National'}, 'media': {'shortName': 'ESPN+'}},
            {'type': {'shortName': 'Web'}, 'market': {'type': 'Local'}, 'media': {'shortName': 'App'}},
        ]}
        result = get_primary_broadcast(data)
        assert result['media']['shortName'] == 'ESPN+'

    def test_falls_back_to_first(self):
        data = {'broadcasts': [
            {'type': {'shortName': 'Web'}, 'market': {'type': 'Local'}, 'media': {'shortName': 'App'}},
        ]}
        result = get_primary_broadcast(data)
        assert result['media']['shortName'] == 'App'

    def test_no_broadcasts(self):
        result = get_primary_broadcast({})
        assert result is None


class TestGetGameDetails:
    """Test game detail extraction from example files."""

    def test_mens_game_details(self):
        data = _load_game_json('mens_game')
        details = get_game_details(data, 'mens_game.json')
        assert details['game_id'] != 'unknown'
        assert details['teams'] is not None
        assert len(details['teams']) == 2

    def test_womens_game_details(self):
        data = _load_game_json('womens_game')
        details = get_game_details(data, 'womens_game.json')
        assert details['game_id'] != 'unknown'
        assert len(details['teams']) == 2

    def test_6ot_game_linescores(self):
        """6 OT game should have 8+ linescores (2 halves + 6 OT)."""
        data = _load_game_json('mens_6ot_game')
        details = get_game_details(data, 'mens_6ot_game.json')
        for team in details['teams']:
            if 'linescores' in team:
                assert len(team['linescores']) >= 8, (
                    f"6OT game should have 8+ linescores, got {len(team['linescores'])}"
                )

    def test_venue_extraction(self):
        data = _load_game_json('mens_game')
        details = get_game_details(data, 'mens_game.json')
        assert details['venue_name'] is not None or details['venue_city'] is not None

    def test_attendance_extraction(self):
        """At least some games should have attendance."""
        for game_name in GAME_FILES:
            data = _load_game_json(game_name)
            details = get_game_details(data, f'{game_name}.json')
            if details['attendance'] is not None:
                assert isinstance(details['attendance'], (int, float))
                return
        pytest.skip("No games with attendance data found")

    def test_broadcast_extraction(self):
        data = _load_game_json('mens_game')
        details = get_game_details(data, 'mens_game.json')
        # broadcast may or may not be present, but the field should exist
        assert 'broadcast' in details


class TestGameProcessingStructure:
    """Verify structural invariants of processed data."""

    @pytest.fixture(scope="class")
    def processed_games(self):
        """Process all example games once for the class."""
        results = {}
        for game_name in GAME_FILES:
            results[game_name] = _process_game(game_name)
        return results

    def test_all_games_process_successfully(self, processed_games):
        for game_name, result in processed_games.items():
            assert result.get('processed'), f"{game_name} failed: {result.get('error')}"

    def test_game_info_has_one_row(self, processed_games):
        for game_name, result in processed_games.items():
            df = result['data']['game_info']
            assert len(df) == 1, f"{game_name}: game_info should have 1 row, got {len(df)}"

    def test_teams_info_has_two_rows(self, processed_games):
        for game_name, result in processed_games.items():
            df = result['data']['teams_info']
            assert len(df) == 2, f"{game_name}: teams_info should have 2 rows, got {len(df)}"

    def test_team_stats_has_two_rows(self, processed_games):
        for game_name, result in processed_games.items():
            df = result['data']['team_stats']
            assert len(df) == 2, f"{game_name}: team_stats should have 2 rows, got {len(df)}"

    def test_player_stats_has_players(self, processed_games):
        for game_name, result in processed_games.items():
            df = result['data']['player_stats']
            assert len(df) > 0, f"{game_name}: player_stats should have rows"

    def test_play_by_play_has_plays(self, processed_games):
        for game_name, result in processed_games.items():
            df = result['data']['play_by_play']
            assert len(df) > 0, f"{game_name}: play_by_play should have rows"

    def test_game_ids_consistent(self, processed_games):
        """All data types for a game should reference the same game_id."""
        for game_name, result in processed_games.items():
            game_id = result['game_id']
            for dt in DATA_TYPES:
                df = result['data'].get(dt, pd.DataFrame())
                if not df.empty and 'game_id' in df.columns:
                    unique_ids = df['game_id'].unique()
                    assert len(unique_ids) == 1, (
                        f"{game_name}/{dt}: expected 1 game_id, got {unique_ids}"
                    )
                    assert str(unique_ids[0]) == str(game_id), (
                        f"{game_name}/{dt}: game_id mismatch {unique_ids[0]} != {game_id}"
                    )

    def test_player_stats_have_required_columns(self, processed_games):
        required = ['game_id', 'team_id', 'player_id', 'player_name', 'PTS']
        for game_name, result in processed_games.items():
            df = result['data']['player_stats']
            for col in required:
                assert col in df.columns, f"{game_name}: player_stats missing column '{col}'"

    def test_team_stats_have_required_columns(self, processed_games):
        required = ['game_id', 'team_id', 'PTS']
        for game_name, result in processed_games.items():
            df = result['data']['team_stats']
            for col in required:
                assert col in df.columns, f"{game_name}: team_stats missing column '{col}'"

    def test_split_stats_present(self, processed_games):
        """FG/3PT/FT should be split into _MADE, _ATT, _PCT."""
        split_stats = ['FG_MADE', 'FG_ATT', 'FG_PCT', '3PT_MADE', '3PT_ATT', '3PT_PCT',
                       'FT_MADE', 'FT_ATT', 'FT_PCT']
        for game_name, result in processed_games.items():
            for dt in ['player_stats', 'team_stats']:
                df = result['data'][dt]
                for col in split_stats:
                    assert col in df.columns, f"{game_name}/{dt} missing '{col}'"

    def test_dnp_players_have_null_stats(self, processed_games):
        """DNP players should have NaN for all stat columns."""
        stat_cols = ['PTS', 'REB', 'AST', 'FG_MADE', 'FG_ATT']
        for game_name, result in processed_games.items():
            df = result['data']['player_stats']
            dnp = df[df['dnp'] == True]
            if len(dnp) > 0:
                for col in stat_cols:
                    if col in dnp.columns:
                        assert dnp[col].isna().all(), (
                            f"{game_name}: DNP players should have NaN for {col}"
                        )

    def test_6ot_game_has_more_plays(self, processed_games):
        """The 6OT game should have significantly more plays than a normal game."""
        normal = processed_games['mens_game']['data']['play_by_play']
        overtime = processed_games['mens_6ot_game']['data']['play_by_play']
        assert len(overtime) > len(normal), "6OT game should have more plays than normal game"
