import pytest
import pandas as pd
from espn_data.scraper import get_game_data
from espn_data.processor import process_game_data


@pytest.fixture
def stats_game_id():
    """Fixture to provide a game ID with rich stats data."""
    return "401373084"  # Arkansas @ UConn game from test_stats.py


@pytest.fixture
def stats_season():
    """Fixture to provide a season year for stats tests."""
    return 2022  # Season for the Arkansas @ UConn game


def test_stats_processing(stats_game_id, stats_season):
    """Test the player and team stats processing with a sample game."""
    processed_data = process_game_data(stats_game_id, stats_season)

    assert processed_data is not None, "Failed to process game data"

    # Check for data key
    assert "data" in processed_data, "Missing data in processed data"

    # Extract data object
    data = processed_data["data"]

    # Check for game_info
    assert "game_info" in data, "Missing game_info in data"

    # Check for player stats
    assert "player_stats" in data, "Missing player_stats in data"
    player_stats = data["player_stats"]

    # Convert to DataFrame if not already a DataFrame
    player_df = player_stats if isinstance(player_stats, pd.DataFrame) else pd.DataFrame(player_stats)

    # Skip if empty
    if player_df.empty:
        pytest.skip("No player stats available for this game")

    # Check columns
    essential_player_columns = ['player_id', 'player_name', 'team_id', 'team_name', 'PTS']
    for col in essential_player_columns:
        assert col in player_df.columns, f"Missing essential player column: {col}"

    # Check for team stats
    assert "team_stats" in data, "Missing team_stats in data"
    team_stats = data["team_stats"]

    # Convert to DataFrame if not already a DataFrame
    team_df = team_stats if isinstance(team_stats, pd.DataFrame) else pd.DataFrame(team_stats)

    # Skip if empty
    if team_df.empty:
        pytest.skip("No team stats available for this game")

    # Check columns
    essential_team_columns = ['team_id', 'team_name', 'PTS', 'FG_PCT', '3PT_PCT', 'FT_PCT']
    for col in essential_team_columns:
        assert col in team_df.columns, f"Missing essential team column: {col}"


def test_player_stats_completeness(stats_game_id, stats_season):
    """Test that player stats have all required fields."""
    processed_data = process_game_data(stats_game_id, stats_season)

    assert "data" in processed_data, "Missing data in processed data"
    data = processed_data["data"]

    assert "player_stats" in data, "Missing player_stats in data"
    player_stats = data["player_stats"]

    # Convert to DataFrame if not already a DataFrame
    player_df = player_stats if isinstance(player_stats, pd.DataFrame) else pd.DataFrame(player_stats)

    # Skip if empty
    if player_df.empty:
        pytest.skip("No player stats available for this game")

    # Check for completeness of key stats
    key_player_stats = [
        'MIN', 'FG_MADE', 'FG_ATT', 'FG_PCT', '3PT_MADE', '3PT_ATT', '3PT_PCT', 'FT_MADE', 'FT_ATT', 'FT_PCT', 'REB',
        'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS'
    ]

    for stat in key_player_stats:
        assert stat in player_df.columns, f"Missing player stat column: {stat}"

        # Check for non-null values for players who played in the game
        # DNP players will have null values, so we need to filter them out
        non_dnp_players = player_df[player_df['dnp'] != True]

        # Only check if we have non-DNP players
        if len(non_dnp_players) > 0:
            non_null_count = non_dnp_players[stat].count()
            assert non_null_count > 0, f"No values found for player stat: {stat} for players who played"


def test_dnp_player_null_stats(stats_game_id, stats_season):
    """Test that DNP players have null values for stats."""
    processed_data = process_game_data(stats_game_id, stats_season)

    assert "data" in processed_data, "Missing data in processed data"
    data = processed_data["data"]

    assert "player_stats" in data, "Missing player_stats in data"
    player_stats = data["player_stats"]

    # Convert to DataFrame if not already a DataFrame
    player_df = player_stats if isinstance(player_stats, pd.DataFrame) else pd.DataFrame(player_stats)

    # Skip if empty
    if player_df.empty:
        pytest.skip("No player stats available for this game")

    # Find DNP players if any
    dnp_players = player_df[player_df['dnp'] == True]

    # Skip if no DNP players in this game
    if dnp_players.empty:
        pytest.skip("No DNP players in this game")

    # Key stats that should be null for DNP players
    key_stats = [
        'MIN', 'FG_MADE', 'FG_ATT', 'FG_PCT', '3PT_MADE', '3PT_ATT', '3PT_PCT', 'FT_MADE', 'FT_ATT', 'FT_PCT', 'OREB',
        'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS'
    ]

    # Check that all key stats are null for DNP players
    for stat in key_stats:
        if stat in dnp_players.columns:
            # Check all values are NaN
            assert dnp_players[stat].isna().all(), f"Found non-null {stat} values for DNP players"


def test_team_stats_dtypes(stats_game_id, stats_season):
    """Test that team stats have the correct data types."""
    processed_data = process_game_data(stats_game_id, stats_season)

    assert "data" in processed_data, "Missing data in processed data"
    data = processed_data["data"]

    assert "team_stats" in data, "Missing team_stats in data"
    team_stats = data["team_stats"]

    # Convert to DataFrame if not already a DataFrame
    team_df = team_stats if isinstance(team_stats, pd.DataFrame) else pd.DataFrame(team_stats)

    # Skip if empty
    if team_df.empty:
        pytest.skip("No team stats available for this game")

    # Check that string-formatted stats are kept as strings
    string_format_stats = ['FG', '3PT', 'FT']
    for stat in string_format_stats:
        if stat in team_df.columns:
            assert team_df[stat].dtype == object, f"{stat} should be kept as object/string type"

    # Check that numeric stats are converted to numeric
    numeric_stats = [
        'PTS', 'REB', 'AST', 'STL', 'BLK', 'TO', 'OREB', 'DREB', 'FG_MADE', 'FG_ATT', 'FG_PCT', '3PT_MADE', '3PT_ATT',
        '3PT_PCT', 'FT_MADE', 'FT_ATT', 'FT_PCT'
    ]

    for stat in numeric_stats:
        if stat in team_df.columns:
            # Check it's numeric (either int or float dtype)
            is_numeric = pd.api.types.is_numeric_dtype(team_df[stat])
            assert is_numeric, f"{stat} should be a numeric type but is {team_df[stat].dtype}"


def test_optimize_dataframe_dtypes():
    """Test the optimize_dataframe_dtypes function works correctly."""
    from espn_data.processor import optimize_dataframe_dtypes
    import numpy as np

    # Create a test dataframe with various data types and null values
    test_data = {
        "game_id": ["401234567", "401234568", "401234569"],
        "team_id": ["123", "456", "789"],
        "player_id": ["p123", "p456", "p789"],
        "position_id": ["1", "2", None],
        "jersey": ["3", "9", "12"],
        "starter": [True, False, True],
        "dnp": [False, True, False],
        "MIN": ["23:15", None, "31:40"],
        "PTS": ["15", None, "22"],
        "FG": ["5-10", None, "8-15"],
        "3PT": ["1-3", None, "2-5"]
    }

    df = pd.DataFrame(test_data)

    # Optimize as player_stats
    optimized_df = optimize_dataframe_dtypes(df, "player_stats")

    # Check ID columns are converted to Int64
    assert pd.api.types.is_integer_dtype(optimized_df["player_id"].dtype), "player_id should be Int64"
    assert pd.api.types.is_integer_dtype(optimized_df["team_id"].dtype), "team_id should be Int64"
    assert pd.api.types.is_integer_dtype(optimized_df["game_id"].dtype), "game_id should be Int64"

    # Check string-format stats are kept as strings
    assert optimized_df["FG"].dtype == object, "FG should remain as object"
    assert optimized_df["3PT"].dtype == object, "3PT should remain as object"

    # Check that DNP player has null values
    dnp_row = optimized_df[optimized_df["dnp"] == True]
    if not dnp_row.empty:
        assert pd.isna(dnp_row["PTS"].iloc[0]), "PTS should be NaN for DNP player"
