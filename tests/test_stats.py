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

        # Check for non-null values (at least some players should have values)
        non_null_count = player_df[stat].count()
        assert non_null_count > 0, f"No values found for player stat: {stat}"
