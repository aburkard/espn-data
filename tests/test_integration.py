import pytest
import pandas as pd
from pathlib import Path
import tempfile
import os

from espn_data.scraper import get_game_data, get_all_teams
from espn_data.processor import process_game_data


@pytest.fixture
def integration_game_ids():
    """Fixture to provide game IDs for end-to-end testing."""
    return ["401480248", "401373084"]  # Using IDs from previous tests


@pytest.fixture
def test_season():
    """Fixture to provide a season year for tests."""
    return 2023


def test_end_to_end(integration_game_ids, test_season):
    """Test the complete data pipeline from fetching to processing."""

    # Get data for test games
    processed_games = []
    for game_id in integration_game_ids:
        try:
            # 1. Get raw game data from API - pass the season parameter
            game_data = get_game_data(game_id, test_season)
            assert game_data is not None, f"Failed to get game data for {game_id}"

            # 2. Process the game data
            processed_data = process_game_data(game_id, test_season)
            assert processed_data is not None, f"Failed to process game data for {game_id}"

            processed_games.append(processed_data)
        except Exception as e:
            pytest.fail(f"Error processing game {game_id}: {str(e)}")

    assert len(processed_games) > 0, "No games were successfully processed"

    # Test that we can create DataFrames from the processed data
    for processed_data in processed_games:
        # Check for data
        assert "data" in processed_data, "Missing data in processed data"
        data = processed_data["data"]

        # Check game info
        assert "game_info" in data, "Missing game info"

        # Check team stats if present
        if "team_stats" in data and data["team_stats"] is not None:
            team_stats = data["team_stats"]
            team_stats_df = team_stats if isinstance(team_stats, pd.DataFrame) else pd.DataFrame(team_stats)

            if not team_stats_df.empty:
                # Check for standard stats columns
                standard_columns = ['team_id', 'team_name', 'PTS']
                for col in standard_columns:
                    assert col in team_stats_df.columns, f"Missing standard column: {col}"

        # Check player stats if present
        if "player_stats" in data and data["player_stats"] is not None:
            player_stats = data["player_stats"]
            player_stats_df = player_stats if isinstance(player_stats, pd.DataFrame) else pd.DataFrame(player_stats)

            if not player_stats_df.empty:
                # Check for required player fields
                required_columns = ['player_id', 'player_name', 'team_id', 'team_name']
                for col in required_columns:
                    assert col in player_stats_df.columns, f"Missing required column: {col}"


def test_teams_and_games_connection(test_season):
    """Test that we can get team data and game data with team information."""
    # Get all teams
    teams = get_all_teams()
    assert teams is not None, "Failed to get teams"
    assert len(teams) > 300, "Got insufficient number of teams"

    # Create a set of team IDs
    team_ids = set()
    for team_entry in teams:
        if "team" in team_entry and "id" in team_entry["team"]:
            team_ids.add(str(team_entry["team"]["id"]))

    # Get a game and check that it has team data
    game_id = "401480248"  # Use a sample game
    processed_data = process_game_data(game_id, test_season)

    assert "data" in processed_data, "Missing data in processed data"
    data = processed_data["data"]

    # Check for team stats which should have team IDs
    assert "team_stats" in data, "Missing team_stats in data"
    team_stats = data["team_stats"]

    # Convert to DataFrame if not already a DataFrame
    team_df = team_stats if isinstance(team_stats, pd.DataFrame) else pd.DataFrame(team_stats)

    # Skip if empty
    if team_df.empty:
        pytest.skip("No team stats available for this game")

    # Check that we have at least two teams (home and away)
    assert len(team_df) >= 2, "Expected at least 2 teams in team stats"

    # Get the team IDs from the team stats
    assert "team_id" in team_df.columns, "Missing team_id column in team stats"
    assert "team_name" in team_df.columns, "Missing team_name column in team stats"

    # Verify that each team has a valid ID and name
    for _, row in team_df.iterrows():
        team_id = str(row["team_id"])
        team_name = row["team_name"]

        # Check that the team ID is a valid integer
        assert team_id.isdigit(), f"Team ID {team_id} is not a valid integer"

        # Check that the team name is not empty
        assert team_name and len(team_name) > 0, "Team name is empty"
