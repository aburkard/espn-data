import os
import pytest
from pathlib import Path
import pandas as pd

from espn_data.utils import load_json
from espn_data.processor import get_game_details, process_game_data


def test_game_details(sample_game_id, season):
    """Test the get_game_details function with a sample game file."""
    # Path to the game data file
    game_file = Path(f"espn_data/data/raw/{season}/games/{sample_game_id}.json")

    if not game_file.exists():
        pytest.skip(f"Game file not found: {game_file}")

    # Load the game data
    game_data = load_json(game_file)
    assert game_data is not None, "Failed to load game data"

    # Process the data
    details = get_game_details(game_data)
    assert details is not None, "Failed to get game details"

    # Check for important fields
    assert "game_id" in details, "Missing game_id in details"
    assert "home_team" in details, "Missing home_team in details"
    assert "away_team" in details, "Missing away_team in details"
    assert "date" in details, "Missing date in details"


def test_game_processing(sample_game_id, season):
    """Test the process_game_data function with a sample game."""
    # Process the game data
    processed_data = process_game_data(sample_game_id, season)

    assert processed_data is not None, "Failed to process game data"
    assert "data" in processed_data, "Missing data in processed data"

    data = processed_data["data"]
    assert "game_info" in data, "Missing game_info in data"

    # Check for team stats
    assert "team_stats" in data, "Missing team_stats in data"
    team_stats = data["team_stats"]

    # Convert to DataFrame if not already a DataFrame
    team_df = team_stats if isinstance(team_stats, pd.DataFrame) else pd.DataFrame(team_stats)

    # Skip if empty
    if team_df.empty:
        pytest.skip("No team stats available for this game")

    # Check for standard columns
    standard_columns = [
        'FG', 'FG_MADE', 'FG_ATT', 'FG_PCT', '3PT', '3PT_MADE', '3PT_ATT', '3PT_PCT', 'FT', 'FT_MADE', 'FT_ATT',
        'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS'
    ]

    for col in standard_columns:
        assert col in team_df.columns, f"Missing standard column: {col}"

    # Check that verbose columns are gone
    verbose_columns = [
        'fieldGoalsMade', 'fieldGoalsAttempted', 'fieldGoalPct', 'threePointFieldGoalsMade',
        'threePointFieldGoalsAttempted', 'threePointFieldGoalPct', 'freeThrowsMade', 'freeThrowsAttempted',
        'freeThrowPct', 'totalRebounds', 'offensiveRebounds', 'defensiveRebounds', 'assists', 'steals', 'blocks',
        'turnovers', 'fouls'
    ]

    for col in verbose_columns:
        assert col not in team_df.columns, f"Verbose column still present: {col}"
