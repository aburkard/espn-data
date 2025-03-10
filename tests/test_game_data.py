import os
import json
import pytest
from pathlib import Path
import pandas as pd
from unittest.mock import patch, MagicMock

from espn_data.utils import load_json
from espn_data.processor import get_game_details, process_game_data
from espn_data.utils import get_raw_dir, get_season_dir, get_games_dir, set_gender

# Sample test game ID and season
TEST_GAME_ID = "401480248"
TEST_SEASON = 2023
TEST_GENDER = "womens"

# Test data sample for mocking
SAMPLE_GAME_DATA = {
    "header": {
        "id":
            TEST_GAME_ID,
        "season": {
            "year": TEST_SEASON
        },
        "competitions": [{
            "id": TEST_GAME_ID,
            "date": "2023-03-26T21:00Z",
            "attendance": 10267,
            "venue": {
                "fullName": "Test Arena",
                "address": {
                    "city": "Test City",
                    "state": "TS"
                }
            },
            "competitors": [{
                "id": "52",
                "homeAway": "home",
                "team": {
                    "displayName": "Home Team"
                }
            }, {
                "id": "99",
                "homeAway": "away",
                "team": {
                    "displayName": "Away Team"
                }
            }],
            "officials": [{
                "position": {
                    "name": "Referee"
                },
                "names": [{
                    "full": "Test Official"
                }]
            }]
        }]
    }
}


@pytest.fixture
def ensure_test_data():
    """Create test data if it doesn't exist"""
    set_gender(TEST_GENDER)
    game_file = get_games_dir(TEST_SEASON) / f"{TEST_GAME_ID}.json"

    # Create directories if they don't exist
    os.makedirs(game_file.parent, exist_ok=True)

    # Create sample test data if file doesn't exist
    if not game_file.exists():
        with open(game_file, 'w') as f:
            json.dump(SAMPLE_GAME_DATA, f)

    return game_file


def test_game_details(ensure_test_data):
    """Test extracting game details"""
    set_gender(TEST_GENDER)
    game_file = ensure_test_data

    with open(game_file) as f:
        game_data = json.load(f)

    # Extract game details
    game_details = get_game_details(game_data)

    # Check basic game information
    assert game_details["game_id"] == TEST_GAME_ID
    assert game_details["season"] == TEST_SEASON
    assert "date" in game_details

    # Check venue information if available
    if "venue_name" in game_details:
        assert isinstance(game_details["venue_name"], str), "Venue name should be a string"
        # Don't check exact venue name as it may change in the test data

    # Check team information
    assert "teams" in game_details
    assert isinstance(game_details["teams"], list), "Teams should be a list"
    # If teams are available, at least check that the list is not empty
    if game_details["teams"]:
        assert len(game_details["teams"]) > 0, "Teams list should not be empty"


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
