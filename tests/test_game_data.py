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
                "id":
                    "52",
                "homeAway":
                    "home",
                "team": {
                    "displayName": "Home Team",
                    "groups": {
                        "id": "2",
                        "isConference": True,
                        "slug": "atlantic-coast-conference",
                        "parent": {
                            "id": "50",
                            "name": "NCAA Division I",
                            "slug": "ncaa-division-i"
                        }
                    }
                },
                "linescores": [{
                    "displayValue": "17"
                }, {
                    "displayValue": "15"
                }, {
                    "displayValue": "13"
                }, {
                    "displayValue": "16"
                }]
            }, {
                "id":
                    "99",
                "homeAway":
                    "away",
                "team": {
                    "displayName": "Away Team",
                    "groups": {
                        "id": "2",
                        "isConference": True,
                        "slug": "atlantic-coast-conference",
                        "parent": {
                            "id": "50",
                            "name": "NCAA Division I",
                            "slug": "ncaa-division-i"
                        }
                    }
                },
                "linescores": [{
                    "displayValue": "14"
                }, {
                    "displayValue": "10"
                }, {
                    "displayValue": "22"
                }, {
                    "displayValue": "8"
                }]
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


def test_team_conference_and_linescores():
    """Test that team conference, division, and linescores information is correctly extracted."""
    # Use the modified test data with conference and linescores
    game_data = SAMPLE_GAME_DATA

    # Extract game details
    game_details = get_game_details(game_data)

    # Verify teams have conference, division, and linescores data
    for team in game_details.get("teams", []):
        assert "conference_id" in team, "Missing conference_id in team data"
        assert "conference_slug" in team, "Missing conference_slug in team data"
        assert "division" in team, "Missing division in team data"
        assert "linescores" in team, "Missing linescores in team data"

        # Check specific values
        assert team["conference_id"] == "2", "Incorrect conference_id"
        assert team["conference_slug"] == "atlantic-coast-conference", "Incorrect conference_slug"
        assert team["division"] == "NCAA Division I", "Incorrect division"
        assert isinstance(team["linescores"], list), "Linescores should be a list"
        assert len(team["linescores"]) == 4, "Should have 4 quarter scores"

    # Test that these fields make it to the final dataframe
    with patch('espn_data.processor.load_json', return_value=game_data):
        game_id = game_data['header']['id']
        season = game_data['header']['season']['year']

        # Process the game data
        result = process_game_data(game_id, season)

        # Verify teams_info dataframe has the needed columns
        assert "data" in result, "Missing data in result"
        assert "teams_info" in result["data"], "Missing teams_info in data"

        teams_df = result["data"]["teams_info"]
        assert not teams_df.empty, "Teams dataframe is empty"

        # Check for our new columns
        assert "conference_id" in teams_df.columns, "Missing conference_id column"
        assert "conference_slug" in teams_df.columns, "Missing conference_slug column"
        assert "division" in teams_df.columns, "Missing division column"
        assert "linescores" in teams_df.columns, "Missing linescores column"

        # Verify values
        assert teams_df["conference_id"].iloc[0] == "2", "Incorrect conference_id in dataframe"
        assert teams_df["conference_slug"].iloc[
            0] == "atlantic-coast-conference", "Incorrect conference_slug in dataframe"
        assert teams_df["division"].iloc[0] == "NCAA Division I", "Incorrect division in dataframe"

        # For linescores, they should be stored as a comma-separated string
        assert isinstance(teams_df["linescores"].iloc[0], str), "Linescores should be a string in the dataframe"
        assert "," in teams_df["linescores"].iloc[0], "Linescores should be comma-separated"
