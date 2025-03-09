#!/usr/bin/env python
"""
Test script to verify our changes to the game data processing code.
"""

import os
import json
import logging
import traceback
from pathlib import Path

from espn_data.utils import load_json
from espn_data.processor import get_game_details, process_game_data

# Configure logging to see information
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("espn_data")


def test_game_details():
    """Test the get_game_details function with a sample game file."""
    # Try a different game ID that might have more complete data
    game_id = "401480248"  # This file was among the largest, so may have more data
    season = 2023  # The season where we found the file

    # Path to the game data file
    game_file = Path(f"espn_data/data/raw/{season}/games/{game_id}.json")

    if not game_file.exists():
        print(f"Game file not found: {game_file}")
        return

    print(f"Testing game processing with file: {game_file}")

    # Load the game data
    game_data = load_json(game_file)

    # Check if header->competitions exist in the data
    print("\nVerifying data structure:")
    if 'header' in game_data:
        print("✓ Header found")
        if 'competitions' in game_data['header'] and game_data['header']['competitions']:
            print(f"✓ Competitions found: {len(game_data['header']['competitions'])}")

            competition = game_data['header']['competitions'][0]
            print("\nCompetition data structure:")

            if 'status' in competition:
                print("✓ Status found")
                print(f"Status content: {json.dumps(competition['status'], indent=2)}")
            else:
                print("✗ Status not found!")

            if 'broadcasts' in competition:
                print(f"✓ Broadcasts found: {len(competition['broadcasts'])}")
                print(f"Broadcasts content: {json.dumps(competition['broadcasts'], indent=2)}")
            else:
                print("✗ Broadcasts not found!")

            if 'groups' in competition:
                print("✓ Groups found")
                print(f"Groups content: {json.dumps(competition['groups'], indent=2)}")
            else:
                print("✗ Groups not found!")
        else:
            print("✗ No competitions found in header!")
    else:
        print("✗ No header found in game data!")

    # Test get_game_details function
    print("\n=== Testing get_game_details ===")
    game_details = get_game_details(game_data)

    # Print the complete details for debugging
    print("\nComplete game_details structure:")
    print(json.dumps(game_details, indent=2, default=str))

    # Check if status is extracted correctly
    print("\nStatus data:")
    if game_details.get("status"):
        print(f"  ID: {game_details['status'].get('id')}")
        print(f"  Name: {game_details['status'].get('name')}")
        print(f"  State: {game_details['status'].get('state')}")
        print(f"  Completed: {game_details['status'].get('completed')}")
        print(f"  Description: {game_details['status'].get('description')}")
        print(f"  Detail: {game_details['status'].get('detail')}")
        print(f"  Short Detail: {game_details['status'].get('short_detail')}")
    else:
        print("  No status data found!")

    # Check if broadcasts are extracted correctly
    print("\nBroadcast data:")
    if game_details.get("broadcasts"):
        for i, broadcast in enumerate(game_details["broadcasts"]):
            print(f"  Broadcast {i+1}:")
            print(f"    Type: {broadcast.get('type')}")
            print(f"    Market: {broadcast.get('market')}")
            print(f"    Media: {broadcast.get('media')}")
            print(f"    Language: {broadcast.get('lang')}")
            print(f"    Region: {broadcast.get('region')}")
    else:
        print("  No broadcast data found!")

    # Check if groups/conference data is extracted correctly
    print("\nConference data:")
    if game_details.get("groups"):
        print(f"  ID: {game_details['groups'].get('id')}")
        print(f"  Name: {game_details['groups'].get('name')}")
        print(f"  Abbreviation: {game_details['groups'].get('abbreviation')}")
        print(f"  Short Name: {game_details['groups'].get('short_name')}")
    else:
        print("  No conference data found!")


def test_game_processing():
    """Test processing a sample game file to verify our changes."""
    # Game ID from the example in the notebook
    game_id = "401373084"
    season = 2023  # The season where we found the file

    # Path to the game data file
    game_file = Path(f"espn_data/data/raw/{season}/games/{game_id}.json")

    if not game_file.exists():
        print(f"Game file not found: {game_file}")
        return

    print(f"Testing game processing with file: {game_file}")

    # Load the game data
    game_data = load_json(game_file)

    # Test get_game_details function
    print("\n=== Testing get_game_details ===")
    game_details = get_game_details(game_data)

    # Check if status is extracted correctly
    print("\nStatus data:")
    if game_details.get("status"):
        print(f"  ID: {game_details['status'].get('id')}")
        print(f"  Name: {game_details['status'].get('name')}")
        print(f"  State: {game_details['status'].get('state')}")
        print(f"  Completed: {game_details['status'].get('completed')}")
        print(f"  Description: {game_details['status'].get('description')}")
        print(f"  Detail: {game_details['status'].get('detail')}")
        print(f"  Short Detail: {game_details['status'].get('short_detail')}")
    else:
        print("  No status data found!")

    # Check if broadcasts are extracted correctly
    print("\nBroadcast data:")
    if game_details.get("broadcasts"):
        for i, broadcast in enumerate(game_details["broadcasts"]):
            print(f"  Broadcast {i+1}:")
            print(f"    Type: {broadcast.get('type')}")
            print(f"    Market: {broadcast.get('market')}")
            print(f"    Media: {broadcast.get('media')}")
            print(f"    Language: {broadcast.get('lang')}")
            print(f"    Region: {broadcast.get('region')}")
    else:
        print("  No broadcast data found!")

    # Check if groups/conference data is extracted correctly
    print("\nConference data:")
    if game_details.get("groups"):
        print(f"  ID: {game_details['groups'].get('id')}")
        print(f"  Name: {game_details['groups'].get('name')}")
        print(f"  Abbreviation: {game_details['groups'].get('abbreviation')}")
        print(f"  Short Name: {game_details['groups'].get('short_name')}")
    else:
        print("  No conference data found!")

    # Test the full process_game_data function
    print("\n=== Testing process_game_data ===")
    processed_data = process_game_data(game_id, season)

    # Check game_info in the processed data
    print("\nGame Info:")
    if "game_info" in processed_data:
        game_info = processed_data["game_info"]
        print(f"  Status: {game_info.get('status')}")
        print(f"  State: {game_info.get('state')}")
        print(f"  Completed: {game_info.get('completed')}")
        print(f"  Broadcast: {game_info.get('broadcast')}")
        print(f"  Conference: {game_info.get('conference')}")
    else:
        print("  No game_info in processed data!")

    # Check broadcasts data
    print("\nDetailed Broadcast Data:")
    if "broadcasts" in processed_data and processed_data["broadcasts"]:
        for i, broadcast in enumerate(processed_data["broadcasts"]):
            print(f"  Broadcast {i+1}:")
            print(f"    Type: {broadcast.get('type')}")
            print(f"    Market: {broadcast.get('market')}")
            print(f"    Media: {broadcast.get('media')}")
    else:
        print("  No detailed broadcast data found!")


def test_multiple_games():
    """Test multiple games to find one with conference data."""
    game_ids = [
        "401488918",  # Try different games to find one with groups data
        "401480237",
        "401527966"
    ]
    season = 2023

    for game_id in game_ids:
        print(f"\n{'=' * 50}")
        print(f"TESTING GAME: {game_id}")
        print(f"{'=' * 50}")

        # Path to the game data file
        game_file = Path(f"espn_data/data/raw/{season}/games/{game_id}.json")

        if not game_file.exists():
            print(f"Game file not found: {game_file}")
            continue

        # Load the game data
        game_data = load_json(game_file)

        # Check if competition has groups data
        if 'header' in game_data and 'competitions' in game_data['header'] and game_data['header']['competitions']:
            competition = game_data['header']['competitions'][0]
            if 'groups' in competition:
                print("✓ FOUND GAME WITH GROUPS DATA!")
                print(f"Groups content: {json.dumps(competition['groups'], indent=2)}")

                # Test get_game_details function with this game
                game_details = get_game_details(game_data)

                # Check if groups/conference data is extracted correctly
                print("\nExtracted Conference data:")
                if game_details.get("groups"):
                    print(f"  ID: {game_details['groups'].get('id')}")
                    print(f"  Name: {game_details['groups'].get('name')}")
                    print(f"  Abbreviation: {game_details['groups'].get('abbreviation')}")
                    print(f"  Short Name: {game_details['groups'].get('short_name')}")
                    break
                else:
                    print("  No conference data found in extracted details!")
            else:
                print("✗ No groups data found in this game")
        else:
            print("✗ Invalid data structure in this game")

    print(f"\n{'=' * 50}")
    print("TESTING COMPLETE")
    print(f"{'=' * 50}")


def test_end_to_end():
    """Test the complete end-to-end processing with a game that has all the data we need."""
    game_id = "401488918"  # Game with conference data
    season = 2023

    print(f"\n{'=' * 50}")
    print(f"TESTING END-TO-END PROCESSING FOR GAME: {game_id}")
    print(f"{'=' * 50}")

    try:
        # Process the game data
        processed_data = process_game_data(game_id, season)

        # Check game_info in the processed data
        print("\nGame Info:")
        if processed_data and "game_info" in processed_data:
            game_info = processed_data["game_info"]
            print(f"  Game ID: {game_info.get('game_id')}")
            print(f"  Date: {game_info.get('date')}")
            print(f"  Venue: {game_info.get('venue')}")
            print(f"  Status: {game_info.get('status')}")
            print(f"  State: {game_info.get('state')}")
            print(f"  Completed: {game_info.get('completed')}")
            print(f"  Broadcast: {game_info.get('broadcast')}")
            print(f"  Conference: {game_info.get('conference')}")
        else:
            print("  No game_info in processed data!")

        # Check if broadcast data was processed
        print("\nDetailed Broadcast Data:")
        if processed_data and "broadcasts" in processed_data and processed_data["broadcasts"]:
            for i, broadcast in enumerate(processed_data["broadcasts"]):
                print(f"  Broadcast {i+1}:")
                print(f"    Type: {broadcast.get('type')}")
                print(f"    Market: {broadcast.get('market')}")
                print(f"    Media: {broadcast.get('media')}")
        else:
            print("  No detailed broadcast data found!")

        print(f"\n{'=' * 50}")
        print("TEST COMPLETED SUCCESSFULLY!")
        print(f"{'=' * 50}")

    except Exception as e:
        print(f"ERROR PROCESSING GAME: {e}")
        traceback.print_exc()
        print(f"\n{'=' * 50}")
        print("TEST FAILED!")
        print(f"{'=' * 50}")


def test_team_stats_columns():
    """Test the standardized team stats columns."""
    game_id = "401488918"  # Game with good data
    season = 2023

    print(f"\n{'=' * 50}")
    print(f"TESTING TEAM STATS COLUMN STANDARDIZATION FOR GAME: {game_id}")
    print(f"{'=' * 50}")

    try:
        # Process the game data
        processed_data = process_game_data(game_id, season)

        # Check team stats columns
        if processed_data and "team_stats" in processed_data:
            team_stats = processed_data["team_stats"]
            if team_stats:
                # Convert to DataFrame to see column names
                import pandas as pd
                team_stats_df = pd.DataFrame(team_stats)

                print("\nTeam Stats Columns:")
                print(f"Number of columns: {len(team_stats_df.columns)}")
                print(team_stats_df.columns.tolist())

                # Check for duplicates or non-standard names
                print("\nChecking for standard stat names:")
                standard_columns = [
                    'FG', 'FG_MADE', 'FG_ATT', 'FG_PCT', '3PT', '3PT_MADE', '3PT_ATT', '3PT_PCT', 'FT', 'FT_MADE',
                    'FT_ATT', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS'
                ]

                for col in standard_columns:
                    if col in team_stats_df.columns:
                        print(f"  ✓ {col} found")
                    else:
                        print(f"  ✗ {col} missing")

                # Check for old-style verbose column names
                print("\nChecking if verbose column names are gone:")
                verbose_columns = [
                    'fieldGoalsMade', 'fieldGoalsAttempted', 'fieldGoalPct', 'threePointFieldGoalsMade',
                    'threePointFieldGoalsAttempted', 'threePointFieldGoalPct', 'freeThrowsMade', 'freeThrowsAttempted',
                    'freeThrowPct', 'totalRebounds', 'offensiveRebounds', 'defensiveRebounds', 'assists', 'steals',
                    'blocks', 'turnovers', 'fouls'
                ]

                for col in verbose_columns:
                    if col in team_stats_df.columns:
                        print(f"  ✗ {col} still present")
                    else:
                        print(f"  ✓ {col} removed")

                # Show all duplicate or redundant columns by finding columns with similar data
                print("\nSample row data for a team:")
                sample_row = team_stats_df.iloc[0].to_dict()
                print(json.dumps(sample_row, indent=2, default=str))

            else:
                print("No team stats found for this game!")
        else:
            print("No team_stats in processed data!")

        print(f"\n{'=' * 50}")
        print("TEAM STATS COLUMN TEST COMPLETED!")
        print(f"{'=' * 50}")

    except Exception as e:
        print(f"ERROR TESTING TEAM STATS: {e}")
        traceback.print_exc()
        print(f"\n{'=' * 50}")
        print("TEAM STATS COLUMN TEST FAILED!")
        print(f"{'=' * 50}")


if __name__ == "__main__":
    try:
        test_end_to_end()
        test_team_stats_columns()
    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()
