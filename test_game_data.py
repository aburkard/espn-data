import json
from pathlib import Path
from pprint import pprint
from espn_data.processor import get_game_details, process_game_data

# Load the example game data
with open('example_data/game.json', 'r') as f:
    game_data = json.load(f)

# Extract game details
game_details = get_game_details(game_data)

# Print team info to verify conference and division
print("\n===== TEAM INFORMATION FROM get_game_details =====")
for i, team in enumerate(game_details.get("teams", [])):
    print(f"\nTeam {i+1}:")
    print(f"Name: {team.get('name')}")
    print(f"Conference: {team.get('conference')}")
    print(f"Division: {team.get('division')}")
    print(f"Linescores: {team.get('linescores')}")

# Now test if these fields make it to the final dataframe
# Mock the process_game_data function with our loaded data
game_id = game_data.get('gameId', 'unknown')
if 'header' in game_data and 'id' in game_data['header']:
    game_id = game_data['header']['id']

# Mock season for testing
season = 2023


# Create a fake function to simulate loading the game data
def mock_load_json(path):
    return game_data


# Store the original function to restore later
import espn_data.processor

original_load_json = espn_data.processor.load_json

# Replace with our mock
espn_data.processor.load_json = mock_load_json

try:
    # Process the game data
    result = process_game_data(game_id, season)

    # Check if the teams_info dataframe has our columns
    if "data" in result and "teams_info" in result["data"]:
        teams_df = result["data"]["teams_info"]

        print("\n===== TEAMS DATAFRAME COLUMNS =====")
        print(teams_df.columns.tolist())

        print("\n===== TEAMS DATAFRAME CONTENT =====")
        print(teams_df[["team_name", "conference", "division", "linescores"]].to_string())
    else:
        print("No teams_info dataframe found in result")
finally:
    # Restore the original function
    espn_data.processor.load_json = original_load_json
