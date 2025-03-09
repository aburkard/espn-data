#!/usr/bin/env python3
"""
Script to fix the game data extraction code in the notebook.
"""

import json
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("notebook_fix")


def fix_notebook_game_extraction():
    """Fix the game data extraction code in the notebook."""
    try:
        # Load the notebook
        with open('notebook.ipynb', 'r') as f:
            notebook = json.load(f)

        # Find the cell with the game data extraction code
        for cell in notebook['cells']:
            if cell['cell_type'] == 'code':
                source = cell['source']
                if any('# Extract basic game info' in line for line in source):
                    # This is the cell we want to fix
                    print("Found the cell to fix!")

                    # Create the fixed code
                    fixed_code = [
                        "# Get a game ID from the schedule\n", "game_id = schedule[0]['id']\n",
                        "print(f\"Getting data for game {game_id}: {schedule[0]['name']}\")\n", "\n",
                        "# Fetch game data\n", "game_data = get_game_data(game_id)\n", "\n",
                        "# Use the helper function for cleaner code\n",
                        "from espn_data.processor import get_game_details\n", "\n", "# Extract basic game info\n",
                        "if 'header' in game_data:\n", "    header = game_data['header']\n", "    \n",
                        "    # Get game details using helper function\n", "    details = get_game_details(game_data)\n",
                        "    \n", "    # Display game information\n", "    print(f\"Game date: {details['date']}\")\n",
                        "    \n", "    venue_str = details['venue_name']\n", "    if details['venue_location']:\n",
                        "        venue_str += f\" ({details['venue_location']})\"\n",
                        "    print(f\"Venue: {venue_str}\")\n", "    \n", "    if details['attendance'] is not None:\n",
                        "        print(f\"Attendance: {details['attendance']}\")\n", "    \n",
                        "    # Show competitors\n", "    if 'competitions' in header and header['competitions']:\n",
                        "        competition = header['competitions'][0]\n",
                        "        if 'competitors' in competition:\n", "            print(\"\\nCompetitors:\")\n",
                        "            for team in competition['competitors']:\n",
                        "                team_name = team.get('team', {}).get('displayName', 'Unknown')\n",
                        "                score = team.get('score', 'N/A')\n",
                        "                home_away = team.get('homeAway', 'N/A').upper()\n",
                        "                winner = \"(WINNER)\" if team.get('winner', False) else \"\"\n",
                        "                print(f\"  {team_name} ({home_away}): {score} {winner}\")\n"
                    ]

                    # Alternate implementation without helper function if you prefer
                    fixed_code_alt = [
                        "# Get a game ID from the schedule\n", "game_id = schedule[0]['id']\n",
                        "print(f\"Getting data for game {game_id}: {schedule[0]['name']}\")\n", "\n",
                        "# Fetch game data\n", "game_data = get_game_data(game_id)\n", "\n",
                        "# Extract basic game info\n", "if 'header' in game_data:\n",
                        "    header = game_data['header']\n", "    \n", "    # Get date from competitions array\n",
                        "    game_date = None\n", "    if 'competitions' in header and header['competitions']:\n",
                        "        game_date = header['competitions'][0].get('date')\n",
                        "    print(f\"Game date: {game_date}\")\n", "    \n", "    # Get venue from gameInfo object\n",
                        "    venue_name = None\n", "    venue_city = None\n", "    venue_state = None\n",
                        "    if 'gameInfo' in game_data and 'venue' in game_data['gameInfo']:\n",
                        "        venue = game_data['gameInfo']['venue']\n",
                        "        venue_name = venue.get('fullName')\n", "        if 'address' in venue:\n",
                        "            venue_city = venue['address'].get('city')\n",
                        "            venue_state = venue['address'].get('state')\n", "    \n", "    if venue_name:\n",
                        "        location = f\"{venue_city}, {venue_state}\" if venue_city and venue_state else \"\"\n",
                        "        print(f\"Venue: {venue_name} {location}\")\n", "    else:\n",
                        "        print(\"Venue: Not available\")\n", "    \n", "    # Show attendance if available\n",
                        "    if 'gameInfo' in game_data and 'attendance' in game_data['gameInfo']:\n",
                        "        attendance = game_data['gameInfo']['attendance']\n",
                        "        print(f\"Attendance: {attendance}\")\n", "    \n", "    # Show competitors\n",
                        "    if 'competitions' in header and header['competitions']:\n",
                        "        competition = header['competitions'][0]\n",
                        "        if 'competitors' in competition:\n", "            print(\"\\nCompetitors:\")\n",
                        "            for team in competition['competitors']:\n",
                        "                team_name = team.get('team', {}).get('displayName', 'Unknown')\n",
                        "                score = team.get('score', 'N/A')\n",
                        "                home_away = team.get('homeAway', 'N/A').upper()\n",
                        "                winner = \"(WINNER)\" if team.get('winner', False) else \"\"\n",
                        "                print(f\"  {team_name} ({home_away}): {score} {winner}\")\n"
                    ]

                    # Replace the cell source with fixed code
                    cell['source'] = fixed_code_alt
                    break

        # Write the updated notebook
        with open('notebook_fixed.ipynb', 'w') as f:
            json.dump(notebook, f, indent=2)

        print("Fixed notebook saved as notebook_fixed.ipynb")
        print("To use it, rename it to notebook.ipynb or open it directly")
        return True

    except Exception as e:
        print(f"Error fixing notebook: {e}")
        return False


def fix_notebook():
    """Fix the notebook.ipynb file to include information about the get_game_details fix."""
    try:
        # Read the fixed notebook
        with open('notebook_fixed.ipynb', 'r') as f:
            notebook = json.load(f)

        # Find the Example 4 markdown cell
        for i, cell in enumerate(notebook['cells']):
            if cell['cell_type'] == 'markdown' and 'source' in cell:
                source = ''.join(cell['source'])
                if "## Example 4: Process Game Data" in source:
                    # Update the markdown to mention the fix
                    new_source = [
                        "## Example 4: Process Game Data\n", "\n",
                        "Let's process the game data into structured formats. The processor has been improved to correctly extract team information from the game data, ensuring that player and team statistics are properly processed.\n"
                    ]
                    notebook['cells'][i]['source'] = new_source
                    logger.info("Updated Example 4 markdown cell")
                    break

        # Save the updated notebook to the original file
        with open('notebook.ipynb', 'w') as f:
            json.dump(notebook, f, indent=2)

        logger.info("Notebook updated successfully")
        return True

    except Exception as e:
        logger.error(f"Error updating notebook: {e}")
        return False


if __name__ == "__main__":
    success1 = fix_notebook_game_extraction()
    success2 = fix_notebook()
    sys.exit(0 if (success1 and success2 is not None) else 1)
