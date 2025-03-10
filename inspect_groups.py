import json
from pprint import pprint

# Load the example game data
with open('example_data/game.json', 'r') as f:
    game_data = json.load(f)

# Check if the groups structure exists
if ('header' in game_data and 'competitions' in game_data['header'] and len(game_data['header']['competitions']) > 0 and
        'competitors' in game_data['header']['competitions'][0] and
        len(game_data['header']['competitions'][0]['competitors']) > 0):

    # Get the first team
    team1 = game_data['header']['competitions'][0]['competitors'][0]
    print("\nTeam 1 structure:")
    print(f"Team ID: {team1.get('team', {}).get('id')}")
    print(f"Team Name: {team1.get('team', {}).get('displayName')}")

    # Check if groups field exists
    if 'team' in team1 and 'groups' in team1['team']:
        print("\nGroups field found:")
        groups = team1['team']['groups']
        pprint(groups)

        # Check for conference information
        if isinstance(groups, dict):
            print("\nConference information from groups dictionary:")
            print(f"Is Conference: {groups.get('isConference')}")
            print(f"Conference ID: {groups.get('id')}")
            print(f"Conference Slug: {groups.get('slug')}")

            # Try to derive the conference name from the slug
            if 'slug' in groups:
                slug = groups.get('slug')
                derived_name = ' '.join(word.capitalize() for word in slug.split('-'))
                print(f"Derived Conference Name: {derived_name}")

    else:
        print("\nGroups field not found in team data")
        print("Keys in team:", team1['team'].keys())

    # Check linescores field
    if 'linescores' in team1:
        print("\nLinescores field found:")
        pprint(team1['linescores'])
    else:
        print("\nLinescores field not found")

    # Check conference name alternatives
    print("\nAlternative conference sources:")
    if 'conferenceId' in team1.get('team', {}):
        print(f"Team conferenceId: {team1['team']['conferenceId']}")
    if 'conference' in team1.get('team', {}):
        print(f"Team conference: {team1['team']['conference']}")
    if 'conferenceCompetition' in game_data['header']['competitions'][0]:
        print(f"Competition conferenceCompetition: {game_data['header']['competitions'][0]['conferenceCompetition']}")
else:
    print("Required structure not found in game data")
