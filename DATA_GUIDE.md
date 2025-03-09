# ESPN Women's Basketball Data Access Guide

This guide shows how to properly access data elements in the ESPN API responses.

## Game Data Structure

When you get game data from the ESPN API using `get_game_data(game_id)`, the data is structured as follows:

### Game Date and Time

The game date is stored in the `header.competitions[0].date` path:

```python
game_date = None
if 'header' in game_data and 'competitions' in game_data['header'] and game_data['header']['competitions']:
    game_date = game_data['header']['competitions'][0].get('date')
```

### Venue Information

Venue information is stored in the `gameInfo.venue` path:

```python
venue_name = None
venue_location = None
if 'gameInfo' in game_data and 'venue' in game_data['gameInfo']:
    venue = game_data['gameInfo']['venue']
    venue_name = venue.get('fullName')

    # Get venue location
    if 'address' in venue:
        city = venue['address'].get('city', '')
        state = venue['address'].get('state', '')
        venue_location = f"{city}, {state}"
```

### Attendance

Attendance information is in `gameInfo.attendance`:

```python
attendance = None
if 'gameInfo' in game_data and 'attendance' in game_data['gameInfo']:
    attendance = game_data['gameInfo']['attendance']
```

### Team and Score Information

Team information is in the `header.competitions[0].competitors` array:

```python
if 'header' in game_data and 'competitions' in game_data['header'] and game_data['header']['competitions']:
    competition = game_data['header']['competitions'][0]
    if 'competitors' in competition:
        for team in competition['competitors']:
            team_name = team.get('team', {}).get('displayName', 'Unknown')
            score = team.get('score', 'N/A')
            home_away = team.get('homeAway', 'N/A')
            winner = team.get('winner', False)
```

### Box Score Information

Box score information is in the `boxscore` section:

```python
if 'boxscore' in game_data and 'players' in game_data['boxscore']:
    for team in game_data['boxscore']['players']:
        team_id = team.get('team', {}).get('id', '')
        team_name = team.get('team', {}).get('name', '')

        if 'statistics' in team:
            for stat_group in team['statistics']:
                stat_name = stat_group.get('name', '')

                if 'athletes' in stat_group:
                    for player in stat_group['athletes']:
                        player_id = player.get('athlete', {}).get('id', '')
                        player_name = player.get('athlete', {}).get('displayName', '')
                        # ... access other player stats
```

### Play-by-Play Information

Play-by-play information is in the `plays` array:

```python
if 'plays' in game_data:
    for play in game_data['plays']:
        play_id = play.get('id', '')
        period = play.get('period', {}).get('number', '')
        clock = play.get('clock', {}).get('displayValue', '')
        text = play.get('text', '')
        # ... access other play details
```

## Helper Functions

The `espn_data.processor` module provides a `get_game_details` utility function to easily extract key information:

```python
from espn_data.processor import get_game_details

game_data = get_game_data(game_id)
details = get_game_details(game_data)

print(f"Game date: {details['date']}")
print(f"Venue: {details['venue_name']} {details['venue_location'] or ''}")
print(f"Attendance: {details['attendance']}")
```

## Correcting Notebook Examples

If you encounter issues with missing data in the Jupyter notebook examples, make sure you're accessing data using the correct paths as shown in this guide.
