"""Process and transform ESPN data into structured formats."""

import os
import json
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from datetime import datetime, timezone
import argparse

from espn_data.utils import (load_json, save_json, get_teams_file, get_schedules_dir, get_games_dir, get_processed_dir,
                             get_csv_dir, get_parquet_dir, get_csv_teams_file, get_parquet_teams_file,
                             get_csv_season_dir, get_parquet_season_dir, get_csv_games_dir, get_parquet_games_dir,
                             set_gender, get_current_gender)
from espn_data.scraper import get_game_data, DEFAULT_SEASONS

logger = logging.getLogger("espn_data")

# Create base directories
BASE_DIR = Path(__file__).parent.parent  # Go up one level to workspace root
DATA_DIR = BASE_DIR / "data"

# Ensure base directories exist
os.makedirs(DATA_DIR, exist_ok=True)
# The rest of the directories will be created as needed in the specific functions


def process_teams_data(force: bool = False) -> pd.DataFrame:
    """
    Process teams data into a structured dataframe.
    
    Args:
        force: If True, force reprocessing even if processed files exist
        
    Returns:
        DataFrame with teams information
    """
    # Check if processed teams data exists
    csv_teams_file = get_csv_teams_file()
    if not force and csv_teams_file.exists():
        logger.info("Using cached processed teams data")
        return pd.read_csv(csv_teams_file)

    logger.info("Processing teams data")

    # Get the teams data file
    teams_file = get_teams_file()

    if not teams_file.exists():
        logger.warning("Teams data file not found")
        return pd.DataFrame()

    teams_data = load_json(teams_file)

    if not teams_data:
        logger.warning("No teams data found")
        return pd.DataFrame()

    # Extract relevant team info
    teams = []
    for team in teams_data:
        team_info = {
            "id": team.get("id", None),
            "slug": team.get("slug", None),
            "abbreviation": team.get("abbreviation", None),
            "display_name": team.get("displayName", None),
            "short_name": team.get("shortDisplayName", None),
            "name": team.get("name", None),
            "nickname": team.get("nickname", None),
            "location": team.get("location", None),
            "color": team.get("color", None),
            "alternate_color": team.get("alternateColor", None),
            "logo": team.get("logos", [{}])[0].get("href", None) if "logos" in team and team["logos"] else None,
            "conference_id": team.get("conference", {}).get("id", None) if "conference" in team else None,
            "conference_name": team.get("conference", {}).get("name", None) if "conference" in team else None,
        }
        teams.append(team_info)

    # Convert to DataFrame
    teams_df = pd.DataFrame(teams)

    if not teams_df.empty:
        # Optimize datatypes
        teams_df = optimize_dataframe_dtypes(teams_df, "teams")

        # Save to CSV and Parquet at the top level
        os.makedirs(csv_teams_file.parent, exist_ok=True)
        teams_df.to_csv(csv_teams_file, index=False)
        teams_df.to_parquet(get_parquet_teams_file(), index=False)
        logger.info(f"Processed {len(teams_df)} teams")
    else:
        logger.warning("No teams data to save")

    return teams_df


def convert_clock_to_seconds(clock_str):
    """Convert a clock string (MM:SS) to seconds."""
    if not clock_str or not isinstance(clock_str, str):
        return None

    try:
        # Handle formats like "10:00"
        parts = clock_str.split(':')
        if len(parts) == 2:
            minutes, seconds = parts
            return int(minutes) * 60 + int(seconds)
        return None
    except (ValueError, TypeError):
        return None


def get_broadcasts(game_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get broadcasts from game data."""
    return game_data.get('broadcasts', []) or game_data.get('header', {}).get('competitions', [{}])[0].get(
        'broadcasts', []) or game_data.get('gameInfo', {}).get('broadcasts', [])


def get_primary_broadcast(game_data: Dict[str, Any]) -> Dict[str, Any]:
    """Get the primary broadcast from game data."""
    broadcasts = get_broadcasts(game_data)

    # If no broadcasts, return None
    if not broadcasts:
        return None

    # 1. A broadcast with type TV and market National
    for broadcast in broadcasts:
        market_type = broadcast.get('market', {}).get('type', '').lower()
        type_name = broadcast.get('type', {}).get('shortName', '').lower()
        if type_name == 'tv' and market_type == 'national':
            return broadcast

    # 2. Any TV broadcast
    for broadcast in broadcasts:
        type_name = broadcast.get('type', {}).get('shortName', '').lower()
        if type_name == 'tv':
            return broadcast

    # 3. Any National broadcast
    for broadcast in broadcasts:
        market_type = broadcast.get('market', {}).get('type', '').lower()
        if market_type == 'national':
            return broadcast

    # 4. The first broadcast in the array
    return broadcasts[0]


def get_game_details(game_data: Dict[str, Any], filename: str = None) -> Dict[str, Any]:
    """
    Extract key game details like date, venue, etc. from game data.
    
    Args:
        game_data: Raw game data from the API
        filename: Optional filename the data came from, used as fallback for game_id
        
    Returns:
        Dictionary with extracted details
    """
    # Try to get the game ID from various possible places
    game_id = 'unknown'

    # First try the 'gameId' field
    if 'gameId' in game_data:
        game_id = game_data['gameId']
    # Then try header.id
    elif 'header' in game_data and 'id' in game_data['header']:
        game_id = game_data['header']['id']
    # Then try header.competitions[0].id if available
    elif ('header' in game_data and 'competitions' in game_data['header'] and
          isinstance(game_data['header']['competitions'], list) and len(game_data['header']['competitions']) > 0 and
          'id' in game_data['header']['competitions'][0]):
        game_id = game_data['header']['competitions'][0]['id']
    # If we still don't have a valid game_id but we have a filename, try to extract from there
    elif filename:
        # Try to extract game_id from filename
        basename = os.path.basename(filename)
        if basename.endswith('.json'):
            potential_id = os.path.splitext(basename)[0]
            if potential_id and potential_id != 'unknown':
                game_id = potential_id
                logger.debug(f"Extracted game_id {game_id} from filename {filename}")

    logger.debug(f"Game {game_id}: Extracting game details")

    # Initialize the game_details dictionary
    game_details = {
        "game_id": game_id,
        "date": None,
        "venue_id": None,
        "venue_name": None,
        "venue_city": None,
        "venue_state": None,
        "attendance": None,
        "status": None,
        "neutral_site": None,
        "format": None,
        "completed": None,
        "broadcast": None,
        "broadcast_market": None,
        "broadcast_type": None,
        "conference": None,
        "teams": []
    }

    # Extract season information
    if 'season' in game_data and isinstance(game_data['season'], dict):
        game_details["season"] = game_data['season'].get('year', None)
    elif 'season' in game_data.get('header', {}) and isinstance(game_data['header'], dict):
        game_details["season"] = game_data['header'].get('season', {}).get('year', None)

    # Extract boxscore availability information
    competitions = game_data.get('header', {}).get('competitions', [{}])
    competition = (competitions or [{}])[0]
    group = competition.get('groups', {})  # This is correct, groups has a dict
    game_details["boxscore_source"] = competition.get('boxscoreSource')
    game_details["boxscore_available"] = competition.get('boxscoreAvailable')
    game_details["play_by_play_source"] = competition.get('playByPlaySource')
    game_details["is_conference_game"] = competition.get('conferenceCompetition')
    game_details["conference_id"] = group.get('id')
    game_details["conference_name"] = group.get('name')
    game_details["conference_abbreviation"] = group.get('abbreviation')

    # Extract date information
    if 'date' in game_data:
        game_details["date"] = game_data['date']

    # Extract venue information
    venue_data = game_data.get('gameInfo', {}).get('venue', {})
    if venue_data:
        game_details["venue_id"] = venue_data.get('id', None)
        game_details["venue_name"] = venue_data.get('fullName', None)
        address = venue_data.get('address', {})
        game_details["venue_city"] = address.get('city', None)
        game_details["venue_state"] = address.get('state', None)

    # Extract attendance information
    if 'gameInfo' in game_data and 'attendance' in game_data['gameInfo']:
        game_details["attendance"] = game_data['gameInfo']['attendance']
    elif 'attendance' in game_data:
        game_details["attendance"] = game_data['attendance']
    elif 'boxscore' in game_data and 'attendance' in game_data['boxscore']:
        game_details["attendance"] = game_data['boxscore']['attendance']
    elif 'header' in game_data and 'competitions' in game_data['header'] and isinstance(
            game_data['header']['competitions'], list) and len(game_data['header']['competitions']) > 0:
        game_details["attendance"] = game_data['header']['competitions'][0].get('attendance')

    # Extract game status information
    status_type = None
    if 'header' in game_data and 'competitions' in game_data['header']:
        competitions = game_data['header']['competitions']
        if competitions and isinstance(competitions, list) and 'status' in competitions[0]:
            status_type = competitions[0]['status'].get('type', {})
    elif 'status' in game_data:
        status_type = game_data['status'].get('type', {})

    if status_type and isinstance(status_type, dict):
        game_details["status"] = status_type.get('name', None)
        game_details["completed"] = status_type.get('completed', False)
    else:
        game_details["status"] = None
        game_details["completed"] = False

    # Extract neutral site information
    if 'header' in game_data and 'competitions' in game_data['header'] and isinstance(
            game_data['header']['competitions'], list) and len(game_data['header']['competitions']) > 0:
        competition = game_data['header']['competitions'][0]
        game_details["neutral_site"] = competition.get('neutralSite', False)

    # Extract game format information
    if 'format' in game_data:
        game_details["format"] = game_data['format']
    elif 'header' in game_data and 'competitions' in game_data['header'] and isinstance(
            game_data['header']['competitions'], list) and len(game_data['header']['competitions']) > 0:
        competition = game_data['header']['competitions'][0]
        if 'format' in competition:
            game_details["format"] = competition['format']

    # Extract broadcast information
    broadcast = get_primary_broadcast(game_data)
    if broadcast:
        game_details["broadcast"] = broadcast.get('media', {}).get('shortName', None)
        game_details["broadcast_market"] = broadcast.get('market', {}).get('type', None)
        game_details["broadcast_type"] = broadcast.get('type', {}).get('shortName', None)

    # Extract team information
    if 'header' in game_data and 'competitions' in game_data['header'] and isinstance(
            game_data['header']['competitions'], list) and len(game_data['header']['competitions']) > 0:
        competition = game_data['header']['competitions'][0]
        if 'competitors' in competition and isinstance(competition['competitors'], list):
            for team in competition['competitors']:
                if isinstance(team, dict) and 'team' in team and isinstance(team['team'], dict):
                    team_info = {
                        "id": team['team'].get('id', None),
                        "name": team['team'].get('displayName', None),
                        "abbreviation": team['team'].get('abbreviation', None),
                        "location": team['team'].get('location', None),
                        "nickname": team['team'].get('name', None),
                        "color": team['team'].get('color', None),
                        "home_away": team.get('homeAway', None),
                        "score": team.get('score', None),
                        "winner": team.get('winner', False),
                        "groups_slug": team.get('team', {}).get("groups", {}).get("slug", None),
                    }

                    # Add linescores if available
                    if 'linescores' in team and isinstance(team['linescores'], list):
                        team_info['linescores'] = [
                            line.get('displayValue') for line in team['linescores'] if isinstance(line, dict)
                        ]

                    game_details["teams"].append(team_info)

    # Create team lookup map for play-by-play processing
    team_lookup = {}
    for team in game_details["teams"]:
        team_lookup[team["id"]] = {"name": team["name"], "abbreviation": team["abbreviation"]}

    # Extract play_by_play data and fill in empty team names
    if 'plays' in game_data and isinstance(game_data['plays'], list):
        # Process play-by-play data to fill in empty team names
        for play in game_data['plays']:
            if isinstance(play, dict) and 'team' in play:
                # Check if team is a dictionary
                if isinstance(play['team'], dict):
                    team_id = play['team'].get('id', None)
                    if team_id and not play['team'].get('name') and team_id in team_lookup:
                        # Fill empty team name from our lookup
                        play['team']['name'] = team_lookup[team_id]['name']
                # If team is a string, we might need to handle it differently
                # For now, we'll just log it
                elif isinstance(play['team'], str):
                    logger.debug(f"Game {game_id}: Found play with team as string: {play['team']}")

            # Try to fill in player names from athlete info if available
            for player_num in [1, 2]:
                player_key = f'athlete{player_num}'
                if player_key in play and isinstance(play[player_key], dict):
                    player_id = play[player_key].get('id', None)
                    if player_id and not play[player_key].get('displayName'):
                        # Look for this player in raw data
                        if 'boxscore' in game_data and 'players' in game_data['boxscore']:
                            for team_players in game_data['boxscore']['players']:
                                if 'statistics' in team_players and isinstance(team_players['statistics'], list):
                                    for stat_group in team_players['statistics']:
                                        if 'athletes' in stat_group and isinstance(stat_group['athletes'], list):
                                            for player in stat_group['athletes']:
                                                if 'athlete' in player and isinstance(
                                                        player['athlete'],
                                                        dict) and player['athlete'].get('id') == player_id:
                                                    play[player_key]['displayName'] = player['athlete'].get(
                                                        'displayName', '')
                                                    break

    return game_details


def process_game_data(game_id: str, season: int, verbose: bool = False) -> Dict[str, Any]:
    """
    Process detailed data for a game.
    
    Args:
        game_id: ESPN game ID
        season: Season year
        verbose: If True, log detailed information about the game processing
        
    Returns:
        Dictionary with processing results
    """
    logger.debug(f"Processing game {game_id} for season {season}")
    result = {"game_id": game_id, "season": season, "processed": False, "error": None}

    # Ensure we're still using the correct gender
    current_gender = get_current_gender()
    logger.debug(f"Using gender: {current_gender}")

    try:
        # Get raw game data
        data_path = get_games_dir(season) / f"{game_id}.json"
        if not data_path.exists():
            logger.warning(f"Game data for {game_id} in season {season} not found. Fetching it now.")
            # Don't pass gender here, as it will call set_gender and override our setting
            game_data = get_game_data(game_id, season, verbose_cache=False)
        else:
            game_data = load_json(data_path)

        logger.debug(f"Game {game_id}: Loaded raw data, checking structure...")

        if game_data is None:
            logger.error(f"Game {game_id}: Raw data is None")
            return {"game_id": game_id, "season": season, "processed": False, "error": "Raw data is None"}

        # Log top-level keys for debugging
        top_keys = list(game_data.keys()) if isinstance(game_data, dict) else "Not a dictionary"
        logger.debug(f"Game {game_id}: Top-level keys: {top_keys}")

        # Initialize our data structures
        game_info = {}
        teams_info = []
        player_stats = []
        team_stats = []
        play_by_play = []
        officials_data = []  # For referee data
        broadcasts_data = []  # For broadcast data

        # 1. Extract game info
        if isinstance(game_data, dict):
            logger.debug(f"Game {game_id}: Extracting game details")
            game_details = get_game_details(game_data, data_path)

            logger.debug(f"Game {game_id}: Game details extracted, building game_info")

            game_info = {
                "game_id": game_id,
                "date": game_details["date"],
                "venue_id": game_details["venue_id"],
                "venue_name": game_details["venue_name"],
                "venue_city": game_details["venue_city"],
                "venue_state": game_details["venue_state"],
                "attendance": game_details["attendance"],
                "status":
                    (game_details["status"] if isinstance(game_details.get("status"), str) else
                     (game_details.get("status", {}).get("description", None) or game_details.get("status", {}).get(
                         "short_detail", None) or game_details.get("status", {}).get("name", None)) if isinstance(
                             game_details.get("status"), dict) else None),
                "neutral_site": game_details["neutral_site"],
                "completed": game_details["completed"],
                "broadcast": game_details["broadcast"],
                "broadcast_market": game_details["broadcast_market"],
                "broadcast_type": game_details["broadcast_type"],
                "regulation_clock": game_details.get("regulation_clock", 600.0),
                "overtime_clock": game_details.get("overtime_clock", 300.0),
                "period_name": game_details.get("period_name", "Quarter"),
                "num_periods": game_details.get("num_periods", 4),
                "boxscore_source": game_details["boxscore_source"],
                "boxscore_available": game_details["boxscore_available"],
                "play_by_play_source": game_details["play_by_play_source"],
                "is_conference_game": game_details["is_conference_game"],
                "conference_id": game_details["conference_id"],
                "conference_name": game_details["conference_name"],
                "conference_abbreviation": game_details["conference_abbreviation"]
            }

            logger.debug(f"Game {game_id}: Game info built successfully")

            # Process officials/referees
            logger.debug(f"Game {game_id}: Processing officials data")
            for official in game_data.get("gameInfo", {}).get("officials", []):
                if official is None:
                    logger.warning(f"Game {game_id}: Found None official entry")
                    continue

                official_data = {
                    "game_id": game_id,
                    "name": official.get("fullName", None),
                    "display_name": official.get("display_name", None),
                    "position": official.get("position", {}).get("name", None),
                    "position_id": official.get("position", {}).get("id", None),
                    "order": official.get("order", 0)
                }
                officials_data.append(official_data)

            # Process broadcasts data
            logger.debug(f"Game {game_id}: Processing broadcast data")
            for broadcast in get_broadcasts(game_data):
                if broadcast is None:
                    logger.warning(f"Game {game_id}: Found None broadcast entry")
                    continue

                broadcast_data = {
                    "game_id": game_id,
                    "type": broadcast.get("type", {}).get("shortName", None),
                    "market": broadcast.get("market", {}).get("type", None),
                    "media": broadcast.get("media", {}).get("shortName", None),
                    "lang": broadcast.get("lang", None),
                    "region": broadcast.get("region", None)
                }
                broadcasts_data.append(broadcast_data)

            # 2. Extract team information
            logger.debug(f"Game {game_id}: Processing team information")
            for team in game_details.get("teams", []):
                if team is None:
                    logger.warning(f"Game {game_id}: Found None team entry")
                    continue

                team_info = {
                    "game_id": game_id,
                    "team_id": team.get("id", None),
                    "team_name": team.get("name", None),
                    "team_abbreviation": team.get("abbreviation", None),
                    "team_location": team.get("location", None),
                    "team_nickname": team.get("nickname", None),
                    "team_color": team.get("color", None),
                    "home_away": team.get("home_away", None),
                    "score": team.get("score", None),
                    "winner": team.get("winner", False),
                    "groups_slug": team.get("groups_slug", None),
                }

                # Add linescores if available
                if 'linescores' in team and isinstance(team['linescores'], list):
                    # Convert to string for storage in dataframe
                    team_info["linescores"] = ','.join(str(score) for score in team['linescores'])

                teams_info.append(team_info)

            # 3. Extract player statistics
            logger.debug(f"Game {game_id}: Extracting player statistics")
            if game_data.get('boxscore') and game_data['boxscore'].get('players'):
                logger.debug(f"Game {game_id}: Found boxscore.players")
                for team_data in game_data['boxscore']['players']:
                    if not isinstance(team_data, dict):
                        logger.warning(f"Game {game_id}: team_data is not a dictionary")
                        continue

                    team_id = None
                    team_name = None
                    team_abbrev = None

                    if team_data.get('team') and isinstance(team_data['team'], dict):
                        team_id = team_data['team'].get('id', None)
                        team_name = team_data['team'].get('displayName', None)
                        team_abbrev = team_data['team'].get('abbreviation', None)
                    else:
                        logger.warning(f"Game {game_id}: team is None or not a dictionary")

                    # Process each statistic group
                    if team_data.get('statistics'):
                        logger.debug(f"Game {game_id}: Processing team statistics for {team_name}")
                        for stat in team_data['statistics']:
                            if not isinstance(stat, dict):
                                logger.warning(f"Game {game_id}: stat is not a dictionary")
                                continue

                            # Get stat keys and labels
                            stat_keys = stat.get('keys', [])
                            stat_labels = stat.get('names', []) or stat.get('labels', [])

                            if not stat_keys:
                                logger.warning(f"Game {game_id}: Empty stat_keys")

                            # Process each player
                            if stat.get('athletes') and isinstance(stat['athletes'], list):
                                for athlete in stat['athletes']:
                                    if not isinstance(athlete, dict):
                                        logger.warning(f"Game {game_id}: athlete is not a dictionary")
                                        continue

                                    # Get player info
                                    player_id = None
                                    player_name = None
                                    player_position = None
                                    player_jersey = None
                                    starter = False
                                    dnp = False

                                    # Extract player info
                                    if athlete.get('athlete') and isinstance(athlete['athlete'], dict):
                                        player_id = athlete['athlete'].get('id', None)
                                        player_name = athlete['athlete'].get('displayName', None)

                                        if athlete['athlete'].get('position') and isinstance(
                                                athlete['athlete']['position'], dict):
                                            player_position = athlete['athlete']['position'].get('abbreviation', None)

                                        player_jersey = athlete['athlete'].get('jersey', None)
                                    else:
                                        logger.warning(f"Game {game_id}: athlete.athlete is None or not a dictionary")

                                    # Check starter and DNP status
                                    if athlete.get('starter'):
                                        starter = bool(athlete['starter'])

                                    if athlete.get('didNotPlay'):
                                        dnp = bool(athlete['didNotPlay'])

                                    # Collect player stats
                                    player_record = {
                                        "game_id": game_id,
                                        "team_id": team_id,
                                        "team_name": team_name,
                                        "team_abbrev": team_abbrev,
                                        "player_id": player_id,
                                        "player_name": player_name,
                                        "position": player_position,
                                        "jersey": player_jersey,
                                        "starter": starter,
                                        "dnp": dnp
                                    }

                                    # Add stats
                                    stat_values = athlete.get('stats', [])
                                    for i, key in enumerate(stat_keys):
                                        if i < len(stat_values):
                                            # Use labels instead of keys for column names
                                            if i < len(stat_labels):
                                                column_name = stat_labels[i]
                                                player_record[column_name] = stat_values[i]
                                            else:
                                                player_record[key] = stat_values[i]

                                    # Process combined stats like FG, 3PT, FT
                                    for column_name in ['FG', '3PT', 'FT']:
                                        if column_name in player_record and isinstance(
                                                player_record[column_name], str) and '-' in player_record[column_name]:
                                            try:
                                                made, attempted = player_record[column_name].split('-')
                                                player_record[f"{column_name}_MADE"] = int(made)
                                                player_record[f"{column_name}_ATT"] = int(attempted)

                                                # Calculate percentage
                                                try:
                                                    pct = round(
                                                        int(made) / int(attempted) * 100 if int(attempted) > 0 else 0,
                                                        1)
                                                    player_record[f"{column_name}_PCT"] = pct
                                                except (ValueError, ZeroDivisionError):
                                                    player_record[f"{column_name}_PCT"] = 0
                                            except (ValueError, TypeError):
                                                # For invalid formats, set values to NaN
                                                player_record[f"{column_name}_MADE"] = np.nan
                                                player_record[f"{column_name}_ATT"] = np.nan
                                                player_record[f"{column_name}_PCT"] = np.nan
                                        elif dnp:
                                            # For DNP players, explicitly set stats to NaN
                                            player_record[f"{column_name}_MADE"] = np.nan
                                            player_record[f"{column_name}_ATT"] = np.nan
                                            player_record[f"{column_name}_PCT"] = np.nan

                                    # Standardize column names to match team stats
                                    rename_map = {
                                        'minutes': 'MIN',
                                        'offensiveRebounds': 'OREB',
                                        'defensiveRebounds': 'DREB',
                                        'rebounds': 'REB',
                                        'assists': 'AST',
                                        'steals': 'STL',
                                        'blocks': 'BLK',
                                        'turnovers': 'TO',
                                        'fouls': 'PF',
                                        'points': 'PTS'
                                    }

                                    # Apply the renaming to standardize to team stat format
                                    for old_name, new_name in rename_map.items():
                                        if old_name in player_record:
                                            player_record[new_name] = player_record.pop(old_name)

                                    # For DNP players, make sure all stat fields are explicitly set to NaN
                                    if dnp:
                                        stat_fields = [
                                            'MIN', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS'
                                        ]
                                        for field in stat_fields:
                                            player_record[field] = np.nan

                                    player_stats.append(player_record)
                            else:
                                # It's way too common for a game to be missing player box score stats to be a warning
                                logger.debug(f"Game {game_id}: No athletes or not a list in stat_group")
                    else:
                        logger.warning(f"Game {game_id}: No statistics in team_data")

            # 4. Extract team box score statistics
            if 'boxscore' in game_data and 'teams' in game_data['boxscore']:
                for team_data in game_data['boxscore']['teams']:
                    if not isinstance(team_data, dict):
                        continue

                    team_id = None
                    team_name = None
                    team_abbrev = None
                    home_away = None

                    if 'team' in team_data and isinstance(team_data['team'], dict):
                        team_id = team_data['team'].get('id', None)
                        team_name = team_data['team'].get('displayName', None)
                        team_abbrev = team_data['team'].get('abbreviation', None)

                    home_away = team_data.get('homeAway', None)

                    # Create basic team record
                    team_record = {
                        "game_id": game_id,
                        "team_id": team_id,
                        "team_name": team_name,
                        "team_abbreviation": team_abbrev,
                        "home_away": home_away,
                    }

                    # Add the points from the team's score in game details
                    for team_info in game_details.get("teams", []):
                        if team_info.get("id") == team_id:
                            try:
                                team_record["PTS"] = int(team_info.get("score", 0))
                            except (ValueError, TypeError):
                                team_record["PTS"] = 0
                            break

                    # Process statistics
                    if 'statistics' in team_data and isinstance(team_data['statistics'], list):
                        for stat in team_data['statistics']:
                            if not isinstance(stat, dict):
                                continue

                            # Get the values we need
                            display_value = stat.get('displayValue', None)

                            # Directly use abbreviation if available, fall back to label or name
                            column_name = stat.get('abbreviation', None) or stat.get('label', None) or stat.get(
                                'name', None)

                            # Skip if no column name or already processed
                            if not column_name or column_name in team_record:
                                continue

                            # Store the display value
                            team_record[column_name] = display_value

                            # Process combined stats like FG, 3PT, FT
                            if column_name in ['FG', '3PT', 'FT'] and '-' in display_value:
                                try:
                                    made, attempted = display_value.split('-')
                                    team_record[f"{column_name}_MADE"] = int(made)
                                    team_record[f"{column_name}_ATT"] = int(attempted)

                                    # Calculate percentage
                                    try:
                                        pct = round(int(made) / int(attempted) * 100 if int(attempted) > 0 else 0, 1)
                                        team_record[f"{column_name}_PCT"] = pct
                                    except (ValueError, ZeroDivisionError):
                                        team_record[f"{column_name}_PCT"] = 0
                                except (ValueError, TypeError):
                                    # For invalid formats, explicitly set as NaN
                                    team_record[f"{column_name}_MADE"] = np.nan
                                    team_record[f"{column_name}_ATT"] = np.nan
                                    team_record[f"{column_name}_PCT"] = np.nan

                            # Convert numeric values
                            elif display_value.replace('.', '', 1).isdigit():
                                try:
                                    if '.' in display_value:
                                        team_record[column_name] = float(display_value)
                                    else:
                                        team_record[column_name] = int(display_value)
                                except (ValueError, TypeError):
                                    # If conversion fails, keep as string
                                    pass
                            # Handle missing/empty values
                            elif not display_value or display_value.lower() in ['n/a', '-']:
                                team_record[column_name] = np.nan

                    # Post-process to fix just a few inconsistencies and remove duplicates
                    standardize_map = {
                        # Rebound standardization (since OR/DR vs OREB/DREB is inconsistent)
                        'OR': 'OREB',
                        'DR': 'DREB',
                        # Remove duplicate percentage columns
                        'FG%': None,
                        '3P%': None,
                        'FT%': None
                    }

                    # Convert verbose labels to standard abbreviations only when abbreviation is missing
                    if 'Rebounds' in team_record and 'REB' not in team_record:
                        team_record['REB'] = team_record.pop('Rebounds')
                    if 'Offensive Rebounds' in team_record and 'OREB' not in team_record:
                        team_record['OREB'] = team_record.pop('Offensive Rebounds')
                    if 'Defensive Rebounds' in team_record and 'DREB' not in team_record:
                        team_record['DREB'] = team_record.pop('Defensive Rebounds')
                    if 'Assists' in team_record and 'AST' not in team_record:
                        team_record['AST'] = team_record.pop('Assists')
                    if 'Steals' in team_record and 'STL' not in team_record:
                        team_record['STL'] = team_record.pop('Steals')
                    if 'Blocks' in team_record and 'BLK' not in team_record:
                        team_record['BLK'] = team_record.pop('Blocks')
                    if 'Turnovers' in team_record and 'TO' not in team_record:
                        team_record['TO'] = team_record.pop('Turnovers')
                    if 'Fouls' in team_record and 'PF' not in team_record:
                        team_record['PF'] = team_record.pop('Fouls')

                    # Apply the small standardization map for the few edge cases
                    for old_name, new_name in standardize_map.items():
                        if old_name in team_record:
                            if new_name:  # Rename column
                                team_record[new_name] = team_record[old_name]
                            # Always remove old name
                            team_record.pop(old_name)

                    team_stats.append(team_record)

            # 4. Extract play-by-play data
            if 'plays' in game_data:
                # Create a mapping of playId to win probability data if available
                win_prob_mapping = {}
                if 'winprobability' in game_data and isinstance(game_data['winprobability'], list):
                    for prob in game_data['winprobability']:
                        if isinstance(prob, dict) and 'playId' in prob:
                            win_prob_mapping[prob['playId']] = {
                                'home_win_percentage': prob.get('homeWinPercentage', None),
                                'tie_percentage': prob.get('tiePercentage', None)
                            }

                # Create team and player lookup maps for filling empty names
                team_lookup = {}
                player_lookup = {}

                # Populate team lookup from teams_info data
                for team_info in teams_info:
                    team_id = str(team_info.get("team_id", None) or "")
                    if team_id:
                        team_lookup[team_id] = {
                            "name": team_info.get("team_name", None),
                            "abbreviation": team_info.get("team_abbreviation", None)
                        }

                # Populate player lookup from player_stats data
                for player_stat in player_stats:
                    player_id = str(player_stat.get("player_id", None) or "")
                    if player_id:
                        player_lookup[player_id] = {"name": player_stat.get("player_name", None)}

                for play in game_data['plays']:
                    if not isinstance(play, dict):
                        continue

                    play_info = {
                        "game_id":
                            game_id,
                        "play_id":
                            play.get("id", None),
                        "sequence_number":
                            play.get("sequenceNumber", None),
                        "period":
                            play.get("period", {}).get("number", None)
                            if isinstance(play.get("period"), dict) else None,
                        "period_display":
                            play.get("period", {}).get("displayValue", None)
                            if isinstance(play.get("period"), dict) else None,
                        "clock":
                            play.get("clock", {}).get("displayValue", None)
                            if isinstance(play.get("clock"), dict) else None,
                        "clock_seconds":
                            convert_clock_to_seconds(
                                play.get("clock", {}).get("displayValue", None) if isinstance(play.get("clock"), dict
                                                                                             ) else ""),
                        "team_id":
                            play.get("team", {}).get("id", None)
                            if 'team' in play and isinstance(play.get("team"), dict) else None,
                        "team_name":
                            play.get("team", {}).get("name", None)
                            if 'team' in play and isinstance(play.get("team"), dict) else None,
                        "play_type":
                            play.get("type", {}).get("text", None) if isinstance(play.get("type"), dict) else None,
                        "play_type_id":
                            play.get("type", {}).get("id", None) if isinstance(play.get("type"), dict) else None,
                        "text":
                            play.get("text", None),
                        "score_home":
                            play.get("homeScore", None),
                        "score_away":
                            play.get("awayScore", None),
                        "scoring_play":
                            play.get("scoringPlay", False),
                        "score_value":
                            play.get("scoreValue", 0),
                        "shooting_play":
                            play.get("shootingPlay", False),
                        "coordinate_x":
                            play.get("coordinate", {}).get("x", None)
                            if 'coordinate' in play and isinstance(play.get("coordinate"), dict) else None,
                        "coordinate_y":
                            play.get("coordinate", {}).get("y", None)
                            if 'coordinate' in play and isinstance(play.get("coordinate"), dict) else None,
                        "wallclock":
                            play.get("wallclock", None),
                    }

                    # Extract player information
                    for i in range(1, 3):  # Get data for player 1 and player 2
                        athlete_key = f"athlete{i}"
                        if athlete_key in play and isinstance(play[athlete_key], dict):
                            play_info[f"player_{i}_id"] = play[athlete_key].get("id", None)
                            play_info[f"player_{i}_name"] = play[athlete_key].get("displayName", None)
                            play_info[f"player_{i}_role"] = play[athlete_key].get("role", None)

                    # Check for additional player fields like participantsCodes, athletesInvolved, etc.
                    player_ids = []

                    # Check for participantsCodes field
                    if 'participantsCodes' in play and isinstance(play['participantsCodes'], list):
                        player_ids.extend(play['participantsCodes'])

                    # Check for athletesInvolved field
                    if 'athletesInvolved' in play and isinstance(play['athletesInvolved'], list):
                        for athlete in play['athletesInvolved']:
                            if isinstance(athlete, dict) and 'id' in athlete:
                                player_ids.append(athlete['id'])
                            elif isinstance(athlete, str):
                                player_ids.append(athlete)

                    # Check for participants field
                    if 'participants' in play and isinstance(play['participants'], list):
                        for idx, participant in enumerate(play['participants']):
                            if isinstance(participant, dict) and 'athlete' in participant and isinstance(
                                    participant['athlete'], dict):
                                player_id = participant['athlete'].get('id', None)
                                if player_id:
                                    player_ids.append(player_id)
                                    # Add each participant's player ID as a separate column
                                    play_info[f"participant_{idx+1}_id"] = player_id

                    # Add all player IDs as a comma-separated string
                    if player_ids:
                        # Remove duplicates while preserving order
                        unique_player_ids = []
                        for pid in player_ids:
                            if pid not in unique_player_ids:
                                unique_player_ids.append(pid)

                        play_info["all_player_ids"] = ",".join(str(pid) for pid in unique_player_ids)

                    # Also check for a generic athletes field
                    if 'athletes' in play and isinstance(play['athletes'], list):
                        athlete_data = []
                        for athlete in play['athletes']:
                            if isinstance(athlete, dict):
                                athlete_id = None
                                athlete_name = None

                                if 'id' in athlete:
                                    athlete_id = athlete['id']
                                elif 'athlete' in athlete and isinstance(athlete['athlete'], dict):
                                    athlete_id = athlete['athlete'].get('id', None)
                                    athlete_name = athlete['athlete'].get('displayName', None)

                                if athlete_id:
                                    athlete_data.append(str(athlete_id))

                        if athlete_data:
                            play_info["athletes_data"] = ",".join(athlete_data)

                    # Add win probability data if available for this play
                    play_id = play.get("id", None)
                    if play_id in win_prob_mapping:
                        play_info["home_win_percentage"] = win_prob_mapping[play_id]["home_win_percentage"]
                        play_info["away_win_percentage"] = 1.0 - win_prob_mapping[play_id][
                            "home_win_percentage"] if win_prob_mapping[play_id][
                                "home_win_percentage"] is not None else None
                        play_info["tie_percentage"] = win_prob_mapping[play_id]["tie_percentage"]

                    # Fill in empty team names if we have a valid team_id and it's in our lookup
                    if play_info["team_id"] and not play_info["team_name"]:
                        team_id_str = str(play_info["team_id"])
                        if team_id_str in team_lookup:
                            play_info["team_name"] = team_lookup[team_id_str]["name"]
                            logger.debug(f"Filled empty team name for team ID {team_id_str} in play {play_id}")

                    # Fill in empty player names if we have valid player_ids and they're in our lookup
                    for i in range(1, 3):
                        player_id_key = f"player_{i}_id"
                        player_name_key = f"player_{i}_name"

                        if player_id_key in play_info and player_name_key in play_info:
                            if play_info[player_id_key] and not play_info[player_name_key]:
                                player_id_str = str(play_info[player_id_key])
                                if player_id_str in player_lookup:
                                    play_info[player_name_key] = player_lookup[player_id_str]["name"]
                                    logger.debug(
                                        f"Filled empty player name for player ID {player_id_str} in play {play_id}")

                    play_by_play.append(play_info)

        # Instead of saving individual files, return all the processed data
        result = {
            "game_id": game_id,
            "season": season,
            "processed": True,
            "data": {
                "game_info": pd.DataFrame([game_info]) if game_info else pd.DataFrame(),
                "teams_info": pd.DataFrame(teams_info) if teams_info else pd.DataFrame(),
                "player_stats": pd.DataFrame(player_stats) if player_stats else pd.DataFrame(),
                "team_stats": pd.DataFrame(team_stats) if team_stats else pd.DataFrame(),
                "play_by_play": pd.DataFrame(play_by_play) if play_by_play else pd.DataFrame(),
                "officials": pd.DataFrame(officials_data) if officials_data else pd.DataFrame(),
                "broadcasts": pd.DataFrame(broadcasts_data) if broadcasts_data else pd.DataFrame()
            }
        }

        if verbose:
            logger.info(f"Successfully processed data for game {game_id} in season {season}")
        else:
            logger.debug(f"Successfully processed data for game {game_id} in season {season}")

        return result

    except Exception as e:
        error_msg = f"Error processing game {game_id} in season {season}: {str(e)}"
        logger.error(error_msg)
        # Log stack trace for debugging
        logger.debug(f"Stack trace for game {game_id} error:", exc_info=True)
        return {"game_id": game_id, "season": season, "processed": False, "error": str(e)}


def process_game_with_season(game_id, season, force, gender=None, verbose=False):
    """
    Helper function to process a game with season information.
    
    Args:
        game_id: ESPN game ID
        season: Season year
        force: Whether to force reprocessing
        gender: Either "mens" or "womens" (if None, uses current setting)
        verbose: If True, log detailed information
    
    Returns:
        Result of process_game_data with processed data structures
    """
    try:
        # Ensure correct gender in child process if provided
        if gender:
            set_gender(gender)

        # Note: force parameter is not used by process_game_data
        return process_game_data(game_id, season, verbose)
    except Exception as e:
        # This helps debug any errors
        logger.error(f"Error in process_game_with_season for game {game_id}: {e}")
        # Return a minimal valid result
        return {"game_id": game_id, "season": season, "processed": False, "error": f"Error: {e}"}


def optimize_dataframe_dtypes(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """
    Optimize datatypes in a dataframe to reduce memory usage and improve consistency.
    
    Args:
        df: The dataframe to optimize
        data_type: The type of data in the dataframe (e.g., "player_stats", "team_stats", etc.)
        
    Returns:
        DataFrame with proper dtypes
    """
    if df.empty:
        return df

    # Make a copy to avoid modifying original
    result_df = df.copy()

    # Common ID columns to convert to integers across all dataframes
    id_columns = {
        "game_id": True,
        "venue_id": True,
        "team_id": True,
        "player_id": True,
        "player_1_id": True,
        "player_2_id": True,
        "position_id": True,
        "play_type_id": True,
        "sequence_number": True
    }

    # Common columns that should be categorical across all dataframes
    categorical_columns = [
        "home_away", "type", "market", "lang", "region", "team_abbreviation", "position", "status", "play_type"
    ]

    # Dataframe-specific columns to convert
    datatype_conversions = {
        "broadcasts": {
            # No specific additional conversions
        },
        "game_info": {
            "attendance": "Int64",  # Nullable integer
            "date": "datetime64[ns]",  # Convert date strings to datetime
            "neutral_site": "bool",
            "completed": "bool"
        },
        "game_summary": {
            "error": "categorical",  # Most errors are empty or a few unique values
            "processed": "bool"
        },
        "officials": {
            "position": "categorical",
            "name": "categorical",
            "display_name": "categorical"
        },
        "play_by_play": {
            "play_id": False,  # Don't convert this to int as it may be too large
            "clock_seconds": "Int64",
            "score_home": "Int64",
            "score_away": "Int64",
            "score_value": "Int64",
            "coordinate_x": "float64",
            "coordinate_y": "float64",
            "home_win_percentage": "float64",
            "away_win_percentage": "float64",
            "tie_percentage": "float64",
            "period_display": "categorical",
            "wallclock": "datetime64[ns]"  # Convert wallclock to datetime
        },
        "player_stats": {
            "jersey": "Int64",
            "MIN": "float64",  # Keep as float to handle DNP/null
            "OREB": "float64",
            "DREB": "float64",
            "REB": "float64",
            "AST": "float64",
            "STL": "float64",
            "BLK": "float64",
            "TO": "float64",
            "PF": "float64",
            "PTS": "float64",
            "FG": False,
            "3PT": False,
            "FT": False,
            "starter": "bool",
            "dnp": "bool"
        },
        "schedules": {
            "season": "Int64",
            "event_date": "datetime64[ns]"  # Convert event_date to datetime
        },
        "team_stats": {
            # Prevent inadvertent conversion of raw stats that may contain "-"
            "FG": False,
            "3PT": False,
            "FT": False,
            "PTS": "float64",
            "REB": "float64",
            "AST": "float64",
            "STL": "float64",
            "BLK": "float64",
            "TO": "float64",
            "TTO": "float64",
            "ToTO": "float64",
            "TECH": "float64",
            "PTS OFF TO": "float64",
            "FBPs": "float64",
            "PIP": "float64",
            "PF": "float64",
            "LL": "float64",
            "OREB": "float64",
            "DREB": "float64"
        },
        "teams": {
            "conference_id": "Int64"
        },
        "teams_info": {
            "score": "Int64",
            "winner": "bool",
            "team_color": "categorical",
            "team_location": "categorical",
            "team_nickname": "categorical",
            "groups_slug": "categorical"
        }
    }

    # Process common ID columns
    for col in id_columns:
        if col in result_df.columns:
            try:
                # If column contains strings that look like integers, convert to Int64 (nullable integer)
                if result_df[col].dtype == 'object' and id_columns[col]:
                    # Check if all non-null values can be converted to integers
                    non_null_values = result_df[col].dropna()
                    if len(non_null_values) > 0:
                        try:
                            # Try converting to integers
                            result_df[col] = pd.to_numeric(result_df[col], errors='coerce').astype('Int64')
                            logger.debug(f"Converted {col} to Int64 in {data_type}")
                        except Exception as e:
                            # If conversion fails, keep as object
                            logger.debug(f"Could not convert {col} to Int64 in {data_type}: {str(e)}")
            except Exception as e:
                logger.warning(f"Error optimizing column {col} in {data_type}: {str(e)}")

    # Process common categorical columns
    for col in categorical_columns:
        if col in result_df.columns and result_df[col].dtype == 'object':
            try:
                # Only convert to categorical if it's a string column and has fewer than 100 unique values
                if result_df[col].nunique() < 100:
                    result_df[col] = result_df[col].astype('category')
                    logger.debug(f"Converted {col} to categorical in {data_type}")
            except Exception as e:
                logger.warning(f"Error converting {col} to categorical in {data_type}: {str(e)}")

    # Special handling for format column in game_info
    if data_type == "game_info" and "format" in result_df.columns:
        try:
            # Check if format column contains JSON-like strings
            if result_df["format"].dtype == 'object':
                # Try to extract regulation and overtime clocks
                result_df["regulation_clock"] = result_df["format"].apply(lambda x: x.get("regulation", {}).get("clock")
                                                                          if isinstance(x, dict) else None)

                result_df["overtime_clock"] = result_df["format"].apply(lambda x: x.get("overtime", {}).get("clock")
                                                                        if isinstance(x, dict) else None)

                result_df["period_name"] = result_df["format"].apply(
                    lambda x: x.get("regulation", {}).get("displayName") if isinstance(x, dict) else None)

                result_df["num_periods"] = result_df["format"].apply(lambda x: x.get("regulation", {}).get("periods")
                                                                     if isinstance(x, dict) else None)

                logger.debug(f"Extracted format components in {data_type}")
        except Exception as e:
            logger.warning(f"Error processing format column in {data_type}: {str(e)}")

    # Process dataframe-specific conversions
    if data_type in datatype_conversions:
        for col, dtype in datatype_conversions[data_type].items():
            if col in result_df.columns and dtype:
                try:
                    # Convert to specified dtype
                    if dtype == "Int64":
                        result_df[col] = pd.to_numeric(result_df[col], errors='coerce').astype('Int64')
                    elif dtype == "datetime64[ns]":
                        # Special handling for datetime conversion
                        result_df[col] = pd.to_datetime(result_df[col], errors='coerce')
                    elif dtype == "categorical":
                        # Only convert to categorical if it has a reasonable number of unique values
                        if result_df[col].nunique() < 100:
                            result_df[col] = result_df[col].astype('category')
                    elif dtype == "bool":
                        # Handle various representations of boolean values
                        if result_df[col].dtype != 'bool':
                            result_df[col] = result_df[col].map({
                                True: True,
                                'True': True,
                                'true': True,
                                1: True,
                                '1': True,
                                False: False,
                                'False': False,
                                'false': False,
                                0: False,
                                '0': False
                            }).astype('bool')
                    else:
                        # For float conversions, ensure NaNs are preserved
                        if dtype == "float64":
                            result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
                        else:
                            result_df[col] = result_df[col].astype(dtype)
                except Exception as e:
                    logger.warning(f"Error converting {col} to {dtype} in {data_type}: {str(e)}")
            elif col in result_df.columns and dtype is False:
                # Explicitly skip conversion for this column
                logger.debug(f"Skipping conversion for {col} in {data_type} as requested")

    # Convert any empty strings to NaN in all object columns
    for col in result_df.columns:
        if result_df[col].dtype == 'object':
            # Replace empty strings with NaN
            result_df[col] = result_df[col].replace('', np.nan)
            logger.debug(f"Converted empty strings to NaN in column {col} of {data_type}")

    # Special handling for player_stats to ensure DNP players have proper null values
    if data_type == "player_stats":
        # If dnp is True, ensure all stat columns are set to NaN
        if "dnp" in result_df.columns:
            stat_columns = [
                "MIN", "FG_MADE", "FG_ATT", "FG_PCT", "3PT_MADE", "3PT_ATT", "3PT_PCT", "FT_MADE", "FT_ATT", "FT_PCT",
                "OREB", "DREB", "REB", "AST", "STL", "BLK", "TO", "PF", "PTS"
            ]

            for col in stat_columns:
                if col in result_df.columns:
                    # Set stats to NaN where dnp is True
                    dnp_mask = result_df["dnp"] == True
                    if dnp_mask.any():
                        result_df.loc[dnp_mask, col] = np.nan
                        logger.debug(f"Set {col} to NaN for {dnp_mask.sum()} DNP players in {data_type}")

            # Convert string stat columns to numeric
            basic_stat_columns = ["OREB", "DREB", "REB", "AST", "STL", "BLK", "TO", "PF", "PTS"]
            for col in basic_stat_columns:
                if col in result_df.columns and result_df[col].dtype == 'object':
                    try:
                        # Convert string stats to numeric, handling 'DNP' and other non-numeric values
                        result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
                        logger.debug(f"Converted {col} from string to numeric in {data_type}")
                    except Exception as e:
                        logger.warning(f"Error converting {col} to numeric in {data_type}: {str(e)}")

    # Fill empty names in play_by_play when we have valid IDs
    if data_type == "play_by_play":
        for id_col, name_col in [("team_id", "team_name"), ("player_1_id", "player_1_name"),
                                 ("player_2_id", "player_2_name")]:
            if id_col in result_df.columns and name_col in result_df.columns:
                # Check if we have rows with valid IDs but empty names
                mask = result_df[id_col].notna() & result_df[name_col].isna()
                if mask.any():
                    logger.debug(f"Found {mask.sum()} rows with valid {id_col} but empty {name_col}")
                    # Note: We would need team/player lookup tables to properly fill these

    return result_df


def remove_redundant_columns(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """
    Remove redundant columns from dataframes to optimize storage and clarity.
    
    Args:
        df: The dataframe to optimize
        data_type: The type of data in the dataframe (e.g., "player_stats", "team_stats", etc.)
        
    Returns:
        DataFrame with redundant columns removed
    """
    if df.empty:
        return df

    # Make a copy to avoid modifying original
    result_df = df.copy()

    # For player_stats and team_stats, remove string columns where we have parsed numeric versions
    if data_type in ["player_stats", "team_stats"]:
        # Map of redundant columns to keep/remove
        redundant_columns = {
            # Format strings we can remove when we have the parsed values
            "FG": ["FG_MADE", "FG_ATT", "FG_PCT"],
            "3PT": ["3PT_MADE", "3PT_ATT", "3PT_PCT"],
            "FT": ["FT_MADE", "FT_ATT", "FT_PCT"]
        }

        for str_col, parsed_cols in redundant_columns.items():
            if str_col in result_df.columns:
                # Check if all the parsed columns exist
                if all(col in result_df.columns for col in parsed_cols):
                    # Verify that the parsed columns have valid data
                    if result_df[parsed_cols].notna().all(axis=1).mean() > 0.9:  # If >90% rows have parsed data
                        # Safe to drop the redundant string column
                        logger.debug(f"Removing redundant column {str_col} from {data_type} as we have parsed values")
                        result_df = result_df.drop(columns=[str_col])

    # For game_info, remove redundant broadcast info if we have detailed broadcast data
    if data_type == "game_info" and "broadcast" in result_df.columns and "broadcast_market" in result_df.columns:
        # We would need to check that broadcasts table is properly linked by game_id
        # For now, just log that these could be candidates for removal
        logger.debug(f"Game_info has broadcast columns that may be redundant with broadcasts table")

    # If we extracted format components, consider removing the raw format column
    if data_type == "game_info" and "format" in result_df.columns:
        extracted_cols = ["regulation_clock", "overtime_clock", "period_name", "num_periods"]
        if all(col in result_df.columns for col in extracted_cols):
            if result_df[extracted_cols].notna().any(axis=1).mean() > 0.9:  # If >90% rows have extracted data
                logger.debug(f"Removing format column from game_info as we have extracted the components")
                result_df = result_df.drop(columns=["format"])

    # Return optimized dataframe
    return result_df


def process_all_games(season: int,
                      max_workers: int = 4,
                      force: bool = False,
                      verbose: bool = False) -> Dict[str, pd.DataFrame]:
    """
    Process all games for a specific season.
    
    Processes all games and saves consolidated data files for each data type:
    - game_info.csv/parquet: All game information
    - teams_info.csv/parquet: All teams information 
    - player_stats.csv/parquet: All player statistics
    - team_stats.csv/parquet: All team statistics
    - play_by_play.csv/parquet: All play-by-play data
    - officials.csv/parquet: All officials data
    - broadcasts.csv/parquet: All broadcast information
    
    Args:
        season: Season year to process
        max_workers: Maximum number of concurrent processes
        force: If True, force reprocessing even if processed files exist
        verbose: If True, log detailed information including each processed game
        
    Returns:
        Dictionary with dataframes for this season
    """
    logger.info(f"Processing games for season {season}")

    # Get raw data directory for this season
    raw_games_dir = get_games_dir(season)

    if not os.path.exists(raw_games_dir):
        logger.warning(f"No raw game data found for season {season}")
        return {"game_summary": pd.DataFrame()}

    # Get list of game files
    game_files = [f for f in os.listdir(raw_games_dir) if f.endswith('.json')]
    game_ids = [os.path.splitext(f)[0] for f in game_files]

    logger.info(f"Found {len(game_ids)} games to process for season {season}")

    # Process games in parallel and collect data for consolidated files
    results = []
    game_results = {}

    # Initialize empty DataFrames for each data type to hold all games' data
    for data_type in [
            "game_info", "teams_info", "player_stats", "team_stats", "play_by_play", "officials", "broadcasts"
    ]:
        game_results[data_type] = []

    # Define a helper function to process results from each game
    def process_game_result(result):
        if result.get("processed", False) and "data" in result:
            # Add record to game summary
            game_results["game_summary"].append({
                "game_id": result["game_id"],
                "season": result.get("season", season),
                "processed": True,
                "error": None
            })

            # Add each data type to its respective consolidated list
            for data_type, df in result["data"].items():
                if not df.empty:
                    game_results[data_type].append(df)
        else:
            # Get error message if present
            error_msg = result.get("error", "Unknown error")

            # Log the error for debugging
            if error_msg != "Unknown error":
                logger.debug(f"Game {result['game_id']} processing error: {error_msg}")

            # Add error record to game summary
            game_results["game_summary"].append({
                "game_id": result["game_id"],
                "season": result.get("season", season),
                "processed": False,
                "error": error_msg
            })

    # Initialize game_summary as a special case
    game_results["game_summary"] = []

    # Get current gender to pass to child processes
    current_gender = get_current_gender()
    logger.info(f"Processing games with gender: {current_gender}")

    # Process all games
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs
        futures = [
            executor.submit(process_game_with_season, game_id, season, force, current_gender, verbose)
            for game_id in game_ids
        ]

        # Process results as they complete
        for future in as_completed(futures):
            try:
                result = future.result()
                process_game_result(result)
            except Exception as e:
                error_msg = f"Error processing game result: {str(e)}"
                logger.error(error_msg)
                # Add an error record for the failed game
                # Note: We can't know which game_id failed here, so we create a generic error record
                game_results["game_summary"].append({
                    "game_id": "error",
                    "season": season,
                    "processed": False,
                    "error": error_msg
                })

    # Create combined DataFrames
    combined_dfs = {}
    for data_type, df_list in game_results.items():
        if df_list:
            try:
                # Handle special case for game_summary which might be a list of dictionaries
                if data_type == "game_summary":
                    # Convert any dictionaries to dataframes before concatenating
                    df_objects = []
                    for item in df_list:
                        if isinstance(item, pd.DataFrame):
                            df_objects.append(item)
                        elif isinstance(item, dict):
                            # Create DataFrame with correct handling of None for error field
                            item_df = pd.DataFrame([item])
                            df_objects.append(item_df)

                    if df_objects:
                        combined_df = pd.concat(df_objects, ignore_index=True)
                        # Ensure error column is properly typed to handle None values
                        if 'error' in combined_df.columns:
                            # Log stats on error column before cleaning
                            null_count = combined_df['error'].isna().sum()
                            empty_count = (combined_df['error'] == '').sum()

                            logger.debug(
                                f"Game summary error stats before cleaning - null: {null_count}, empty string: {empty_count}"
                            )

                            # Count games with actual errors
                            has_error = (~combined_df['error'].isna()) & (combined_df['error'] != '')
                            error_count = has_error.sum()

                            if error_count > 0:
                                logger.debug(f"Found {error_count} games with error messages")
                                # Log a few examples
                                error_examples = combined_df[has_error].head(3)
                                for _, row in error_examples.iterrows():
                                    logger.debug(f"Error example - Game {row['game_id']}: {row['error']}")

                            # Convert empty strings to None
                            combined_df['error'] = combined_df['error'].replace('', None)

                            # Log stats after cleaning
                            null_count_after = combined_df['error'].isna().sum()
                            logger.debug(f"Game summary error null count after cleaning: {null_count_after}")
                    else:
                        combined_df = pd.DataFrame()
                else:
                    # Standard case for other data types
                    # Filter out empty DataFrames to avoid FutureWarning
                    non_empty_dfs = [df for df in df_list if not df.empty]
                    if non_empty_dfs:
                        # Filter out all-NA columns from each DataFrame before concatenation
                        cleaned_dfs = []
                        for df in non_empty_dfs:
                            # Drop columns where all values are NA
                            df_cleaned = df.dropna(axis=1, how='all')
                            if not df_cleaned.empty:
                                cleaned_dfs.append(df_cleaned)

                        if cleaned_dfs:
                            combined_df = pd.concat(cleaned_dfs, ignore_index=True)
                        else:
                            combined_df = pd.DataFrame()
                    else:
                        combined_df = pd.DataFrame()

                # Optimize datatypes
                combined_df = optimize_dataframe_dtypes(combined_df, data_type)

                # Remove redundant columns
                combined_df = remove_redundant_columns(combined_df, data_type)

                combined_dfs[data_type] = combined_df
                logger.info(f"Created combined {data_type} DataFrame with {len(combined_df)} rows")
            except Exception as e:
                logger.error(f"Error creating combined DataFrame for {data_type}: {str(e)}")
                combined_dfs[data_type] = pd.DataFrame()
        else:
            combined_dfs[data_type] = pd.DataFrame()

    # Make directories if they don't exist
    csv_season_dir = get_csv_season_dir(season)
    parquet_season_dir = get_parquet_season_dir(season)

    os.makedirs(csv_season_dir, exist_ok=True)
    os.makedirs(parquet_season_dir, exist_ok=True)

    # Save summary files
    for data_type, df in combined_dfs.items():
        if not df.empty:
            try:
                # Special handling for game_summary to include any failed games that aren't in the dataframe
                if data_type == "game_summary":
                    # Get the schedules dataframe to ensure we have all games
                    try:
                        schedules_file = get_parquet_season_dir(season) / "schedules.parquet"
                        if os.path.exists(schedules_file):
                            schedules_df = pd.read_parquet(schedules_file)
                            if not schedules_df.empty and 'game_id' in schedules_df.columns:
                                # Get list of all scheduled game IDs
                                all_scheduled_game_ids = set(schedules_df['game_id'].unique())
                                # Get list of all game IDs in the current dataframe
                                processed_game_ids = set(df['game_id'].unique())
                                # Find missing games
                                missing_game_ids = all_scheduled_game_ids - processed_game_ids

                                if missing_game_ids:
                                    logger.warning(
                                        f"Found {len(missing_game_ids)} games in schedule that aren't in game_summary")
                                    # Create records for missing games
                                    missing_records = []
                                    for game_id in missing_game_ids:
                                        # Try to find the game in the schedule
                                        game_info = schedules_df[schedules_df['game_id'] == game_id].iloc[
                                            0] if not schedules_df[schedules_df['game_id'] == game_id].empty else {}
                                        missing_records.append({
                                            "game_id": game_id,
                                            "season": season,
                                            "processed": False,
                                            "error": "Game failed to process completely"
                                        })
                                        logger.warning(f"Added missing game to summary: {game_id}")

                                    # Add missing records to dataframe
                                    df = pd.concat([df, pd.DataFrame(missing_records)], ignore_index=True)
                                    logger.info(
                                        f"Updated game_summary to include {len(missing_game_ids)} missing games")
                    except Exception as e:
                        logger.error(f"Error trying to reconcile game_summary with schedules: {str(e)}")

                # Save as CSV
                csv_path = csv_season_dir / f"{data_type}.csv"
                df.to_csv(csv_path, index=False)
                logger.info(f"Saved {data_type}.csv with {len(df)} rows")

                # Save as Parquet
                parquet_path = parquet_season_dir / f"{data_type}.parquet"
                df.to_parquet(parquet_path, index=False)
                logger.info(f"Saved {data_type}.parquet with {len(df)} rows")
            except Exception as e:
                logger.error(f"Error saving {data_type} files: {str(e)}")

    return combined_dfs


def process_schedules(season: int, force: bool = False) -> pd.DataFrame:
    """
    Process schedule data for a specific season.
    
    Args:
        season: Season year to process
        force: If True, force reprocessing even if processed files exist
        
    Returns:
        DataFrame with all schedules for this season
    """
    logger.info(f"Processing schedules for season {season}")

    # Check if processed schedules already exist
    csv_season_dir = get_csv_season_dir(season)
    csv_schedules_file = csv_season_dir / "schedules.csv"

    if not force and csv_schedules_file.exists():
        logger.info(f"Using cached schedules for season {season}")
        # Read with parse_dates to ensure datetime type
        return pd.read_csv(csv_schedules_file, parse_dates=['event_date'])

    # Get all schedule files for this season
    regular_season_schedules_dir = get_schedules_dir(season)
    postseason_schedules_dir = get_schedules_dir(season, schedule_type="postseason")
    if not regular_season_schedules_dir.exists() and not postseason_schedules_dir.exists():
        logger.warning(f"No schedules directory found for season {season}")
        return pd.DataFrame()

    regular_season_schedule_files = list(regular_season_schedules_dir.glob("*.json"))
    postseason_schedule_files = list(postseason_schedules_dir.glob("*.json"))
    schedule_files = regular_season_schedule_files + postseason_schedule_files

    if not schedule_files:
        logger.warning(f"No schedule files found for season {season}")
        return pd.DataFrame()

    logger.info(f"Found {len(schedule_files)} team schedule files for season {season}")

    all_games = []

    # Process each team's schedule
    for schedule_file in schedule_files:
        team_id = schedule_file.stem
        schedule_data = load_json(schedule_file)

        if not schedule_data:
            continue

        for event in schedule_data.get("events", []):
            event_id = event.get("id", None)
            event_date = event.get("date", None)

            # More robust ISO date parsing
            if event_date:
                try:
                    # Try to parse the date string
                    # Handle various ISO formats including Z vs +00:00
                    dt = pd.to_datetime(event_date, errors='coerce')
                    if pd.notna(dt):
                        event_date = dt
                    else:
                        # Fallback to string manipulation for oddly formatted dates
                        dt = datetime.fromisoformat(event_date.replace('Z', '+00:00'))
                        event_date = dt
                except (ValueError, TypeError):
                    # If parsing fails, just keep as string
                    logger.debug(f"Could not parse date {event_date} for event {event_id}")

            for competition in event.get("competitions", []):
                game_id = competition.get("id", None)

                for team_data in competition.get("competitors", []):
                    opponent_id = team_data.get("id", None)

                    if opponent_id != team_id:
                        all_games.append({
                            "team_id": team_id,
                            "game_id": game_id,
                            "event_id": event_id,
                            "event_date": event_date,
                            "season": season,
                            "opponent_id": opponent_id
                        })

    # Convert to dataframe
    schedules_df = pd.DataFrame(all_games)

    # Optimize datatypes
    schedules_df = optimize_dataframe_dtypes(schedules_df, "schedules")

    # Ensure event_date is datetime
    if 'event_date' in schedules_df.columns and schedules_df['event_date'].dtype != 'datetime64[ns]':
        try:
            schedules_df['event_date'] = pd.to_datetime(schedules_df['event_date'], errors='coerce')
            logger.debug("Converted event_date to datetime")
        except Exception as e:
            logger.warning(f"Error converting event_date to datetime: {str(e)}")

    # Ensure directory exists
    csv_season_dir = get_csv_season_dir(season)
    parquet_season_dir = get_parquet_season_dir(season)

    os.makedirs(csv_season_dir, exist_ok=True)
    os.makedirs(parquet_season_dir, exist_ok=True)

    # Save to CSV and Parquet
    try:
        schedules_df.to_csv(csv_schedules_file, index=False)
        schedules_df.to_parquet(parquet_season_dir / "schedules.parquet", index=False)
        logger.info(f"Saved schedules for season {season} with {len(schedules_df)} games")
    except Exception as e:
        logger.error(f"Error saving schedules for season {season}: {str(e)}")

    return schedules_df


def process_season_data(season: int,
                        max_workers: int = 4,
                        force: bool = False,
                        verbose: bool = False) -> Dict[str, Any]:
    """
    Process all data for a specific season.
    
    Args:
        season: The season to process
        max_workers: Maximum number of parallel worker processes
        force: If True, force reprocessing even if processed files exist
        verbose: If True, log detailed information including each processed game
        
    Returns:
        Dictionary with processing statistics
    """
    logger.info(f"Processing data for season {season}")

    # Create the directories if they don't exist
    try:
        os.makedirs(get_csv_season_dir(season), exist_ok=True)
        os.makedirs(get_parquet_season_dir(season), exist_ok=True)
    except Exception as e:
        logger.error(f"Error creating directories for season {season}: {e}")
        return {"season": season, "total_games": 0, "success_games": 0, "error_games": 0, "error": str(e)}

    # Process schedules for the season
    schedules_df = process_schedules(season, force=force)

    if schedules_df.empty:
        logger.warning(f"No schedule data found for season {season}")
        return {
            "season": season,
            "total_games": 0,
            "success_games": 0,
            "error_games": 0,
            "error": "No schedule data found"
        }

    # Process games data
    # Deduplicate game IDs to avoid counting the same game multiple times
    unique_game_ids = schedules_df['game_id'].unique()
    game_id_season_pairs = sorted([(game_id, season) for game_id in unique_game_ids])
    total_games = len(game_id_season_pairs)

    logger.info(f"Processing {total_games} games for season {season}")

    success_count = 0
    error_count = 0

    try:
        processed_data = process_all_games(season, max_workers=max_workers, force=force, verbose=verbose)

        # Count successful and failed games
        for dataset_name, df in processed_data.items():
            if not df.empty:
                if dataset_name == 'game_summary':
                    # Count games where processed=True as successful
                    success_count = len(df[df['processed'] == True]) if 'processed' in df.columns else 0
                    # Count games where error is not None as error games
                    error_games = df[df['error'].notna()] if 'error' in df.columns else pd.DataFrame()
                    error_count = len(error_games)

                    # If error_count is 0 but success_count < total_games, calculate as before
                    if error_count == 0 and success_count < total_games:
                        error_count = total_games - success_count
                        logger.warning(
                            f"Found discrepancy: {success_count} successful games out of {total_games} total, but no error messages found."
                        )

                    # Log detailed error information for debugging
                    if not error_games.empty:
                        logger.warning(f"Season {season} games with errors ({len(error_games)}):")
                        for _, row in error_games.iterrows():
                            logger.warning(f"Game {row['game_id']} error: {row['error']}")
                    elif error_count > 0:
                        logger.warning(
                            f"Season {season} has {error_count} games with errors, but no error details were found in the dataframe."
                        )
    except Exception as e:
        logger.error(f"Error processing games for season {season}: {e}")
        error_count = total_games

    # Return stats about the processing
    return {"season": season, "total_games": total_games, "success_games": success_count, "error_games": error_count}


def process_all_data(seasons: Optional[List[int]] = None,
                     max_workers: int = 4,
                     gender: str = None,
                     game_ids: Optional[List[str]] = None,
                     force: bool = False,
                     verbose: bool = False) -> None:
    """
    Process all data for the specified seasons.

    Args:
        seasons: List of seasons to process (if None, all available seasons are processed)
        max_workers: Maximum number of parallel worker processes
        gender: Either "mens" or "womens" (if None, uses current setting)
        game_ids: Optional list of specific game IDs to process
        force: If True, force reprocessing even if processed files exist
        verbose: If True, log detailed information including each processed game
    """
    if gender:
        set_gender(gender)

    logger.info(f"Processing all data for {get_current_gender()} basketball")

    # Process teams data first
    teams_df = process_teams_data(force=force)
    logger.info(f"Processed {len(teams_df)} teams")

    # Determine which seasons to process
    if seasons is None:
        seasons = DEFAULT_SEASONS

    # Create a summary of processing results
    summary = {
        "total_seasons": len(seasons),
        "processed_seasons": 0,
        "total_games": 0,
        "success_games": 0,
        "error_games": 0
    }

    # Process each season
    for season in tqdm(seasons, desc="Processing seasons"):
        logger.info(f"Processing season {season}")
        try:
            # If specific game IDs are provided, only process those
            if game_ids:
                logger.info(f"Processing only specific games: {', '.join(game_ids)}")
                games_dir = get_games_dir(season)

                # Create result containers to match the process_season_data structure
                season_results = {
                    "games_df": None,
                    "pbp_df": None,
                    "boxscores_df": None,
                    "teams_df": None,
                    "season": season
                }

                # Process only the specified games
                game_results = {}
                for game_id in game_ids:
                    game_file = games_dir / f"{game_id}.json"
                    if game_file.exists():
                        try:
                            result = process_game_data(game_id, season)
                            game_results[game_id] = result
                        except Exception as e:
                            logger.error(f"Error processing game {game_id}: {e}")
                            summary['error_games'] += 1

                # Create dataframes from the individual game results
                if game_results:
                    # Convert game results to dataframes
                    all_games = []
                    all_pbp = []
                    all_boxscores = []

                    for game_id, result in game_results.items():
                        if "game" in result:
                            all_games.append(result["game"])
                        if "pbp" in result:
                            all_pbp.extend(result["pbp"])
                        if "boxscores" in result:
                            all_boxscores.extend(result["boxscores"])

                    if all_games:
                        season_results["games_df"] = pd.DataFrame(all_games)
                    if all_pbp:
                        season_results["pbp_df"] = pd.DataFrame(all_pbp)
                    if all_boxscores:
                        season_results["boxscores_df"] = pd.DataFrame(all_boxscores)

                    # Save the dataframes - need to manually do what process_season_data does
                    season_dir_csv = get_csv_season_dir(season)
                    season_dir_parquet = get_parquet_season_dir(season)

                    # Create necessary directories
                    os.makedirs(season_dir_csv, exist_ok=True)
                    os.makedirs(season_dir_parquet, exist_ok=True)

                    # Save each dataframe if it exists
                    if season_results["games_df"] is not None and not season_results["games_df"].empty:
                        games_csv_file = season_dir_csv / "games.csv"
                        games_parquet_file = season_dir_parquet / "games.parquet"
                        season_results["games_df"].to_csv(games_csv_file, index=False)
                        season_results["games_df"].to_parquet(games_parquet_file, index=False)

                    if season_results["pbp_df"] is not None and not season_results["pbp_df"].empty:
                        pbp_csv_file = season_dir_csv / "play_by_play.csv"
                        pbp_parquet_file = season_dir_parquet / "play_by_play.parquet"
                        season_results["pbp_df"].to_csv(pbp_csv_file, index=False)
                        season_results["pbp_df"].to_parquet(pbp_parquet_file, index=False)

                    if season_results["boxscores_df"] is not None and not season_results["boxscores_df"].empty:
                        boxscores_csv_file = season_dir_csv / "boxscores.csv"
                        boxscores_parquet_file = season_dir_parquet / "boxscores.parquet"
                        season_results["boxscores_df"].to_csv(boxscores_csv_file, index=False)
                        season_results["boxscores_df"].to_parquet(boxscores_parquet_file, index=False)

                    summary['success_games'] += len(game_results)
                    summary['total_games'] += len(game_results)
            else:
                # Process the entire season
                result = process_season_data(season, max_workers, force, verbose)

                # Update summary with this season's data
                summary["processed_seasons"] += 1
                summary["total_games"] += result.get("total_games", 0)
                summary["success_games"] += result.get("success_games", 0)
                summary["error_games"] += result.get("error_games", 0)

            logger.info(f"Completed processing season {season}: "
                        f"{result.get('success_games', 0)} games processed, "
                        f"{result.get('error_games', 0)} games with errors")
        except Exception as e:
            logger.error(f"Error processing season {season}: {e}")

    # Log summary of all processing
    logger.info(f"Data processing complete. Summary:")
    logger.info(f"  Seasons processed: {summary['processed_seasons']}/{summary['total_seasons']}")
    logger.info(f"  Total games: {summary['total_games']}")
    logger.info(f"  Successfully processed games: {summary['success_games']}")
    logger.info(f"  Games with errors: {summary['error_games']}")

    if summary['error_games'] > 0:
        logger.warning(f"There were {summary['error_games']} games with processing errors. "
                       f"Check the log for details.")
    else:
        logger.info("All games processed successfully!")


def main() -> None:
    """
    Command-line interface for the processor.
    """
    parser = argparse.ArgumentParser(description="Process ESPN college basketball data")

    parser.add_argument("--seasons", "-s", type=int, nargs="+", help="Seasons to process (e.g., 2022 2023)")
    parser.add_argument("--max-workers",
                        "-w",
                        type=int,
                        default=4,
                        help="Maximum number of concurrent processes (default: 4)")
    parser.add_argument("--gender",
                        "-g",
                        type=str,
                        choices=["mens", "womens"],
                        help="Gender (mens or womens, default is womens)")
    parser.add_argument("--force", "-f", action="store_true", help="Force reprocessing even if files exist locally")

    args = parser.parse_args()

    process_all_data(seasons=args.seasons, max_workers=args.max_workers, gender=args.gender, force=args.force)


if __name__ == "__main__":
    main()
