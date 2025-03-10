"""Process and transform ESPN data into structured formats."""

import os
import json
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from datetime import datetime
import argparse

from espn_data.utils import (load_json, save_json, get_teams_file, get_schedules_dir, get_games_dir, get_processed_dir,
                             get_csv_dir, get_parquet_dir, get_csv_teams_file, get_parquet_teams_file,
                             get_csv_season_dir, get_parquet_season_dir, get_csv_games_dir, get_parquet_games_dir,
                             set_gender, get_current_gender)
from espn_data.scraper import get_game_data, DEFAULT_SEASONS

logger = logging.getLogger("espn_data")

# Ensure only the base directories exist, season dirs will be created as needed
# os.makedirs(PROCESSED_DIR, exist_ok=True)
# os.makedirs(CSV_DIR, exist_ok=True)
# os.makedirs(PARQUET_DIR, exist_ok=True)
# os.makedirs(CSV_GAMES_DIR, exist_ok=True)
# os.makedirs(PARQUET_GAMES_DIR, exist_ok=True)

# Instead of
# BASE_DIR = Path(__file__).parent
# DATA_DIR = BASE_DIR / "data"

# Change to
BASE_DIR = Path(__file__).parent.parent  # Go up one level to workspace root
DATA_DIR = BASE_DIR / "data"


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
            "id": team.get("id", ""),
            "slug": team.get("slug", ""),
            "abbreviation": team.get("abbreviation", ""),
            "display_name": team.get("displayName", ""),
            "short_name": team.get("shortDisplayName", ""),
            "name": team.get("name", ""),
            "nickname": team.get("nickname", ""),
            "location": team.get("location", ""),
            "color": team.get("color", ""),
            "alternate_color": team.get("alternateColor", ""),
            "logo": team.get("logos", [{}])[0].get("href", "") if "logos" in team and team["logos"] else "",
            "conference_id": team.get("conference", {}).get("id", "") if "conference" in team else "",
            "conference_name": team.get("conference", {}).get("name", "") if "conference" in team else "",
        }
        teams.append(team_info)

    # Convert to DataFrame
    teams_df = pd.DataFrame(teams)

    if not teams_df.empty:
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


def get_game_details(game_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract key game details like date, venue, etc. from game data.
    
    Args:
        game_data: Raw game data from the API
        
    Returns:
        Dictionary with extracted details
    """
    # Add detailed logging for debugging purposes
    game_id = game_data.get('gameId', 'unknown')
    logger.debug(f"Game {game_id}: Extracting game details")

    details = {
        "date": None,
        "venue_id": None,
        "venue_name": None,
        "venue_location": None,
        "venue_city": None,
        "venue_state": None,
        "attendance": None,
        "neutral_site": False,
        "teams": [],
        "officials": [],  # Added officials list
        "format": None,  # Added format information
        "status": {},  # Changed to empty dict from None
        "broadcasts": [],  # Added broadcasts field
        "groups": {},  # Changed to empty dict from None
    }

    if not game_data:
        logger.warning(f"Game {game_id}: Empty game_data provided")
        return details

    # Log top-level keys for debugging
    top_keys = list(game_data.keys()) if isinstance(game_data, dict) else "Not a dictionary"
    logger.debug(f"Game {game_id}: Game data top-level keys: {top_keys}")

    # Extract date and team information - first try from header for backward compatibility
    header = game_data.get('header')
    if header is not None:
        competitions = header.get('competitions', [])
        if competitions and isinstance(competitions, list) and len(competitions) > 0:
            competition = competitions[0]
            if isinstance(competition, dict):
                # Extract date
                details["date"] = competition.get('date')
                logger.debug(f"Game {game_id}: Date extracted: {details['date']}")

                # Extract neutral site info
                details["neutral_site"] = competition.get('neutralSite', False)

                # Extract team information from competitions
                competitors = competition.get('competitors', [])
                if competitors and isinstance(competitors, list):
                    logger.debug(f"Game {game_id}: Found {len(competitors)} competitors")
                    for competitor in competitors:
                        if not isinstance(competitor, dict):
                            continue

                        team = competitor.get('team', {})
                        if not team or not isinstance(team, dict):
                            continue

                        team_info = {
                            "id": team.get('id', ''),
                            "display_name": team.get('displayName', ''),
                            "abbreviation": team.get('abbreviation', ''),
                            "location": team.get('location', ''),
                            "name": team.get('name', ''),
                            "color": team.get('color', ''),
                            "home_away": competitor.get('homeAway', ''),
                            "winner": competitor.get('winner', False),
                            "score": competitor.get('score', 0)
                        }
                        details["teams"].append(team_info)

                    logger.debug(f"Game {game_id}: Extracted {len(details['teams'])} teams")

                # Extract status information
                status = competition.get('status', {})
                if status and isinstance(status, dict):
                    status_type = status.get('type', {})
                    if status_type and isinstance(status_type, dict):
                        details["status"] = {
                            "id": status_type.get('id', ''),
                            "name": status_type.get('name', ''),
                            "state": status_type.get('state', ''),
                            "completed": status_type.get('completed', False),
                            "description": status_type.get('description', ''),
                            "detail": status_type.get('detail', ''),
                            "short_detail": status_type.get('shortDetail', '')
                        }
                        logger.debug(f"Game {game_id}: Status extracted, completed={details['status']['completed']}")

                # Extract broadcasts information
                broadcasts = competition.get('broadcasts', [])
                if broadcasts and isinstance(broadcasts, list):
                    for broadcast in broadcasts:
                        if not isinstance(broadcast, dict):
                            continue

                        broadcast_type = broadcast.get('type', {})
                        broadcast_market = broadcast.get('market', {})
                        broadcast_media = broadcast.get('media', {})

                        broadcast_info = {
                            "type": broadcast_type.get('shortName', '') if isinstance(broadcast_type, dict) else '',
                            "market": broadcast_market.get('type', '') if isinstance(broadcast_market, dict) else '',
                            "media": broadcast_media.get('shortName', '') if isinstance(broadcast_media, dict) else '',
                            "lang": broadcast.get('lang', ''),
                            "region": broadcast.get('region', '')
                        }
                        details["broadcasts"].append(broadcast_info)

                    logger.debug(f"Game {game_id}: Extracted {len(details['broadcasts'])} broadcasts")

                # Extract groups (conference) information
                groups = competition.get('groups', {})
                if groups and isinstance(groups, dict):
                    details["groups"] = {
                        "id": groups.get('id', ''),
                        "name": groups.get('name', ''),
                        "abbreviation": groups.get('abbreviation', ''),
                        "short_name": groups.get('shortName', ''),
                        "midsize_name": groups.get('midsizeName', '')
                    }
                    logger.debug(f"Game {game_id}: Groups info extracted, name={details['groups']['name']}")

    # Try to enhance team data with boxscore information if available
    boxscore = game_data.get('boxscore', {})
    if boxscore and isinstance(boxscore, dict):
        boxscore_teams = boxscore.get('teams', [])
        if boxscore_teams and isinstance(boxscore_teams, list):
            # If we have no teams from header, create them from boxscore
            if not details["teams"]:
                for box_team in boxscore_teams:
                    if not isinstance(box_team, dict):
                        continue

                    team = box_team.get('team', {})
                    if not team or not isinstance(team, dict):
                        continue

                    team_info = {
                        "id": team.get('id', ''),
                        "display_name": team.get('displayName', ''),
                        "abbreviation": team.get('abbreviation', ''),
                        "location": team.get('location', ''),
                        "name": team.get('name', ''),
                        "color": team.get('color', ''),
                        "home_away": box_team.get('homeAway', ''),
                        "winner": False,  # Can't determine from boxscore alone
                        "score": 0  # Will need to calculate from statistics
                    }
                    details["teams"].append(team_info)

                logger.debug(f"Game {game_id}: Created {len(details['teams'])} teams from boxscore")

            # Otherwise, enhance existing teams with additional boxscore data
            else:
                # This would be a place to add more detailed team statistics if needed
                pass

    # Extract venue information from gameInfo
    game_info = game_data.get('gameInfo', {})
    if game_info and isinstance(game_info, dict):
        venue = game_info.get('venue', {})
        if venue and isinstance(venue, dict):
            details["venue_id"] = venue.get('id')
            details["venue_name"] = venue.get('fullName')
            logger.debug(f"Game {game_id}: Venue extracted: {details['venue_name']}")

            # Get venue location
            address = venue.get('address', {})
            if address and isinstance(address, dict):
                city = address.get('city', '')
                state = address.get('state', '')
                details["venue_city"] = city
                details["venue_state"] = state
                if city and state:
                    details["venue_location"] = f"{city}, {state}"
                logger.debug(f"Game {game_id}: Venue location: {details['venue_location']}")

        # Extract attendance
        attendance = game_info.get('attendance')
        details["attendance"] = attendance
        logger.debug(f"Game {game_id}: Attendance: {attendance}")

        # Extract officials/referees
        officials = game_info.get('officials', [])
        if officials and isinstance(officials, list):
            for official in officials:
                if not isinstance(official, dict):
                    continue

                position = official.get('position', {})
                official_info = {
                    "name": official.get('fullName', ''),
                    "display_name": official.get('displayName', ''),
                    "position": position.get('displayName', '') if isinstance(position, dict) else '',
                    "position_id": position.get('id', '') if isinstance(position, dict) else '',
                    "order": official.get('order', 0)
                }
                details["officials"].append(official_info)

            logger.debug(f"Game {game_id}: Extracted {len(details['officials'])} officials")

    # Extract format information
    format_data = game_data.get('format')
    if format_data:
        details["format"] = format_data
        logger.debug(f"Game {game_id}: Format extracted")

    logger.debug(f"Game {game_id}: Game details extraction complete")
    return details


def process_game_data(game_id: str, season: int, force: bool = False) -> Dict[str, Any]:
    """
    Process game data into structured format with game info, team stats, and play-by-play data.
    
    Args:
        game_id: The ESPN game ID
        season: The season this game belongs to
        force: If True, force reprocessing even if processed data exists
        
    Returns:
        Dictionary containing processed game data structures
    """
    # We no longer check for individual files since we don't save them anymore

    try:
        # Get raw game data
        data_path = get_games_dir(season) / f"{game_id}.json"
        if not data_path.exists():
            logger.warning(f"Game data for {game_id} in season {season} not found. Fetching it now.")
            game_data = get_game_data(game_id, season)
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
            game_details = get_game_details(game_data)

            logger.debug(f"Game {game_id}: Game details extracted, building game_info")

            game_info = {
                "game_id":
                    game_id,
                "date":
                    game_details["date"],
                "venue_id":
                    game_details["venue_id"],
                "venue":
                    game_details["venue_name"],
                "venue_location":
                    game_details["venue_location"],
                "venue_city":
                    game_details["venue_city"],
                "venue_state":
                    game_details["venue_state"],
                "attendance":
                    game_details["attendance"],
                "status": (game_details.get("status", {}).get("description", "") or
                           game_details.get("status", {}).get("short_detail", "") or
                           game_details.get("status", {}).get("name", "")),
                "state":
                    game_details.get("status", {}).get("state", ""),
                "neutral_site":
                    game_details.get("neutral_site", False),
                "format":
                    game_details.get("format", None),
                "completed":
                    game_details.get("status", {}).get("completed", False),
                "broadcast":
                    ", ".join([b.get("media", "") for b in game_details.get("broadcasts", []) if b.get("media")]),
                "broadcast_market":
                    ", ".join([b.get("market", "") for b in game_details.get("broadcasts", []) if b.get("market")]),
                "conference":
                    game_details.get("groups", {}).get("name", ""),
            }

            logger.debug(f"Game {game_id}: Game info built successfully")

            # Process officials/referees
            logger.debug(f"Game {game_id}: Processing officials data")
            for official in game_details.get("officials", []):
                if official is None:
                    logger.warning(f"Game {game_id}: Found None official entry")
                    continue

                official_data = {
                    "game_id": game_id,
                    "name": official.get("name", ""),
                    "display_name": official.get("display_name", ""),
                    "position": official.get("position", ""),
                    "position_id": official.get("position_id", ""),
                    "order": official.get("order", 0)
                }
                officials_data.append(official_data)

            # Process broadcasts data
            logger.debug(f"Game {game_id}: Processing broadcast data")
            for broadcast in game_details.get("broadcasts", []):
                if broadcast is None:
                    logger.warning(f"Game {game_id}: Found None broadcast entry")
                    continue

                broadcast_data = {
                    "game_id": game_id,
                    "type": broadcast.get("type", ""),
                    "market": broadcast.get("market", ""),
                    "media": broadcast.get("media", ""),
                    "lang": broadcast.get("lang", ""),
                    "region": broadcast.get("region", "")
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
                    "team_id": team.get("id", ""),
                    "team_name": team.get("display_name", ""),
                    "team_abbreviation": team.get("abbreviation", ""),
                    "team_location": team.get("location", ""),
                    "team_nickname": team.get("name", ""),
                    "team_color": team.get("color", ""),
                    "home_away": team.get("home_away", ""),
                    "score": team.get("score", 0),
                    "winner": team.get("winner", False),
                }
                teams_info.append(team_info)

            # 3. Extract player statistics
            logger.debug(f"Game {game_id}: Extracting player statistics")
            if game_data.get('boxscore') and game_data['boxscore'].get('players'):
                logger.debug(f"Game {game_id}: Found boxscore.players")
                for team_data in game_data['boxscore']['players']:
                    if not isinstance(team_data, dict):
                        logger.warning(f"Game {game_id}: team_data is not a dictionary")
                        continue

                    team_id = ""
                    team_name = ""
                    team_abbrev = ""

                    if team_data.get('team') and isinstance(team_data['team'], dict):
                        team_id = team_data['team'].get('id', '')
                        team_name = team_data['team'].get('displayName', '')
                        team_abbrev = team_data['team'].get('abbreviation', '')
                    else:
                        logger.warning(f"Game {game_id}: team is None or not a dictionary")

                    # Process each statistic group
                    if team_data.get('statistics'):
                        logger.debug(f"Game {game_id}: Processing team statistics for {team_name}")
                        for stat_group in team_data['statistics']:
                            if not isinstance(stat_group, dict):
                                logger.warning(f"Game {game_id}: stat_group is not a dictionary")
                                continue

                            # Get stat keys and labels
                            stat_keys = stat_group.get('keys', [])
                            stat_labels = stat_group.get('names', []) or stat_group.get('labels', [])

                            if not stat_keys:
                                logger.warning(f"Game {game_id}: Empty stat_keys")

                            # Process each player
                            if stat_group.get('athletes') and isinstance(stat_group['athletes'], list):
                                for athlete in stat_group['athletes']:
                                    if not isinstance(athlete, dict):
                                        logger.warning(f"Game {game_id}: athlete is not a dictionary")
                                        continue

                                    # Get player info
                                    player_id = ""
                                    player_name = ""
                                    player_position = ""
                                    player_jersey = ""
                                    starter = False
                                    dnp = False

                                    # Extract player info
                                    if athlete.get('athlete') and isinstance(athlete['athlete'], dict):
                                        player_id = athlete['athlete'].get('id', '')
                                        player_name = athlete['athlete'].get('displayName', '')

                                        if athlete['athlete'].get('position') and isinstance(
                                                athlete['athlete']['position'], dict):
                                            player_position = athlete['athlete']['position'].get('abbreviation', '')

                                        player_jersey = athlete['athlete'].get('jersey', '')
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
                                            player_record[key] = stat_values[i]

                                    player_stats.append(player_record)
                            else:
                                logger.warning(f"Game {game_id}: No athletes or not a list in stat_group")
                    else:
                        logger.warning(f"Game {game_id}: No statistics in team_data")

            # 4. Extract team box score statistics
            if 'boxscore' in game_data and 'teams' in game_data['boxscore']:
                for team_data in game_data['boxscore']['teams']:
                    if not isinstance(team_data, dict):
                        continue

                    team_id = ""
                    team_name = ""
                    team_abbrev = ""
                    home_away = ""

                    if 'team' in team_data and isinstance(team_data['team'], dict):
                        team_id = team_data['team'].get('id', '')
                        team_name = team_data['team'].get('displayName', '')
                        team_abbrev = team_data['team'].get('abbreviation', '')

                    home_away = team_data.get('homeAway', '')

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
                            display_value = stat.get('displayValue', '')

                            # Directly use abbreviation if available, fall back to label or name
                            column_name = stat.get('abbreviation', '') or stat.get('label', '') or stat.get('name', '')

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
                                    pass

                            # Convert numeric values
                            elif display_value.replace('.', '', 1).isdigit():
                                try:
                                    if '.' in display_value:
                                        team_record[column_name] = float(display_value)
                                    else:
                                        team_record[column_name] = int(display_value)
                                except (ValueError, TypeError):
                                    pass

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

                for play in game_data['plays']:
                    if not isinstance(play, dict):
                        continue

                    play_info = {
                        "game_id":
                            game_id,
                        "play_id":
                            play.get("id", ""),
                        "sequence_number":
                            play.get("sequenceNumber", ""),
                        "period":
                            play.get("period", {}).get("number", "") if isinstance(play.get("period"), dict) else "",
                        "period_display":
                            play.get("period", {}).get("displayValue", "")
                            if isinstance(play.get("period"), dict) else "",
                        "clock":
                            play.get("clock", {}).get("displayValue", "")
                            if isinstance(play.get("clock"), dict) else "",
                        "clock_seconds":
                            convert_clock_to_seconds(
                                play.get("clock", {}).get("displayValue", "") if isinstance(play.get("clock"), dict
                                                                                           ) else ""),
                        "team_id":
                            play.get("team", {}).get("id", "")
                            if 'team' in play and isinstance(play.get("team"), dict) else "",
                        "team_name":
                            play.get("team", {}).get("name", "")
                            if 'team' in play and isinstance(play.get("team"), dict) else "",
                        "play_type":
                            play.get("type", {}).get("text", "") if isinstance(play.get("type"), dict) else "",
                        "play_type_id":
                            play.get("type", {}).get("id", "") if isinstance(play.get("type"), dict) else "",
                        "text":
                            play.get("text", ""),
                        "score_home":
                            play.get("homeScore", ""),
                        "score_away":
                            play.get("awayScore", ""),
                        "scoring_play":
                            play.get("scoringPlay", False),
                        "score_value":
                            play.get("scoreValue", 0),
                        "shooting_play":
                            play.get("shootingPlay", False),
                        "coordinate_x":
                            play.get("coordinate", {}).get("x", "")
                            if 'coordinate' in play and isinstance(play.get("coordinate"), dict) else "",
                        "coordinate_y":
                            play.get("coordinate", {}).get("y", "")
                            if 'coordinate' in play and isinstance(play.get("coordinate"), dict) else "",
                        "wallclock":
                            play.get("wallclock", ""),
                    }

                    # Add win probability data if available for this play
                    play_id = play.get("id", "")
                    if play_id in win_prob_mapping:
                        play_info["home_win_percentage"] = win_prob_mapping[play_id]["home_win_percentage"]
                        play_info["away_win_percentage"] = 1.0 - win_prob_mapping[play_id][
                            "home_win_percentage"] if win_prob_mapping[play_id][
                                "home_win_percentage"] is not None else None
                        play_info["tie_percentage"] = win_prob_mapping[play_id]["tie_percentage"]

                    # Add player information if available
                    if 'participants' in play and isinstance(play['participants'], list):
                        for i, participant in enumerate(play["participants"]):
                            if not isinstance(participant, dict):
                                continue

                            player_id = participant.get("athlete", {}).get("id", "") if isinstance(
                                participant.get("athlete"), dict) else ""
                            play_info[f"player_{i+1}_id"] = player_id

                            # Try to get player name - might need to be resolved later
                            player_name = participant.get("athlete", {}).get("displayName", "") if isinstance(
                                participant.get("athlete"), dict) else ""
                            play_info[f"player_{i+1}_name"] = player_name

                            # Get role
                            play_info[f"player_{i+1}_role"] = participant.get("type", {}).get("text", "") if isinstance(
                                participant.get("type"), dict) else ""

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

        logger.info(f"Successfully processed data for game {game_id} in season {season}")

        return result

    except Exception as e:
        logger.error(f"Error processing game {game_id} in season {season}: {str(e)}")
        return {"game_id": game_id, "season": season, "processed": False, "error": str(e)}


def process_game_with_season(args):
    """
    Helper function to unpack arguments for process_game_data.
    
    Args:
        args: Tuple of (game_id, season, force)
    
    Returns:
        Result of process_game_data with processed data structures
    """
    game_id, season, force = args
    return process_game_data(game_id, season, force)


def process_all_games(season: int, max_workers: int = 4, force: bool = False) -> Dict[str, pd.DataFrame]:
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

    if game_ids:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Create argument list for each game
            args_list = [(game_id, season, force) for game_id in game_ids]

            # Process games in parallel
            for result in executor.map(process_game_with_season, args_list):
                results.append(result)

                # For successful processing, collect the data for consolidation
                if result.get("processed", False) and "data" in result:
                    # Collect each data type
                    for data_type, df in result["data"].items():
                        if not df.empty:
                            game_results[data_type].append(df)

    # Create and save game summary dataframe for this season
    processed_games = [r for r in results if r.get("processed", False)]
    logger.info(f"Successfully processed {len(processed_games)} of {len(game_ids)} games")

    # Save the summary to both CSV and Parquet formats
    data = [{
        "game_id": r.get("game_id", ""),
        "season": season,
        "processed": r.get("processed", False),
        "error": r.get("error", "")
    } for r in results]
    summary_df = pd.DataFrame(data)

    # Ensure directories exist
    csv_season_dir = get_csv_season_dir(season)
    parquet_season_dir = get_parquet_season_dir(season)
    os.makedirs(csv_season_dir, exist_ok=True)
    os.makedirs(parquet_season_dir, exist_ok=True)

    # Save summary files
    summary_df.to_csv(csv_season_dir / "game_summary.csv", index=False)
    summary_df.to_parquet(parquet_season_dir / "game_summary.parquet", index=False)

    # Create consolidated DataFrames and save to files
    consolidated_dfs = {}
    for data_type, dataframes in game_results.items():
        if dataframes:
            try:
                # Concatenate all dataframes for this data type
                combined_df = pd.concat(dataframes, ignore_index=True)
                consolidated_dfs[data_type] = combined_df

                # Save as CSV and Parquet
                combined_df.to_csv(csv_season_dir / f"{data_type}.csv", index=False)
                combined_df.to_parquet(parquet_season_dir / f"{data_type}.parquet", index=False)

                logger.info(f"Saved consolidated {data_type} data with {len(combined_df)} records for season {season}")
            except Exception as e:
                logger.error(f"Error saving consolidated {data_type} data for season {season}: {str(e)}")
                consolidated_dfs[data_type] = pd.DataFrame()

    return {"game_summary": summary_df, **consolidated_dfs}


def process_schedules(season: int, force: bool = False) -> pd.DataFrame:
    """
    Process schedules for all teams for a specific season.
    
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
        return pd.read_csv(csv_schedules_file)

    # Get all schedule files for this season
    schedules_dir = get_schedules_dir(season)
    if not schedules_dir.exists():
        logger.warning(f"No schedules directory found for season {season}")
        return pd.DataFrame()

    schedule_files = list(schedules_dir.glob("*.json"))

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
            event_id = event.get("id", "")
            event_date = event.get("date", "")

            # Use string manipulation for dates - some entries might not follow ISO format
            if event_date:
                try:
                    # Try to parse the date string
                    dt = datetime.fromisoformat(event_date.replace('Z', '+00:00'))
                    event_date = dt.date().isoformat()
                except (ValueError, TypeError):
                    # If parsing fails, just use the string as is
                    pass

            for competition in event.get("competitions", []):
                game_id = competition.get("id", "")

                for team_data in competition.get("competitors", []):
                    opponent_id = team_data.get("id", "")

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

    if not schedules_df.empty:
        # Save to CSV and Parquet in the season directory
        os.makedirs(csv_season_dir, exist_ok=True)

        parquet_season_dir = get_parquet_season_dir(season)
        os.makedirs(parquet_season_dir, exist_ok=True)

        # Save files
        schedules_df.to_csv(csv_schedules_file, index=False)
        schedules_df.to_parquet(parquet_season_dir / "schedules.parquet", index=False)
    else:
        logger.warning(f"No schedule data to save for season {season}")

    return schedules_df


def process_season_data(season: int, max_workers: int = 4, force: bool = False) -> Dict[str, Any]:
    """
    Process all data for a specific season.
    
    Args:
        season: The season year to process
        max_workers: Maximum number of concurrent processes
        force: If True, force reprocessing even if processed files exist
        
    Returns:
        Dictionary with processing summary
    """
    logger.info(f"Processing data for season {season}")

    # Process schedules for this season
    schedules_df = process_schedules(season, force=force)

    # Process games for this season
    game_summary = process_all_games(season, max_workers=max_workers, force=force)

    # Create summary statistics
    summary = {
        "schedules_count":
            len(schedules_df),
        "games_count":
            len(game_summary["game_summary"]),
        "processed_games_count": (game_summary["game_summary"]["processed"].sum()
                                  if "processed" in game_summary["game_summary"].columns else 0)
    }

    # Log summary
    logger.info(f"Season {season}: {summary['schedules_count']} schedule entries, "
                f"{summary['games_count']} games, {summary['processed_games_count']} processed games")

    return summary


def process_all_data(seasons: Optional[List[int]] = None,
                     max_workers: int = 4,
                     gender: str = None,
                     force: bool = False) -> None:
    """
    Process all ESPN data for specified seasons.
    
    Args:
        seasons: List of seasons to process (default: DEFAULT_SEASONS)
        max_workers: Maximum concurrent processes for parallel processing
        gender: Either "mens" or "womens" (if None, uses current setting)
        force: If True, force reprocessing even if processed files exist
    """
    if gender:
        set_gender(gender)

    logger.info(f"Starting data processing for {get_current_gender()} basketball")

    if seasons is None:
        seasons = DEFAULT_SEASONS

    logger.info(f"Processing data for seasons {seasons}")

    # First, process teams data
    process_teams_data(force)

    # Then process each season
    for season in seasons:
        process_season_data(season, max_workers=max_workers, force=force)

    logger.info(f"Data saved in: {get_processed_dir()}")


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
