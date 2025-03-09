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

from espn_data.utils import (load_json, save_json, get_teams_file, get_schedules_dir, get_games_dir, get_processed_dir,
                             get_csv_dir, get_parquet_dir, get_csv_teams_file, get_parquet_teams_file,
                             get_csv_season_dir, get_parquet_season_dir, get_csv_games_dir, get_parquet_games_dir)
from espn_data.scraper import get_game_data

logger = logging.getLogger("espn_data")

# Ensure only the base directories exist, season dirs will be created as needed
# os.makedirs(PROCESSED_DIR, exist_ok=True)
# os.makedirs(CSV_DIR, exist_ok=True)
# os.makedirs(PARQUET_DIR, exist_ok=True)
# os.makedirs(CSV_GAMES_DIR, exist_ok=True)
# os.makedirs(PARQUET_GAMES_DIR, exist_ok=True)


def process_teams_data() -> pd.DataFrame:
    """
    Process teams data into a structured DataFrame.
    
    Returns:
        DataFrame with team information
    """
    logger.info("Processing teams data")

    # Load from the top-level teams file
    teams_data = load_json(get_teams_file())
    if not teams_data:
        logger.error("No teams data found")
        return pd.DataFrame()

    # Extract relevant fields
    teams_list = []
    for team_entry in teams_data:
        try:
            # Handle nested 'team' structure in the raw data
            team = team_entry.get("team", team_entry)

            team_dict = {
                "team_id": team.get("id", ""),
                "uid": team.get("uid", ""),
                "slug": team.get("slug", ""),
                "abbreviation": team.get("abbreviation", ""),
                "display_name": team.get("displayName", ""),
                "short_display_name": team.get("shortDisplayName", ""),
                "name": team.get("name", ""),
                "nickname": team.get("nickname", ""),
                "location": team.get("location", ""),
                "color": team.get("color", ""),
                "alternate_color": team.get("alternateColor", ""),
                "is_active": team.get("isActive", True),
                "is_all_star": team.get("isAllStar", False),
                "logo": team.get("logos", [{}])[0].get("href", "") if team.get("logos") else "",
            }
            teams_list.append(team_dict)
        except Exception as e:
            logger.error(f"Error processing team: {e}")

    # Convert to DataFrame
    teams_df = pd.DataFrame(teams_list)

    # Save to CSV and Parquet at the top level
    if not teams_df.empty:
        teams_df.to_csv(get_csv_teams_file(), index=False)
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
    details = {
        "date": None,
        "venue_name": None,
        "venue_location": None,
        "attendance": None,
        "teams": [],
        "officials": [],  # Added officials list
        "format": None,  # Added format information
        "status": None,  # Added status field
        "broadcasts": [],  # Added broadcasts field
        "groups": None,  # Added groups field
    }

    if not game_data:
        return details

    # Extract date from competitions array
    if 'header' in game_data and 'competitions' in game_data['header'] and game_data['header']['competitions']:
        competition = game_data['header']['competitions'][0]
        details["date"] = competition.get('date')

        # Extract team information from competitions
        if 'competitors' in competition:
            for competitor in competition['competitors']:
                if 'team' in competitor:
                    team_info = {
                        "id": competitor['team'].get('id', ''),
                        "display_name": competitor['team'].get('displayName', ''),
                        "abbreviation": competitor['team'].get('abbreviation', ''),
                        "location": competitor['team'].get('location', ''),
                        "name": competitor['team'].get('name', ''),
                        "color": competitor['team'].get('color', ''),
                        "home_away": competitor.get('homeAway', ''),
                        "winner": competitor.get('winner', False),
                        "score": competitor.get('score', 0)
                    }
                    details["teams"].append(team_info)

        # Extract status information
        if 'status' in competition and 'type' in competition['status']:
            status_type = competition['status']['type']
            details["status"] = {
                "id": status_type.get('id', ''),
                "name": status_type.get('name', ''),
                "state": status_type.get('state', ''),
                "completed": status_type.get('completed', False),
                "description": status_type.get('description', ''),
                "detail": status_type.get('detail', ''),
                "short_detail": status_type.get('shortDetail', '')
            }

        # Extract broadcasts information
        if 'broadcasts' in competition and isinstance(competition['broadcasts'], list):
            for broadcast in competition['broadcasts']:
                broadcast_info = {
                    "type": broadcast.get('type', {}).get('shortName', ''),
                    "market": broadcast.get('market', {}).get('type', ''),
                    "media": broadcast.get('media', {}).get('shortName', ''),
                    "lang": broadcast.get('lang', ''),
                    "region": broadcast.get('region', '')
                }
                details["broadcasts"].append(broadcast_info)

        # Extract groups (conference) information
        if 'groups' in competition and isinstance(competition['groups'], dict):
            groups = competition['groups']
            details["groups"] = {
                "id": groups.get('id', ''),
                "name": groups.get('name', ''),
                "abbreviation": groups.get('abbreviation', ''),
                "short_name": groups.get('shortName', ''),
                "midsize_name": groups.get('midsizeName', '')
            }

    # Extract venue information from gameInfo
    if 'gameInfo' in game_data and 'venue' in game_data['gameInfo']:
        venue = game_data['gameInfo']['venue']
        details["venue_name"] = venue.get('fullName')

        # Get venue location
        if 'address' in venue:
            city = venue['address'].get('city', '')
            state = venue['address'].get('state', '')
            if city and state:
                details["venue_location"] = f"{city}, {state}"

    # Extract attendance
    if 'gameInfo' in game_data and 'attendance' in game_data['gameInfo']:
        details["attendance"] = game_data['gameInfo']['attendance']

    # Extract officials/referees
    if 'gameInfo' in game_data and 'officials' in game_data['gameInfo']:
        for official in game_data['gameInfo']['officials']:
            official_info = {
                "name": official.get('fullName', ''),
                "display_name": official.get('displayName', ''),
                "position": official.get('position', {}).get('displayName', ''),
                "position_id": official.get('position', {}).get('id', ''),
                "order": official.get('order', 0)
            }
            details["officials"].append(official_info)

    # Extract format information
    if 'format' in game_data:
        details["format"] = game_data["format"]

    return details


def process_game_data(game_id: str, season: int) -> Dict[str, Any]:
    """
    Process game data into structured format with game info, team stats, and play-by-play data.
    For each game, saves individual files for each data type.
    
    Args:
        game_id: The ESPN game ID
        season: The season this game belongs to
        
    Returns:
        Dictionary containing processed game data (also saved to individual files)
    """
    try:
        # Get raw game data
        data_path = get_games_dir(season) / f"{game_id}.json"
        if not data_path.exists():
            logger.warning(f"Game data for {game_id} in season {season} not found. Fetching it now.")
            game_data = get_game_data(game_id, season)
        else:
            game_data = load_json(data_path)

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
            game_details = get_game_details(game_data)

            game_info = {
                "game_id":
                    game_id,
                "date":
                    game_details["date"],
                "venue":
                    game_details["venue_name"],
                "venue_location":
                    game_details["venue_location"],
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
                "conference":
                    game_details.get("groups", {}).get("name", ""),
            }

            # Process officials/referees
            for official in game_details.get("officials", []):
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
            for broadcast in game_details.get("broadcasts", []):
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
            for team in game_details["teams"]:
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
            if 'boxscore' in game_data and 'players' in game_data['boxscore']:
                for team_data in game_data['boxscore']['players']:
                    if not isinstance(team_data, dict):
                        continue

                    team_id = ""
                    team_name = ""
                    team_abbrev = ""

                    if 'team' in team_data and isinstance(team_data['team'], dict):
                        team_id = team_data['team'].get('id', '')
                        team_name = team_data['team'].get('displayName', '')
                        team_abbrev = team_data['team'].get('abbreviation', '')

                    # Process each statistic group
                    if 'statistics' in team_data:
                        for stat_group in team_data['statistics']:
                            if not isinstance(stat_group, dict):
                                continue

                            # Get stat keys and labels
                            stat_keys = stat_group.get('keys', [])
                            stat_labels = stat_group.get('names', []) or stat_group.get('labels', [])

                            # Process each player
                            if 'athletes' in stat_group and isinstance(stat_group['athletes'], list):
                                for athlete in stat_group['athletes']:
                                    if not isinstance(athlete, dict):
                                        continue

                                    # Get player info
                                    player_id = ""
                                    player_name = ""
                                    player_position = ""
                                    player_jersey = ""
                                    starter = False
                                    dnp = False

                                    if 'athlete' in athlete and isinstance(athlete['athlete'], dict):
                                        player_id = athlete['athlete'].get('id', '')
                                        player_name = athlete['athlete'].get('displayName', '')
                                        player_jersey = athlete['athlete'].get('jersey', '')

                                        if 'position' in athlete['athlete'] and isinstance(
                                                athlete['athlete']['position'], dict):
                                            player_position = athlete['athlete']['position'].get('displayName', '')

                                    starter = athlete.get('starter', False)
                                    dnp = athlete.get('didNotPlay', False)

                                    # Create basic player record
                                    player_record = {
                                        "game_id": game_id,
                                        "team_id": team_id,
                                        "team_name": team_name,
                                        "team_abbreviation": team_abbrev,
                                        "player_id": player_id,
                                        "player_name": player_name,
                                        "position": player_position,
                                        "jersey": player_jersey,
                                        "starter": starter,
                                        "did_not_play": dnp,
                                    }

                                    # Add stats
                                    stats = athlete.get('stats', [])
                                    if stats and len(stats) == len(stat_labels):
                                        for i, stat_value in enumerate(stats):
                                            # Handle a variety of stat formats
                                            stat_label = stat_labels[i] if i < len(stat_labels) else f"stat_{i}"

                                            # Parse stats like "4-12" into made and attempted
                                            if '-' in str(stat_value) and '/' not in str(stat_value):
                                                # Handle stats like FG: "4-12", 3PT: "0-4", FT: "8-10"
                                                try:
                                                    made, attempted = stat_value.split('-')
                                                    player_record[
                                                        stat_label] = stat_value  # Store original for reference
                                                    player_record[f"{stat_label}_MADE"] = int(made)
                                                    player_record[f"{stat_label}_ATT"] = int(attempted)

                                                    # Calculate percentage for common shooting stats
                                                    if stat_label in ['FG', '3PT', 'FT']:
                                                        try:
                                                            pct = round(
                                                                int(made) / int(attempted) *
                                                                100 if int(attempted) > 0 else 0, 1)
                                                            player_record[f"{stat_label}_PCT"] = pct
                                                        except (ValueError, ZeroDivisionError):
                                                            player_record[f"{stat_label}_PCT"] = 0
                                                except (ValueError, AttributeError):
                                                    player_record[stat_label] = stat_value
                                            else:
                                                # Handle numerical stats
                                                try:
                                                    # Convert stats to appropriate type if possible
                                                    if stat_value.replace('.', '', 1).isdigit():
                                                        # It's a number or decimal
                                                        if '.' in stat_value:
                                                            player_record[stat_label] = float(stat_value)
                                                        else:
                                                            player_record[stat_label] = int(stat_value)
                                                    else:
                                                        player_record[stat_label] = stat_value
                                                except (ValueError, AttributeError):
                                                    player_record[stat_label] = stat_value

                                    player_stats.append(player_record)

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

        # Save individual files for each data type in separate format directories
        csv_game_dir = get_csv_games_dir(season) / game_id
        parquet_game_dir = get_parquet_games_dir(season) / game_id

        os.makedirs(csv_game_dir, exist_ok=True)
        os.makedirs(parquet_game_dir, exist_ok=True)

        # Save as separate CSV and Parquet
        if game_info:
            game_df = pd.DataFrame([game_info])
            game_df.to_csv(csv_game_dir / "game_info.csv", index=False)
            game_df.to_parquet(parquet_game_dir / "game_info.parquet", index=False)

        if teams_info:
            teams_df = pd.DataFrame(teams_info)
            teams_df.to_csv(csv_game_dir / "teams_info.csv", index=False)
            teams_df.to_parquet(parquet_game_dir / "teams_info.parquet", index=False)

        if player_stats:
            players_df = pd.DataFrame(player_stats)
            players_df.to_csv(csv_game_dir / "player_stats.csv", index=False)
            players_df.to_parquet(parquet_game_dir / "player_stats.parquet", index=False)

        if team_stats:
            team_stats_df = pd.DataFrame(team_stats)
            team_stats_df.to_csv(csv_game_dir / "team_stats.csv", index=False)
            team_stats_df.to_parquet(parquet_game_dir / "team_stats.parquet", index=False)

        if play_by_play:
            pbp_df = pd.DataFrame(play_by_play)
            pbp_df.to_csv(csv_game_dir / "play_by_play.csv", index=False)
            pbp_df.to_parquet(parquet_game_dir / "play_by_play.parquet", index=False)

        # Save officials data
        if officials_data:
            officials_df = pd.DataFrame(officials_data)
            officials_df.to_csv(csv_game_dir / "officials.csv", index=False)
            officials_df.to_parquet(parquet_game_dir / "officials.parquet", index=False)

        # Save broadcasts data
        if broadcasts_data:
            broadcasts_df = pd.DataFrame(broadcasts_data)
            broadcasts_df.to_csv(csv_game_dir / "broadcasts.csv", index=False)
            broadcasts_df.to_parquet(parquet_game_dir / "broadcasts.parquet", index=False)

        logger.info(f"Successfully processed and saved data for game {game_id} in season {season}")

        # Return all processed data for backward compatibility
        return {
            "game_info": game_info,
            "teams_info": teams_info,
            "player_stats": player_stats,
            "team_stats": team_stats,
            "play_by_play": play_by_play,
            "officials": officials_data,
            "broadcasts": broadcasts_data
        }

    except Exception as e:
        logger.error(f"Error processing game {game_id} in season {season}: {e}")
        return {}


def process_game_with_season(args):
    """Helper function to unpack arguments for process_game_data to work with multiprocessing."""
    game_id, season = args
    return process_game_data(game_id, season)


def process_all_games(season: int, max_workers: int = 4) -> Dict[str, pd.DataFrame]:
    """
    Process all downloaded game data for a specific season.
    Each game data is processed and saved to individual files.
    
    Args:
        season: The season to process
        max_workers: Maximum number of concurrent processes
        
    Returns:
        Dictionary of summary DataFrames
    """
    logger.info(f"Processing all game data for season {season}")

    # Find all game files for this season
    games_dir = get_games_dir(season)
    if not games_dir.exists():
        logger.warning(f"No games directory found for season {season}")
        return {"game_summary": pd.DataFrame()}

    game_files = list(games_dir.glob("*.json"))
    game_ids = [f.stem for f in game_files]

    logger.info(f"Found {len(game_ids)} games to process for season {season}")

    # Process games in parallel - each game will save its own files
    game_data_with_season = [(game_id, season) for game_id in game_ids]

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Use the helper function instead of a lambda
        results = list(
            tqdm(executor.map(process_game_with_season, game_data_with_season),
                 total=len(game_data_with_season),
                 desc=f"Processing games for {season}"))

    # Create summary DataFrames (small, just references to the individual files)
    summary_games = []

    for game_id in game_ids:
        csv_game_path = get_csv_games_dir(season) / game_id
        parquet_game_path = get_parquet_games_dir(season) / game_id

        if (parquet_game_path / "game_info.parquet").exists():
            summary_games.append({
                "game_id": game_id,
                "season": season,
                "processed": True,
                "csv_path": str(csv_game_path),
                "parquet_path": str(parquet_game_path),
                "has_game_info": (parquet_game_path / "game_info.parquet").exists(),
                "has_teams_info": (parquet_game_path / "teams_info.parquet").exists(),
                "has_player_stats": (parquet_game_path / "player_stats.parquet").exists(),
                "has_team_stats": (parquet_game_path / "team_stats.parquet").exists(),
                "has_play_by_play": (parquet_game_path / "play_by_play.parquet").exists(),
                "has_officials": (parquet_game_path / "officials.parquet").exists(),
            })
        else:
            summary_games.append({
                "game_id": game_id,
                "season": season,
                "processed": False,
                "csv_path": str(csv_game_path),
                "parquet_path": str(parquet_game_path),
            })

    # Create and save game summary dataframe for this season
    summary_df = pd.DataFrame(summary_games)

    # Save the summary to both CSV and Parquet formats
    csv_season_dir = get_csv_season_dir(season)
    parquet_season_dir = get_parquet_season_dir(season)

    os.makedirs(csv_season_dir, exist_ok=True)
    os.makedirs(parquet_season_dir, exist_ok=True)

    if not summary_df.empty:
        summary_df.to_csv(csv_season_dir / "game_summary.csv", index=False)
        summary_df.to_parquet(parquet_season_dir / "game_summary.parquet", index=False)

    logger.info(
        f"Processed {len(summary_df)} games for season {season}, created individual game files and summary index")

    return {"game_summary": summary_df}


def process_schedules(season: int) -> pd.DataFrame:
    """
    Process schedule data into a structured DataFrame for a specific season.
    
    Args:
        season: The season to process
        
    Returns:
        DataFrame with schedule information
    """
    logger.info(f"Processing schedule data for season {season}")

    all_games = []

    # Get the schedules directory for this season
    schedules_dir = get_schedules_dir(season)
    if not schedules_dir.exists():
        logger.warning(f"No schedules directory found for season {season}")
        return pd.DataFrame()

    schedule_files = list(schedules_dir.glob("*.json"))

    for schedule_file in tqdm(schedule_files, desc=f"Processing schedules for season {season}"):
        try:
            team_id = schedule_file.stem
            schedule_data = load_json(schedule_file)

            for game in schedule_data:
                game_info = {
                    "team_id":
                        team_id,
                    "game_id":
                        game.get("id", ""),
                    "season":
                        season,
                    "date":
                        game.get("date", ""),
                    "name":
                        game.get("name", ""),
                    "short_name":
                        game.get("shortName", ""),
                    "is_conference_game":
                        game.get("conference", {}).get("isConference", False) if "conference" in game else False,
                    "neutral_site":
                        game.get("neutralSite", False),
                    "venue_name":
                        game.get("competitions", [{}])[0].get("venue", {}).get("fullName", "")
                        if "competitions" in game and game["competitions"] else "",
                    "venue_city":
                        game.get("competitions", [{}])[0].get("venue", {}).get("address", {}).get("city", "")
                        if "competitions" in game and game["competitions"] else "",
                    "venue_state":
                        game.get("competitions", [{}])[0].get("venue", {}).get("address", {}).get("state", "")
                        if "competitions" in game and game["competitions"] else "",
                    "completed":
                        game.get("status", {}).get("type", {}).get("completed", False)
                        if "status" in game and "type" in game["status"] else False,
                    "away_team_id":
                        game.get("competitions", [{}])[0].get("competitors", [{}])[1].get("id", "")
                        if "competitions" in game and game["competitions"] and
                        len(game["competitions"][0].get("competitors", [])) > 1 else "",
                    "away_team_name":
                        game.get("competitions", [{}])[0].get("competitors", [{}])[1].get("team", {}).get("name", "")
                        if "competitions" in game and game["competitions"] and
                        len(game["competitions"][0].get("competitors", [])) > 1 else "",
                    "away_team_abbrev":
                        game.get("competitions", [{}])[0].get("competitors", [{}])[1].get("team", {}).get(
                            "abbreviation", "") if "competitions" in game and game["competitions"] and
                        len(game["competitions"][0].get("competitors", [])) > 1 else "",
                    "away_score":
                        game.get("competitions", [{}])[0].get("competitors", [{}])[1].get("score", "")
                        if "competitions" in game and game["competitions"] and
                        len(game["competitions"][0].get("competitors", [])) > 1 else "",
                    "home_team_id":
                        game.get("competitions", [{}])[0].get("competitors", [{}])[0].get("id", "")
                        if "competitions" in game and game["competitions"] and
                        len(game["competitions"][0].get("competitors", [])) > 0 else "",
                    "home_team_name":
                        game.get("competitions", [{}])[0].get("competitors", [{}])[0].get("team", {}).get("name", "")
                        if "competitions" in game and game["competitions"] and
                        len(game["competitions"][0].get("competitors", [])) > 0 else "",
                    "home_team_abbrev":
                        game.get("competitions", [{}])[0].get("competitors", [{}])[0].get("team", {}).get(
                            "abbreviation", "") if "competitions" in game and game["competitions"] and
                        len(game["competitions"][0].get("competitors", [])) > 0 else "",
                    "home_score":
                        game.get("competitions", [{}])[0].get("competitors", [{}])[0].get("score", "")
                        if "competitions" in game and game["competitions"] and
                        len(game["competitions"][0].get("competitors", [])) > 0 else "",
                }
                all_games.append(game_info)

        except Exception as e:
            logger.error(f"Error processing schedule for team {schedule_file.stem} in season {season}: {e}")

    # Convert to DataFrame
    schedules_df = pd.DataFrame(all_games)

    # Save to CSV and Parquet in the season directory
    csv_season_dir = get_csv_season_dir(season)
    parquet_season_dir = get_parquet_season_dir(season)

    os.makedirs(csv_season_dir, exist_ok=True)
    os.makedirs(parquet_season_dir, exist_ok=True)

    if not schedules_df.empty:
        schedules_df.to_csv(csv_season_dir / "schedules.csv", index=False)
        schedules_df.to_parquet(parquet_season_dir / "schedules.parquet", index=False)
        logger.info(f"Processed {len(schedules_df)} schedule entries for season {season}")
    else:
        logger.warning(f"No schedule data to save for season {season}")

    return schedules_df


def process_all_data(seasons: Optional[List[int]] = None, max_workers: int = 4) -> None:
    """
    Process all data: teams, schedules, and games for specified seasons.
    
    Args:
        seasons: List of seasons to process
        max_workers: Maximum number of concurrent processes to use
    """
    if seasons is None:
        # Default to recent seasons if none specified
        seasons = [2022, 2023]

    logger.info(f"Processing all data for seasons: {seasons}")

    # Process teams (this is not season-specific)
    teams_df = process_teams_data()

    season_summaries = {}

    # Process each season
    for season in seasons:
        logger.info(f"Processing data for season {season}")

        # Process schedules for this season
        schedules_df = process_schedules(season)

        # Process games for this season
        game_summary = process_all_games(season, max_workers=max_workers)

        season_summaries[season] = {
            "schedules_count":
                len(schedules_df),
            "games_count":
                len(game_summary["game_summary"]),
            "processed_games_count":
                game_summary["game_summary"]["processed"].sum()
                if "processed" in game_summary["game_summary"].columns else 0
        }

    # Log summary of processing
    logger.info("Data processing complete. All files saved.")
    logger.info(f"Processed {len(teams_df)} teams")

    for season, summary in season_summaries.items():
        logger.info(f"Season {season}: {summary['schedules_count']} schedule entries, "
                    f"{summary['games_count']} games, {summary['processed_games_count']} processed games")

    # Log directory information
    logger.info(f"Data saved in: {get_processed_dir()}")
    logger.info(f"CSV files stored in: {get_csv_dir()}")
    logger.info(f"Parquet files stored in: {get_parquet_dir()}")


def main() -> None:
    """Main entry point for the processor."""
    import argparse

    parser = argparse.ArgumentParser(description="Process ESPN women's basketball data")
    parser.add_argument("--seasons", type=int, nargs="+", help="List of seasons to process (e.g., 2022 2023)")
    parser.add_argument("--max-workers",
                        type=int,
                        default=4,
                        help="Maximum number of concurrent processes (default: 4)")

    args = parser.parse_args()

    # Run the processor
    process_all_data(seasons=args.seasons, max_workers=args.max_workers)


if __name__ == "__main__":
    main()
