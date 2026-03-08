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

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
os.makedirs(DATA_DIR, exist_ok=True)

# Mapping from verbose ESPN stat names to standard abbreviations
_VERBOSE_STAT_NAMES = {
    'minutes': 'MIN',
    'offensiveRebounds': 'OREB',
    'defensiveRebounds': 'DREB',
    'rebounds': 'REB',
    'assists': 'AST',
    'steals': 'STL',
    'blocks': 'BLK',
    'turnovers': 'TO',
    'fouls': 'PF',
    'points': 'PTS',
    'Rebounds': 'REB',
    'Offensive Rebounds': 'OREB',
    'Defensive Rebounds': 'DREB',
    'Assists': 'AST',
    'Steals': 'STL',
    'Blocks': 'BLK',
    'Turnovers': 'TO',
    'Fouls': 'PF',
    'OR': 'OREB',
    'DR': 'DREB',
}

# Columns to remove after splitting into _MADE/_ATT/_PCT
_DUPLICATE_PCT_COLUMNS = {'FG%', '3P%', 'FT%'}

# Stat fields that should be NaN for DNP players
_DNP_STAT_FIELDS = [
    'MIN', 'FG_MADE', 'FG_ATT', 'FG_PCT', '3PT_MADE', '3PT_ATT', '3PT_PCT',
    'FT_MADE', 'FT_ATT', 'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL',
    'BLK', 'TO', 'PF', 'PTS',
]

# Shooting stats to split from "made-attempted" format
_SHOOTING_STATS = ['FG', '3PT', 'FT']


# ---------------------------------------------------------------------------
# Small utility helpers
# ---------------------------------------------------------------------------

def _split_shooting_stat(value: str, prefix: str) -> dict:
    """Split a shooting stat like '15-25' into _MADE, _ATT, _PCT fields.

    Returns a dict with keys like FG_MADE, FG_ATT, FG_PCT.
    On failure, returns NaN values.
    """
    try:
        made_s, att_s = value.split('-')
        made, att = int(made_s), int(att_s)
        pct = round(made / att * 100, 1) if att > 0 else 0
        return {f"{prefix}_MADE": made, f"{prefix}_ATT": att, f"{prefix}_PCT": pct}
    except (ValueError, TypeError, ZeroDivisionError):
        return {f"{prefix}_MADE": np.nan, f"{prefix}_ATT": np.nan, f"{prefix}_PCT": np.nan}


def _nan_shooting_stats() -> dict:
    """Return NaN values for all shooting stat fields (for DNP players)."""
    result = {}
    for stat in _SHOOTING_STATS:
        result[f"{stat}_MADE"] = np.nan
        result[f"{stat}_ATT"] = np.nan
        result[f"{stat}_PCT"] = np.nan
    return result


def _get_competition(game_data: dict) -> dict:
    """Get the first competition entry from game data, or empty dict."""
    competitions = game_data.get('header', {}).get('competitions', [])
    if competitions and isinstance(competitions, list):
        return competitions[0]
    return {}


def _extract_game_id(game_data: dict, filename: str = None) -> str:
    """Extract game ID from game data, falling back to filename."""
    if 'gameId' in game_data:
        return game_data['gameId']

    header = game_data.get('header', {})
    if 'id' in header:
        return header['id']

    competition = _get_competition(game_data)
    if 'id' in competition:
        return competition['id']

    if filename:
        basename = os.path.basename(str(filename))
        if basename.endswith('.json'):
            potential_id = os.path.splitext(basename)[0]
            if potential_id and potential_id != 'unknown':
                logger.debug(f"Extracted game_id {potential_id} from filename {filename}")
                return potential_id

    return 'unknown'


def _extract_team_identity(team_data: dict) -> tuple:
    """Extract (team_id, team_name, team_abbrev) from a boxscore team entry."""
    team = team_data.get('team')
    if team and isinstance(team, dict):
        return (team.get('id'), team.get('displayName'), team.get('abbreviation'))
    return (None, None, None)


# ---------------------------------------------------------------------------
# Teams processing
# ---------------------------------------------------------------------------

def process_teams_data(force: bool = False) -> pd.DataFrame:
    """Process teams data into a structured dataframe."""
    csv_teams_file = get_csv_teams_file()
    if not force and csv_teams_file.exists():
        logger.info("Using cached processed teams data")
        return pd.read_csv(csv_teams_file)

    logger.info("Processing teams data")
    teams_file = get_teams_file()

    if not teams_file.exists():
        logger.warning("Teams data file not found")
        return pd.DataFrame()

    teams_data = load_json(teams_file)
    if not teams_data:
        logger.warning("No teams data found")
        return pd.DataFrame()

    teams = []
    for team in teams_data:
        teams.append({
            "id": team.get("id"),
            "slug": team.get("slug"),
            "abbreviation": team.get("abbreviation"),
            "display_name": team.get("displayName"),
            "short_name": team.get("shortDisplayName"),
            "name": team.get("name"),
            "nickname": team.get("nickname"),
            "location": team.get("location"),
            "color": team.get("color"),
            "alternate_color": team.get("alternateColor"),
            "logo": team.get("logos", [{}])[0].get("href") if team.get("logos") else None,
            "conference_id": team.get("conference", {}).get("id") if "conference" in team else None,
            "conference_name": team.get("conference", {}).get("name") if "conference" in team else None,
        })

    teams_df = pd.DataFrame(teams)

    if not teams_df.empty:
        teams_df = optimize_dataframe_dtypes(teams_df, "teams")
        os.makedirs(csv_teams_file.parent, exist_ok=True)
        teams_df.to_csv(csv_teams_file, index=False)
        teams_df.to_parquet(get_parquet_teams_file(), index=False)
        logger.info(f"Processed {len(teams_df)} teams")
    else:
        logger.warning("No teams data to save")

    return teams_df


# ---------------------------------------------------------------------------
# Clock / broadcast helpers
# ---------------------------------------------------------------------------

def convert_clock_to_seconds(clock_str):
    """Convert a clock string (MM:SS) to seconds."""
    if not clock_str or not isinstance(clock_str, str):
        return None
    try:
        parts = clock_str.split(':')
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        return None
    except (ValueError, TypeError):
        return None


def get_broadcasts(game_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Get broadcasts from game data, checking multiple locations."""
    return (game_data.get('broadcasts', [])
            or _get_competition(game_data).get('broadcasts', [])
            or game_data.get('gameInfo', {}).get('broadcasts', []))


def get_primary_broadcast(game_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Get the primary broadcast, preferring national TV."""
    broadcasts = get_broadcasts(game_data)
    if not broadcasts:
        return None

    # Priority: national TV > any TV > any national > first
    for broadcast in broadcasts:
        market_type = broadcast.get('market', {}).get('type', '').lower()
        type_name = broadcast.get('type', {}).get('shortName', '').lower()
        if type_name == 'tv' and market_type == 'national':
            return broadcast

    for broadcast in broadcasts:
        if broadcast.get('type', {}).get('shortName', '').lower() == 'tv':
            return broadcast

    for broadcast in broadcasts:
        if broadcast.get('market', {}).get('type', '').lower() == 'national':
            return broadcast

    return broadcasts[0]


# ---------------------------------------------------------------------------
# Game detail extraction
# ---------------------------------------------------------------------------

def get_game_details(game_data: Dict[str, Any], filename: str = None) -> Dict[str, Any]:
    """Extract key game details from raw API data."""
    game_id = _extract_game_id(game_data, filename)
    competition = _get_competition(game_data)
    group = competition.get('groups', {})

    logger.debug(f"Game {game_id}: Extracting game details")

    game_details = {
        "game_id": game_id,
        "date": game_data.get('date'),
        "venue_id": None,
        "venue_name": None,
        "venue_city": None,
        "venue_state": None,
        "attendance": None,
        "status": None,
        "neutral_site": competition.get('neutralSite', False),
        "format": competition.get('format') or game_data.get('format'),
        "completed": False,
        "broadcast": None,
        "broadcast_market": None,
        "broadcast_type": None,
        "conference": None,
        "teams": [],
        # Boxscore/conference fields
        "boxscore_source": competition.get('boxscoreSource'),
        "boxscore_available": competition.get('boxscoreAvailable'),
        "play_by_play_source": competition.get('playByPlaySource'),
        "is_conference_game": competition.get('conferenceCompetition'),
        "conference_id": group.get('id'),
        "conference_name": group.get('name'),
        "conference_abbreviation": group.get('abbreviation'),
    }

    # Season
    if 'season' in game_data and isinstance(game_data['season'], dict):
        game_details["season"] = game_data['season'].get('year')
    elif isinstance(game_data.get('header'), dict):
        game_details["season"] = game_data['header'].get('season', {}).get('year')

    # Venue
    venue_data = game_data.get('gameInfo', {}).get('venue', {})
    if venue_data:
        game_details["venue_id"] = venue_data.get('id')
        game_details["venue_name"] = venue_data.get('fullName')
        address = venue_data.get('address', {})
        game_details["venue_city"] = address.get('city')
        game_details["venue_state"] = address.get('state')

    # Attendance — check multiple locations
    for source in [
        game_data.get('gameInfo', {}).get('attendance'),
        game_data.get('attendance'),
        game_data.get('boxscore', {}).get('attendance'),
        competition.get('attendance'),
    ]:
        if source is not None:
            game_details["attendance"] = source
            break

    # Status
    status_type = None
    if competition and 'status' in competition:
        status_type = competition['status'].get('type', {})
    elif 'status' in game_data:
        status_type = game_data['status'].get('type', {})

    if status_type and isinstance(status_type, dict):
        game_details["status"] = status_type.get('name')
        game_details["completed"] = status_type.get('completed', False)

    # Broadcast
    broadcast = get_primary_broadcast(game_data)
    if broadcast:
        game_details["broadcast"] = broadcast.get('media', {}).get('shortName')
        game_details["broadcast_market"] = broadcast.get('market', {}).get('type')
        game_details["broadcast_type"] = broadcast.get('type', {}).get('shortName')

    # Teams
    competitors = competition.get('competitors', [])
    if isinstance(competitors, list):
        for team in competitors:
            if not isinstance(team, dict) or not isinstance(team.get('team'), dict):
                continue

            team_obj = team['team']
            groups = team_obj.get("groups", {})
            parent = groups.get("parent", {})

            team_info = {
                "id": team_obj.get('id'),
                "name": team_obj.get('displayName'),
                "abbreviation": team_obj.get('abbreviation'),
                "location": team_obj.get('location'),
                "nickname": team_obj.get('name'),
                "color": team_obj.get('color'),
                "home_away": team.get('homeAway'),
                "score": team.get('score'),
                "winner": team.get('winner', False),
                "groups_slug": groups.get("slug"),
                "conference_id": groups.get("id"),
                "conference_slug": groups.get("slug"),
                "division": parent.get("name"),
            }

            if 'linescores' in team and isinstance(team['linescores'], list):
                team_info['linescores'] = [
                    line.get('displayValue') for line in team['linescores'] if isinstance(line, dict)
                ]

            game_details["teams"].append(team_info)

    # Build team lookup and fill empty team/player names in play-by-play
    team_lookup = {t["id"]: {"name": t["name"], "abbreviation": t["abbreviation"]}
                   for t in game_details["teams"]}

    if 'plays' in game_data and isinstance(game_data['plays'], list):
        for play in game_data['plays']:
            if not isinstance(play, dict) or 'team' not in play:
                continue

            if isinstance(play['team'], dict):
                tid = play['team'].get('id')
                if tid and not play['team'].get('name') and tid in team_lookup:
                    play['team']['name'] = team_lookup[tid]['name']
            elif isinstance(play['team'], str):
                logger.debug(f"Game {game_id}: Found play with team as string: {play['team']}")

            # Fill player names from boxscore if missing
            for player_num in [1, 2]:
                player_key = f'athlete{player_num}'
                athlete = play.get(player_key)
                if not isinstance(athlete, dict):
                    continue
                player_id = athlete.get('id')
                if not player_id or athlete.get('displayName'):
                    continue
                # Search boxscore for this player's name
                for team_players in game_data.get('boxscore', {}).get('players', []):
                    for stat_group in team_players.get('statistics', []):
                        if not isinstance(stat_group, dict):
                            continue
                        for player in stat_group.get('athletes', []):
                            if (isinstance(player, dict) and isinstance(player.get('athlete'), dict)
                                    and player['athlete'].get('id') == player_id):
                                athlete['displayName'] = player['athlete'].get('displayName', '')

    return game_details


# ---------------------------------------------------------------------------
# Extraction helpers for process_game_data
# ---------------------------------------------------------------------------

def _extract_officials(game_id: str, game_data: dict) -> list:
    """Extract officials/referees from game data."""
    officials = []
    for official in game_data.get("gameInfo", {}).get("officials", []):
        if official is None:
            continue
        officials.append({
            "game_id": game_id,
            "name": official.get("fullName"),
            "display_name": official.get("display_name"),
            "position": official.get("position", {}).get("name"),
            "position_id": official.get("position", {}).get("id"),
            "order": official.get("order", 0),
        })
    return officials


def _extract_broadcasts(game_id: str, game_data: dict) -> list:
    """Extract broadcast information from game data."""
    broadcasts = []
    for broadcast in get_broadcasts(game_data):
        if broadcast is None:
            continue
        broadcasts.append({
            "game_id": game_id,
            "type": broadcast.get("type", {}).get("shortName"),
            "market": broadcast.get("market", {}).get("type"),
            "media": broadcast.get("media", {}).get("shortName"),
            "lang": broadcast.get("lang"),
            "region": broadcast.get("region"),
        })
    return broadcasts


def _extract_teams_info(game_id: str, game_details: dict) -> list:
    """Extract team information rows from game details."""
    teams_info = []
    for team in game_details.get("teams", []):
        if team is None:
            continue

        team_info = {
            "game_id": game_id,
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "team_abbreviation": team.get("abbreviation"),
            "team_location": team.get("location"),
            "team_nickname": team.get("nickname"),
            "team_color": team.get("color"),
            "home_away": team.get("home_away"),
            "score": team.get("score"),
            "winner": team.get("winner", False),
            "groups_slug": team.get("groups_slug"),
            "conference_id": team.get("conference_id"),
            "conference_slug": team.get("conference_slug"),
            "division": team.get("division"),
        }

        if 'linescores' in team and isinstance(team['linescores'], list):
            team_info["linescores"] = ','.join(str(s) for s in team['linescores'])

        teams_info.append(team_info)
    return teams_info


def _extract_player_stats(game_id: str, game_data: dict) -> list:
    """Extract per-player statistics from boxscore data."""
    player_stats = []
    boxscore_players = game_data.get('boxscore', {}).get('players', [])

    for team_data in boxscore_players:
        if not isinstance(team_data, dict):
            continue

        team_id, team_name, team_abbrev = _extract_team_identity(team_data)

        for stat_group in team_data.get('statistics', []):
            if not isinstance(stat_group, dict):
                continue

            stat_keys = stat_group.get('keys', [])
            stat_labels = stat_group.get('names', []) or stat_group.get('labels', [])

            for athlete in stat_group.get('athletes', []):
                if not isinstance(athlete, dict):
                    continue

                athlete_info = athlete.get('athlete', {})
                if not isinstance(athlete_info, dict):
                    athlete_info = {}

                position_info = athlete_info.get('position')
                starter = bool(athlete.get('starter'))
                dnp = bool(athlete.get('didNotPlay'))

                record = {
                    "game_id": game_id,
                    "team_id": team_id,
                    "team_name": team_name,
                    "team_abbrev": team_abbrev,
                    "player_id": athlete_info.get('id'),
                    "player_name": athlete_info.get('displayName'),
                    "position": position_info.get('abbreviation') if isinstance(position_info, dict) else None,
                    "jersey": athlete_info.get('jersey'),
                    "starter": starter,
                    "dnp": dnp,
                }

                # Map stat values to labels
                stat_values = athlete.get('stats', [])
                for i, key in enumerate(stat_keys):
                    if i < len(stat_values):
                        col = stat_labels[i] if i < len(stat_labels) else key
                        record[col] = stat_values[i]

                # Split shooting stats (e.g. "15-25" → _MADE, _ATT, _PCT)
                for stat_name in _SHOOTING_STATS:
                    val = record.get(stat_name)
                    if isinstance(val, str) and '-' in val:
                        record.update(_split_shooting_stat(val, stat_name))
                    elif dnp:
                        record.update(_nan_shooting_stats())

                # Rename verbose stat names to standard abbreviations
                for old_name, new_name in _VERBOSE_STAT_NAMES.items():
                    if old_name in record:
                        record[new_name] = record.pop(old_name)

                # Ensure DNP players have NaN for all stat fields
                if dnp:
                    for field in ['MIN', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF', 'PTS']:
                        record[field] = np.nan

                player_stats.append(record)

    return player_stats


def _extract_team_stats(game_id: str, game_data: dict, game_details: dict) -> list:
    """Extract per-team aggregate statistics from boxscore data."""
    team_stats = []
    boxscore_teams = game_data.get('boxscore', {}).get('teams', [])

    for team_data in boxscore_teams:
        if not isinstance(team_data, dict):
            continue

        team_id, team_name, team_abbrev = _extract_team_identity(team_data)

        record = {
            "game_id": game_id,
            "team_id": team_id,
            "team_name": team_name,
            "team_abbreviation": team_abbrev,
            "home_away": team_data.get('homeAway'),
        }

        # Add points from game details
        for team_info in game_details.get("teams", []):
            if team_info.get("id") == team_id:
                try:
                    record["PTS"] = int(team_info.get("score", 0))
                except (ValueError, TypeError):
                    record["PTS"] = 0
                break

        # Process each stat
        for stat in team_data.get('statistics', []):
            if not isinstance(stat, dict):
                continue

            display_value = stat.get('displayValue')
            column_name = stat.get('abbreviation') or stat.get('label') or stat.get('name')

            if not column_name or column_name in record:
                continue

            record[column_name] = display_value

            # Split shooting stats
            if column_name in _SHOOTING_STATS and display_value and '-' in display_value:
                record.update(_split_shooting_stat(display_value, column_name))
            elif display_value and display_value.replace('.', '', 1).isdigit():
                # Convert numeric values
                try:
                    record[column_name] = float(display_value) if '.' in display_value else int(display_value)
                except (ValueError, TypeError):
                    pass
            elif not display_value or (isinstance(display_value, str) and display_value.lower() in ['n/a', '-']):
                record[column_name] = np.nan

        # Standardize stat names
        for old_name, new_name in _VERBOSE_STAT_NAMES.items():
            if old_name in record and new_name not in record:
                record[new_name] = record.pop(old_name)

        # Remove duplicate percentage columns
        for col in _DUPLICATE_PCT_COLUMNS:
            record.pop(col, None)

        team_stats.append(record)

    return team_stats


def _extract_play_by_play(game_id: str, game_data: dict, teams_info: list, player_stats: list) -> list:
    """Extract play-by-play data from game data."""
    plays_data = game_data.get('plays', [])
    if not plays_data:
        return []

    # Build win probability lookup
    win_prob_mapping = {}
    for prob in game_data.get('winprobability', []):
        if isinstance(prob, dict) and 'playId' in prob:
            win_prob_mapping[prob['playId']] = {
                'home_win_percentage': prob.get('homeWinPercentage'),
                'tie_percentage': prob.get('tiePercentage'),
            }

    # Build team/player lookup maps for filling empty names
    team_lookup = {}
    for ti in teams_info:
        tid = str(ti.get("team_id") or "")
        if tid:
            team_lookup[tid] = {"name": ti.get("team_name"), "abbreviation": ti.get("team_abbreviation")}

    player_lookup = {}
    for ps in player_stats:
        pid = str(ps.get("player_id") or "")
        if pid:
            player_lookup[pid] = {"name": ps.get("player_name")}

    play_by_play = []
    for play in plays_data:
        if not isinstance(play, dict):
            continue

        period = play.get("period")
        clock = play.get("clock")
        team = play.get("team") if isinstance(play.get("team"), dict) else None
        play_type = play.get("type") if isinstance(play.get("type"), dict) else None

        clock_display = clock.get("displayValue") if isinstance(clock, dict) else None

        play_info = {
            "game_id": game_id,
            "play_id": play.get("id"),
            "sequence_number": play.get("sequenceNumber"),
            "period": period.get("number") if isinstance(period, dict) else None,
            "period_display": period.get("displayValue") if isinstance(period, dict) else None,
            "clock": clock_display,
            "clock_seconds": convert_clock_to_seconds(clock_display or ""),
            "team_id": team.get("id") if team else None,
            "team_name": team.get("name") if team else None,
            "play_type": play_type.get("text") if play_type else None,
            "play_type_id": play_type.get("id") if play_type else None,
            "text": play.get("text"),
            "score_home": play.get("homeScore"),
            "score_away": play.get("awayScore"),
            "scoring_play": play.get("scoringPlay", False),
            "score_value": play.get("scoreValue", 0),
            "shooting_play": play.get("shootingPlay", False),
            "coordinate_x": None,
            "coordinate_y": None,
            "wallclock": play.get("wallclock"),
        }

        # Coordinates
        coord = play.get("coordinate")
        if isinstance(coord, dict):
            play_info["coordinate_x"] = coord.get("x")
            play_info["coordinate_y"] = coord.get("y")

        # Player info (athlete1, athlete2)
        for i in range(1, 3):
            athlete = play.get(f"athlete{i}")
            if isinstance(athlete, dict):
                play_info[f"player_{i}_id"] = athlete.get("id")
                play_info[f"player_{i}_name"] = athlete.get("displayName")
                play_info[f"player_{i}_role"] = athlete.get("role")

        # Collect all player IDs from various sources
        player_ids = []

        if isinstance(play.get('participantsCodes'), list):
            player_ids.extend(play['participantsCodes'])

        for athlete in (play.get('athletesInvolved') or []):
            if isinstance(athlete, dict) and 'id' in athlete:
                player_ids.append(athlete['id'])
            elif isinstance(athlete, str):
                player_ids.append(athlete)

        for idx, participant in enumerate(play.get('participants') or []):
            if isinstance(participant, dict):
                p_athlete = participant.get('athlete')
                if isinstance(p_athlete, dict):
                    pid = p_athlete.get('id')
                    if pid:
                        player_ids.append(pid)
                        play_info[f"participant_{idx+1}_id"] = pid

        if player_ids:
            # Deduplicate while preserving order
            seen = set()
            unique = []
            for pid in player_ids:
                if pid not in seen:
                    seen.add(pid)
                    unique.append(pid)
            play_info["all_player_ids"] = ",".join(str(pid) for pid in unique)

        # Athletes field
        if isinstance(play.get('athletes'), list):
            athlete_ids = []
            for athlete in play['athletes']:
                if isinstance(athlete, dict):
                    aid = athlete.get('id')
                    if not aid and isinstance(athlete.get('athlete'), dict):
                        aid = athlete['athlete'].get('id')
                    if aid:
                        athlete_ids.append(str(aid))
            if athlete_ids:
                play_info["athletes_data"] = ",".join(athlete_ids)

        # Win probability
        play_id = play.get("id")
        if play_id in win_prob_mapping:
            wp = win_prob_mapping[play_id]
            play_info["home_win_percentage"] = wp["home_win_percentage"]
            home_wp = wp["home_win_percentage"]
            play_info["away_win_percentage"] = (1.0 - home_wp) if home_wp is not None else None
            play_info["tie_percentage"] = wp["tie_percentage"]

        # Fill empty team/player names from lookups
        if play_info["team_id"] and not play_info.get("team_name"):
            tid_str = str(play_info["team_id"])
            if tid_str in team_lookup:
                play_info["team_name"] = team_lookup[tid_str]["name"]

        for i in range(1, 3):
            pid_key = f"player_{i}_id"
            pname_key = f"player_{i}_name"
            if play_info.get(pid_key) and not play_info.get(pname_key):
                pid_str = str(play_info[pid_key])
                if pid_str in player_lookup:
                    play_info[pname_key] = player_lookup[pid_str]["name"]

        play_by_play.append(play_info)

    return play_by_play


# ---------------------------------------------------------------------------
# Main game processing
# ---------------------------------------------------------------------------

def process_game_data(game_id: str, season: int, verbose: bool = False) -> Dict[str, Any]:
    """Process detailed data for a single game.

    Returns a dict with keys: game_id, season, processed, error, data.
    data contains DataFrames: game_info, teams_info, player_stats, team_stats,
    play_by_play, officials, broadcasts.
    """
    logger.debug(f"Processing game {game_id} for season {season}")

    try:
        # Load raw game data
        data_path = get_games_dir(season) / f"{game_id}.json"
        if data_path.exists():
            game_data = load_json(data_path)
        else:
            logger.warning(f"Game data for {game_id} in season {season} not found. Fetching it now.")
            game_data = get_game_data(game_id, season, verbose_cache=False)

        if game_data is None or not isinstance(game_data, dict):
            return {"game_id": game_id, "season": season, "processed": False, "error": "Raw data is None"}

        logger.debug(f"Game {game_id}: Top-level keys: {list(game_data.keys())}")

        # Extract all components
        game_details = get_game_details(game_data, data_path)

        # Build game_info row
        status = game_details["status"]
        if isinstance(status, dict):
            status = status.get("description") or status.get("short_detail") or status.get("name")

        game_info = {
            "game_id": game_id,
            "date": game_details["date"],
            "venue_id": game_details["venue_id"],
            "venue_name": game_details["venue_name"],
            "venue_city": game_details["venue_city"],
            "venue_state": game_details["venue_state"],
            "attendance": game_details["attendance"],
            "status": status if isinstance(status, str) else None,
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
            "conference_abbreviation": game_details["conference_abbreviation"],
        }

        teams_info = _extract_teams_info(game_id, game_details)
        officials_data = _extract_officials(game_id, game_data)
        broadcasts_data = _extract_broadcasts(game_id, game_data)
        player_stats = _extract_player_stats(game_id, game_data)
        team_stats = _extract_team_stats(game_id, game_data, game_details)
        play_by_play = _extract_play_by_play(game_id, game_data, teams_info, player_stats)

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
                "broadcasts": pd.DataFrame(broadcasts_data) if broadcasts_data else pd.DataFrame(),
            }
        }

        if verbose:
            logger.info(f"Successfully processed data for game {game_id} in season {season}")
        else:
            logger.debug(f"Successfully processed data for game {game_id} in season {season}")

        return result

    except Exception as e:
        logger.error(f"Error processing game {game_id} in season {season}: {e}")
        logger.debug(f"Stack trace for game {game_id} error:", exc_info=True)
        return {"game_id": game_id, "season": season, "processed": False, "error": str(e)}


def process_game_with_season(game_id, season, force, gender=None, verbose=False):
    """Helper for multiprocessing: process a game with gender context."""
    try:
        if gender:
            set_gender(gender)
        return process_game_data(game_id, season, verbose)
    except Exception as e:
        logger.error(f"Error in process_game_with_season for game {game_id}: {e}")
        return {"game_id": game_id, "season": season, "processed": False, "error": f"Error: {e}"}


# ---------------------------------------------------------------------------
# DataFrame optimization
# ---------------------------------------------------------------------------

# Columns that should be converted to nullable integer
_ID_COLUMNS = {
    "game_id", "venue_id", "team_id", "player_id", "player_1_id",
    "player_2_id", "position_id", "play_type_id", "sequence_number",
}

# Columns that should be categorical
_CATEGORICAL_COLUMNS = [
    "home_away", "type", "market", "lang", "region",
    "team_abbreviation", "position", "status", "play_type",
]

# Per-datatype column type overrides
_DTYPE_OVERRIDES = {
    "broadcasts": {},
    "game_info": {
        "attendance": "Int64",
        "date": "datetime64[ns]",
        "neutral_site": "bool",
        "completed": "bool",
    },
    "game_summary": {
        "error": "categorical",
        "processed": "bool",
    },
    "officials": {
        "position": "categorical",
        "name": "categorical",
        "display_name": "categorical",
    },
    "play_by_play": {
        "play_id": False,  # Don't convert — may be too large
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
        "wallclock": "datetime64[ns]",
    },
    "player_stats": {
        "jersey": "Int64",
        "MIN": "float64",
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
        "dnp": "bool",
    },
    "schedules": {
        "season": "Int64",
        "event_date": "datetime64[ns]",
    },
    "team_stats": {
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
        "DREB": "float64",
    },
    "teams": {
        "conference_id": "Int64",
    },
    "teams_info": {
        "score": "Int64",
        "winner": "bool",
        "team_color": "categorical",
        "team_location": "categorical",
        "team_nickname": "categorical",
        "groups_slug": "categorical",
    },
}


def _convert_column(df, col, dtype):
    """Convert a single DataFrame column to the target dtype."""
    try:
        if dtype == "Int64":
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        elif dtype == "datetime64[ns]":
            df[col] = pd.to_datetime(df[col], errors='coerce')
        elif dtype == "categorical":
            if df[col].nunique() < 100:
                df[col] = df[col].astype('category')
        elif dtype == "bool":
            if df[col].dtype != 'bool':
                df[col] = df[col].map({
                    True: True, 'True': True, 'true': True, 1: True, '1': True,
                    False: False, 'False': False, 'false': False, 0: False, '0': False,
                }).astype('bool')
        elif dtype == "float64":
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            df[col] = df[col].astype(dtype)
    except Exception as e:
        logger.warning(f"Error converting {col} to {dtype}: {e}")


def optimize_dataframe_dtypes(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """Optimize datatypes in a dataframe for memory usage and consistency."""
    if df.empty:
        return df

    result_df = df.copy()

    # Convert ID columns to nullable integer
    for col in _ID_COLUMNS:
        if col in result_df.columns and result_df[col].dtype == 'object':
            non_null = result_df[col].dropna()
            if len(non_null) > 0:
                _convert_column(result_df, col, "Int64")

    # Convert common categorical columns
    for col in _CATEGORICAL_COLUMNS:
        if col in result_df.columns and result_df[col].dtype == 'object':
            if result_df[col].nunique() < 100:
                try:
                    result_df[col] = result_df[col].astype('category')
                except Exception as e:
                    logger.warning(f"Error converting {col} to categorical: {e}")

    # Extract format components for game_info
    if data_type == "game_info" and "format" in result_df.columns:
        try:
            if result_df["format"].dtype == 'object':
                result_df["regulation_clock"] = result_df["format"].apply(
                    lambda x: x.get("regulation", {}).get("clock") if isinstance(x, dict) else None)
                result_df["overtime_clock"] = result_df["format"].apply(
                    lambda x: x.get("overtime", {}).get("clock") if isinstance(x, dict) else None)
                result_df["period_name"] = result_df["format"].apply(
                    lambda x: x.get("regulation", {}).get("displayName") if isinstance(x, dict) else None)
                result_df["num_periods"] = result_df["format"].apply(
                    lambda x: x.get("regulation", {}).get("periods") if isinstance(x, dict) else None)
        except Exception as e:
            logger.warning(f"Error processing format column: {e}")

    # Apply per-datatype overrides
    overrides = _DTYPE_OVERRIDES.get(data_type, {})
    for col, dtype in overrides.items():
        if col in result_df.columns and dtype:
            _convert_column(result_df, col, dtype)

    # Replace empty strings with NaN in object columns
    for col in result_df.columns:
        if result_df[col].dtype == 'object':
            result_df[col] = result_df[col].replace('', np.nan)

    # Player stats: ensure DNP players have null stats, convert string stats to numeric
    if data_type == "player_stats" and "dnp" in result_df.columns:
        dnp_mask = result_df["dnp"] == True
        if dnp_mask.any():
            for col in _DNP_STAT_FIELDS:
                if col in result_df.columns:
                    result_df.loc[dnp_mask, col] = np.nan

        for col in ["OREB", "DREB", "REB", "AST", "STL", "BLK", "TO", "PF", "PTS"]:
            if col in result_df.columns and result_df[col].dtype == 'object':
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce')

    return result_df


def remove_redundant_columns(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """Remove redundant columns from dataframes."""
    if df.empty:
        return df

    result_df = df.copy()

    # Remove string shooting stat columns when parsed versions exist
    if data_type in ["player_stats", "team_stats"]:
        for str_col in _SHOOTING_STATS:
            parsed_cols = [f"{str_col}_MADE", f"{str_col}_ATT", f"{str_col}_PCT"]
            if str_col in result_df.columns and all(c in result_df.columns for c in parsed_cols):
                if result_df[parsed_cols].notna().all(axis=1).mean() > 0.9:
                    result_df = result_df.drop(columns=[str_col])

    # Remove raw format column if components were extracted
    if data_type == "game_info" and "format" in result_df.columns:
        extracted = ["regulation_clock", "overtime_clock", "period_name", "num_periods"]
        if all(c in result_df.columns for c in extracted):
            if result_df[extracted].notna().any(axis=1).mean() > 0.9:
                result_df = result_df.drop(columns=["format"])

    return result_df


# ---------------------------------------------------------------------------
# Season-level processing
# ---------------------------------------------------------------------------

def process_all_games(season: int, max_workers: int = 4, force: bool = False,
                      verbose: bool = False) -> Dict[str, pd.DataFrame]:
    """Process all games for a season and save consolidated data files."""
    logger.info(f"Processing games for season {season}")

    raw_games_dir = get_games_dir(season)
    if not os.path.exists(raw_games_dir):
        logger.warning(f"No raw game data found for season {season}")
        return {"game_summary": pd.DataFrame()}

    game_files = [f for f in os.listdir(raw_games_dir) if f.endswith('.json')]
    game_ids = [os.path.splitext(f)[0] for f in game_files]
    logger.info(f"Found {len(game_ids)} games to process for season {season}")

    # Collect results by data type
    data_type_names = ["game_info", "teams_info", "player_stats", "team_stats",
                       "play_by_play", "officials", "broadcasts"]
    game_results = {dt: [] for dt in data_type_names}
    game_summary = []

    current_gender = get_current_gender()
    logger.info(f"Processing games with gender: {current_gender}")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_game_with_season, gid, season, force, current_gender, verbose)
            for gid in game_ids
        ]

        for future in as_completed(futures):
            try:
                result = future.result()
            except Exception as e:
                logger.error(f"Error processing game result: {e}")
                game_summary.append({
                    "game_id": "error", "season": season,
                    "processed": False, "error": str(e),
                })
                continue

            if result.get("processed") and "data" in result:
                game_summary.append({
                    "game_id": result["game_id"], "season": result.get("season", season),
                    "processed": True, "error": None,
                })
                for dt, df in result["data"].items():
                    if not df.empty:
                        game_results[dt].append(df)
            else:
                game_summary.append({
                    "game_id": result["game_id"], "season": result.get("season", season),
                    "processed": False, "error": result.get("error", "Unknown error"),
                })

    # Combine all results into consolidated DataFrames
    combined_dfs = {}

    # Game summary
    if game_summary:
        summary_df = pd.DataFrame(game_summary)
        if 'error' in summary_df.columns:
            summary_df['error'] = summary_df['error'].replace('', None)
        combined_dfs["game_summary"] = optimize_dataframe_dtypes(summary_df, "game_summary")
    else:
        combined_dfs["game_summary"] = pd.DataFrame()

    # Standard data types
    for dt in data_type_names:
        df_list = game_results[dt]
        if not df_list:
            combined_dfs[dt] = pd.DataFrame()
            continue

        try:
            non_empty = [df for df in df_list if not df.empty]
            if non_empty:
                # Drop all-NA columns before concat
                cleaned = [df.dropna(axis=1, how='all') for df in non_empty]
                cleaned = [df for df in cleaned if not df.empty]
                if cleaned:
                    combined_df = pd.concat(cleaned, ignore_index=True)
                    combined_df = optimize_dataframe_dtypes(combined_df, dt)
                    combined_df = remove_redundant_columns(combined_df, dt)
                    combined_dfs[dt] = combined_df
                    logger.info(f"Created combined {dt} DataFrame with {len(combined_df)} rows")
                else:
                    combined_dfs[dt] = pd.DataFrame()
            else:
                combined_dfs[dt] = pd.DataFrame()
        except Exception as e:
            logger.error(f"Error creating combined DataFrame for {dt}: {e}")
            combined_dfs[dt] = pd.DataFrame()

    # Save to disk
    csv_season_dir = get_csv_season_dir(season)
    parquet_season_dir = get_parquet_season_dir(season)
    os.makedirs(csv_season_dir, exist_ok=True)
    os.makedirs(parquet_season_dir, exist_ok=True)

    for dt, df in combined_dfs.items():
        if df.empty:
            continue
        try:
            # Reconcile game_summary with schedules if available
            if dt == "game_summary":
                try:
                    schedules_file = parquet_season_dir / "schedules.parquet"
                    if schedules_file.exists():
                        schedules_df = pd.read_parquet(schedules_file)
                        if not schedules_df.empty and 'game_id' in schedules_df.columns:
                            scheduled_ids = set(schedules_df['game_id'].unique())
                            processed_ids = set(df['game_id'].unique())
                            missing_ids = scheduled_ids - processed_ids
                            if missing_ids:
                                logger.warning(f"Found {len(missing_ids)} games in schedule not in game_summary")
                                missing = pd.DataFrame([{
                                    "game_id": gid, "season": season,
                                    "processed": False, "error": "Game failed to process completely",
                                } for gid in missing_ids])
                                df = pd.concat([df, missing], ignore_index=True)
                except Exception as e:
                    logger.error(f"Error reconciling game_summary with schedules: {e}")

            df.to_csv(csv_season_dir / f"{dt}.csv", index=False)
            df.to_parquet(parquet_season_dir / f"{dt}.parquet", index=False)
            logger.info(f"Saved {dt} with {len(df)} rows")
        except Exception as e:
            logger.error(f"Error saving {dt} files: {e}")

    return combined_dfs


def process_schedules(season: int, force: bool = False) -> pd.DataFrame:
    """Process schedule data for a specific season."""
    logger.info(f"Processing schedules for season {season}")

    csv_season_dir = get_csv_season_dir(season)
    csv_schedules_file = csv_season_dir / "schedules.csv"

    if not force and csv_schedules_file.exists():
        logger.info(f"Using cached schedules for season {season}")
        return pd.read_csv(csv_schedules_file, parse_dates=['event_date'])

    regular_dir = get_schedules_dir(season)
    postseason_dir = get_schedules_dir(season, schedule_type="postseason")

    if not regular_dir.exists() and not postseason_dir.exists():
        logger.warning(f"No schedules directory found for season {season}")
        return pd.DataFrame()

    schedule_files = list(regular_dir.glob("*.json")) + list(postseason_dir.glob("*.json"))
    if not schedule_files:
        logger.warning(f"No schedule files found for season {season}")
        return pd.DataFrame()

    logger.info(f"Found {len(schedule_files)} team schedule files for season {season}")

    all_games = []
    for schedule_file in schedule_files:
        team_id = schedule_file.stem
        schedule_data = load_json(schedule_file)
        if not schedule_data:
            continue

        for event in schedule_data.get("events", []):
            event_id = event.get("id")
            event_date = event.get("date")

            if event_date:
                try:
                    dt = pd.to_datetime(event_date, errors='coerce')
                    if pd.notna(dt):
                        event_date = dt
                    else:
                        event_date = datetime.fromisoformat(event_date.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    logger.debug(f"Could not parse date {event_date} for event {event_id}")

            for competition in event.get("competitions", []):
                game_id = competition.get("id")
                for team_data in competition.get("competitors", []):
                    opponent_id = team_data.get("id")
                    if opponent_id != team_id:
                        all_games.append({
                            "team_id": team_id,
                            "game_id": game_id,
                            "event_id": event_id,
                            "event_date": event_date,
                            "season": season,
                            "opponent_id": opponent_id,
                        })

    schedules_df = pd.DataFrame(all_games)
    schedules_df = optimize_dataframe_dtypes(schedules_df, "schedules")

    if 'event_date' in schedules_df.columns and schedules_df['event_date'].dtype != 'datetime64[ns]':
        try:
            schedules_df['event_date'] = pd.to_datetime(schedules_df['event_date'], errors='coerce')
        except Exception as e:
            logger.warning(f"Error converting event_date to datetime: {e}")

    parquet_season_dir = get_parquet_season_dir(season)
    os.makedirs(csv_season_dir, exist_ok=True)
    os.makedirs(parquet_season_dir, exist_ok=True)

    try:
        schedules_df.to_csv(csv_schedules_file, index=False)
        schedules_df.to_parquet(parquet_season_dir / "schedules.parquet", index=False)
        logger.info(f"Saved schedules for season {season} with {len(schedules_df)} games")
    except Exception as e:
        logger.error(f"Error saving schedules for season {season}: {e}")

    return schedules_df


def process_season_data(season: int, max_workers: int = 4, force: bool = False,
                        verbose: bool = False) -> Dict[str, Any]:
    """Process all data for a specific season."""
    logger.info(f"Processing data for season {season}")

    try:
        os.makedirs(get_csv_season_dir(season), exist_ok=True)
        os.makedirs(get_parquet_season_dir(season), exist_ok=True)
    except Exception as e:
        logger.error(f"Error creating directories for season {season}: {e}")
        return {"season": season, "total_games": 0, "success_games": 0, "error_games": 0, "error": str(e)}

    schedules_df = process_schedules(season, force=force)
    if schedules_df.empty:
        logger.warning(f"No schedule data found for season {season}")
        return {"season": season, "total_games": 0, "success_games": 0, "error_games": 0, "error": "No schedule data"}

    total_games = len(schedules_df['game_id'].unique())
    logger.info(f"Processing {total_games} games for season {season}")

    success_count = 0
    error_count = 0

    try:
        processed_data = process_all_games(season, max_workers=max_workers, force=force, verbose=verbose)

        for name, df in processed_data.items():
            if not df.empty and name == 'game_summary' and 'processed' in df.columns:
                success_count = len(df[df['processed'] == True])
                error_games = df[df['error'].notna()] if 'error' in df.columns else pd.DataFrame()
                error_count = len(error_games)

                if error_count == 0 and success_count < total_games:
                    error_count = total_games - success_count

                if not error_games.empty:
                    logger.warning(f"Season {season} games with errors ({len(error_games)}):")
                    for _, row in error_games.iterrows():
                        logger.warning(f"Game {row['game_id']} error: {row['error']}")
    except Exception as e:
        logger.error(f"Error processing games for season {season}: {e}")
        error_count = total_games

    return {"season": season, "total_games": total_games, "success_games": success_count, "error_games": error_count}


def process_all_data(seasons: Optional[List[int]] = None, max_workers: int = 4,
                     gender: str = None, game_ids: Optional[List[str]] = None,
                     force: bool = False, verbose: bool = False) -> None:
    """Process all data for the specified seasons."""
    if gender:
        set_gender(gender)

    logger.info(f"Processing all data for {get_current_gender()} basketball")

    teams_df = process_teams_data(force=force)
    logger.info(f"Processed {len(teams_df)} teams")

    if seasons is None:
        seasons = DEFAULT_SEASONS

    summary = {
        "total_seasons": len(seasons),
        "processed_seasons": 0,
        "total_games": 0,
        "success_games": 0,
        "error_games": 0,
    }

    for season in tqdm(seasons, desc="Processing seasons"):
        logger.info(f"Processing season {season}")
        try:
            if game_ids:
                # Process only specific games
                logger.info(f"Processing only specific games: {', '.join(game_ids)}")
                games_dir = get_games_dir(season)

                game_results = {}
                for gid in game_ids:
                    game_file = games_dir / f"{gid}.json"
                    if game_file.exists():
                        try:
                            result = process_game_data(gid, season)
                            if result.get("processed"):
                                game_results[gid] = result
                            else:
                                summary['error_games'] += 1
                        except Exception as e:
                            logger.error(f"Error processing game {gid}: {e}")
                            summary['error_games'] += 1

                if game_results:
                    # Consolidate results from individual games
                    data_types = ["game_info", "teams_info", "player_stats", "team_stats",
                                  "play_by_play", "officials", "broadcasts"]

                    season_dir_csv = get_csv_season_dir(season)
                    season_dir_parquet = get_parquet_season_dir(season)
                    os.makedirs(season_dir_csv, exist_ok=True)
                    os.makedirs(season_dir_parquet, exist_ok=True)

                    for dt in data_types:
                        dfs = [r["data"][dt] for r in game_results.values()
                               if "data" in r and dt in r["data"] and not r["data"][dt].empty]
                        if dfs:
                            combined = pd.concat(dfs, ignore_index=True)
                            combined = optimize_dataframe_dtypes(combined, dt)
                            combined = remove_redundant_columns(combined, dt)
                            combined.to_csv(season_dir_csv / f"{dt}.csv", index=False)
                            combined.to_parquet(season_dir_parquet / f"{dt}.parquet", index=False)

                    summary['success_games'] += len(game_results)
                    summary['total_games'] += len(game_ids)

                result = {"success_games": len(game_results), "error_games": len(game_ids) - len(game_results)}
            else:
                result = process_season_data(season, max_workers, force, verbose)
                summary["processed_seasons"] += 1
                summary["total_games"] += result.get("total_games", 0)
                summary["success_games"] += result.get("success_games", 0)
                summary["error_games"] += result.get("error_games", 0)

            logger.info(f"Completed processing season {season}: "
                        f"{result.get('success_games', 0)} games processed, "
                        f"{result.get('error_games', 0)} games with errors")
        except Exception as e:
            logger.error(f"Error processing season {season}: {e}")

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
    """Command-line interface for the processor."""
    parser = argparse.ArgumentParser(description="Process ESPN college basketball data")
    parser.add_argument("--seasons", "-s", type=int, nargs="+", help="Seasons to process (e.g., 2022 2023)")
    parser.add_argument("--max-workers", "-w", type=int, default=4,
                        help="Maximum number of concurrent processes (default: 4)")
    parser.add_argument("--gender", "-g", type=str, choices=["mens", "womens"],
                        help="Gender (mens or womens, default is womens)")
    parser.add_argument("--force", "-f", action="store_true", help="Force reprocessing even if files exist locally")
    args = parser.parse_args()

    process_all_data(seasons=args.seasons, max_workers=args.max_workers, gender=args.gender, force=args.force)


if __name__ == "__main__":
    main()
