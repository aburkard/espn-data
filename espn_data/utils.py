"""Utility functions for ESPN data scraping."""

import os
import json
import time
import logging
from pathlib import Path
import requests
from typing import Dict, Any, List, Optional, Union

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("espn_scraper.log"),
                              logging.StreamHandler()])
logger = logging.getLogger("espn_data")

# Constants
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

# Remove fixed directory definitions that don't include season
# TEAMS_FILE = DATA_DIR / "teams.json"
# SCHEDULES_DIR = DATA_DIR / "schedules"
# GAMES_DIR = DATA_DIR / "games"


# Create directory functions that include seasons
def get_raw_dir() -> Path:
    """Get the base raw data directory."""
    return DATA_DIR / "raw"


def get_season_dir(base_dir: Path, season: int) -> Path:
    """Get the directory for a specific season."""
    return base_dir / str(season)


def get_teams_file() -> Path:
    """Get the teams file path."""
    return get_raw_dir() / "teams.json"


def get_schedules_dir(season: int) -> Path:
    """Get the schedules directory for a season."""
    return get_season_dir(get_raw_dir(), season) / "schedules"


def get_games_dir(season: int) -> Path:
    """Get the games directory for a season."""
    return get_season_dir(get_raw_dir(), season) / "games"


def get_processed_dir() -> Path:
    """Get the base processed data directory."""
    return DATA_DIR / "processed"


def get_csv_dir() -> Path:
    """Get the CSV directory."""
    return get_processed_dir() / "csv"


def get_parquet_dir() -> Path:
    """Get the Parquet directory."""
    return get_processed_dir() / "parquet"


def get_csv_teams_file() -> Path:
    """Get the CSV teams file path."""
    return get_csv_dir() / "teams.csv"


def get_parquet_teams_file() -> Path:
    """Get the Parquet teams file path."""
    return get_parquet_dir() / "teams.parquet"


def get_csv_season_dir(season: int) -> Path:
    """Get the CSV directory for a specific season."""
    return get_csv_dir() / str(season)


def get_parquet_season_dir(season: int) -> Path:
    """Get the Parquet directory for a specific season."""
    return get_parquet_dir() / str(season)


def get_csv_games_dir(season: int) -> Path:
    """Get the CSV games directory for a specific season."""
    return get_csv_season_dir(season) / "games"


def get_parquet_games_dir(season: int) -> Path:
    """Get the Parquet games directory for a specific season."""
    return get_parquet_season_dir(season) / "games"


# Ensure base directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(get_raw_dir(), exist_ok=True)
os.makedirs(get_processed_dir(), exist_ok=True)
os.makedirs(get_csv_dir(), exist_ok=True)
os.makedirs(get_parquet_dir(), exist_ok=True)

# Season directories will be created as needed when processing data

# URL Templates
TEAMS_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams"
TEAM_SCHEDULE_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/schedule"
GAME_DATA_URL = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/summary?event={game_id}"

# Request headers to mimic a browser
HEADERS = {
    "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept":
        "application/json, text/plain, */*",
    "Accept-Language":
        "en-US,en;q=0.9",
    "Referer":
        "https://www.espn.com/",
    "Origin":
        "https://www.espn.com",
}


def make_request(url: str,
                 params: Optional[Dict[str, Any]] = None,
                 retries: int = 3,
                 backoff_factor: float = 0.5) -> Dict[str, Any]:
    """
    Make an HTTP request with retry logic and error handling.
    
    Args:
        url: The URL to request
        params: Optional query parameters
        retries: Number of retries before giving up
        backoff_factor: Factor to determine wait time between retries
        
    Returns:
        JSON response as dictionary
    """
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, headers=HEADERS)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            wait_time = backoff_factor * (2**attempt)
            logger.warning(f"Request failed ({url}): {e}. Retrying in {wait_time:.1f} seconds...")

            if attempt < retries - 1:
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to retrieve data after {retries} attempts: {url}")
                raise

    # This should never execute due to the raise in the loop
    return {}


def save_json(data: Any, file_path: Union[str, Path]) -> None:
    """
    Save data as JSON to the specified file path.
    
    Args:
        data: Data to save
        file_path: Path to save the file
    """
    file_path = Path(file_path)
    os.makedirs(file_path.parent, exist_ok=True)

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

    logger.info(f"Data saved to {file_path}")


def load_json(file_path: Union[str, Path]) -> Any:
    """
    Load JSON data from file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Loaded data
    """
    file_path = Path(file_path)

    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return None

    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_team_schedule_params(team_id: str, season: Optional[int] = None) -> Dict[str, Any]:
    """
    Get parameters for team schedule request.
    
    Args:
        team_id: ESPN team ID
        season: Optional season year (e.g., 2022 for 2021-2022 season)
        
    Returns:
        Dictionary of query parameters
    """
    params = {}
    if season:
        params["season"] = season
    return params


def extract_team_id(team_link: str) -> str:
    """
    Extract team ID from team link.
    
    Args:
        team_link: Link to team
        
    Returns:
        Team ID as string
    """
    return team_link.split('/')[-2]


def extract_game_id(game_link: str) -> str:
    """
    Extract game ID from game link or full URL.
    
    Args:
        game_link: Link containing game ID
        
    Returns:
        Game ID as string
    """
    # Handle different formats of game links
    if '?gameId=' in game_link:
        return game_link.split('?gameId=')[-1].split('&')[0]
    elif '/gameId/' in game_link:
        return game_link.split('/gameId/')[-1].split('/')[0]
    elif 'event=' in game_link:
        return game_link.split('event=')[-1].split('&')[0]
    else:
        # Default to last path component
        return game_link.rstrip('/').split('/')[-1]


def get_team_count() -> int:
    """
    Get the total count of women's college basketball teams from ESPN.
    
    Returns:
        Total number of teams
    """
    from espn_data.utils import make_request, TEAMS_URL
    import logging

    logger = logging.getLogger("espn_data")

    # Request with minimal data to get count
    params = {"limit": 1}

    response = make_request(TEAMS_URL, params=params)

    if not response or "sports" not in response:
        logger.error("Failed to retrieve teams count")
        return 0

    try:
        # The count should be in the response metadata
        count = response.get("count", 0)
        if count > 0:
            return count

        # If count not directly available, check the sports metadata
        sports = response.get("sports", [])
        if sports and len(sports) > 0:
            leagues = sports[0].get("leagues", [])
            if leagues and len(leagues) > 0:
                return leagues[0].get("count", 0)

        # If we can't find a count, return a sensible default
        return 400  # There are around 350-400 Division I, II, and III NCAA women's basketball teams

    except Exception as e:
        logger.error(f"Error getting team count: {e}")
        return 0
