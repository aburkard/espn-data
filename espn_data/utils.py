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
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"

# URL templates based on gender
URL_TEMPLATES = {
    "mens": {
        "TEAMS_URL":
            "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams",
        "TEAM_SCHEDULE_URL":
            "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{team_id}/schedule",
        "GAME_DATA_URL":
            "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={game_id}"
    },
    "womens": {
        "TEAMS_URL":
            "https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams",
        "TEAM_SCHEDULE_URL":
            "https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/schedule",
        "GAME_DATA_URL":
            "https://site.web.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/summary?event={game_id}"
    }
}

# Use None initially, will be set to "womens" by default on first call to set_gender or get_current_gender
CURRENT_GENDER = None


def set_gender(gender: str) -> None:
    """
    Set the current gender for basketball data.
    
    Args:
        gender: Either "mens" or "womens"
    """
    global CURRENT_GENDER
    if gender not in ["mens", "womens"]:
        raise ValueError("Gender must be either 'mens' or 'womens'")
    # logger.info(f"Setting gender to {gender}")
    CURRENT_GENDER = gender
    # logger.info(f"Set current gender to {CURRENT_GENDER}")


def get_current_gender() -> str:
    """Get the current gender setting."""
    global CURRENT_GENDER
    # Default to womens if not set
    if CURRENT_GENDER is None:
        CURRENT_GENDER = "womens"
    # logger.info(f"Current gender: {CURRENT_GENDER}")
    return CURRENT_GENDER


# URL accessor functions
def get_teams_url() -> str:
    """Get the teams URL for the current gender."""
    return URL_TEMPLATES[get_current_gender()]["TEAMS_URL"]


def get_team_schedule_url() -> str:
    """Get the team schedule URL template for the current gender."""
    return URL_TEMPLATES[get_current_gender()]["TEAM_SCHEDULE_URL"]


def get_game_data_url() -> str:
    """Get the game data URL template for the current gender."""
    return URL_TEMPLATES[get_current_gender()]["GAME_DATA_URL"]


# Create directory functions that include gender and seasons
def get_raw_dir() -> Path:
    """Get the raw data directory for the current gender."""
    return DATA_DIR / "raw" / get_current_gender()


def get_season_dir(base_dir: Path, season: int) -> Path:
    """Get the directory for a specific season."""
    return base_dir / str(season)


def get_teams_file() -> Path:
    """Get the teams file path."""
    return get_raw_dir() / "teams.json"


def get_schedules_dir(season: int, schedule_type: str = "regular") -> Path:
    """
    Get the schedules directory for a season.
    
    Args:
        season: Season year
        schedule_type: Type of schedule ('regular' or 'postseason')
    
    Returns:
        Path to the schedules directory
    """
    base_dir = get_season_dir(get_raw_dir(), season) / "schedules"
    return base_dir / schedule_type


def get_games_dir(season: int) -> Path:
    """Get the games directory for a season."""
    return get_season_dir(get_raw_dir(), season) / "games"


def get_processed_dir() -> Path:
    """Get the processed data directory for the current gender."""
    return DATA_DIR / "processed" / get_current_gender()


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
os.makedirs(DATA_DIR / "raw" / "mens", exist_ok=True)
os.makedirs(DATA_DIR / "raw" / "womens", exist_ok=True)
os.makedirs(DATA_DIR / "processed" / "mens", exist_ok=True)
os.makedirs(DATA_DIR / "processed" / "womens", exist_ok=True)
os.makedirs(DATA_DIR / "processed" / "mens" / "csv", exist_ok=True)
os.makedirs(DATA_DIR / "processed" / "mens" / "parquet", exist_ok=True)
os.makedirs(DATA_DIR / "processed" / "womens" / "csv", exist_ok=True)
os.makedirs(DATA_DIR / "processed" / "womens" / "parquet", exist_ok=True)

# Season directories will be created as needed when processing data


# For backward compatibility, we'll add property-like getters
def get_TEAMS_URL():
    return get_teams_url()


def get_TEAM_SCHEDULE_URL():
    return get_team_schedule_url()


def get_GAME_DATA_URL():
    return get_game_data_url()


# These might be used elsewhere as constants, but we'll define them as None
# and use the above functions to access them
TEAMS_URL = None
TEAM_SCHEDULE_URL = None
GAME_DATA_URL = None

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


def get_team_schedule_params(team_id: str, season: Optional[int] = None, seasontype: int = 2) -> Dict[str, Any]:
    """
    Get parameters for team schedule request.
    
    Args:
        team_id: ESPN team ID
        season: Optional season year (e.g., 2022 for 2021-2022 season)
        seasontype: Season type (2 for regular season, 3 for postseason)
        
    Returns:
        Dictionary of query parameters
    """
    params = {}
    if season:
        params["season"] = season
    params["seasontype"] = seasontype
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
    # Avoid circular import
    from espn_data.utils import make_request
    import logging

    logger = logging.getLogger("espn_data")

    # Request with minimal data to get count
    params = {"limit": 1}

    response = make_request(get_TEAMS_URL(), params=params)

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
