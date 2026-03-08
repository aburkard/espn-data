"""Utility functions for ESPN data scraping."""

import os
import json
import time
import logging
from dataclasses import dataclass, field
from pathlib import Path
import requests
from typing import Dict, Any, Optional, Union

logger = logging.getLogger("espn_data")

# Paths
BASE_DIR = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent

# URL templates by gender
_URL_TEMPLATES = {
    "mens": {
        "teams": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams",
        "team": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{team_id}",
        "schedule": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/{team_id}/schedule",
        "game": "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={game_id}",
    },
    "womens": {
        "teams": "https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams",
        "team": "https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}",
        "schedule": "https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/schedule",
        "game": "https://site.web.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/summary?event={game_id}",
    },
}

# Request headers to mimic a browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.espn.com/",
    "Origin": "https://www.espn.com",
}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class Config:
    """Central configuration — set once at program start, read everywhere."""
    gender: str = "womens"
    data_dir: Path = field(default_factory=lambda: PROJECT_ROOT / "data")

    def __post_init__(self):
        if self.gender not in ("mens", "womens"):
            raise ValueError("Gender must be either 'mens' or 'womens'")
        self.data_dir = Path(self.data_dir)


_config = Config()


def configure(gender: str = None, data_dir: Union[str, Path] = None) -> None:
    """Set global configuration. Call once at program start."""
    if gender is not None:
        if gender not in ("mens", "womens"):
            raise ValueError("Gender must be either 'mens' or 'womens'")
        _config.gender = gender
    if data_dir is not None:
        _config.data_dir = Path(data_dir)


def get_config() -> Config:
    """Get the current configuration."""
    return _config


# Backward-compat aliases
def set_gender(gender: str) -> None:
    configure(gender=gender)


def get_current_gender() -> str:
    return _config.gender


# ---------------------------------------------------------------------------
# URL accessors
# ---------------------------------------------------------------------------

def _get_url(key: str) -> str:
    """Get a URL template for the current gender."""
    return _URL_TEMPLATES[_config.gender][key]


def get_teams_url() -> str:
    return _get_url("teams")


def get_team_url() -> str:
    return _get_url("team")


def get_team_schedule_url() -> str:
    return _get_url("schedule")


def get_game_data_url() -> str:
    return _get_url("game")


# Backward-compat aliases (used by test_teams.py and get_team_count)
get_TEAMS_URL = get_teams_url
get_TEAM_URL = get_team_url
get_TEAM_SCHEDULE_URL = get_team_schedule_url
get_GAME_DATA_URL = get_game_data_url


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

def get_raw_dir() -> Path:
    """Get the raw data directory for the current gender."""
    return _config.data_dir / "raw" / _config.gender


def get_season_dir(base_dir: Path, season: int) -> Path:
    """Get the directory for a specific season."""
    return base_dir / str(season)


def get_teams_file() -> Path:
    return get_raw_dir() / "teams.json"


def get_schedules_dir(season: int, schedule_type: str = "regular") -> Path:
    return get_season_dir(get_raw_dir(), season) / "schedules" / schedule_type


def get_games_dir(season: int) -> Path:
    return get_season_dir(get_raw_dir(), season) / "games"


def get_processed_dir() -> Path:
    return _config.data_dir / "processed" / _config.gender


def get_csv_dir() -> Path:
    return get_processed_dir() / "csv"


def get_parquet_dir() -> Path:
    return get_processed_dir() / "parquet"


def get_csv_teams_file() -> Path:
    return get_csv_dir() / "teams.csv"


def get_parquet_teams_file() -> Path:
    return get_parquet_dir() / "teams.parquet"


def get_csv_season_dir(season: int) -> Path:
    return get_csv_dir() / str(season)


def get_parquet_season_dir(season: int) -> Path:
    return get_parquet_dir() / str(season)


def get_csv_games_dir(season: int) -> Path:
    return get_csv_season_dir(season) / "games"


def get_parquet_games_dir(season: int) -> Path:
    return get_parquet_season_dir(season) / "games"


# ---------------------------------------------------------------------------
# Ensure base directories exist (called at import time)
# ---------------------------------------------------------------------------

def ensure_dirs():
    """Create the base directory structure for the current config."""
    d = _config.data_dir
    for gender in ("mens", "womens"):
        for subdir in ("raw", "processed"):
            os.makedirs(d / subdir / gender, exist_ok=True)
        for fmt in ("csv", "parquet"):
            os.makedirs(d / "processed" / gender / fmt, exist_ok=True)

ensure_dirs()


# ---------------------------------------------------------------------------
# HTTP requests
# ---------------------------------------------------------------------------

def make_request(url: str, params: Optional[Dict[str, Any]] = None,
                 retries: int = 3, backoff_factor: float = 0.5) -> Dict[str, Any]:
    """Make an HTTP request with retry logic and exponential backoff."""
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, headers=HEADERS)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            wait_time = backoff_factor * (2 ** attempt)
            logger.warning(f"Request failed ({url}): {e}. Retrying in {wait_time:.1f}s...")
            if attempt < retries - 1:
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to retrieve data after {retries} attempts: {url}")
                raise
    return {}


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def save_json(data: Any, file_path: Union[str, Path]) -> None:
    """Save data as JSON to the specified file path."""
    file_path = Path(file_path)
    os.makedirs(file_path.parent, exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    logger.info(f"Data saved to {file_path}")


def load_json(file_path: Union[str, Path]) -> Any:
    """Load JSON data from file."""
    file_path = Path(file_path)
    if not file_path.exists():
        logger.warning(f"File not found: {file_path}")
        return None
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def get_team_schedule_params(team_id: str, season: Optional[int] = None, seasontype: int = 2) -> Dict[str, Any]:
    """Get query parameters for a team schedule request."""
    params = {}
    if season:
        params["season"] = season
    params["seasontype"] = seasontype
    return params


def extract_team_id(team_link: str) -> str:
    """Extract team ID from a team link URL."""
    return team_link.split('/')[-2]


def extract_game_id(game_link: str) -> str:
    """Extract game ID from a game link or full URL."""
    if '?gameId=' in game_link:
        return game_link.split('?gameId=')[-1].split('&')[0]
    elif '/gameId/' in game_link:
        return game_link.split('/gameId/')[-1].split('/')[0]
    elif 'event=' in game_link:
        return game_link.split('event=')[-1].split('&')[0]
    else:
        return game_link.rstrip('/').split('/')[-1]


def get_team_count() -> int:
    """Get the total count of college basketball teams from ESPN."""
    params = {"limit": 1}
    try:
        response = make_request(get_teams_url(), params=params)
    except Exception as e:
        logger.error(f"Error getting team count: {e}")
        return 0

    if not response or "sports" not in response:
        logger.error("Failed to retrieve teams count")
        return 0

    count = response.get("count", 0)
    if count > 0:
        return count

    sports = response.get("sports", [])
    if sports:
        leagues = sports[0].get("leagues", [])
        if leagues:
            return leagues[0].get("count", 0)

    return 400  # sensible default
