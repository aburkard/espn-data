"""Scrape ESPN College Basketball data."""

import os
import json
import asyncio
import logging
import math
import random
import aiohttp
from typing import Dict, List, Set, Any, Optional, Tuple
from pathlib import Path
from tqdm import tqdm
from datetime import datetime

from espn_data.utils import (make_request, load_json, save_json, get_teams_file, get_schedules_dir, get_games_dir,
                             get_raw_dir, get_season_dir, get_current_gender, configure, get_teams_url, get_team_url,
                             get_team_schedule_params, get_team_schedule_url, get_game_data_url)
from espn_data.const import MISSING_MENS_TEAMS, MISSING_WOMENS_TEAMS

logger = logging.getLogger("espn_data")

DEFAULT_SEASONS = list(range(2002, 2024))
DEFAULT_CONCURRENCY = 5
DEFAULT_DELAY = 0.5


# ---------------------------------------------------------------------------
# Team fetching
# ---------------------------------------------------------------------------

def get_team_by_id(team_id: str) -> Dict[str, Any]:
    """Get information for a specific team by ID."""
    logger.info(f"Fetching team data for team ID {team_id}")
    url = get_team_url().format(team_id=team_id)

    try:
        team_data = make_request(url)
        if not team_data:
            logger.warning(f"No data found for team {team_id}")
            return {}
        return team_data.get("team", team_data)
    except Exception as e:
        logger.error(f"Error fetching data for team {team_id}: {e}")
        return {}


def get_all_teams(max_teams: Optional[int] = None,
                  force: bool = False) -> List[Dict[str, Any]]:
    """Get information for all teams with pagination."""
    teams_file = get_teams_file()

    # Use cache if available
    if not force and teams_file.exists():
        logger.info("Using cached teams data")
        try:
            teams = load_json(teams_file)
            if teams:
                return teams[:max_teams] if max_teams else teams
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error loading cached teams data: {e}")

    logger.info("Fetching all teams data")

    try:
        all_teams = _fetch_all_teams_paginated()
        _add_missing_teams(all_teams)

        if max_teams:
            all_teams = all_teams[:max_teams]

        os.makedirs(teams_file.parent, exist_ok=True)
        save_json(all_teams, teams_file)
        return all_teams

    except Exception as e:
        logger.error(f"Unexpected error fetching teams data: {e}")
        return []


def _fetch_all_teams_paginated() -> List[Dict[str, Any]]:
    """Fetch all teams using pagination."""
    limit = 500
    page = 1
    all_teams = []

    while True:
        logger.info(f"Fetching teams - page {page}, limit {limit}")
        try:
            data = make_request(get_teams_url(), params={'limit': limit, 'page': page})
        except Exception as e:
            logger.error(f"Error fetching teams data on page {page}: {e}")
            if not all_teams:
                raise
            break

        if not data or "sports" not in data:
            logger.error(f"Failed to get teams data on page {page}")
            break

        # Extract teams from response
        teams = []
        for sport in data.get("sports", []):
            for league in sport.get("leagues", []):
                for entry in league.get("teams", []):
                    teams.append(entry.get("team", {}))

        all_teams.extend(teams)

        # If we got fewer than the limit, we've reached the end
        if len(teams) < limit:
            break
        page += 1

    logger.info(f"Found {len(all_teams)} teams across {page} pages")
    return all_teams


def _add_missing_teams(all_teams: List[Dict[str, Any]]) -> None:
    """Add known missing teams that ESPN's API doesn't return."""
    team_ids = {str(t["id"]) for t in all_teams}

    current_gender = get_current_gender()
    missing_teams = MISSING_MENS_TEAMS if current_gender == "mens" else MISSING_WOMENS_TEAMS
    if not missing_teams:
        return

    logger.info(f"Processing missing {current_gender} teams")
    for missing in missing_teams:
        tid = str(missing["team_id"])
        if tid in team_ids:
            continue

        logger.info(f"Fetching missing team: {missing['name']} (ID: {tid})")
        team_data = get_team_by_id(tid)

        if team_data and "id" in team_data:
            team_data["first_d1_season"] = missing.get("first_d1_season")
            team_data["last_d1_season"] = missing.get("last_d1_season")
            all_teams.append(team_data)
        else:
            logger.warning(f"Could not fetch team {tid} from API, using missing teams constant")
            all_teams.append({
                "id": missing["team_id"],
                "displayName": missing["name"],
                "first_d1_season": missing.get("first_d1_season"),
                "last_d1_season": missing.get("last_d1_season"),
            })

        team_ids.add(tid)


# ---------------------------------------------------------------------------
# Schedule fetching
# ---------------------------------------------------------------------------

def get_team_schedule(team_id: str, season: Optional[int] = None,
                      force: bool = False, schedule_type: str = "regular") -> Dict[str, Any]:
    """Get schedule data for a team for a specific season."""
    if not season:
        now = datetime.now()
        season = now.year if now.month > 6 else now.year - 1

    seasontype = 3 if schedule_type == "postseason" else 2

    logger.info(f"Fetching {schedule_type} schedule for team {team_id} for season {season}")

    # Check cache
    output_file = get_schedules_dir(season, schedule_type) / f"{team_id}.json"
    if not force and output_file.exists():
        logger.info(f"Using cached {schedule_type} schedule for team {team_id} in season {season}")
        return load_json(output_file)

    params = get_team_schedule_params(team_id, season, seasontype)
    url = get_team_schedule_url().format(team_id=team_id)

    try:
        data = make_request(url, params)

        # Filter to only games from the requested season
        if data and "events" in data:
            data["events"] = [
                game for game in data.get("events", [])
                if game.get("season", {}).get("year") == season
            ]

        return data or {"events": []}

    except Exception as e:
        logger.error(f"Error fetching {schedule_type} schedule for team {team_id} in season {season}: {e}")
        return {"events": []}


# ---------------------------------------------------------------------------
# Game data fetching
# ---------------------------------------------------------------------------

def get_game_data(game_id: str, season: int,
                  force: bool = False, verbose_cache: bool = True) -> Dict[str, Any]:
    """Get ESPN game data for a specific game. Saves to disk."""
    games_dir = get_games_dir(season)
    output_file = games_dir / f"{game_id}.json"

    if not force and output_file.exists():
        if verbose_cache:
            logger.info(f"Using cached data for game {game_id}")
        return load_json(output_file)

    logger.info(f"Fetching data for game {game_id}")
    url = get_game_data_url().format(game_id=game_id)

    try:
        game_data = make_request(url)
        if not game_data:
            logger.warning(f"No data found for game {game_id}")
            return {}

        os.makedirs(games_dir, exist_ok=True)
        save_json(game_data, output_file)
        return game_data

    except Exception as e:
        logger.error(f"Error fetching data for game {game_id}: {e}")
        return {}


async def fetch_game_async(session: aiohttp.ClientSession, game_id: str, season: int,
                           force: bool = False,
                           verbose_cache: bool = True) -> Tuple[str, Dict[str, Any], int]:
    """Fetch ESPN game data asynchronously. Saves to disk."""
    games_dir = get_games_dir(season)
    output_file = games_dir / f"{game_id}.json"

    if not force and output_file.exists():
        if verbose_cache:
            logger.info(f"Using cached data for game {game_id}")
        return game_id, load_json(output_file), 200

    url = get_game_data_url().format(game_id=game_id)

    try:
        async with session.get(url) as response:
            status = response.status
            if status == 200:
                game_data = await response.json()
                os.makedirs(games_dir, exist_ok=True)
                save_json(game_data, output_file)
                return game_id, game_data, status
            else:
                logger.warning(f"Error fetching game {game_id}: {status}")
                return game_id, {}, status
    except Exception as e:
        logger.error(f"Exception while fetching game {game_id}: {e}")
        return game_id, {}, 0


async def fetch_games_batch(game_data_list: List[Tuple[str, int]],
                            concurrency: int = DEFAULT_CONCURRENCY,
                            delay: float = DEFAULT_DELAY,
                            force: bool = False,
                            verbose_cache: bool = True) -> Dict[str, Dict[str, Any]]:
    """Fetch and save a batch of games in parallel."""
    # Separate cached vs uncached games
    games_map = {}
    if not force:
        uncached = []
        for game_id, season in game_data_list:
            output_file = get_games_dir(season) / f"{game_id}.json"
            if output_file.exists():
                games_map[game_id] = load_json(output_file)
            else:
                uncached.append((game_id, season))

        if games_map:
            logger.info(f"Using cached data for {len(games_map)} games")
        if not uncached:
            return games_map

        game_data_list = uncached

    logger.info(f"Fetching batch of {len(game_data_list)} games with concurrency {concurrency}")

    semaphore = asyncio.Semaphore(concurrency)

    async def fetch_with_semaphore(session, game_id, season):
        async with semaphore:
            if delay > 0:
                await asyncio.sleep(random.uniform(0, delay))
            return await fetch_game_async(session, game_id, season,
                                          force=force, verbose_cache=verbose_cache)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_with_semaphore(session, gid, s) for gid, s in game_data_list]
        results = await asyncio.gather(*tasks)

        for game_id, game_data, status in results:
            if game_data:
                games_map[game_id] = game_data

    logger.info(f"Fetched {len(games_map)} games successfully")
    return games_map


# ---------------------------------------------------------------------------
# Schedule parsing
# ---------------------------------------------------------------------------

def extract_game_ids_from_schedules(seasons: Optional[List[int]] = None) -> Set[Tuple[str, int]]:
    """Extract game IDs from all team schedules."""
    if seasons is None:
        seasons = DEFAULT_SEASONS

    game_data = set()

    for season in seasons:
        for schedule_type in ("regular", "postseason"):
            schedules_dir = get_schedules_dir(season, schedule_type)
            if not schedules_dir.exists():
                logger.warning(f"No {schedule_type} schedule directory found for season {season}")
                continue

            schedule_files = list(schedules_dir.glob("*.json"))
            logger.info(f"Found {len(schedule_files)} team {schedule_type} schedules for season {season}")

            for schedule_file in schedule_files:
                try:
                    data = load_json(schedule_file)
                    for game in data.get("events", []):
                        if "id" in game:
                            game_data.add((game["id"], season))
                except Exception as e:
                    logger.error(f"Error extracting game IDs from {schedule_file}: {e}")

    logger.info(f"Extracted {len(game_data)} unique game IDs across all seasons")
    return game_data


# ---------------------------------------------------------------------------
# Main scraping orchestration
# ---------------------------------------------------------------------------

def _fetch_schedules_for_team(team_id: str, seasons: List[int], force: bool):
    """Fetch regular and postseason schedules for a single team across seasons."""
    for season in seasons:
        for schedule_type in ("regular", "postseason"):
            output_file = get_schedules_dir(season, schedule_type) / f"{team_id}.json"
            if not force and output_file.exists():
                logger.info(f"Using cached {schedule_type} schedule for team {team_id} in season {season}")
                continue
            try:
                games = get_team_schedule(team_id, season, force, schedule_type)
                save_json(games, output_file)
            except Exception as e:
                logger.error(f"Error getting {schedule_type} schedule for team {team_id} season {season}: {e}")


async def scrape_all_data(concurrency: int = DEFAULT_CONCURRENCY,
                          delay: float = DEFAULT_DELAY,
                          seasons: Optional[List[int]] = None,
                          team_id: Optional[str] = None,
                          gender: str = None,
                          data_dir: str = None,
                          game_ids: Optional[List[str]] = None,
                          force: bool = False,
                          verbose: bool = False) -> None:
    """Scrape all ESPN college basketball data."""
    configure(gender=gender, data_dir=data_dir)

    logger.info(f"Starting scraper for {get_current_gender()} college basketball data")

    if seasons is None:
        seasons = DEFAULT_SEASONS
    logger.info(f"Starting full data scrape for seasons {min(seasons)}-{max(seasons)}")

    # Step 1: Get all teams
    teams_file = get_teams_file()
    if not force and teams_file.exists():
        logger.info("Using cached teams data")
        teams = load_json(teams_file)
    else:
        teams = get_all_teams(force=force)
        if not teams:
            logger.error("Failed to retrieve teams, aborting")
            return
        os.makedirs(teams_file.parent, exist_ok=True)
        save_json(teams, teams_file)

    # Step 2: Get schedules
    for season in seasons:
        logger.info(f"Processing season {season}")

        # Ensure directory structure
        season_dir = get_season_dir(get_raw_dir(), season)
        for subdir in ("", "schedules", "schedules/regular", "schedules/postseason"):
            os.makedirs(season_dir / subdir, exist_ok=True)

        if team_id:
            _fetch_schedules_for_team(team_id, [season], force)
        else:
            for team in teams:
                _fetch_schedules_for_team(str(team["id"]), [season], force)

    # Step 3: Collect game IDs
    if game_ids:
        logger.info(f"Using {len(game_ids)} specific game IDs provided via command line")
        game_ids_set = {(gid, season) for gid in game_ids for season in seasons}
    else:
        game_ids_set = extract_game_ids_from_schedules(seasons)

    logger.info(f"Found {len(game_ids_set)} unique games across all seasons")

    # Step 4: Fetch game data in batches
    if game_ids_set:
        logger.info("Fetching all game data")
        game_ids_list = list(game_ids_set)
        batch_size = 100

        for i in range(0, len(game_ids_list), batch_size):
            batch = game_ids_list[i:i + batch_size]
            logger.info(f"Processing batch {i // batch_size + 1}/{math.ceil(len(game_ids_list) / batch_size)}")
            await fetch_games_batch(batch, concurrency, delay, force, verbose)

    logger.info("Data scraping complete")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """Command-line interface for scraper."""
    import argparse

    parser = argparse.ArgumentParser(description="Scrape ESPN college basketball data")
    parser.add_argument("--seasons", "-s", type=int, nargs="+", help="Seasons to scrape (e.g., 2022 2023)")
    parser.add_argument("--concurrency", "-c", type=int, default=DEFAULT_CONCURRENCY,
                        help=f"Number of concurrent requests (default: {DEFAULT_CONCURRENCY})")
    parser.add_argument("--delay", "-d", type=float, default=DEFAULT_DELAY,
                        help=f"Delay between requests in seconds (default: {DEFAULT_DELAY})")
    parser.add_argument("--team", "-t", type=str, help="Team ID to scrape (for testing)")
    parser.add_argument("--gender", "-g", type=str, choices=["mens", "womens"],
                        help="Gender (mens or womens, default is womens)")
    parser.add_argument("--output-dir", "-o", type=str,
                        help="Output data directory (default: data/)")
    parser.add_argument("--force", "-f", action="store_true", help="Force refetch data even if it exists locally")
    args = parser.parse_args()

    asyncio.run(scrape_all_data(
        concurrency=args.concurrency, delay=args.delay, seasons=args.seasons,
        team_id=args.team, gender=args.gender, data_dir=args.output_dir, force=args.force,
    ))


if __name__ == "__main__":
    main()
