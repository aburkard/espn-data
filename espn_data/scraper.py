"""Scrape ESPN College Basketball data."""

import os
import json
import time
import asyncio
import logging
import aiohttp
import random
import math
from typing import Dict, List, Set, Any, Optional, Tuple
from pathlib import Path
from tqdm import tqdm
from datetime import datetime

from espn_data.utils import (make_request, load_json, save_json, get_teams_file, get_schedules_dir, get_games_dir,
                             get_raw_dir, get_season_dir, get_current_gender, set_gender, get_teams_url,
                             get_team_schedule_params, get_team_schedule_url, get_game_data_url)

logger = logging.getLogger("espn_data")

# Default values
DEFAULT_SEASONS = list(range(2002, 2024))  # NCAA basketball data from 2002-2023
DEFAULT_CONCURRENCY = 5
DEFAULT_DELAY = 0.5


def get_all_teams(gender: str = None, max_teams: Optional[int] = None, force: bool = False) -> List[Dict[str, Any]]:
    """
    Get information for all teams.
    
    Args:
        gender: Either "mens" or "womens" (if None, uses current setting)
        max_teams: Maximum number of teams to fetch (for testing)
        force: If True, force refetch even if data exists
        
    Returns:
        List of team data dictionaries
    """
    if gender:
        set_gender(gender)

    teams_file = get_teams_file()
    if not force and teams_file.exists():
        logger.info("Using cached teams data")
        try:
            teams = load_json(teams_file)
            if teams:
                return teams[:max_teams] if max_teams else teams
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Error loading cached teams data: {e}")
            # Continue to fetch data if cached file is corrupted

    logger.info("Fetching all teams data")

    try:
        # Initialize parameters for pagination
        limit = 500  # Set a high limit to minimize API calls
        page = 1
        all_teams = []
        has_more = True

        # Continue fetching until there are no more teams to retrieve
        while has_more:
            params = {'limit': limit, 'page': page}

            logger.info(f"Fetching teams - page {page}, limit {limit}")
            try:
                data = make_request(get_teams_url(), params=params)
            except Exception as e:
                logger.error(f"Error fetching teams data on page {page}: {e}")
                # Wait and retry or break depending on your error handling strategy
                if not all_teams:  # If we haven't got any teams yet, this is a fatal error
                    raise
                break  # If we already have some teams, we can return what we have

            if not data or "sports" not in data:
                logger.error(f"Failed to get teams data on page {page}")
                break

            # Extract teams from current page
            teams = []
            sports = data.get("sports", [])

            for sport in sports:
                leagues = sport.get("leagues", [])

                for league in leagues:
                    team_list = league.get("teams", [])

                    for team_entry in team_list:
                        team = team_entry.get("team", {})
                        teams.append(team)

            # Add teams from current page to the overall list
            all_teams.extend(teams)

            # Properly check for more pages - only if we received exactly the limit,
            # there might be more
            has_more = len(teams) == limit
            page += 1

        logger.info(f"Found {len(all_teams)} teams across {page-1} pages")

        # Limit number of teams for testing
        if max_teams:
            all_teams = all_teams[:max_teams]

        # Save the data to disk
        os.makedirs(teams_file.parent, exist_ok=True)
        save_json(all_teams, teams_file)

        return all_teams
    except Exception as e:
        logger.error(f"Unexpected error fetching teams data: {e}")
        return []


def get_team_schedule(team_id: str,
                      season: Optional[int] = None,
                      gender: str = None,
                      force: bool = False) -> Dict[str, Any]:
    """
    Get schedule data for a team for a specific season.
    
    Args:
        team_id: ESPN team ID
        season: Season year to get schedule for (e.g., 2022 for 2021-2022 season)
        gender: Either "mens" or "womens" (if None, uses current setting)
        force: If True, force refetch even if data exists
        
    Returns:
        Dictionary containing game data
    """
    if gender:
        set_gender(gender)

    if not season:
        # Default to current season
        now = datetime.now()
        season = now.year if now.month > 6 else now.year - 1

    logger.info(f"Fetching schedule for team {team_id} for season {season}")

    # Check if schedule already exists
    output_file = get_schedules_dir(season) / f"{team_id}.json"
    if not force and output_file.exists():
        logger.info(f"Using cached schedule for team {team_id} in season {season}")
        return load_json(output_file)

    params = get_team_schedule_params(team_id, season)
    url = get_team_schedule_url().format(team_id=team_id)

    try:
        data = make_request(url, params)

        # Filter to only include games for the requested season
        if data and "events" in data:
            season_games = []
            for game in data.get("events", []):
                if "season" in game and "year" in game["season"]:
                    game_season = game["season"]["year"]
                    if game_season == season:
                        season_games.append(game)

            # Replace all events with only those from the requested season
            data["events"] = season_games

        return data or {"events": []}

    except Exception as e:
        logger.error(f"Error fetching schedule for team {team_id} in season {season}: {e}")
        return {"events": []}


def get_game_data(game_id: str, season: int, gender: str = None, force: bool = False) -> Dict[str, Any]:
    """
    Get detailed data for a specific game.
    
    Args:
        game_id: ESPN game ID
        season: Season year
        gender: Either "mens" or "womens" (if None, uses current setting)
        force: If True, force refetch even if data exists
        
    Returns:
        Game data dictionary
        
    Note:
        This function saves the game data to disk internally.
    """
    if gender:
        set_gender(gender)

    # Check if game data already exists
    games_dir = get_games_dir(season)
    output_file = games_dir / f"{game_id}.json"

    if not force and output_file.exists():
        logger.info(f"Using cached data for game {game_id}")
        return load_json(output_file)

    logger.info(f"Fetching data for game {game_id}")

    url = get_game_data_url().format(game_id=game_id)
    logger.info(f"Using URL: {url}")

    try:
        game_data = make_request(url)

        if not game_data:
            logger.warning(f"No data found for game {game_id}")
            return {}

        # Ensure games directory for this season exists
        os.makedirs(games_dir, exist_ok=True)

        # Save game data
        save_json(game_data, output_file)

        return game_data

    except Exception as e:
        logger.error(f"Error fetching data for game {game_id}: {e}")
        return {}


async def fetch_game_async(session: aiohttp.ClientSession,
                           game_id: str,
                           season: int,
                           gender: str = None,
                           force: bool = False) -> Tuple[str, Dict[str, Any], int]:
    """
    Asynchronously fetch game data.
    
    Args:
        session: aiohttp client session
        game_id: ESPN game ID
        season: Season year
        gender: Either "mens" or "womens" (if None, uses current setting)
        force: If True, force refetch even if data exists
        
    Returns:
        Tuple of (game_id, game_data, HTTP status code)
        
    Note:
        This function saves the game data to disk internally.
    """
    if gender:
        set_gender(gender)

    # Check if game data already exists
    games_dir = get_games_dir(season)
    output_file = games_dir / f"{game_id}.json"

    if not force and output_file.exists():
        logger.info(f"Using cached data for game {game_id}")
        return game_id, load_json(output_file), 200

    url = get_game_data_url().format(game_id=game_id)

    try:
        async with session.get(url) as response:
            status = response.status

            if status == 200:
                game_data = await response.json()

                # Ensure games directory for this season exists
                os.makedirs(games_dir, exist_ok=True)

                # Save game data
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
                            gender: str = None,
                            force: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    Fetch and save a batch of games in parallel.
    
    Args:
        game_data_list: List of (game_id, season) tuples
        concurrency: Maximum number of concurrent requests
        delay: Delay between requests in seconds
        gender: Either "mens" or "womens" (if None, uses current setting)
        force: If True, force refetch even if data exists
        
    Returns:
        Dictionary mapping game_ids to game data
    """
    if gender:
        set_gender(gender)

    logger.info(f"Fetching batch of {len(game_data_list)} games with concurrency {concurrency}")

    # Map for quick lookup and results storage
    games_map = {}

    # Create asyncio semaphore to limit concurrency
    semaphore = asyncio.Semaphore(concurrency)

    async def fetch_with_semaphore(session, game_id, season):
        async with semaphore:
            if delay > 0:
                await asyncio.sleep(random.uniform(0, delay))
            return await fetch_game_async(session, game_id, season, gender=gender, force=force)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for game_id, season in game_data_list:
            tasks.append(fetch_with_semaphore(session, game_id, season))

        results = await asyncio.gather(*tasks)

        for game_id, game_data, status in results:
            if game_data:  # Only store if we have data
                games_map[game_id] = game_data

    logger.info(f"Fetched {len(games_map)} games successfully")
    return games_map


def extract_game_ids_from_schedules(seasons: Optional[List[int]] = None, gender: str = None) -> Set[Tuple[str, int]]:
    """
    Extract game IDs from all team schedules.
    
    Args:
        seasons: List of seasons to process (default: all seasons)
        gender: Either "mens" or "womens" (if None, uses current setting)
        
    Returns:
        Set of (game_id, season) tuples
    """
    if gender:
        set_gender(gender)

    if seasons is None:
        seasons = DEFAULT_SEASONS

    game_data = set()

    for season in seasons:
        schedules_dir = get_schedules_dir(season)
        if not schedules_dir.exists():
            logger.warning(f"No schedule directory found for season {season}")
            continue

        schedule_files = list(schedules_dir.glob("*.json"))
        logger.info(f"Found {len(schedule_files)} team schedules for season {season}")

        for schedule_file in schedule_files:
            try:
                schedule_data = load_json(schedule_file)

                # Access the "events" array in the schedule data
                for game in schedule_data.get("events", []):
                    if "id" in game:
                        game_data.add((game["id"], season))

            except Exception as e:
                logger.error(f"Error extracting game IDs from {schedule_file}: {e}")

    logger.info(f"Extracted {len(game_data)} unique game IDs across all seasons")
    return game_data


async def scrape_all_data(concurrency: int = DEFAULT_CONCURRENCY,
                          delay: float = DEFAULT_DELAY,
                          seasons: Optional[List[int]] = None,
                          team_id: Optional[str] = None,
                          gender: str = None,
                          force: bool = False) -> None:
    """
    Scrape all ESPN college basketball data.
    
    Args:
        concurrency: Maximum number of concurrent requests
        delay: Delay between requests in seconds
        seasons: List of seasons to scrape (default: all seasons)
        team_id: Optional team ID to scrape only one team (for testing)
        gender: Either "mens" or "womens" (if None, uses current setting)
        force: If True, force refetch even if data exists
    """
    if gender:
        set_gender(gender)

    logger.info(f"Starting scraper for {get_current_gender()} college basketball data")

    if seasons is None:
        seasons = DEFAULT_SEASONS

    logger.info(f"Starting full data scrape for seasons {min(seasons)}-{max(seasons)}")

    # Step 1: Get all teams once (not per season)
    teams_file = get_teams_file()
    if not force and teams_file.exists():
        logger.info("Using cached teams data")
        teams = load_json(teams_file)
    else:
        teams = get_all_teams(gender, force=force)
        if not teams:
            logger.error("Failed to retrieve teams, aborting")
            return

        # Save teams data at the top level
        os.makedirs(teams_file.parent, exist_ok=True)
        save_json(teams, teams_file)

    # Step 2: Get schedules for specific team or all teams
    for season in seasons:
        logger.info(f"Processing season {season}")

        # Create the season directory
        season_dir = get_season_dir(get_raw_dir(), season)
        os.makedirs(season_dir, exist_ok=True)

        # Ensure schedules directory exists
        schedules_dir = get_schedules_dir(season)
        os.makedirs(schedules_dir, exist_ok=True)

        if team_id:
            # Only get schedule for the specified team
            team_schedules_file = schedules_dir / f"{team_id}.json"
            if not force and team_schedules_file.exists():
                logger.info(f"Using cached schedule for team {team_id} in season {season}")
            else:
                games = get_team_schedule(team_id, season, gender, force)

                # Save schedule data for this season
                output_file = schedules_dir / f"{team_id}.json"
                save_json(games, output_file)
        else:
            # Get schedules for all teams
            for team in teams:
                team_id_inner = team["id"]
                team_schedules_file = schedules_dir / f"{team_id_inner}.json"

                if not force and team_schedules_file.exists():
                    logger.info(f"Using cached schedule for team {team_id_inner} in season {season}")
                    continue

                try:
                    games = get_team_schedule(team_id_inner, season, gender, force)

                    # Save schedule data for this season
                    output_file = schedules_dir / f"{team_id_inner}.json"
                    save_json(games, output_file)

                except Exception as e:
                    logger.error(f"Error getting schedule for team {team_id_inner}: {e}")

    # Step 3: Extract unique game IDs from all team schedules
    game_ids = extract_game_ids_from_schedules(seasons, gender)
    logger.info(f"Found {len(game_ids)} unique games across all seasons")

    # Step 4: Get game data for all games
    if game_ids:
        logger.info("Fetching all game data")
        game_ids_list = list(game_ids)

        # Split into smaller batches for better progress tracking
        batch_size = 100
        for i in range(0, len(game_ids_list), batch_size):
            batch = game_ids_list[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{math.ceil(len(game_ids_list)/batch_size)}")
            await fetch_games_batch(batch, concurrency, delay, gender, force)

    logger.info("Data scraping complete")


def main() -> None:
    """
    Command-line interface for scraper.
    """
    import argparse

    # Parse arguments
    parser = argparse.ArgumentParser(description="Scrape ESPN college basketball data")

    parser.add_argument("--seasons", "-s", type=int, nargs="+", help="Seasons to scrape (e.g., 2022 2023)")
    parser.add_argument("--concurrency",
                        "-c",
                        type=int,
                        default=DEFAULT_CONCURRENCY,
                        help=f"Number of concurrent requests (default: {DEFAULT_CONCURRENCY})")
    parser.add_argument("--delay",
                        "-d",
                        type=float,
                        default=DEFAULT_DELAY,
                        help=f"Delay between requests in seconds (default: {DEFAULT_DELAY})")
    parser.add_argument("--team", "-t", type=str, help="Team ID to scrape (for testing)")
    parser.add_argument("--gender",
                        "-g",
                        type=str,
                        choices=["mens", "womens"],
                        help="Gender (mens or womens, default is womens)")
    parser.add_argument("--force", "-f", action="store_true", help="Force refetch data even if it exists locally")

    args = parser.parse_args()

    asyncio.run(
        scrape_all_data(concurrency=args.concurrency,
                        delay=args.delay,
                        seasons=args.seasons,
                        team_id=args.team,
                        gender=args.gender,
                        force=args.force))


if __name__ == "__main__":
    main()
