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
                             get_raw_dir, get_season_dir, get_current_gender, set_gender, get_teams_url, get_team_url,
                             get_team_schedule_params, get_team_schedule_url, get_game_data_url)
from espn_data.const import MISSING_MENS_TEAMS, MISSING_WOMENS_TEAMS

logger = logging.getLogger("espn_data")

# Default values
DEFAULT_SEASONS = list(range(2002, 2024))  # NCAA basketball data from 2002-2023
DEFAULT_CONCURRENCY = 5
DEFAULT_DELAY = 0.5


def get_team_by_id(team_id: str, gender: str = None) -> Dict[str, Any]:
    """
    Get information for a specific team by ID.
    
    Args:
        team_id: ESPN team ID
        gender: Either "mens" or "womens" (if None, uses current setting)
        
    Returns:
        Team data dictionary
    """
    if gender:
        set_gender(gender)

    logger.info(f"Fetching team data for team ID {team_id}")

    url = get_team_url().format(team_id=team_id)

    try:
        team_data = make_request(url)

        if not team_data:
            logger.warning(f"No data found for team {team_id}")
            return {}

        # Extract the team object from the response
        if "team" in team_data:
            return team_data["team"]
        else:
            logger.warning(f"Unexpected response format for team {team_id}")
            return team_data  # Return whatever we got

    except Exception as e:
        logger.error(f"Error fetching data for team {team_id}: {e}")
        return {}


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

        # Extract the team IDs already fetched to avoid duplicates
        team_ids = {str(team["id"]) for team in all_teams}

        # Process missing teams based on the current gender
        current_gender = get_current_gender()
        missing_teams = MISSING_MENS_TEAMS if current_gender == "mens" else MISSING_WOMENS_TEAMS

        if missing_teams:
            logger.info(f"Processing missing {current_gender} teams")
            # Add missing teams that aren't already in the list
            for missing_team in missing_teams:
                team_id = str(missing_team["team_id"])
                if team_id not in team_ids:
                    logger.info(f"Fetching missing team: {missing_team['name']} (ID: {team_id})")
                    team_data = get_team_by_id(team_id)

                    if team_data and "id" in team_data:
                        # Add the first and last D1 season information to the team data
                        team_data["first_d1_season"] = missing_team.get("first_d1_season")
                        team_data["last_d1_season"] = missing_team.get("last_d1_season")
                        all_teams.append(team_data)
                        team_ids.add(team_id)
                    else:
                        # If the team couldn't be fetched by ID, use the data from missing teams list
                        logger.warning(
                            f"Could not fetch team {team_id} from API, using data from missing teams constant")
                        dummy_team = {
                            "id": missing_team["team_id"],
                            "displayName": missing_team["name"],
                            "first_d1_season": missing_team.get("first_d1_season"),
                            "last_d1_season": missing_team.get("last_d1_season")
                        }
                        all_teams.append(dummy_team)
                        team_ids.add(team_id)

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
                      force: bool = False,
                      schedule_type: str = "regular") -> Dict[str, Any]:
    """
    Get schedule data for a team for a specific season.
    
    Args:
        team_id: ESPN team ID
        season: Season year to get schedule for (e.g., 2022 for 2021-2022 season)
        gender: Either "mens" or "womens" (if None, uses current setting)
        force: If True, force refetch even if data exists
        schedule_type: Type of schedule ('regular' or 'postseason')
        
    Returns:
        Dictionary containing game data
    """
    if gender:
        set_gender(gender)

    if not season:
        # Default to current season
        now = datetime.now()
        season = now.year if now.month > 6 else now.year - 1

    # Determine seasontype based on schedule_type
    seasontype = 3 if schedule_type == "postseason" else 2

    logger.info(f"Fetching {schedule_type} schedule for team {team_id} for season {season}")

    # Check if schedule already exists
    output_file = get_schedules_dir(season, schedule_type) / f"{team_id}.json"
    if not force and output_file.exists():
        logger.info(f"Using cached {schedule_type} schedule for team {team_id} in season {season}")
        return load_json(output_file)

    params = get_team_schedule_params(team_id, season, seasontype)
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
        logger.error(f"Error fetching {schedule_type} schedule for team {team_id} in season {season}: {e}")
        return {"events": []}


def get_game_data(game_id: str,
                  season: int,
                  gender: str = None,
                  force: bool = False,
                  verbose_cache: bool = True) -> Dict[str, Any]:
    """
    Get ESPN game data for a specific game.
    
    Args:
        game_id: ESPN game ID
        season: Season year (e.g., 2022 for 2021-22 season)
        gender: Either "mens" or "womens" (if None, uses current setting)
        force: If True, force refetch even if data exists
        verbose_cache: If True, log when using cached data
        
    Returns:
        Game data dictionary
        
    Note:
        This function saves the game data to disk internally.
    """
    # Only set gender if explicitly passed
    if gender is not None:
        set_gender(gender)

    # Check if game data already exists
    games_dir = get_games_dir(season)
    output_file = games_dir / f"{game_id}.json"

    if not force and output_file.exists():
        if verbose_cache:
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
                           force: bool = False,
                           verbose_cache: bool = True) -> Tuple[str, Dict[str, Any], int]:
    """
    Fetch ESPN game data asynchronously.
    
    Args:
        session: aiohttp client session
        game_id: ESPN game ID
        season: Season year (e.g., 2022 for 2021-22 season)
        gender: Either "mens" or "womens" (if None, uses current setting)
        force: If True, force refetch even if data exists
        verbose_cache: If True, log when using cached data
        
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
        if verbose_cache:
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
                            force: bool = False,
                            verbose_cache: bool = True) -> Dict[str, Dict[str, Any]]:
    """
    Fetch and save a batch of games in parallel.
    
    Args:
        game_data_list: List of (game_id, season) tuples
        concurrency: Maximum number of concurrent requests
        delay: Delay between requests in seconds
        gender: Either "mens" or "womens" (if None, uses current setting)
        force: If True, force refetch even if data exists
        verbose_cache: If True, log when using cached data
        
    Returns:
        Dictionary mapping game_ids to game data
    """
    if gender:
        set_gender(gender)

    # Pre-filter already cached games if not forcing refetch
    if not force:
        filtered_list = []
        cached_games = {}

        for game_id, season in game_data_list:
            games_dir = get_games_dir(season)
            output_file = games_dir / f"{game_id}.json"

            if output_file.exists():
                # Load cached data without logging each one
                cached_games[game_id] = load_json(output_file)
            else:
                filtered_list.append((game_id, season))

        if cached_games:
            logger.info(f"Using cached data for {len(cached_games)} games")

        # If all games are cached, return early
        if not filtered_list:
            return cached_games

        game_data_list = filtered_list

    logger.info(f"Fetching batch of {len(game_data_list)} games with concurrency {concurrency}")

    # Map for quick lookup and results storage
    games_map = {} if force else cached_games

    # Create asyncio semaphore to limit concurrency
    semaphore = asyncio.Semaphore(concurrency)

    async def fetch_with_semaphore(session, game_id, season):
        async with semaphore:
            if delay > 0:
                await asyncio.sleep(random.uniform(0, delay))
            return await fetch_game_async(session,
                                          game_id,
                                          season,
                                          gender=gender,
                                          force=force,
                                          verbose_cache=verbose_cache)

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
        # Check both regular and postseason schedules
        for schedule_type in ["regular", "postseason"]:
            schedules_dir = get_schedules_dir(season, schedule_type)
            if not schedules_dir.exists():
                logger.warning(f"No {schedule_type} schedule directory found for season {season}")
                continue

            schedule_files = list(schedules_dir.glob("*.json"))
            logger.info(f"Found {len(schedule_files)} team {schedule_type} schedules for season {season}")

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
                          game_ids: Optional[List[str]] = None,
                          force: bool = False,
                          verbose: bool = False) -> None:
    """
    Scrape all ESPN college basketball data.
    
    Args:
        concurrency: Maximum number of concurrent requests
        delay: Delay between requests in seconds
        seasons: List of seasons to scrape (default: all seasons)
        team_id: Optional team ID to scrape only one team (for testing)
        gender: Either "mens" or "womens" (if None, uses current setting)
        game_ids: Optional list of specific game IDs to scrape
        force: If True, force refetch even if data exists
        verbose: If True, log detailed information (e.g., each cached game)
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

        # Ensure schedules base directory exists
        schedules_base_dir = season_dir / "schedules"
        os.makedirs(schedules_base_dir, exist_ok=True)

        # Ensure both types of schedule directories exist
        regular_schedules_dir = get_schedules_dir(season, "regular")
        postseason_schedules_dir = get_schedules_dir(season, "postseason")
        os.makedirs(regular_schedules_dir, exist_ok=True)
        os.makedirs(postseason_schedules_dir, exist_ok=True)

        if team_id:
            # Only get schedules for the specified team
            for schedule_type in ["regular", "postseason"]:
                schedules_dir = get_schedules_dir(season, schedule_type)
                team_schedules_file = schedules_dir / f"{team_id}.json"

                if not force and team_schedules_file.exists():
                    logger.info(f"Using cached {schedule_type} schedule for team {team_id} in season {season}")
                else:
                    games = get_team_schedule(team_id, season, gender, force, schedule_type)

                    # Save schedule data for this season
                    save_json(games, team_schedules_file)
        else:
            # Get schedules for all teams
            for team in teams:
                team_id_inner = team["id"]

                for schedule_type in ["regular", "postseason"]:
                    schedules_dir = get_schedules_dir(season, schedule_type)
                    team_schedules_file = schedules_dir / f"{team_id_inner}.json"

                    if not force and team_schedules_file.exists():
                        logger.info(
                            f"Using cached {schedule_type} schedule for team {team_id_inner} in season {season}")
                        continue

                    try:
                        games = get_team_schedule(team_id_inner, season, gender, force, schedule_type)

                        # Save schedule data for this season
                        save_json(games, team_schedules_file)

                    except Exception as e:
                        logger.error(f"Error getting schedule for team {team_id_inner}: {e}")

    # Step 3: Extract unique game IDs from all team schedules
    if game_ids:
        # Use the specific game IDs provided with seasons
        logger.info(f"Using {len(game_ids)} specific game IDs provided via command line")
        season_game_ids = set()
        for game_id in game_ids:
            # For simplicity, assume all games are in all provided seasons
            # A more robust approach would determine the correct season for each game
            for season in seasons:
                season_game_ids.add((game_id, season))
        game_ids_set = season_game_ids
    else:
        game_ids_set = extract_game_ids_from_schedules(seasons, gender)

    logger.info(f"Found {len(game_ids_set)} unique games across all seasons")

    # Step 4: Get game data for all games
    if game_ids_set:
        logger.info("Fetching all game data")
        game_ids_list = list(game_ids_set)

        # Split into smaller batches for better progress tracking
        batch_size = 100
        for i in range(0, len(game_ids_list), batch_size):
            batch = game_ids_list[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{math.ceil(len(game_ids_list)/batch_size)}")
            await fetch_games_batch(batch, concurrency, delay, gender, force, verbose)

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
