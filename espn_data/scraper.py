"""Scrape ESPN College Basketball data."""

import os
import json
import time
import asyncio
import logging
import aiohttp
from typing import Dict, List, Set, Any, Optional, Tuple
from pathlib import Path
from tqdm import tqdm

from espn_data.utils import (make_request, load_json, save_json, get_teams_file, get_schedules_dir, get_games_dir,
                             get_raw_dir, get_season_dir, get_current_gender, set_gender, get_teams_url)

logger = logging.getLogger("espn_data")

# Default values
DEFAULT_SEASONS = list(range(2002, 2024))  # NCAA basketball data from 2002-2023
DEFAULT_CONCURRENCY = 5
DEFAULT_DELAY = 0.5


def get_all_teams(gender: str = None, max_teams: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Fetch all college basketball teams from ESPN for the specified gender.
    
    Args:
        gender: Either "mens" or "womens" (if None, uses current setting)
        max_teams: Maximum number of teams to retrieve (for testing)
        
    Returns:
        List of team data dictionaries
    """
    if gender:
        set_gender(gender)

    logger.info(f"Fetching all {get_current_gender()} college basketball teams")

    all_teams = []
    page = 1
    limit = 500  # Maximum allowed by ESPN API

    # Use a higher limit to reduce number of requests
    while True:
        logger.info(f"Fetching teams page {page} with limit {limit}")

        # Build URL with pagination
        url = f"{get_teams_url()}?limit={limit}&page={page}"

        # Fetch data
        response_data = make_request(url)

        if not response_data or "sports" not in response_data:
            logger.error("Invalid response format")
            break

        try:
            # Extract teams
            teams = response_data["sports"][0]["leagues"][0]["teams"]
            team_count = len(teams)
            logger.info(f"Retrieved {team_count} teams on page {page}")

            # Add teams to result list
            for team_entry in teams:
                if "team" in team_entry:
                    all_teams.append(team_entry["team"])

            # Check if we've reached the maximum
            if max_teams and len(all_teams) >= max_teams:
                all_teams = all_teams[:max_teams]
                break

            # Check if we've reached the end
            if team_count < limit:
                logger.info("Reached last page of results")
                break

            # Move to next page
            page += 1
        except (KeyError, IndexError) as e:
            logger.error(f"Error parsing teams data: {e}")
            break

    logger.info(f"Retrieved {len(all_teams)} teams in total")

    # Return if no teams found
    if not all_teams:
        return []

    return all_teams


def get_team_schedule(team_id: str, seasons: Optional[List[int]] = None, gender: str = None) -> List[Dict[str, Any]]:
    """
    Get schedule for a specific team across multiple seasons.
    
    Args:
        team_id: ESPN team ID
        seasons: List of seasons to fetch (default: all seasons)
        gender: Either "mens" or "womens" (if None, uses current setting)
        
    Returns:
        List of game data dictionaries
    """
    if gender:
        set_gender(gender)

    if seasons is None:
        seasons = DEFAULT_SEASONS

    logger.info(f"Fetching schedule for team {team_id} across {len(seasons)} seasons")

    all_games = []

    for season in seasons:
        logger.info(f"Fetching {season} season for team {team_id}")

        # Build URL with season parameter
        url = f"{TEAM_SCHEDULE_URL.format(team_id=team_id)}?season={season}"

        # Fetch data
        response_data = make_request(url)

        if not response_data or "events" not in response_data:
            logger.warning(f"No schedule data found for team {team_id} in season {season}")
            continue

        # Get games from events
        games = response_data["events"]
        logger.info(f"Found {len(games)} games for team {team_id} in season {season}")

        # Add season identifier to each game
        for game in games:
            game["season"] = season
            all_games.append(game)

        # Ensure season directory structure exists
        schedules_dir = get_schedules_dir(season)
        os.makedirs(schedules_dir, exist_ok=True)

        # Save schedule data for this season
        output_file = schedules_dir / f"{team_id}.json"
        save_json(games, output_file)

    return all_games


def get_game_data(game_id: str, season: int, gender: str = None) -> Dict[str, Any]:
    """
    Get detailed data for a specific game.
    
    Args:
        game_id: ESPN game ID
        season: Season year
        gender: Either "mens" or "womens" (if None, uses current setting)
        
    Returns:
        Game data dictionary
    """
    if gender:
        set_gender(gender)

    logger.info(f"Fetching data for game {game_id}")

    url = GAME_DATA_URL.format(game_id=game_id)
    logger.info(f"Using URL: {url}")

    try:
        game_data = make_request(url)

        if not game_data:
            logger.warning(f"No data found for game {game_id}")
            return {}

        # Ensure games directory for this season exists
        games_dir = get_games_dir(season)
        os.makedirs(games_dir, exist_ok=True)

        # Save game data
        output_file = games_dir / f"{game_id}.json"
        save_json(game_data, output_file)

        return game_data

    except Exception as e:
        logger.error(f"Error fetching data for game {game_id}: {e}")
        return {}


async def fetch_game_async(session: aiohttp.ClientSession,
                           game_id: str,
                           season: int,
                           gender: str = None) -> Tuple[str, Dict[str, Any], int]:
    """
    Fetch game data asynchronously.
    
    Args:
        session: aiohttp client session
        game_id: ESPN game ID
        season: Season year
        gender: Either "mens" or "womens" (if None, uses current setting)
        
    Returns:
        Tuple of (game_id, game_data, season)
    """
    if gender:
        set_gender(gender)

    logger.info(f"Fetching data for game {game_id} (season {season})")

    url = GAME_DATA_URL.format(game_id=game_id)

    try:
        async with session.get(url) as response:
            if response.status != 200:
                logger.warning(f"Non-200 status code for game {game_id}: {response.status}")
                return game_id, {}, season

            game_data = await response.json()

            if not game_data:
                logger.warning(f"No data retrieved for game {game_id}")
                return game_id, {}, season

            # Ensure games directory for this season exists
            games_dir = get_games_dir(season)
            os.makedirs(games_dir, exist_ok=True)

            # Save game data
            output_file = games_dir / f"{game_id}.json"
            save_json(game_data, output_file)

            return game_id, game_data, season

    except Exception as e:
        logger.error(f"Error fetching game {game_id}: {e}")
        return game_id, {}, season


async def fetch_games_batch(game_data_list: List[Tuple[str, int]],
                            concurrency: int = DEFAULT_CONCURRENCY,
                            delay: float = DEFAULT_DELAY,
                            gender: str = None) -> Dict[str, Dict[str, Any]]:
    """
    Fetch a batch of games concurrently.
    
    Args:
        game_data_list: List of (game_id, season) tuples
        concurrency: Maximum number of concurrent requests
        delay: Delay between requests in seconds
        gender: Either "mens" or "womens" (if None, uses current setting)
        
    Returns:
        Dictionary mapping game_id to game data
    """
    if gender:
        set_gender(gender)

    if not game_data_list:
        logger.warning("No game IDs provided")
        return {}

    results = {}
    semaphore = asyncio.Semaphore(concurrency)
    total_games = len(game_data_list)

    logger.info(f"Fetching {total_games} games with concurrency={concurrency}, delay={delay}s")

    async def fetch_with_semaphore(session, game_id, season):
        async with semaphore:
            result = await fetch_game_async(session, game_id, season)
            await asyncio.sleep(delay)
            return result

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_with_semaphore(session, game_id, season) for game_id, season in game_data_list]

        for completed in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Fetching games"):
            game_id, game_data, _ = await completed
            if game_data:
                results[game_id] = game_data

    logger.info(f"Successfully fetched {len(results)} of {total_games} games")
    return results


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

                for game in schedule_data:
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
                          gender: str = None) -> None:
    """
    Scrape all ESPN college basketball data.
    
    Args:
        concurrency: Maximum number of concurrent requests
        delay: Delay between requests in seconds
        seasons: List of seasons to scrape (default: all seasons)
        team_id: Optional team ID to scrape only one team (for testing)
        gender: Either "mens" or "womens" (if None, uses current setting)
    """
    if gender:
        set_gender(gender)

    logger.info(f"Starting scraper for {get_current_gender()} college basketball data")

    if seasons is None:
        seasons = DEFAULT_SEASONS

    logger.info(f"Starting full data scrape for seasons {min(seasons)}-{max(seasons)}")

    # Step 1: Get all teams once (not per season)
    teams = get_all_teams(gender)
    if not teams:
        logger.error("Failed to retrieve teams, aborting")
        return

    # Save teams data at the top level
    teams_file = get_teams_file()
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
            get_team_schedule(team_id, [season], gender)
        else:
            # Get schedules for all teams
            for team in tqdm(teams, desc=f"Fetching team schedules for season {season}"):
                team_id = team["id"] if "id" in team else ""
                if team_id:
                    get_team_schedule(team_id, [season], gender)
                    time.sleep(delay)  # Respect rate limits

    # Step 3: Extract game IDs from schedules
    game_data = list(extract_game_ids_from_schedules(seasons, gender))
    logger.info(f"Found {len(game_data)} unique games to fetch")

    # Step 4: Fetch game data asynchronously
    logger.info(f"Fetching game data with concurrency={concurrency}, delay={delay}")
    await fetch_games_batch(game_data, concurrency, delay, gender)

    logger.info("Full data scrape completed")


def main() -> None:
    """
    Main entry point for the scraper script.
    """
    import argparse
    from datetime import datetime

    # Parse arguments
    parser = argparse.ArgumentParser(description="Scrape ESPN college basketball data")

    # Add gender parameter
    parser.add_argument("--gender",
                        type=str,
                        choices=["mens", "womens"],
                        default="womens",
                        help="Gender of college basketball data to scrape (default: womens)")

    parser.add_argument("--seasons", type=int, nargs="+", help="List of seasons to scrape (e.g., 2020 2021 2022)")
    parser.add_argument("--concurrency",
                        type=int,
                        default=DEFAULT_CONCURRENCY,
                        help=f"Maximum concurrent requests (default: {DEFAULT_CONCURRENCY})")
    parser.add_argument("--delay",
                        type=float,
                        default=DEFAULT_DELAY,
                        help=f"Delay between requests in seconds (default: {DEFAULT_DELAY})")
    parser.add_argument("--team-id", type=str, help="Specific team ID to scrape (for testing)")

    # Process arguments
    args = parser.parse_args()

    # Set gender
    set_gender(args.gender)

    # Determine seasons to scrape
    seasons = args.seasons or list(range(args.start_year, args.end_year + 1))

    # Run scraper
    asyncio.run(
        scrape_all_data(concurrency=args.concurrency,
                        delay=args.delay,
                        seasons=seasons,
                        team_id=args.team_id,
                        gender=args.gender))


if __name__ == "__main__":
    main()
