#!/usr/bin/env python3
"""
Sample script to demonstrate the correct game data extraction.
"""

import sys
import logging
from espn_data.scraper import get_game_data, get_team_schedule

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger("demo")


def main():
    """Demo the correct game data extraction."""
    # Get UConn schedule
    team_id = "41"  # UConn Huskies
    seasons = [2021]  # Just one season for the example

    logger.info(f"Getting schedule for UConn (team_id={team_id})")
    schedule = get_team_schedule(team_id, seasons)

    if not schedule:
        logger.error("No schedule data found for UConn")
        return 1

    # Get the first game
    game_id = schedule[0]['id']
    game_name = schedule[0]['name']
    logger.info(f"Getting data for game {game_id}: {game_name}")

    # Fetch game data
    game_data = get_game_data(game_id)

    # Extract basic game info - CORRECT VERSION
    logger.info("\n===== CORRECT EXTRACTION =====")
    if 'header' in game_data:
        header = game_data['header']

        # Get date from competitions array
        game_date = None
        if 'competitions' in header and header['competitions']:
            game_date = header['competitions'][0].get('date')
        logger.info(f"Game date: {game_date}")

        # Get venue from gameInfo object
        venue_name = None
        venue_city = None
        venue_state = None
        if 'gameInfo' in game_data and 'venue' in game_data['gameInfo']:
            venue = game_data['gameInfo']['venue']
            venue_name = venue.get('fullName')
            if 'address' in venue:
                venue_city = venue['address'].get('city')
                venue_state = venue['address'].get('state')

        if venue_name:
            location = f"{venue_city}, {venue_state}" if venue_city and venue_state else ""
            logger.info(f"Venue: {venue_name} {location}")
        else:
            logger.info("Venue: Not available")

        # Show attendance if available
        if 'gameInfo' in game_data and 'attendance' in game_data['gameInfo']:
            attendance = game_data['gameInfo']['attendance']
            logger.info(f"Attendance: {attendance}")

        # Show competitors
        if 'competitions' in header and header['competitions']:
            competition = header['competitions'][0]
            if 'competitors' in competition:
                logger.info("\nCompetitors:")
                for team in competition['competitors']:
                    team_name = team.get('team', {}).get('displayName', 'Unknown')
                    score = team.get('score', 'N/A')
                    home_away = team.get('homeAway', 'N/A').upper()
                    winner = "(WINNER)" if team.get('winner', False) else ""
                    logger.info(f"  {team_name} ({home_away}): {score} {winner}")

    # Extract basic game info - INCORRECT VERSION (just for comparison)
    logger.info("\n===== INCORRECT EXTRACTION =====")
    if 'header' in game_data:
        header = game_data['header']
        logger.info(f"Game date: {header.get('gameDate')}")
        logger.info(f"Venue: {header.get('venue', {}).get('fullName')}")

        # Show competitors
        if 'competitions' in header and header['competitions']:
            competition = header['competitions'][0]
            if 'competitors' in competition:
                logger.info("\nCompetitors:")
                for team in competition['competitors']:
                    team_name = team.get('team', {}).get('displayName', 'Unknown')
                    score = team.get('score', 'N/A')
                    home_away = team.get('homeAway', 'N/A').upper()
                    winner = "(WINNER)" if team.get('winner', False) else ""
                    logger.info(f"  {team_name} ({home_away}): {score} {winner}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
