#!/usr/bin/env python3
"""
Demo script showing the correct way to extract game data.
Run this script to see proper data extraction in action.
"""

import sys
import logging
from espn_data.scraper import get_game_data
from espn_data.processor import get_game_details

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger("demo")


def main():
    """Demo the correct ways to extract game data."""
    # Use a specific game ID for the demonstration
    game_id = "401373084"  # Arkansas @ UConn game

    logger.info(f"Fetching data for game {game_id}")
    game_data = get_game_data(game_id)

    # Show what keys are available in the game data
    logger.info(f"Game data contains these top-level keys: {list(game_data.keys())}")

    logger.info("\n===== METHOD 1: DIRECT PATH ACCESS =====")

    # Properly extract the game date
    game_date = None
    if 'header' in game_data and 'competitions' in game_data['header'] and game_data['header']['competitions']:
        game_date = game_data['header']['competitions'][0].get('date')
    logger.info(f"Game date: {game_date}")

    # Properly extract venue information
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

    # Extract attendance
    if 'gameInfo' in game_data and 'attendance' in game_data['gameInfo']:
        attendance = game_data['gameInfo']['attendance']
        logger.info(f"Attendance: {attendance}")
    else:
        logger.info("Attendance: Not available")

    logger.info("\n===== METHOD 2: USING HELPER FUNCTION =====")

    # Use the helper function for cleaner code
    details = get_game_details(game_data)

    # Display game information
    logger.info(f"Game date: {details['date']}")

    venue_str = details['venue_name'] or "Not available"
    if details['venue_location']:
        venue_str += f" ({details['venue_location']})"
    logger.info(f"Venue: {venue_str}")

    logger.info(f"Attendance: {details['attendance'] or 'Not available'}")

    logger.info("\n===== TEAM AND SCORE INFORMATION =====")

    # Show competitors info
    if 'header' in game_data and 'competitions' in game_data['header'] and game_data['header']['competitions']:
        competition = game_data['header']['competitions'][0]
        if 'competitors' in competition:
            for team in competition['competitors']:
                team_name = team.get('team', {}).get('displayName', 'Unknown')
                score = team.get('score', 'N/A')
                home_away = team.get('homeAway', 'N/A').upper()
                winner = "WINNER" if team.get('winner', False) else ""
                logger.info(f"  {team_name} ({home_away}): {score} {winner}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
