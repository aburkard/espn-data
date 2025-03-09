#!/usr/bin/env python3
"""
Test script to examine game data structure.
"""

import sys
import json
import logging
from espn_data.scraper import get_game_data
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger("espn_data_test")


def main():
    """Examine game data structure."""
    game_id = "401254961"  # Use the same game ID from the notebook

    # Get game data
    game_data = get_game_data(game_id)

    # Check for game date
    logger.info("Examining game data structure for date and venue info...")

    # Check header structure
    if 'header' in game_data:
        header = game_data['header']
        logger.info(f"Header keys: {header.keys()}")

        # Check for gameDate
        if 'gameDate' in header:
            logger.info(f"Found gameDate: {header['gameDate']}")
        else:
            logger.info("No gameDate in header")

        # Check competitions for date
        if 'competitions' in header and header['competitions']:
            comp = header['competitions'][0]
            logger.info(f"Competition keys: {comp.keys()}")
            if 'date' in comp:
                logger.info(f"Found date in competition: {comp['date']}")

    # Check for venue info
    found_venue = False

    # Check gameInfo
    if 'gameInfo' in game_data:
        gameInfo = game_data['gameInfo']
        logger.info(f"GameInfo keys: {gameInfo.keys()}")

        if 'venue' in gameInfo:
            venue = gameInfo['venue']
            logger.info(f"Found venue in gameInfo: {venue.get('fullName')}")
            found_venue = True

    # If not found yet, check header
    if not found_venue and 'header' in game_data:
        header = game_data['header']
        if 'venue' in header:
            venue = header['venue']
            logger.info(f"Found venue in header: {venue.get('fullName')}")
            found_venue = True

    # Save a small snippet of the structure
    snippet = {}
    if 'header' in game_data:
        snippet['header'] = {k: v for k, v in game_data['header'].items() if k in ['id', 'gameDate', 'venue']}
        if 'competitions' in game_data['header'] and game_data['header']['competitions']:
            snippet['header']['competitions'] = [{
                'date': comp.get('date'),
                'venue': comp.get('venue')
            } for comp in game_data['header']['competitions'][:1]]

    if 'gameInfo' in game_data:
        snippet['gameInfo'] = {k: v for k, v in game_data['gameInfo'].items() if k in ['venue', 'attendance']}

    # Write snippet to file
    with open('game_structure.json', 'w') as f:
        json.dump(snippet, f, indent=2)

    logger.info("Saved structure snippet to game_structure.json")

    return 0


if __name__ == "__main__":
    sys.exit(main())
