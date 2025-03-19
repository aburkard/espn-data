"""Main entry point for the ESPN College Basketball Data Scraper."""

import argparse
import asyncio
import logging
from datetime import datetime
from typing import List, Optional

from espn_data.scraper import scrape_all_data
from espn_data.processor import process_all_data
from espn_data.utils import set_gender, get_current_gender

logger = logging.getLogger("espn_data")


async def main() -> None:
    """
    Main entry point for the full scraping and processing workflow.
    """
    parser = argparse.ArgumentParser(description="ESPN College Basketball Data Scraper",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # Add command group
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--scrape", action="store_true", help="Run only the scraper")
    group.add_argument("--process", action="store_true", help="Run only the processor")

    # Add gender parameter
    parser.add_argument("--gender",
                        type=str,
                        choices=["mens", "womens"],
                        default="womens",
                        help="Gender of college basketball data to scrape/process")

    # Add season parameters
    current_year = datetime.now().year
    parser.add_argument("--seasons",
                        type=int,
                        nargs="+",
                        help=f"List of seasons to scrape (e.g., 2020 2021 2022). Default: 2022-{current_year}")
    parser.add_argument("--start-year", type=int, default=2003, help="Start year for season range (inclusive)")
    parser.add_argument("--end-year", type=int, default=current_year, help="End year for season range (inclusive)")

    # Add performance parameters
    parser.add_argument("--concurrency", type=int, default=5, help="Maximum concurrent requests")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests in seconds")
    parser.add_argument("--max-workers",
                        type=int,
                        default=4,
                        help="Maximum number of concurrent processes for data processing")

    # Add test mode for single team
    parser.add_argument("--team-id", type=str, help="Specific team ID to scrape (for testing)")

    # Add option to process specific games only
    parser.add_argument("--game-ids",
                        type=str,
                        nargs="+",
                        help="List of specific game IDs to scrape or process (e.g., 401580411 401580412)")

    # Add logging level option
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    # Add force option to override cached data
    parser.add_argument("--force",
                        action="store_true",
                        help="Force re-scraping or re-processing of data, ignoring cached files")

    # Add verbose cache option
    parser.add_argument("--verbose",
                        action="store_true",
                        help="Log detailed information (e.g., each cached or processed game)")

    args = parser.parse_args()

    # Configure logging based on command-line argument
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler("espn_scraper.log"),
                                  logging.StreamHandler()])
    # Set the level for the named logger too
    logger.setLevel(log_level)
    logger.info(f"Logging level set to {'DEBUG' if args.debug else 'INFO'}")

    # Set gender
    set_gender(args.gender)
    logger.info(f"Using gender: {args.gender}")

    # Determine seasons to scrape
    seasons: Optional[List[int]] = None
    if args.seasons:
        seasons = args.seasons
    else:
        seasons = list(range(args.start_year, args.end_year + 1))

    logger.info(f"Working with seasons: {min(seasons)} to {max(seasons)}")

    # Run selected workflow
    if args.process:
        # Only run the processor
        logger.info(f"Running data processor for {get_current_gender()} basketball")
        process_all_data(seasons=seasons,
                         max_workers=args.max_workers,
                         gender=args.gender,
                         force=args.force,
                         game_ids=args.game_ids,
                         verbose=args.verbose)
    elif args.scrape:
        # Only run the scraper
        logger.info(f"Running data scraper for {get_current_gender()} basketball")
        await scrape_all_data(concurrency=args.concurrency,
                              delay=args.delay,
                              seasons=seasons,
                              team_id=args.team_id,
                              gender=args.gender,
                              game_ids=args.game_ids,
                              force=args.force,
                              verbose=args.verbose)
    else:
        # Run the full workflow
        logger.info(f"Running full workflow for {get_current_gender()} basketball")
        await scrape_all_data(concurrency=args.concurrency,
                              delay=args.delay,
                              seasons=seasons,
                              team_id=args.team_id,
                              gender=args.gender,
                              game_ids=args.game_ids,
                              force=args.force,
                              verbose=args.verbose)

        # Step 2: Process data
        process_all_data(seasons=seasons,
                         max_workers=args.max_workers,
                         gender=args.gender,
                         game_ids=args.game_ids,
                         force=args.force,
                         verbose=args.verbose)

    logger.info("Workflow completed successfully")


if __name__ == "__main__":
    # Run the command-line interface
    asyncio.run(main())
