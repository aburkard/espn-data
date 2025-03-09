#!/usr/bin/env python3
"""
Test script to verify the number of teams we can fetch from ESPN.
"""

import sys
import logging
import argparse
import time
from espn_data.scraper import get_all_teams
from espn_data.utils import get_team_count, make_request, TEAMS_URL

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger("espn_data_test")


def test_direct_limit():
    """Test directly using the limit parameter."""
    logger.info("Testing direct limit parameter")

    start_time = time.time()
    response = make_request(TEAMS_URL, params={"limit": 500})
    elapsed = time.time() - start_time

    if response and "sports" in response:
        teams = response["sports"][0]["leagues"][0]["teams"]
        team_count = len(teams)
        logger.info(f"Direct limit parameter returned {team_count} teams in {elapsed:.2f} seconds")
        return True, team_count
    else:
        logger.warning("Direct limit parameter failed")
        return False, 0


def test_pagination():
    """Test pagination using the page parameter."""
    logger.info("Testing pagination with page parameter")

    teams = []
    page = 1
    start_time = time.time()

    while True:
        response = make_request(TEAMS_URL, params={"page": page})

        if not response or "sports" not in response:
            break

        try:
            page_teams = response["sports"][0]["leagues"][0]["teams"]
            if not page_teams:
                break

            teams.extend(page_teams)
            logger.info(f"Page {page}: {len(page_teams)} teams")

            page += 1
            time.sleep(0.5)  # Be nice to the API

        except Exception as e:
            logger.error(f"Error: {e}")
            break

    elapsed = time.time() - start_time
    logger.info(f"Pagination returned {len(teams)} teams in {elapsed:.2f} seconds")
    return len(teams)


def test_hybrid():
    """Test using both a high limit and pagination."""
    logger.info("Testing hybrid approach with high limit and pagination")

    teams = []
    page = 1
    limit = 500
    start_time = time.time()

    while True:
        logger.info(f"Fetching page {page} with limit {limit}")
        response = make_request(TEAMS_URL, params={"page": page, "limit": limit})

        if not response or "sports" not in response:
            break

        try:
            page_teams = response["sports"][0]["leagues"][0]["teams"]
            page_team_data = [team["team"] for team in page_teams]

            if not page_team_data:
                break

            teams.extend(page_team_data)
            logger.info(f"Page {page}: {len(page_team_data)} teams")

            # If we got fewer teams than the limit, we've reached the last page
            if len(page_team_data) < limit:
                logger.info("Reached last page of results")
                break

            page += 1
            time.sleep(0.5)  # Be nice to the API

        except Exception as e:
            logger.error(f"Error: {e}")
            break

    elapsed = time.time() - start_time
    logger.info(f"Hybrid approach returned {len(teams)} teams in {elapsed:.2f} seconds")
    return len(teams)


def main():
    """Test the get_all_teams function."""
    parser = argparse.ArgumentParser(description="Test ESPN teams pagination")
    parser.add_argument("--quick", action="store_true", help="Only retrieve one page of teams (quick test)")
    parser.add_argument(
        "--method",
        choices=["auto", "limit", "page", "hybrid"],
        default="auto",
        help="Method to use: auto (use scraper), limit (single request), page (pagination), hybrid (limit+page)")
    args = parser.parse_args()

    logger.info("Starting teams test")

    # Test different methods if requested
    if args.method == "limit":
        logger.info("============= TESTING LIMIT PARAMETER =============")
        success, count = test_direct_limit()
        return 0
    elif args.method == "page":
        logger.info("============= TESTING PAGE PARAMETER =============")
        count = test_pagination()
        return 0
    elif args.method == "hybrid":
        logger.info("============= TESTING HYBRID APPROACH =============")
        count = test_hybrid()
        return 0

    # Get estimated total team count
    estimated_count = get_team_count()
    logger.info(f"Estimated total teams according to API: {estimated_count}")

    # Get actual teams - use max_teams to limit for quick test
    max_teams = 100 if args.quick else None

    logger.info("============= USING SCRAPER IMPLEMENTATION =============")
    start_time = time.time()
    teams = get_all_teams(max_teams=max_teams)
    elapsed = time.time() - start_time

    # Print results
    team_count = len(teams)
    logger.info(f"Retrieved {team_count} teams in {elapsed:.2f} seconds")

    # Display sample teams
    if teams:
        logger.info("\nSample team data:")
        for i, team in enumerate(teams[:10]):
            logger.info(f"{i+1}. {team.get('displayName', 'Unknown')} (ID: {team.get('id', 'Unknown')})")

    # Check if we got a reasonable number of teams
    if team_count < 100 and not args.quick:
        logger.warning("Retrieved fewer teams than expected. Team retrieval may not be working correctly.")
    elif team_count > 300 or (args.quick and team_count > 50):
        logger.info("Team retrieval appears to be working correctly.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
