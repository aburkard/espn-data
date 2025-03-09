#!/usr/bin/env python3
"""
Test script to demonstrate the improved player and team statistics processing.
"""

import sys
import logging
import pandas as pd
from espn_data.scraper import get_game_data
from espn_data.processor import process_game_data

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("stats_test")


def main():
    """Test the player and team stats processing with a sample game."""
    # Use the Arkansas @ UConn game as our test
    game_id = "401373084"

    logger.info(f"Processing stats for game {game_id}")
    processed_data = process_game_data(game_id)

    # 1. Display game information
    if "game_info" in processed_data:
        game_info = processed_data["game_info"]
        logger.info(f"Game: {game_id}")
        logger.info(f"Date: {game_info.get('date')}")
        logger.info(f"Venue: {game_info.get('venue')} {game_info.get('venue_location', '')}")
        logger.info(f"Attendance: {game_info.get('attendance')}")
        logger.info("-" * 50)

    # 2. Display team information and box scores
    if "teams_info" in processed_data:
        for team in processed_data["teams_info"]:
            team_id = team.get("team_id")
            logger.info(f"Team: {team.get('team_name')} ({team.get('home_away', '').upper()})")
            logger.info(f"Score: {team.get('score')}")
            logger.info(f"Winner: {'YES' if team.get('winner') else 'NO'}")
            logger.info("-" * 30)

            # Find team's box score stats
            if "team_stats" in processed_data:
                for stats in processed_data["team_stats"]:
                    if stats.get("team_id") == team_id:
                        logger.info("Box Score:")
                        logger.info(
                            f"FG: {stats.get('fieldGoalsMade-fieldGoalsAttempted')} ({stats.get('fieldGoalPct')}%)")
                        logger.info(
                            f"3PT: {stats.get('threePointFieldGoalsMade-threePointFieldGoalsAttempted')} ({stats.get('threePointFieldGoalPct')}%)"
                        )
                        logger.info(
                            f"FT: {stats.get('freeThrowsMade-freeThrowsAttempted')} ({stats.get('freeThrowPct')}%)")
                        logger.info(
                            f"Rebounds: {stats.get('totalRebounds')} (Off: {stats.get('offensiveRebounds')}, Def: {stats.get('defensiveRebounds')})"
                        )
                        logger.info(f"Assists: {stats.get('assists')}")
                        logger.info(f"Steals: {stats.get('steals')}")
                        logger.info(f"Blocks: {stats.get('blocks')}")
                        logger.info(f"Turnovers: {stats.get('turnovers')}")
                        logger.info(f"Fouls: {stats.get('fouls')}")
                        logger.info(f"Points in Paint: {stats.get('pointsInPaint')}")
                        logger.info(f"Fast Break Points: {stats.get('fastBreakPoints')}")
                        logger.info(f"Points Off Turnovers: {stats.get('turnoverPoints')}")

                        # Print all remaining stats for reference
                        logger.info("All available stats:")
                        stats_df = pd.DataFrame([{
                            k: v
                            for k, v in stats.items()
                            if k not in ["game_id", "team_id", "team_name", "team_abbreviation", "home_away"]
                        }])
                        logger.info(f"\n{stats_df.T}")

            logger.info("-" * 50)

    # 3. Display player stats for each team
    if "player_stats" in processed_data:
        player_stats_df = pd.DataFrame(processed_data["player_stats"])

        # Group by team
        for team_name, team_group in player_stats_df.groupby('team_name'):
            logger.info(f"{team_name} Player Statistics:")

            # For players who actually played
            active_players = team_group[team_group['did_not_play'] == False].copy()

            # Select key stats and reformat for display
            if not active_players.empty:
                # Check available columns
                shot_columns = [col for col in active_players.columns if col in ['FG', 'FG_MADE', 'FG_ATT', 'FG_PCT']]
                three_pt_columns = [
                    col for col in active_players.columns if col in ['3PT', '3PT_MADE', '3PT_ATT', '3PT_PCT']
                ]
                ft_columns = [col for col in active_players.columns if col in ['FT', 'FT_MADE', 'FT_ATT', 'FT_PCT']]

                display_cols = ['player_name', 'position', 'starter']

                # Add minutes if available
                if 'MIN' in active_players.columns:
                    display_cols.append('MIN')

                # Add shooting stats
                if 'FG' in active_players.columns:
                    display_cols.append('FG')
                if 'FG_PCT' in active_players.columns:
                    display_cols.append('FG_PCT')

                # Add three-point stats
                if '3PT' in active_players.columns:
                    display_cols.append('3PT')
                if '3PT_PCT' in active_players.columns:
                    display_cols.append('3PT_PCT')

                # Add free throw stats
                if 'FT' in active_players.columns:
                    display_cols.append('FT')
                if 'FT_PCT' in active_players.columns:
                    display_cols.append('FT_PCT')

                # Add other key stats
                for stat in ['PTS', 'REB', 'AST', 'STL', 'BLK', 'TO', 'PF']:
                    if stat in active_players.columns:
                        display_cols.append(stat)

                # Create a display table
                display_stats = active_players[display_cols].copy()

                # Format the table for better readability
                logger.info(f"\n{display_stats.to_string(index=False)}")

            # Show any DNPs
            dnp_players = team_group[team_group['did_not_play'] == True]
            if not dnp_players.empty:
                logger.info("\nDid Not Play:")
                for _, player in dnp_players.iterrows():
                    logger.info(f"- {player['player_name']}")

            logger.info("-" * 50)

    # 4. Show a summary of available data
    logger.info("\nData Summary:")
    logger.info(f"Teams: {len(processed_data.get('teams_info', []))}")
    logger.info(f"Players: {len(processed_data.get('player_stats', []))}")
    logger.info(f"Team Box Scores: {len(processed_data.get('team_stats', []))}")
    logger.info(f"Plays: {len(processed_data.get('play_by_play', []))}")


if __name__ == "__main__":
    main()
