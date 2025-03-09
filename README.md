# ESPN Women's Basketball Data

A comprehensive scraper and processor for women's college basketball data from ESPN. This tool collects team information, schedules, box scores, play-by-play data, referee information, and other statistics across all available seasons.

## Features

- Fetch data for all women's college basketball teams
- Collect complete schedule history for each team
- Extract detailed game data including box scores and play-by-play data
- Extract referee/officials information for each game
- Process and transform data into analysis-ready formats
- Store data in both CSV and Parquet formats for flexibility
- Organize data by season for better management
- Asynchronous requests for efficient data collection

## Setup

1. Clone this repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Run the tool:
   ```
   python -m espn_data
   ```

## Usage

The tool has several command-line options for flexibility:

### Basic Usage

```bash
# Run the full pipeline (scrape + process) for the last two seasons
python -m espn_data

# Specify specific seasons
python -m espn_data --seasons 2022 2023

# Specify a range of seasons
python -m espn_data --start-year 2020 --end-year 2023
```

### Advanced Options

```bash
# Only scrape data (don't process)
python -m espn_data --scrape --seasons 2023

# Only process previously scraped data
python -m espn_data --process --seasons 2022 2023

# Test with a single team (useful for development)
python -m espn_data --scrape --seasons 2023 --team-id 52

# Adjust concurrency and delays for scraping
python -m espn_data --concurrency 10 --delay 0.2

# Adjust number of parallel workers for processing
python -m espn_data --max-workers 8
```

## Data Sources

The scraper uses ESPN's API endpoints including:

- Teams: `https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams`
- Team Schedules: `https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/schedule?season={season}`
- Game Data: `https://site.web.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/summary?event={game_id}`

## Data Structure

Data is organized in the following structure:

```
data/
├── raw/                        (Raw data from ESPN)
│   ├── teams.json             (Global teams information)
│   ├── 2023/                  (Season-specific data)
│   │   ├── schedules/
│   │   │   └── {TEAM_ID}.json
│   │   └── games/
│   │       └── {GAME_ID}.json
│   └── 2022/
│       └── ...
├── processed/
│   ├── csv/                   (CSV format data)
│   │   ├── teams.csv         (Global teams information)
│   │   ├── 2023/             (Season-specific processed data)
│   │   │   ├── schedules.csv
│   │   │   ├── game_summary.csv
│   │   │   └── games/
│   │   │       └── {GAME_ID}/
│   │   │           ├── game_info.csv
│   │   │           ├── teams_info.csv
│   │   │           ├── player_stats.csv
│   │   │           ├── team_stats.csv
│   │   │           ├── play_by_play.csv
│   │   │           └── officials.csv
│   │   └── 2022/
│   │       └── ...
│   └── parquet/               (Same structure as CSV but with Parquet files)
│       └── ...
```

### Extracted Data Types

For each game, the following data types are extracted:

- **Game Info**: Basic information about the game (date, venue, attendance, etc.)
- **Teams Info**: Information about the participating teams
- **Player Stats**: Individual player statistics
- **Team Stats**: Team-level statistics
- **Play-by-Play**: Detailed play-by-play data
- **Officials**: Information about referees/officials

## Performance Tips

- Use `--concurrency` and `--delay` to balance between speed and avoiding rate limits
- Process data season by season to manage memory usage
- Parquet files are more efficient for analysis than CSV files
- For large datasets, consider using `--max-workers` to adjust parallel processing

## License

MIT
