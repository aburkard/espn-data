# ESPN Women's Basketball Data

A comprehensive scraper for women's college basketball data from ESPN. This tool collects team information, schedules, box scores, play-by-play data, referee information, and other statistics across all available seasons.

## Features

- Fetch data for all women's college basketball teams
- Collect complete schedule history for each team
- Extract detailed game data including box scores and play-by-play
- Store data in structured formats for analysis
- Asynchronous requests for efficient data collection

## Setup

1. Clone this repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```
3. Run the scraper:
   ```
   python -m espn_data.scraper
   ```

## Data Sources

The scraper uses ESPN's API endpoints including:

- Teams: `https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams`
  - Pagination options:
    - Using limit: `https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams?limit=500`
    - Using page: `https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams?page=2`
    - Hybrid approach (most efficient): `https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams?page=1&limit=500`
- Team Schedules: `https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/schedule`
  - Season parameter: `&season=2023`
- Game Data: `https://site.web.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/summary?event={game_id}`

## Output

Data is saved in the `data/` directory with the following structure:

- `teams.json`: Information about all teams
- `schedules/{team_id}.json`: Schedule history for each team
- `games/{game_id}.json`: Detailed game data
