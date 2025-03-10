import pytest
import time
from espn_data.scraper import get_all_teams
from espn_data.utils import get_team_count, make_request, TEAMS_URL


def test_direct_limit():
    """Test directly using the limit parameter."""
    response = make_request(TEAMS_URL, params={"limit": 500})

    assert response is not None, "Failed to get response from teams API"
    assert "sports" in response, "Missing sports field in response"

    teams = response["sports"][0]["leagues"][0]["teams"]
    team_count = len(teams)

    assert team_count > 300, f"Expected more than 300 teams, got {team_count}"
    assert team_count < 500, f"Expected fewer than 500 teams, got {team_count}"


def test_pagination():
    """Test pagination approach to get teams."""
    # Try different page sizes
    page_sizes = [50, 100]
    for page_size in page_sizes:
        all_teams = []
        page = 1

        while True:
            response = make_request(TEAMS_URL, params={"limit": page_size, "page": page})

            assert response is not None, f"Failed to get response for page {page}"
            assert "sports" in response, f"Missing sports field in response for page {page}"

            teams = response["sports"][0]["leagues"][0]["teams"]
            if not teams:
                break

            all_teams.extend(teams)
            page += 1

            # Don't try more than 10 pages
            if page > 10:
                break

        assert len(all_teams) > 300, f"Expected more than 300 teams, got {len(all_teams)}"


def test_get_all_teams_function():
    """Test the get_all_teams function."""
    teams = get_all_teams()

    assert teams is not None, "Failed to get teams"
    assert len(teams) > 300, f"Expected more than 300 teams, got {len(teams)}"

    # Check the structure of team data
    first_team = teams[0]

    # Check for essential team fields
    essential_fields = ['id', 'displayName', 'abbreviation']
    for field in essential_fields:
        assert field in first_team, f"Missing {field} field in team data"

    # Check that the team ID is a valid integer
    assert str(first_team['id']).isdigit(), "Team ID is not a valid integer"

    # Check that the team name is not empty
    assert first_team['displayName'] and len(first_team['displayName']) > 0, "Team name is empty"
