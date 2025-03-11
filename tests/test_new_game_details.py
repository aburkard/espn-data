import json
import pytest
from espn_data.processor import get_game_details


def test_get_game_details_with_new_fields():
    """Test the updated get_game_details function with mock data including the new fields."""
    # Create mock competition data that includes all the new fields we've added
    mock_game_data = {
        'gameId': 'test123',
        'header': {
            'competitions': [{
                'id': '401480241',
                'date': '2022-11-24T16:00Z',
                'neutralSite': True,
                'boxscoreAvailable': True,
                'boxscoreSource': 'full',
                'playByPlaySource': 'full',
                'competitors': [{
                    'id': '52',
                    'homeAway': 'home',
                    'winner': False,
                    'score': '77',
                    'linescores': [{
                        'displayValue': '21'
                    }, {
                        'displayValue': '14'
                    }, {
                        'displayValue': '17'
                    }, {
                        'displayValue': '25'
                    }],
                    'record': [{
                        'type': 'total',
                        'summary': '5-1',
                        'displayValue': '5-1'
                    }, {
                        'type': 'home',
                        'summary': '3-0',
                        'displayValue': '3-0'
                    }],
                    'team': {
                        'id': '52',
                        'location': 'Florida State',
                        'name': 'Seminoles',
                        'abbreviation': 'FSU',
                        'displayName': 'Florida State Seminoles',
                        'color': '782f40',
                        'groups': {
                            'id': '2',
                            'name': 'Atlantic Coast Conference',
                            'slug': 'atlantic-coast-conference',
                            'parent': {
                                'id': '50',
                                'name': 'NCAA Division I',
                                'slug': 'ncaa-division-i'
                            },
                            'isConference': True
                        }
                    }
                }, {
                    'id': '197',
                    'homeAway': 'away',
                    'winner': True,
                    'score': '79',
                    'linescores': [{
                        'displayValue': '29'
                    }, {
                        'displayValue': '20'
                    }, {
                        'displayValue': '10'
                    }, {
                        'displayValue': '20'
                    }],
                    'record': [{
                        'type': 'total',
                        'summary': '5-1',
                        'displayValue': '5-1'
                    }, {
                        'type': 'road',
                        'summary': '1-0',
                        'displayValue': '1-0'
                    }],
                    'team': {
                        'id': '197',
                        'location': 'Oklahoma State',
                        'name': 'Cowgirls',
                        'abbreviation': 'OKST',
                        'displayName': 'Oklahoma State Cowgirls',
                        'color': '000000',
                        'groups': {
                            'id': '8',
                            'name': 'Big 12 Conference',
                            'slug': 'big-12-conference',
                            'parent': {
                                'id': '50',
                                'name': 'NCAA Division I',
                                'slug': 'ncaa-division-i'
                            },
                            'isConference': True
                        }
                    }
                }],
                'status': {
                    'type': {
                        'id': '3',
                        'name': 'STATUS_FINAL',
                        'state': 'post',
                        'completed': True,
                        'description': 'Final',
                        'detail': 'Final',
                        'shortDetail': 'Final'
                    }
                },
                'broadcasts': []
            }]
        }
    }

    # Call the function with our mock data
    details = get_game_details(mock_game_data)

    # Verify the new fields are present and correctly processed
    assert details is not None, "Failed to get game details"

    # Check boxscore availability fields
    assert details["boxscore_available"] is True, "Boxscore available flag not correctly parsed"
    assert details["boxscore_source"] == "full", "Boxscore source not correctly parsed"
    assert details["play_by_play_source"] == "full", "Play-by-play source not correctly parsed"

    # Check team information including new fields
    assert len(details["teams"]) == 2, "Should have extracted 2 teams"

    home_team = next((team for team in details["teams"] if team["home_away"] == "home"), None)
    away_team = next((team for team in details["teams"] if team["home_away"] == "away"), None)

    assert home_team is not None, "Home team not found"
    assert away_team is not None, "Away team not found"

    # Check linescores
    assert len(home_team["linescores"]) == 4, "Home team should have 4 linescores"
    assert home_team["linescores"][0] == "21", "Home team first quarter score incorrect"

    assert len(away_team["linescores"]) == 4, "Away team should have 4 linescores"
    assert away_team["linescores"][0] == "29", "Away team first quarter score incorrect"

    # Check division and conference info
    assert home_team["division"] == "NCAA Division I", "Home team division incorrect"
    assert int(home_team["conference_id"]) == 2, "Home team conference id incorrect"
    assert home_team["conference_slug"] == "atlantic-coast-conference", "Home team conference slug incorrect"

    assert away_team["division"] == "NCAA Division I", "Away team division incorrect"
    assert int(away_team["conference_id"]) == 8, "Away team conference id incorrect"
    assert away_team["conference_slug"] == "big-12-conference", "Away team conference slug incorrect"
