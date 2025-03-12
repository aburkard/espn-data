# Teams who are no longer D1 (and some teams who recently joined D1) aren't returned from ESPN's teams API endpoint.
# We can fetch them individually from the team endpoint.
MISSING_MENS_TEAMS = [{
    "team_id": 3,
    "name": "Birmingham-Southern Panthers",
    "first_d1_season": 2003,
    "last_d1_season": 2006
}, {
    "team_id": 2113,
    "name": "Centenary (LA) Gentlemen",
    "first_d1_season": 1985,
    "last_d1_season": 2011
}, {
    "team_id": 42,
    "name": "Hartford Hawks",
    "first_d1_season": 1985,
    "last_d1_season": 2023
}, {
    "team_id": 2417,
    "name": "Morris Brown Wolverines",
    "first_d1_season": 2002,
    "last_d1_season": 2003
}, {
    "team_id": 2542,
    "name": "Savannah State Tigers",
    "first_d1_season": 2003,
    "last_d1_season": 2019
}, {
    "team_id": 2597,
    "name": "St. Francis Brooklyn Terriers",
    "first_d1_season": 1985,
    "last_d1_season": 2023
}, {
    "team_id": 2736,
    "name": "Winston-Salem Rams",
    "first_d1_season": 2007,
    "last_d1_season": 2010
}, {
    "team_id": 2815,
    "name": "Lindenwood Lions",
    "first_d1_season": 2023,
    "last_d1_season": 2025
}, {
    "team_id": 2511,
    "name": "Queens University Royals",
    "first_d1_season": 2023,
    "last_d1_season": 2025
}, {
    "team_id": 88,
    "name": "Southern Indiana Screaming Eagles",
    "first_d1_season": 2023,
    "last_d1_season": 2025
}]

# List of women's teams that need to be fetched individually
MISSING_WOMENS_TEAMS = [
    # {
    #     "team_id": None,
    #     "name": "Alliant International Gulls"
    # },
    # {
    # They were D2 since 1987, no longer exist now
    #     "team_id": 381,
    #     "name": "Armstrong State Lady Pirates"
    # },
    # {
    #     "team_id": 2041,
    #     "name": "Augusta Jaguars"
    # },
    {
        "team_id": 3,
        "name": "Birmingham-Southern Panthers"
    },
    # {
    #     "team_id": None,
    #     "name": "Brooklyn Bulldogs"
    # },
    {
        "team_id": 2113,
        "name": "Centenary (LA) Gentlemen"  # Yes it's Gentlemen on ESPN, but should be Ladies
    },
    # {
    #     "team_id": None,
    #     "name": "Hardin-Simmons Cowgirls"
    # },
    {
        "team_id": 42,
        "name": "Hartford Hawks"
    },
    {
        "team_id": 2417,
        "name": "Morris Brown Wolverines"
    },
    # {
    #     "team_id": None,
    #     "name": "Northeastern Illinois Golden Eagles"
    # },
    # {
    #     "team_id": None,
    #     "name": "Oklahoma City Stars"
    # },
    {
        "team_id": 2542,
        "name": "Savannah State Tigers"
    },
    {
        "team_id": 2597,
        "name": "St. Francis Brooklyn Terriers"
    },
    # {
    #     "team_id": None,
    #     "name": "Utica Pioneers"
    # },
    {
        "team_id": 2736,
        "name": "Winston-Salem Rams"
    },
    # {
    #     "team_id": None,
    #     "name": "West Texas A&M Buffaloes"
    # },
    {
        "team_id": 2385,
        "name": "Mercyhurst Lakers"
    },
    {
        "team_id": 2698,
        "name": "West Georgia Wolves"
    },
]
