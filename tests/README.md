# ESPN Data Tests

This directory contains tests for the ESPN data processing code. The tests are written using pytest and can be run using the `run_tests.py` script.

## Running Tests

To run all tests:

```bash
python run_tests.py
```

To run specific tests by keyword:

```bash
python run_tests.py -k "test_name"
```

For example, to run only the stats processing tests:

```bash
python run_tests.py -k "stats"
```

## Test Options

The `run_tests.py` script supports the following options:

- `-v`, `--verbose`: Enable verbose output
- `-k`, `--keyword`: Keyword to filter tests
- `-m`, `--mark`: Run tests with specific marks
- `--collect-only`: Only collect tests, don't run them

## Test Structure

The tests are organized into the following files:

- `test_game_data.py`: Tests for game data processing
- `test_stats.py`: Tests for player and team statistics processing
- `test_teams.py`: Tests for team data retrieval
- `test_integration.py`: Integration tests that test the complete data pipeline

## Test Fixtures

Common test fixtures are defined in `conftest.py`, including:

- `sample_game_id`: A sample game ID for testing
- `season`: A season year for testing
- `stats_game_id`: A game ID with rich stats data
- `stats_season`: A season year for stats tests
- `integration_game_ids`: Game IDs for end-to-end testing
- `test_season`: A season year for integration tests
