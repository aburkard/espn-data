import os
import sys
import pytest
import logging

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Set up logging for tests
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])


@pytest.fixture
def sample_game_id():
    """Fixture to provide a sample game ID for tests."""
    return "401480248"  # Using the same ID from test_changes.py


@pytest.fixture
def season():
    """Fixture to provide a season year for tests."""
    return 2023
