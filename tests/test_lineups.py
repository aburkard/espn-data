"""Smoke tests for lineup tracking and prediction."""

from pathlib import Path

import pandas as pd
import pytest

from espn_data.lineups import load_game, predict_lineup_probabilities


@pytest.fixture
def sample_game_path():
    games_dir = Path(__file__).parent.parent / "data" / "raw" / "mens" / "2026" / "games"
    if not games_dir.exists():
        pytest.skip(f"No game data at {games_dir}")
    files = sorted(games_dir.glob("*.json"))
    if not files:
        pytest.skip(f"No game JSON files in {games_dir}")
    return files[0]


def test_predict_lineup_probabilities_returns_dataframe(sample_game_path):
    game = load_game(sample_game_path)
    df = predict_lineup_probabilities(game)

    assert df is not None, "predict_lineup_probabilities returned None"
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) == {"sequence_number", "team_id", "player_id", "probability"}
    assert len(df) > 0


def test_predict_lineup_probabilities_sums_to_five(sample_game_path):
    """Logit-shift normalization should make each (play, team) sum to exactly 5."""
    game = load_game(sample_game_path)
    df = predict_lineup_probabilities(game)

    sums = df.groupby(["sequence_number", "team_id"])["probability"].sum()
    assert (sums - 5.0).abs().max() < 1e-4, (
        f"sum-to-5 invariant violated; min={sums.min():.4f}, max={sums.max():.4f}"
    )


def test_predict_lineup_probabilities_unnormalized(sample_game_path):
    """With normalize=False, probabilities are independent (won't sum to 5)."""
    game = load_game(sample_game_path)
    df = predict_lineup_probabilities(game, normalize=False)

    assert df is not None
    assert (df["probability"] >= 0).all()
    assert (df["probability"] <= 1).all()
