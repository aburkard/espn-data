"""Game control metrics derived from play-by-play data.

Computes team-strength-neutral metrics that capture how a game actually
played out, independent of who was on the court. Useful as features or
targets in predictive models (e.g. March Madness brackets) because they
reduce noise from garbage time, late fouling, and pace variation.

Metrics produced per game (from the home team's perspective):
    final_margin          Raw final score difference
    avg_score_diff        Time-weighted average score differential
    trunc_avg_diff        Same but excluding last 2 minutes of regulation
    avg_naive_wp          Time-weighted average of score-only win probability
    soft_lev_margin       Leverage-weighted score diff (garbage time downweighted)

The naive win probability model uses only score differential and time
remaining (no team identity), so it carries no team-strength leakage.
"""

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

logger = logging.getLogger("espn_data")

# Play types that are non-events for game state tracking
_NOISE_PLAY_TYPES = frozenset([
    "Substitution", "OfficialTVTimeOut", "ShortTimeOut", "RegularTimeOut",
])

REGULATION_SECONDS = 2400  # 2 × 20-minute halves
GARBAGE_CUTOFF_SECONDS = 120  # last 2 minutes excluded for trunc metric


# ---------------------------------------------------------------------------
# Naive (score-only) win probability model
# ---------------------------------------------------------------------------

class NaiveWPModel:
    """Logistic regression: P(home_win) ~ score_diff, score_diff×time_frac, time_frac.

    Trained on one or more seasons of play-by-play data. Uses only in-game
    state (score and clock), never team identity.
    """

    def __init__(self):
        self._model: Optional[LogisticRegression] = None

    @property
    def is_fitted(self) -> bool:
        return self._model is not None

    def fit(self, pbp: pd.DataFrame) -> "NaiveWPModel":
        """Fit the model from raw play-by-play data.

        Parameters
        ----------
        pbp : DataFrame
            Play-by-play with columns: game_id, play_type, score_home,
            score_away, clock_seconds, period, sequence_number.
        """
        plays = _prepare_plays(pbp)

        # Only regulation plays for fitting (OT is bonus time with different dynamics)
        reg = plays[plays["period"] <= 2].copy()
        reg["time_fraction"] = reg["seconds_remaining"] / REGULATION_SECONDS

        # Outcome: did the home team win this game?
        outcomes = (
            plays.sort_values("sequence_number")
            .groupby("game_id")
            .last()[["score_home", "score_away"]]
        )
        outcomes["home_won"] = (outcomes["score_home"] > outcomes["score_away"]).astype(int)

        reg = reg.merge(outcomes[["home_won"]], left_on="game_id", right_index=True)

        X = pd.DataFrame({
            "score_diff": reg["score_diff"],
            "score_diff_x_time": reg["score_diff"] * reg["time_fraction"],
            "time_frac": reg["time_fraction"],
        })

        self._model = LogisticRegression(max_iter=1000)
        self._model.fit(X, reg["home_won"])

        coefs = dict(zip(X.columns, self._model.coef_[0]))
        logger.info(f"NaiveWPModel fitted on {reg.game_id.nunique():,} games, "
                     f"{len(reg):,} play-states. Coefficients: {coefs}")
        return self

    def predict(self, score_diff: np.ndarray, seconds_remaining: np.ndarray) -> np.ndarray:
        """Return P(home_win) for arrays of score_diff and seconds_remaining."""
        if not self.is_fitted:
            raise RuntimeError("Model not fitted. Call .fit() first.")
        time_frac = seconds_remaining / REGULATION_SECONDS
        X = pd.DataFrame({
            "score_diff": score_diff,
            "score_diff_x_time": score_diff * time_frac,
            "time_frac": time_frac,
        })
        return self._model.predict_proba(X)[:, 1]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _prepare_plays(pbp: pd.DataFrame) -> pd.DataFrame:
    """Filter noise plays and add derived columns."""
    plays = pbp[~pbp["play_type"].isin(_NOISE_PLAY_TYPES)].copy()
    plays["score_diff"] = plays["score_home"] - plays["score_away"]
    plays["seconds_remaining"] = plays.apply(
        lambda r: r["clock_seconds"] + max(0, (2 - r["period"])) * 1200
        if r["period"] <= 2
        else r["clock_seconds"],
        axis=1,
    )
    return plays


def _time_weights(seconds_remaining: np.ndarray):
    """Duration-based weights: how long each play-state lasted."""
    durations = np.abs(np.diff(seconds_remaining))
    durations = np.append(durations, 0.0)
    total = durations.sum()
    if total == 0:
        return durations, 0.0
    return durations, total


# ---------------------------------------------------------------------------
# Game-level metric computation
# ---------------------------------------------------------------------------

def compute_game_metrics(
    pbp: pd.DataFrame,
    wp_model: Optional[NaiveWPModel] = None,
    fit_wp: bool = True,
) -> pd.DataFrame:
    """Compute game control metrics for every game in a play-by-play DataFrame.

    Parameters
    ----------
    pbp : DataFrame
        Play-by-play data (as produced by the processor).
    wp_model : NaiveWPModel, optional
        Pre-fitted model. If None and fit_wp is True, one is fitted on the
        provided data.
    fit_wp : bool
        If True and no wp_model is provided, fit one on pbp.

    Returns
    -------
    DataFrame with columns: game_id, final_margin, avg_score_diff,
    trunc_avg_diff, avg_naive_wp, soft_lev_margin.
    All metrics are from the home team's perspective.
    """
    plays = _prepare_plays(pbp)

    if wp_model is None and fit_wp:
        wp_model = NaiveWPModel().fit(pbp)

    # Regulation plays only for metrics
    reg = plays[plays["period"] <= 2].copy()

    if wp_model is not None and wp_model.is_fitted:
        reg["naive_wp"] = wp_model.predict(
            reg["score_diff"].values,
            reg["seconds_remaining"].values,
        )
    else:
        reg["naive_wp"] = np.nan

    rows = []
    for gid, g in reg.groupby("game_id"):
        g = g.sort_values("seconds_remaining", ascending=False)
        if len(g) < 5:
            continue

        sr = g["seconds_remaining"].values
        sd = g["score_diff"].values
        nwp = g["naive_wp"].values
        durations, total_time = _time_weights(sr)

        if total_time == 0:
            continue

        weights = durations / total_time

        # 1. Final margin
        final_margin = sd[-1]

        # 2. Time-weighted average score diff
        avg_score_diff = np.sum(sd * weights)

        # 3. Truncated average (exclude last 2 min)
        mask = sr > GARBAGE_CUTOFF_SECONDS
        d_trunc = durations[mask]
        t_trunc = d_trunc.sum()
        trunc_avg_diff = (
            np.sum(sd[mask] * d_trunc) / t_trunc if t_trunc > 0 else final_margin
        )

        # 4. Average naive WP
        avg_naive_wp = np.sum(nwp * weights) if not np.isnan(nwp).all() else np.nan

        # 5. Soft leverage-weighted margin
        # sqrt(leverage) so blowouts aren't completely zeroed out
        if not np.isnan(nwp).all():
            soft_lev = np.sqrt(np.maximum(0, 1 - 2 * np.abs(nwp - 0.5)))
            slw = soft_lev * durations
            slt = slw.sum()
            soft_lev_margin = np.sum(sd * slw) / slt if slt > 0 else final_margin
        else:
            soft_lev_margin = np.nan

        rows.append({
            "game_id": gid,
            "final_margin": final_margin,
            "avg_score_diff": avg_score_diff,
            "trunc_avg_diff": trunc_avg_diff,
            "avg_naive_wp": avg_naive_wp,
            "soft_lev_margin": soft_lev_margin,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Convenience: compute from parquet files on disk
# ---------------------------------------------------------------------------

def compute_season_metrics(
    season: int,
    gender: str = "mens",
    wp_model: Optional[NaiveWPModel] = None,
) -> pd.DataFrame:
    """Load play-by-play for a season and compute game control metrics.

    Parameters
    ----------
    season : int
    gender : str
    wp_model : NaiveWPModel, optional
        If provided, skip fitting and use this model.

    Returns
    -------
    DataFrame of game-level metrics.
    """
    from espn_data.utils import configure, get_parquet_season_dir

    configure(gender=gender)
    parquet_dir = get_parquet_season_dir(season)
    pbp_path = parquet_dir / "play_by_play.parquet"

    if not pbp_path.exists():
        raise FileNotFoundError(f"No play-by-play data at {pbp_path}")

    pbp = pd.read_parquet(pbp_path)
    logger.info(f"Loaded {len(pbp):,} plays for {gender} {season}")

    return compute_game_metrics(pbp, wp_model=wp_model)


def compute_multi_season_metrics(
    seasons: list[int],
    gender: str = "mens",
) -> pd.DataFrame:
    """Compute game control metrics across multiple seasons.

    Fits a single WP model on all seasons' data for consistency, then
    computes per-game metrics.

    Parameters
    ----------
    seasons : list of int
    gender : str

    Returns
    -------
    DataFrame with game_id, season, and all metric columns.
    """
    from espn_data.utils import configure, get_parquet_season_dir

    configure(gender=gender)

    # Load all play-by-play data
    all_pbp = []
    for season in seasons:
        pbp_path = get_parquet_season_dir(season) / "play_by_play.parquet"
        if not pbp_path.exists():
            logger.warning(f"No play-by-play data for {gender} {season}, skipping")
            continue
        df = pd.read_parquet(pbp_path)
        df["season"] = season
        all_pbp.append(df)

    if not all_pbp:
        return pd.DataFrame()

    combined = pd.concat(all_pbp, ignore_index=True)
    logger.info(f"Loaded {len(combined):,} plays across {len(all_pbp)} seasons")

    # Fit one WP model on all data
    wp_model = NaiveWPModel().fit(combined)

    # Compute metrics per season (preserves season column)
    results = []
    for season in seasons:
        season_pbp = combined[combined["season"] == season]
        if season_pbp.empty:
            continue
        metrics = compute_game_metrics(season_pbp, wp_model=wp_model, fit_wp=False)
        metrics["season"] = season
        results.append(metrics)

    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()
