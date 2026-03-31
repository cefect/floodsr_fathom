"""Tests for Snakemake workflow helper functions."""

from pathlib import Path

import pandas as pd
import pytest

from conftest import config
import smk.snakefile_coms


@pytest.fixture(scope="function")
def fathom_index_dir():
    """Return the configured local Fathom index directory."""
    return (Path(__file__).resolve().parents[1] / config["fathom_index_dir"]).resolve()


@pytest.fixture(scope="function")
def fathom_tiles_dir():
    """Return the configured local Fathom tile directory."""
    return (Path(__file__).resolve().parents[1] / config["fathom_tiles_dir"]).resolve()


@pytest.mark.parametrize(
    "filters_d, debug, tile_cnt",
    [
        pytest.param({"return_period": ["1in1000"], "hazard_type": ["COASTAL"], "protection": ["DEFENDED"]}, False, None, id="coastal_defended_full"),
    ],
)
def test_load_index(fathom_index_dir, fathom_tiles_dir, tmp_path, logger, filters_d, debug, tile_cnt):
    """Load one filtered index dataframe for workflow driving."""
    result = smk.snakefile_coms.load_index(
        fathom_index_dir,
        fathom_tiles_dir=fathom_tiles_dir,
        tile_cnt=tile_cnt,
        filters_d=filters_d,
        cache_dir=tmp_path / ".cache",
        debug=debug,
        use_cache=True,
        logger=logger,
    )

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
