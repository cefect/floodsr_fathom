"""Direct tests for the local Snakemake rule scripts."""

from pathlib import Path

import pytest

from conftest import config
import smk.scripts._01_prep
import smk.scripts._02_hrdem
import smk.scripts._03_tohr


@pytest.fixture(scope="function")
def wet_tile_fp():
    """Return one wet low-resolution tile for local workflow proofs."""
    return (
        Path(__file__).resolve().parents[1]
        / config["fathom_tiles_dir"]
        / "FLOOD_MAP-1ARCSEC-NW_OFFSET-1in1000-PLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
        / "n49w124.tif"
    ).resolve()


@pytest.fixture(scope="function")
def r01_prep():
    """Return one canonical prep asset for downstream workflow tests."""
    r01_prep_fp = (
        Path(__file__).resolve().parents[1]
        / config["out_dir"]
        / "01_prep"
        / "PLUVIAL"
        / "DEFENDED"
        / "1in1000"
        / "n49w124"
        / "r01_prep.tif"
    ).resolve()
    assert r01_prep_fp.exists(), f"missing canonical prep asset:\n    {r01_prep_fp}"
    return r01_prep_fp


@pytest.fixture(scope="function")
def r02_hrdem():
    """Return one canonical HRDEM asset for downstream workflow tests."""
    r02_hrdem_fp = (
        Path(__file__).resolve().parents[1]
        / config["out_dir"]
        / "02_hrdem"
        / "PLUVIAL"
        / "DEFENDED"
        / "1in1000"
        / "n49w124"
        / "r02_hrdem.vrt"
    ).resolve()
    assert r02_hrdem_fp.exists(), f"missing canonical HRDEM asset:\n    {r02_hrdem_fp}"
    assert (r02_hrdem_fp.parent / "r02_hrdem__fetch_tiles").exists(), f"missing canonical HRDEM tile directory:\n    {r02_hrdem_fp.parent / 'r02_hrdem__fetch_tiles'}"
    return r02_hrdem_fp


@pytest.fixture(scope="function")
def dry_r01_prep_fp(tmp_path):
    """Return one temporary prep output path for a dry-tile failure test."""
    return (tmp_path / "dry" / "r01_prep.tif").resolve()


def test_main_01_prep(wet_tile_fp, tmp_path, logger):
    """Run the prep script directly on one wet proof tile."""
    r01_prep_fp = (tmp_path / "r01_prep.tif").resolve()

    result = smk.scripts._01_prep.main_01_prep(
        tile_fp=wet_tile_fp,
        r01_prep_fp=r01_prep_fp,
        cache_dir=r01_prep_fp.parent / ".cache",
        min_depth=0.01,
        manual_window_size=128,
        show_progress=False,
        logger=logger,
    )

    assert isinstance(result, Path)
    assert result.exists()


def test_main_01_prep_dry_tile_raises(wet_tile_fp, dry_r01_prep_fp, logger):
    """Raise a clear error and remove partial output when min_depth masks all cells."""
    with pytest.raises(RuntimeError, match="No wet pixels remain") as exc_info:
        smk.scripts._01_prep.main_01_prep(
            tile_fp=wet_tile_fp,
            r01_prep_fp=dry_r01_prep_fp,
            cache_dir=dry_r01_prep_fp.parent / ".cache",
            min_depth=20.0,
            manual_window_size=128,
            show_progress=False,
            logger=logger,
        )

    assert "Remove this tile from the test set" in str(exc_info.value)
    assert not dry_r01_prep_fp.exists()


@pytest.mark.network
def test_main_02_hrdem(r01_prep, tmp_path, logger):
    """Run the HRDEM fetch script directly on one prepared proof tile."""
    r02_hrdem_fp = (tmp_path / "r02_hrdem.vrt").resolve()

    result = smk.scripts._02_hrdem.main_02_hrdem(
        r01_prep_fp=r01_prep,
        r02_hrdem_fp=r02_hrdem_fp,
        cache_dir=r02_hrdem_fp.parent / ".cache",
        asset_key="dtm",
        use_cache=True,
        force_tiling=True,
        show_progress=False,
        logger=logger,
    )

    assert isinstance(result, Path)
    assert result.exists()


@pytest.mark.network
def test_main_02_hrdem_uses_temp_cache_dir(r01_prep, tmp_path, logger):
    """Write HRDEM cache artifacts into the passed temp cache directory."""
    r02_hrdem_fp = (tmp_path / "r02_hrdem.vrt").resolve()
    cache_dir = (tmp_path / ".cache" / "r02_hrdem").resolve()

    result = smk.scripts._02_hrdem.main_02_hrdem(
        r01_prep_fp=r01_prep,
        r02_hrdem_fp=r02_hrdem_fp,
        cache_dir=cache_dir,
        asset_key="dtm",
        use_cache=True,
        force_tiling=True,
        show_progress=False,
        logger=logger,
    )

    assert isinstance(result, Path)
    assert any(cache_dir.iterdir())


@pytest.mark.network
def test_main_03_tohr(r01_prep, r02_hrdem, tmp_path, logger):
    """Run the ToHR script directly on one prepared proof tile."""
    r03_tohr_fp = (tmp_path / "r03_tohr.vrt").resolve()

    result = smk.scripts._03_tohr.main_03_tohr(
        r01_prep_fp=r01_prep,
        r02_hrdem_fp=r02_hrdem,
        r03_tohr_fp=r03_tohr_fp,
        cache_dir=r03_tohr_fp.parent / ".cache",
        model_version="ResUNet_16x_DEM",
        max_depth=10,
        min_depth_threshold=0.01,
        crs_policy="use-dem",
        window_method="hard",
        show_progress=False,
        logger=logger,
    )

    assert isinstance(result, Path)
    assert result.exists()
