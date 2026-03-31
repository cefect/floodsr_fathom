"""Direct tests for the local Snakemake rule scripts."""

from pathlib import Path

import pytest

from conftest import config
import smk.scripts._01_prep
import smk.scripts._02_hrdem


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
def r01_prep_fp():
    """Return the canonical prep output path for the proof tile."""
    return (
        Path(__file__).resolve().parents[1]
        / config["out_dir"]
        / "01_prep"
        / "PLUVIAL"
        / "DEFENDED"
        / "1in1000"
        / "n49w124"
        / "r01_prep.tif"
    ).resolve()


@pytest.fixture(scope="function")
def r02_hrdem_fp():
    """Return the canonical HRDEM output path for the proof tile."""
    return (
        Path(__file__).resolve().parents[1]
        / config["out_dir"]
        / "02_hrdem"
        / "PLUVIAL"
        / "DEFENDED"
        / "1in1000"
        / "n49w124"
        / "r02_hrdem.tif"
    ).resolve()


@pytest.fixture(scope="function")
def dry_r01_prep_fp(tmp_path):
    """Return one temporary prep output path for a dry-tile failure test."""
    return (tmp_path / "dry" / "r01_prep.tif").resolve()


def test_main_01_prep(wet_tile_fp, r01_prep_fp, logger):
    """Run the prep script directly on one wet proof tile."""
    r01_prep_fp.parent.mkdir(parents=True, exist_ok=True)
    r01_prep_fp.unlink(missing_ok=True)

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
def test_main_02_hrdem(wet_tile_fp, r01_prep_fp, r02_hrdem_fp, logger):
    """Run the HRDEM fetch script directly on one prepared proof tile."""
    r01_prep_fp.parent.mkdir(parents=True, exist_ok=True)
    r02_hrdem_fp.parent.mkdir(parents=True, exist_ok=True)
    r02_hrdem_fp.unlink(missing_ok=True)

    if not r01_prep_fp.exists():
        smk.scripts._01_prep.main_01_prep(
            tile_fp=wet_tile_fp,
            r01_prep_fp=r01_prep_fp,
            cache_dir=r01_prep_fp.parent / ".cache",
            min_depth=0.01,
            manual_window_size=128,
            show_progress=False,
            logger=logger,
        )

    result = smk.scripts._02_hrdem.main_02_hrdem(
        r01_prep_fp=r01_prep_fp,
        r02_hrdem_fp=r02_hrdem_fp,
        cache_dir=r02_hrdem_fp.parent / ".cache",
        asset_key="dtm",
        use_cache=True,
        force_tiling=False,
        show_progress=False,
        logger=logger,
    )

    assert isinstance(result, Path)
    assert result.exists()


@pytest.mark.network
def test_main_02_hrdem_uses_temp_cache_dir(wet_tile_fp, r01_prep_fp, tmp_path, logger):
    """Write HRDEM cache artifacts into the passed temp cache directory."""
    r02_hrdem_fp = (tmp_path / "02_hrdem" / "r02_hrdem.tif").resolve()
    cache_dir = (tmp_path / ".cache" / "r02_hrdem").resolve()

    if not r01_prep_fp.exists():
        smk.scripts._01_prep.main_01_prep(
            tile_fp=wet_tile_fp,
            r01_prep_fp=r01_prep_fp,
            cache_dir=r01_prep_fp.parent / ".cache",
            min_depth=0.01,
            manual_window_size=128,
            show_progress=False,
            logger=logger,
        )

    result = smk.scripts._02_hrdem.main_02_hrdem(
        r01_prep_fp=r01_prep_fp,
        r02_hrdem_fp=r02_hrdem_fp,
        cache_dir=cache_dir,
        asset_key="dtm",
        use_cache=True,
        force_tiling=False,
        show_progress=False,
        logger=logger,
    )

    assert isinstance(result, Path)
    assert any(cache_dir.iterdir())
