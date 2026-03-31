"""CLI-backed Snakemake proof tests for the local Fathom workflow."""

import os, shutil, subprocess, sys
from pathlib import Path

import pytest

from conftest import config
import smk.scripts._01_prep

REPO_ROOT = Path(__file__).resolve().parents[1]
PROFILE_DIR = (REPO_ROOT / "smk" / "profiles").resolve()
CONFIG_FP = (REPO_ROOT / "smk" / "config.yaml").resolve()
INDEX_DIR = (REPO_ROOT / config["fathom_index_dir"]).resolve()
TILES_DIR = (REPO_ROOT / config["fathom_tiles_dir"]).resolve()
WORKFLOW_OUT_DIR = (REPO_ROOT / config["out_dir"]).resolve()


@pytest.fixture(scope="function")
def wet_tile_fp():
    """Return one wet low-resolution tile for CLI rule proofs."""
    return (
        TILES_DIR
        / "FLOOD_MAP-1ARCSEC-NW_OFFSET-1in1000-PLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
        / "n49w124.tif"
    )


@pytest.fixture(scope="function")
def canonical_prep_fp(wet_tile_fp, logger):
    """Ensure the canonical prep output exists for the HRDEM CLI proof."""
    prep_fp = WORKFLOW_OUT_DIR / "01_prep" / "PLUVIAL" / "DEFENDED" / "1in1000" / "n49w124" / "r01_prep.tif"
    prep_fp.parent.mkdir(parents=True, exist_ok=True)
    if not prep_fp.exists():
        smk.scripts._01_prep.main_01_prep(
            tile_fp=wet_tile_fp,
            r01_prep_fp=prep_fp,
            cache_dir=prep_fp.parent / ".cache",
            min_depth=0.01,
            manual_window_size=128,
            show_progress=False,
            logger=logger,
        )
    return prep_fp


def _copy_path(src_path, dst_path):
    """Copy one source file or directory into the temp workflow tree."""
    src_path = Path(src_path)
    dst_path = Path(dst_path)
    assert src_path.exists(), f"Missing prerequisite source:\n    {src_path}"

    if src_path.is_dir():
        shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
    else:
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dst_path)


def _run_rule_proof(tmp_path, run_d):
    """Stage inputs and execute one isolated Snakemake CLI rule proof."""
    temp_root = tmp_path / "smk"
    target_path = temp_root / run_d["target"]

    for stage_d in run_d.get("stage_l", []):
        _copy_path(stage_d["src"], temp_root / stage_d["dst"])

    args = [
        sys.executable,
        "-m",
        "snakemake",
        "--profile",
        "none",
        "--workflow-profile",
        str(PROFILE_DIR),
        "--configfile",
        str(CONFIG_FP),
        "--directory",
        str(temp_root),
        "--cores",
        "1",
        "--allowed-rules",
        run_d["allowed_rule"],
        "--config",
        f"out_dir={config['out_dir']}",
        f"cache_dir={temp_root / config['out_dir'] / '.cache'}",
        f"fathom_tiles_dir={TILES_DIR}",
        f"fathom_index_dir={INDEX_DIR}",
        "--",
        str(run_d["target"]),
    ]

    result = subprocess.run(
        args,
        cwd=REPO_ROOT,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
        check=False,
    )
    return result, target_path


@pytest.mark.smk
def test_r01_prep_cli(tmp_path):
    """Prove the real Snakemake CLI can run r01_prep for one wet tile."""
    run_d = {
        "allowed_rule": "r01_prep",
        "target": Path(config["out_dir"]) / "01_prep" / "PLUVIAL" / "DEFENDED" / "1in1000" / "n49w124" / "r01_prep.tif",
    }
    result, target_path = _run_rule_proof(tmp_path, run_d)

    assert result.returncode == 0
    assert target_path.exists()


@pytest.mark.smk
@pytest.mark.network
def test_r02_hrdem_cli(tmp_path, canonical_prep_fp):
    """Prove the real Snakemake CLI can run r02_hrdem from staged prep output."""
    run_d = {
        "allowed_rule": "r02_hrdem",
        "stage_l": [
            {
                "dst": Path(config["out_dir"]) / "01_prep" / "PLUVIAL" / "DEFENDED" / "1in1000" / "n49w124" / "r01_prep.tif",
                "src": canonical_prep_fp,
            }
        ],
        "target": Path(config["out_dir"]) / "02_hrdem" / "PLUVIAL" / "DEFENDED" / "1in1000" / "n49w124" / "r02_hrdem.vrt",
    }
    result, target_path = _run_rule_proof(tmp_path, run_d)

    assert result.returncode == 0
    assert target_path.exists()
