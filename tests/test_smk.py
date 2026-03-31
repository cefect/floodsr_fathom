"""CLI-backed Snakemake proof tests for the local Fathom workflow."""

import os, shutil, subprocess, sys
from pathlib import Path

import pytest

from conftest import config

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
def r01_prep():
    """Return one canonical prep output path for CLI proof staging."""
    prep_fp = WORKFLOW_OUT_DIR / "01_prep" / "PLUVIAL" / "DEFENDED" / "1in1000" / "n49w124" / "r01_prep.tif"
    assert prep_fp.exists(), f"missing canonical prep asset:\n    {prep_fp}"
    return prep_fp


@pytest.fixture(scope="function")
def r02_hrdem():
    """Return one canonical HRDEM output path for CLI proof staging."""
    hrdem_fp = WORKFLOW_OUT_DIR / "02_hrdem" / "PLUVIAL" / "DEFENDED" / "1in1000" / "n49w124" / "r02_hrdem.vrt"
    assert hrdem_fp.exists(), f"missing canonical HRDEM asset:\n    {hrdem_fp}"
    assert (hrdem_fp.parent / "r02_hrdem__fetch_tiles").exists(), f"missing canonical HRDEM tile directory:\n    {hrdem_fp.parent / 'r02_hrdem__fetch_tiles'}"
    return hrdem_fp


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
def test_r02_hrdem_cli(tmp_path, r01_prep):
    """Prove the real Snakemake CLI can run r02_hrdem from staged prep output."""
    run_d = {
        "allowed_rule": "r02_hrdem",
        "stage_l": [
            {
                "dst": Path(config["out_dir"]) / "01_prep" / "PLUVIAL" / "DEFENDED" / "1in1000" / "n49w124" / "r01_prep.tif",
                "src": r01_prep,
            }
        ],
        "target": Path(config["out_dir"]) / "02_hrdem" / "PLUVIAL" / "DEFENDED" / "1in1000" / "n49w124" / "r02_hrdem.vrt",
    }
    result, target_path = _run_rule_proof(tmp_path, run_d)

    assert result.returncode == 0
    assert target_path.exists()


@pytest.mark.smk
def test_r03_tohr_cli(tmp_path, r01_prep, r02_hrdem):
    """Prove the real Snakemake CLI can run r03_tohr from staged upstream outputs."""
    run_d = {
        "allowed_rule": "r03_tohr",
        "stage_l": [
            {
                "dst": Path(config["out_dir"]) / "01_prep" / "PLUVIAL" / "DEFENDED" / "1in1000" / "n49w124" / "r01_prep.tif",
                "src": r01_prep,
            },
            {
                "dst": Path(config["out_dir"]) / "02_hrdem" / "PLUVIAL" / "DEFENDED" / "1in1000" / "n49w124" / "r02_hrdem.vrt",
                "src": r02_hrdem,
            },
            {
                "dst": Path(config["out_dir"]) / "02_hrdem" / "PLUVIAL" / "DEFENDED" / "1in1000" / "n49w124" / "r02_hrdem__fetch_tiles",
                "src": r02_hrdem.parent / "r02_hrdem__fetch_tiles",
            },
        ],
        "target": Path(config["out_dir"]) / "03_tohr" / "PLUVIAL" / "DEFENDED" / "1in1000" / "n49w124" / "r03_tohr.vrt",
    }
    result, target_path = _run_rule_proof(tmp_path, run_d)

    assert result.returncode == 0
    assert target_path.exists()
