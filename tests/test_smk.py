"""CLI-backed Snakemake proof tests for workflow """


import os, shutil, subprocess, sys
from pathlib import Path


from conftest import config

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_OUT_DIR = REPO_ROOT / config["out_dir"]



def _copy_path(src_path, dst_path):
    """Copy one source file or directory into the temp workflow tree."""
    src_path = Path(src_path)
    dst_path = Path(dst_path)
    assert src_path.exists(), f"Missing prerequisite source:\n    {src_path}"

    # Copy tree inputs exactly once per test run.
    if src_path.is_dir():
        shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
    else:
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dst_path)



def _1stage_run_inputs(temp_root, stage_l):
    """Copy all prerequisite artifacts for one Snakemake proof run."""
 
    for stage_d in stage_l:
        print(f"Staging input for Snakemake proof:\n    {stage_d['src']} -> {temp_root / stage_d['dst']}")
        _copy_path(stage_d["src"], temp_root / stage_d["dst"])


def _2run_snakemake_cli(temp_root, run_d):
    """Execute one isolated Snakemake CLI invocation in a temp workdir."""
    target = str(run_d["target"])
    args = [
        sys.executable,
        "-m",
        "snakemake",
        "--profile",
        "none",
        "--workflow-profile",
        str(run_d["profile"]),
        "--configfile",
        str((REPO_ROOT / "smk" / "config.yaml").resolve()),
        "--directory",
        str(temp_root),
        "--cores",
        "1",
        "--allowed-rules",
        run_d["allowed_rule"],
        "--config",
        *[f"{key}={value}" for key, value in run_d["config_d"].items()],
        "--",
        target,
    ]
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    # Run the real CLI from the repo root so profile-relative paths resolve cleanly.
    return subprocess.run(
        args,
        cwd=REPO_ROOT,
        env=env,
        check=False,
    )


def _run_rule_proof(tmp_path, run_d):
    """Stage inputs, invoke Snakemake, and return the process plus target path."""
    temp_root = tmp_path / "smk"
    target_path = temp_root / run_d["target"]

    # Stage fresh upstream artifacts from the main workflow outputs.
    _1stage_run_inputs(temp_root, run_d.get("stage_l", []))
    result = _2run_snakemake_cli(temp_root, run_d)
    return result, target_path



# @pytest.mark.parametrize("state_id", [pytest.param("50", id="state50")])
# @pytest.mark.smk
# def test_workflow_a_r00_event_prep(tmp_path, state_id):
#     """Prove workflow-A `r00_event_prep` via the Snakemake CLI."""
#     run_d = {
#         "allowed_rule": "r00_event_prep",
#         "config_d": WORKFLOW_A_COMMON_CONFIG_D | {"state_ids": state_id},
#         "profile": WORKFLOW_A_PROFILE,
#         "target": Path(f"workflow_outdir/00_event_prep/{state_id}/event_huc_bbox.pkl"),
#     }
#     result, target_path = _run_rule_proof(tmp_path, run_d)

#     assert result.returncode == 0
#     assert target_path.exists()
#     assert (target_path.parent / "metadata.json").exists()


# @pytest.mark.parametrize("state_id", [pytest.param("50", id="state50")])
# @pytest.mark.smk
# def test_workflow_a_r00b_event_huc_index(tmp_path, state_id):
#     """Prove workflow-A `r00B_event_huc_index` via the Snakemake CLI."""
#     run_d = {
#         "allowed_rule": "r00B_event_huc_index",
#         "config_d": WORKFLOW_A_COMMON_CONFIG_D | {"state_ids": state_id},
#         "profile": WORKFLOW_A_PROFILE,
#         "stage_l": [
#             {
#                 "dst": Path(f"workflow_outdir/00_event_prep/{state_id}/event_huc_index.parquet"),
#                 "src": WORKFLOW_OUT_DIR / "00_event_prep" / state_id / "event_huc_index.parquet",
#             }
#         ],
#         "target": Path("workflow_outdir/00B_event_huc_index/00B_event_huc_index.parquet"),
#     }
#     result, target_path = _run_rule_proof(tmp_path, run_d)

#     assert result.returncode == 0
#     assert target_path.exists()