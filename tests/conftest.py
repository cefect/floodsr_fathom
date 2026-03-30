
import os, logging, sys, hashlib, json
import pytest, yaml
import pathlib
from pathlib import Path

# project parameters
config_path = Path("smk") / "config.yaml"
with open(config_path) as f:
    config = yaml.safe_load(f)

#===============================================================================
# pytest custom config------------
#===============================================================================
def pytest_runtest_teardown(item, nextitem):
    """custom teardown message"""
    test_name = item.name
    print(f"\n{'='*20} Test completed: {test_name} {'='*20}\n\n\n")
    
def pytest_report_header(config):
    """modifies the pytest header to show all of the arguments"""
    return f"pytest arguments: {' '.join(config.invocation_params.args)}"


# -------------------
# ----- Fixtures -----
# -------------------
@pytest.fixture(scope='session')
def logger(tmp_path_factory):
    """Simple logger fixture for the function under test."""
    log = logging.getLogger("pytest")
    log.setLevel(logging.DEBUG)
    log_fp = tmp_path_factory.getbasetemp() / "pytest.log"
    # keep handlers minimal to avoid duplicate logs across runs
    if not log.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")
        handler.setFormatter(formatter)
        log.addHandler(handler)
        file_handler = logging.FileHandler(log_fp)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        log.addHandler(file_handler)

    log.info(f"Logger initialized. Logs will be written to:\n   {log_fp}")
    return log


# -------------------
# ----- HELPERS -----
# -------------------
def _get_hash_from_dict(d):
    """Helper to get a sha256 hash from a dictionary"""
    assert isinstance(d, dict), "Input must be a dictionary"
    if len(d) == 0:
        return None
    else:
        # Ensure deterministic serialization (sorted keys, UTF-8, compact)
        s = json.dumps(d, sort_keys=True, separators=(',', ':'))
        # Compute MD5 
    
        return hashlib.sha256(s.encode('utf-8')).hexdigest()
    

def _get_hash_from_df(df):
    """Helper to get a sha256 hash from a DataFrame"""
    import pandas as pd

    assert isinstance(df, pd.DataFrame), "Input must be a pandas DataFrame"
    if df.empty:
        return None

    hash_values = pd.util.hash_pandas_object(df, index=True)
    return hashlib.sha256(hash_values.values.tobytes()).hexdigest()
