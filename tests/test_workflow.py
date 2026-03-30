"""snakemake workflow tests

NOTE: snakemake search paths are relative to the snakefile by default (we override this in the snakefile to include the repo root).
pytest assumes relative to project
"""
import logging, sys, os, yaml, json, hashlib, pickle, time
from pprint import pprint, pformat
from datetime import datetime, timezone
from pathlib import Path
import warnings
 
import pytest

from conftest import config, _get_hash_from_dict



# # example fixture
# @pytest.fixture(scope="function")
# def _00_event_huc_bbox_fp(stateID):
#     """emuilate rule r00_event_prep"""
#     fp =  os.path.join(config['out_dir'],'00_event_prep',stateID,  'event_huc_bbox.pkl')
 
#     assert os.path.exists(fp), f'00_event_prep_event_huc_bbox_fp for {stateID} not found at \n{fp}'
#     return fp