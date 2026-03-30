"""Reusable helpers for Snakemake workflow configuration."""

from typing import Iterable, Optional, Mapping
import logging
import re, warnings
import hashlib
import json
import pickle
import tempfile
from pathlib import Path

import pandas as pd
import geopandas as gpd
idx = pd.IndexSlice

 

from snakemake.io import ancient


#_ALL_STATE_IDS = STATE_IDS_DF["state_id"].tolist()

# Helper to normalize directory config entries to Path or None
NONE_LIKE = {None, "", "None", "none", "null", "NULL"}
_TRUE_LIKE = {"true", "1", "yes", "y", "on"}
_FALSE_LIKE = {"false", "0", "no", "n", "off"}


def _dir_from_config(val, allow_none=False):
    if val in NONE_LIKE:
        if allow_none:
            return None
        raise ValueError("Required directory config is None-like")
    return Path(val).resolve()


def _coerce_nonelike(val):
    """Return None for none-like strings/values, else the original."""
    return None if val in NONE_LIKE else val


 


def _coerce_bool(val):
    """Coerce config values to bool, honoring common string representations."""
    if isinstance(val, bool):
        return val
    if val in NONE_LIKE:
        return None
    if isinstance(val, str):
        lowered = val.strip().lower()
        if lowered in _TRUE_LIKE:
            return True
        if lowered in _FALSE_LIKE:
            return False
        return bool(lowered)
    return bool(val)


def parse_config_list(
    raw_value: Optional[object],

    dtype: Optional[type] = str,
    result_if_none: Optional[object] = None,
    zero_pad_width: Optional[int] = None,
) -> Optional[list]:
    """
    Normalize Snakemake config inputs that represent list-like values.

    Parameters
    ----------
    raw_value :
        The raw configuration input. Supports None, comma- or whitespace-delimited strings,
        singletons, or iterables.
    dtype :
        Optional callable used to coerce each element. Defaults to `str`. When None,
        elements are returned without coercion.
    result_if_none :
        Value to return when `raw_value` is None or the string "None".
        Defaults to None.
    zero_pad_width :
        When provided with dtype=str, zero-pad each coerced string to this width
        (e.g., width=2 -> "01").
    """
    if raw_value is None or (isinstance(raw_value, str) and raw_value.strip().lower() == "none"):
        return result_if_none

    if zero_pad_width is not None and dtype is not str:
        raise ValueError("zero_pad_width is only supported when dtype is str.")

    def _coerce(value):
        coerced = value if dtype is None else dtype(value)
        if zero_pad_width is not None:
            return str(coerced).zfill(zero_pad_width)
        return coerced

    if isinstance(raw_value, str):
        stripped = raw_value.strip()
        if not stripped:
            return result_if_none
        # Allow comma- or whitespace-delimited lists (handles env vars like "id1 id2" or "id1,id2")
        tokens = [token for token in re.split(r"[,\s]+", stripped) if token]
        return [_coerce(token) for token in tokens]

    if isinstance(raw_value, Iterable) and not isinstance(raw_value, (str, bytes)):
        values = [value for value in raw_value if str(value).strip()]
        return [_coerce(value) for value in values]

    return [_coerce(raw_value)]
 