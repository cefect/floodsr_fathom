"""Reusable helpers for the local Fathom Snakemake workflow."""

from __future__ import annotations

import hashlib, json, logging, pickle, re, sqlite3
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from conf import fathom_scenario_dimensions
import smk.scripts.assertions as assertions

NONE_LIKE = {None, "", "None", "none", "null", "NULL"}
_TRUE_LIKE = {"true", "1", "yes", "y", "on"}
_FALSE_LIKE = {"false", "0", "no", "n", "off"}
_FATHOM_INDEX_RE = re.compile(
    r"^FLOOD_MAP-1ARCSEC-NW_OFFSET-"
    r"(?P<return_period>[^-]+)-"
    r"(?P<hazard_type>[^-]+)-"
    r"(?P<protection>[^-]+)-DEPTH-2020-PERCENTILE50-v3\.1$"
)


def _dir_from_config(val, allow_none=False):
    """Return a directory path from config, optionally allowing None-like values."""
    if val in NONE_LIKE:
        if allow_none:
            return None
        raise ValueError("Required directory config is None-like")
    return Path(val)


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


def parse_config_list(raw_value: Optional[object], dtype: Optional[type] = str, result_if_none: Optional[object] = None):
    """Normalize Snakemake config values that represent list-like selectors."""
    if raw_value is None or (isinstance(raw_value, str) and raw_value.strip().lower() == "none"):
        return result_if_none

    def _coerce(value):
        return value if dtype is None else dtype(value)

    if isinstance(raw_value, str):
        stripped = raw_value.strip()
        if not stripped:
            return result_if_none
        token_l = [token for token in re.split(r"[,\s]+", stripped) if token]
        return [_coerce(token) for token in token_l]

    if isinstance(raw_value, Iterable) and not isinstance(raw_value, (str, bytes)):
        return [_coerce(value) for value in raw_value if str(value).strip()]

    return [_coerce(raw_value)]


def _parse_fathom_index_name(index_name):
    """Parse scenario dimensions from one Fathom index filename stem."""
    match = _FATHOM_INDEX_RE.match(index_name)
    if not match:
        raise ValueError(f"Unexpected Fathom index name: {index_name}")
    return match.groupdict()


def _scenario_count_str(index_df):
    """Build one compact scenario-count report string."""
    if index_df.empty:
        return "<empty>"
    count_df = (
        index_df.groupby(["return_period", "hazard_type", "protection"], dropna=False)
        .size()
        .reset_index(name="tile_count")
        .sort_values(["return_period", "hazard_type", "protection"])
    )
    return count_df.to_string(index=False)


def _cache_key(payload_d):
    """Return one stable cache key for load_index inputs."""
    payload = json.dumps(payload_d, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _index_manifest(index_fp_l):
    """Build one stable manifest for the current index files on disk."""
    return [
        {
            "name": index_fp.name,
            "size": index_fp.stat().st_size,
            "mtime_ns": index_fp.stat().st_mtime_ns,
        }
        for index_fp in index_fp_l
    ]


def _stable_sort_index_df(index_df):
    """Apply one stable scenario-aware sort order to the normalized index dataframe."""
    df = index_df.copy()
    for key, allowed_l in fathom_scenario_dimensions.items():
        df[key] = pd.Categorical(df[key], categories=allowed_l, ordered=True)
    return df.sort_values(["return_period", "hazard_type", "protection", "tileID"], kind="mergesort").reset_index(drop=True)


def _resolve_tile_fp(location, layer_name, fathom_tiles_dir):
    """Resolve one tile filepath from GPKG location data and the configured tile root."""
    location_fp = Path(location)
    rebased_fp = Path(fathom_tiles_dir) / layer_name / location_fp.name
    if location_fp.exists():
        return location_fp.resolve()
    if rebased_fp.exists():
        return rebased_fp.resolve()
    raise FileNotFoundError(
        f"Could not resolve low-res tile path from location '{location}' or rebased path '{rebased_fp}'"
    )


def _load_one_index(index_fp, fathom_tiles_dir):
    """Load one Fathom GPKG index into normalized row records."""
    index_fp = Path(index_fp)
    scenario_d = _parse_fathom_index_name(index_fp.stem)
    layer_name = index_fp.stem
    record_l = []

    with sqlite3.connect(index_fp) as con:
        row_l = con.execute(f'SELECT location FROM "{layer_name}"').fetchall()

    for (location,) in row_l:
        tile_fp = _resolve_tile_fp(location, layer_name=layer_name, fathom_tiles_dir=fathom_tiles_dir)
        record_l.append(
            {
                **scenario_d,
                "tileID": tile_fp.stem,
                "tile_fp": str(tile_fp),
                "index_fp": str(index_fp.resolve()),
                "layer_name": layer_name,
            }
        )

    return record_l


def load_index(
    fathom_index_dir,
    fathom_tiles_dir,
    tile_cnt=None,
    filters_d=None,
    cache_dir=None,
    debug=False,
    use_cache=True,
    logger=None,
):
    """Load, normalize, filter, and cache the Fathom workflow index dataframe."""
    log = logger or logging.getLogger(__name__)
    fathom_index_dir = Path(fathom_index_dir).resolve()
    fathom_tiles_dir = Path(fathom_tiles_dir).resolve()
    cache_dir = Path(cache_dir).resolve() if cache_dir is not None else None
    filters_d = {key: value for key, value in (filters_d or {}).items() if value is not None}

    assert fathom_index_dir.is_dir(), f"missing fathom_index_dir:\n    {fathom_index_dir}"
    assert fathom_tiles_dir.is_dir(), f"missing fathom_tiles_dir:\n    {fathom_tiles_dir}"

    debug = bool(debug)
    tile_cnt = None if tile_cnt in NONE_LIKE else int(tile_cnt)
    index_fp_l = sorted(fathom_index_dir.glob("*.gpkg"))
    assert index_fp_l, f"no .gpkg files found in fathom_index_dir:\n    {fathom_index_dir}"
    cache_payload_d = {
        "fathom_index_dir": str(fathom_index_dir),
        "fathom_tiles_dir": str(fathom_tiles_dir),
        "index_manifest": _index_manifest(index_fp_l),
        "filters_d": filters_d,
        "debug": debug,
        "tile_cnt": tile_cnt if debug else None,
    }

    if cache_dir is not None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_fp = cache_dir / f"{_cache_key(cache_payload_d)}.pkl"
        if use_cache and cache_fp.exists():
            with cache_fp.open("rb") as f:
                index_df = pickle.load(f)
            assertions.assert_load_index_df(index_df, logger=log)
            log.info(f"load_index cache hit:\n    {cache_fp}")
            return index_df
    else:
        cache_fp = None

    record_l = []
    for index_fp in index_fp_l:
        record_l.extend(_load_one_index(index_fp, fathom_tiles_dir=fathom_tiles_dir))

    index_df = pd.DataFrame.from_records(record_l)
    index_df = _stable_sort_index_df(index_df)
    log.info(f"load_index counts before filtering:\n{_scenario_count_str(index_df)}")

    for key, selected_l in filters_d.items():
        assert key in fathom_scenario_dimensions, f"unexpected filter key: {key}"
        bad_values = sorted(set(selected_l).difference(fathom_scenario_dimensions[key]))
        assert not bad_values, f"unexpected filter values for {key}: {bad_values}"
        index_df = index_df.loc[index_df[key].isin(selected_l)].reset_index(drop=True)

    log.info(f"load_index counts after filtering:\n{_scenario_count_str(index_df)}")

    if debug and tile_cnt is not None:
        index_df = (
            index_df.groupby(["return_period", "hazard_type", "protection"], group_keys=False)
            .head(tile_cnt)
            .reset_index(drop=True)
        )
        log.info(f"load_index counts after debug tile limit:\n{_scenario_count_str(index_df)}")

    assertions.assert_load_index_df(index_df, logger=log)

    if cache_fp is not None:
        with cache_fp.open("wb") as f:
            pickle.dump(index_df, f)
        log.info(f"load_index cache write:\n    {cache_fp}")

    return index_df
