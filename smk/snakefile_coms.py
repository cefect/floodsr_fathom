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

import smk.scripts.assertions as assertions
from smk.scripts.coms import datetime_localize_and_convert
from parameters import SENSORS, SENSOR_FETCH_COLLECTION_MAP_D, MASTER_INDEX_DTYPES

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


def get_inference_ckpt_path(sensor_id: str, cfg: Mapping[str, object]) -> str:
    """Resolve and validate inference checkpoint path for a sensor from config mapping."""
    sensor_cfg = SENSORS.get(sensor_id, {})
    infer_cfg = sensor_cfg.get("_04_inference")
    assert infer_cfg is not None, f"No _04_inference config for sensor {sensor_id}"

    cfg_key = infer_cfg.get("weights_config_key")
    assert cfg_key, (
        f"Missing 'weights_config_key' under SENSORS['{sensor_id}']['_04_inference']"
    )

    ckpt_raw = _coerce_nonelike(cfg.get(cfg_key))
    assert ckpt_raw is not None, (
        f"Missing config value '{cfg_key}' for sensor {sensor_id} inference."
    )

    ckpt_fp = Path(ckpt_raw).expanduser()
    assert ckpt_fp.exists(), (
        f"Checkpoint file for sensor {sensor_id} not found at {ckpt_fp} "
        f"(from config key '{cfg_key}')."
    )
    return str(ckpt_fp)


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


def get_stac_item_lib_d(stac_item_lib_dir, sensor_id_l, item_sensor_pairs=None, logger=None):
    """
    Build sensor-to-01B-stac-item-lib path map and validate requested itemIDs.

    Parameters
    ----------
    stac_item_lib_dir : str or Path
        Directory containing `<sensorID>/01B_stac_item_lib.pkl`.
    sensor_id_l : Iterable[str]
        Sensor IDs selected for this workflow spin-up.
    item_sensor_pairs : Iterable[tuple[str, str]] | None, default=None
        Unique `(sensorID, itemID)` pairs to validate against the loaded pickle keys.
    logger : logging.Logger | None, default=None
        Optional logger used for summary messages.

    Returns
    -------
    dict[str, Path]
        Mapping from `sensorID` to `01B_stac_item_lib.pkl` path.
    """
    log = logger or logging.getLogger(__name__)
    stac_item_lib_dir = Path(stac_item_lib_dir).resolve()
    assert stac_item_lib_dir.is_dir(), f"STAC item lib dir not found: {stac_item_lib_dir}"
    assert sensor_id_l is not None and len(sensor_id_l) > 0, "Expected at least one sensorID"

    # Build the sensor -> pickle path map and collect keysets for validation.
    stac_item_lib_d = {}
    stac_item_keys_d = {}
    for sensor_id in sensor_id_l:
        sensor_fp = stac_item_lib_dir / sensor_id / "01B_stac_item_lib.pkl"
        assert sensor_fp.is_file(), (
            f"STAC item lib pickle not found for sensorID {sensor_id} at\n{sensor_fp}"
        )
        stac_item_lib_d[sensor_id] = sensor_fp
        with open(sensor_fp, "rb") as f:
            stac_item_keys_d[sensor_id] = set(pickle.load(f).keys())

    # Validate (sensorID, itemID) pairs against each selected pickle.
    if item_sensor_pairs is not None:
        pair_df = pd.DataFrame(item_sensor_pairs, columns=["sensorID", "itemID"]).drop_duplicates()
        assert len(pair_df) > 0, "Expected at least one (sensorID, itemID) pair for validation."
        missing_map = {}
        for sensor_id, item_id in pair_df.itertuples(index=False, name=None):
            if item_id not in stac_item_keys_d.get(sensor_id, set()):
                missing_map.setdefault(sensor_id, []).append(item_id)

        if len(missing_map) > 0:
            lines = [
                "Missing requested STAC itemIDs in selected 01B_stac_item_lib.pkl files.",
                f"Checked {len(pair_df):,} unique (sensorID, itemID) pairs across {len(sensor_id_l):,} sensor(s).",
            ]
            for sensor_id, item_l in missing_map.items():
                sensor_fp = stac_item_lib_d.get(sensor_id, stac_item_lib_dir / sensor_id / "01B_stac_item_lib.pkl")
                lines.append(
                    f"sensorID={sensor_id} missing={len(item_l):,} pickle=\n    {sensor_fp}"
                )
                for item_id in item_l[:20]:
                    lines.append(f"  - {item_id}")
                if len(item_l) > 20:
                    lines.append(f"  - ... {len(item_l) - 20:,} more")
            raise ValueError("\n".join(lines))

        log.info(
            f"Validated {len(pair_df):,} requested (sensorID, itemID) pairs against selected STAC item libraries."
        )

    return stac_item_lib_d


# def parse_state_ids(raw_state_ids: Optional[object]) -> list[str]:
#     """
#     Normalize the `state_ids` configuration value into a list of zero-padded string integers.

#     Accepts integers, comma-separated strings, generic iterables, or None-like values.
#     Returns the full set of state IDs when no filtering should be applied.
#     All output integers are zero-padded to 2 digits (e.g., '01', '02').
#     """
#     if raw_state_ids is None or raw_state_ids == "None":
#         return list(_ALL_STATE_IDS)

#     if isinstance(raw_state_ids, int):
#         return [f"{raw_state_ids:02d}"]

#     if isinstance(raw_state_ids, str):
#         tokens = [token.strip() for token in raw_state_ids.split(",") if token.strip()]
#         if not tokens:
#             return list(_ALL_STATE_IDS)
#         return [f"{int(token):02d}" for token in tokens]

#     if isinstance(raw_state_ids, Iterable):
#         values = [
#             f"{int(value):02d}"
#             for value in raw_state_ids
#             if value is not None and str(value).strip() != ""
#         ]
#         return values or list(_ALL_STATE_IDS)

#     raise ValueError(
#         "Expected `state_ids` to be an int, string, iterable, or None; "
#         f"received {type(raw_state_ids).__name__}"
#     )


 
# def parse_sensor_ids(raw_sensor_ids):
#     """Parse and validate sensor IDs from config input."""
#     if raw_sensor_ids is None or raw_sensor_ids == 'None':
#         sensor_ids = list(SENSORS.keys())
#     else:
#         sensor_ids = [s.strip() for s in str(raw_sensor_ids).split(',')]

#     invalid_sensors = [s for s in sensor_ids if s not in SENSORS]
#     if invalid_sensors:
#         raise ValueError(f"Invalid sensor IDs: {invalid_sensors}. Available sensors: {list(SENSORS.keys())}")
    
#     #print(f'Using sensors: {sensor_ids}')

#     return sensor_ids


# def parse_item_ids(raw_item_ids: Optional[object]) -> list[str]:
#     """Normalize Snakemake config input into a list of STAC item identifier strings."""
#     if raw_item_ids is None or raw_item_ids == "None":
#         return []

#     if isinstance(raw_item_ids, str):
#         return [token.strip() for token in raw_item_ids.split(",") if token.strip()]

#     if isinstance(raw_item_ids, Iterable) and not isinstance(raw_item_ids, (str, bytes)):
#         return [
#             str(item).strip()
#             for item in raw_item_ids
#             if str(item).strip()
#         ]

#     item_str = str(raw_item_ids).strip()
#     return [item_str] if item_str else []








def _prep_sd_huc_event_index(events_df_raw):
    """standardize columns and index for SD/HUC event index"""
    if 'STATE_ID' not in events_df_raw.columns:
        raise KeyError("Expected column 'STATE_ID' in SD/HUC event index input.")

    events_index_df = (events_df_raw
            #.loc[:, ['EPISODE_ID', 'BEGIN_DATE_TIME', 'END_DATE_TIME','huc_areasqkm']]
            #.reset_index()
            #.drop(columns=['SD_loc_count', 'huc_areasqkm', 'huc_hutype', 'EVENT_TYPE'], errors='ignore')
            .astype({
                #'EPISODE_ID': 'int32', 'EVENT_ID': 'int32',
                     'STATE_ID': 'category', 'huc12':'category'})
            .rename(columns={'EVENT_ID':'eventID', 'EPISODE_ID':'episodeID', 'STATE_ID':'stateID','huc12':'hucID'})
            .set_index(['stateID', 'episodeID', 'eventID', 'hucID'])
            .rename(columns=str.lower)
            .sort_index()
            )

    return events_index_df


def get_sd_huc_event_index(fp):
    """load and prep the event index (NCEI StormDatabase + HUCs)"""

    events_df_raw = pd.read_parquet(fp, engine='pyarrow')#.set_index(['STATE_ID', 'EVENT_ID', 'huc12'])
 
 
    
    return _prep_sd_huc_event_index(events_df_raw).drop(columns=['SD_loc_count', 'huc_areasqkm', 'huc_hutype', 'EVENT_TYPE'], errors='ignore')
    
    """
    events_index_df.info()
    events_df.info()
    """

def load_01C_masterIndex_includes(
                             _01C_masterIndex_includes_fp, 
                             keys_l = ['sensorID', 'collection', 'fetch_collection', 'stateID', 'episodeID', 'itemID', 'eventID']
 
                             ):
    """ build and return a master index of STAC hits
    
    Parameters
    ----------
    _01C_masterIndex_includes_fp : str
        pickled dataframe with multi-indexers.
        contains ALL events collected from the per-State events_gdf

    keys_l : list of str

    Returns
    -------
    index_df : geopandas.geodataframe.GeoDataFram



    
    """
    df_raw= pd.read_parquet(_01C_masterIndex_includes_fp)
 

    assertions.assert_01C_masterIndex(df_raw)
    assert 'include' in df_raw.columns, "'include' column missing from _01C_masterIndex_includes_fp"
    
    sort_cols_pre = [c for c in keys_l if c in df_raw.columns]
    index_df = (
        df_raw
        .dropna(subset=['itemID']) #drop records with no STAC hits
        # .reset_index(drop=True)
        # .drop(columns=['huc_dummy_id']) #no longer needed 
        # .drop_duplicates() #clears out dummy_huc_ids
        .sort_values(by=sort_cols_pre)
        .reset_index(drop=True)
    )

    # Build collection->fetch_collection map from parameters and join to index rows.
    fetch_collection_records = []
    for sensorID, collections_map_d in SENSOR_FETCH_COLLECTION_MAP_D.items():
        fetch_cfg_d = SENSORS[sensorID]["_02_fetch"]
        for collection, fetch_collection_l in collections_map_d.items():
            for fetch_collection in fetch_collection_l:
                fetch_collection_cfg = fetch_cfg_d[fetch_collection]
                fetch_collection_records.append(
                    {
                        "sensorID": sensorID,
                        "collection": collection,
                        "fetch_collection": fetch_collection,
                        "rule": fetch_collection_cfg.get("rule"),
                    }
                )

    fetch_collection_map_df = pd.DataFrame(fetch_collection_records)
    assert len(fetch_collection_map_df) > 0, "No fetch_collection mappings found in parameters.SENSORS"
    assert not fetch_collection_map_df.duplicated(subset=["sensorID", "collection", "fetch_collection"]).any(), (
        "Duplicate sensorID/collection/fetch_collection mapping in parameters.SENSORS"
    )

    index_df = (
        index_df
        .merge(
            fetch_collection_map_df,
            on=["sensorID", "collection"],
            how="left",
            validate="many_to_many",
        )
        .sort_values(by=keys_l)
        .reset_index(drop=True)
    )
    index_df = index_df.astype(
        {k: v for k, v in MASTER_INDEX_DTYPES.items() if k in index_df.columns},
        errors="ignore",
    )

    missing_fetch_map = index_df["fetch_collection"].isna()
    if missing_fetch_map.any():
        missing_pairs = (
            index_df.loc[missing_fetch_map, ["sensorID", "collection"]]
            .drop_duplicates()
            .head(10)
            .to_dict(orient="records")
        )
        raise ValueError(
            "Missing fetch_collection mapping for sensorID/collection pairs in master index: "
            f"{missing_pairs}"
        )
 
    assert not index_df.isna().any().any(), "NaNs present in index_df after processing"
    assert set(index_df['itemID'])==set(df_raw['itemID'].dropna().unique()), "itemIDs do not match after processing"

    

    #print(f'build stac_masterIndex_01C_index_df w/: {index_df.nunique().to_dict()}')

    return index_df


def load_01C_masterIndex_includes_filtered(
    _01C_masterIndex_includes_fp,
    filters,
    index_keys=None,
    cache_dir=None,
    overwrite=False,
    logger=None,
):
    """
    Filter the STAC master index with caching keyed by filter args and file metadata.

    Group/filter logic:
    - Only one of stateID, episodeID, or eventID may be provided at a time.
    - Only one of itemID, collection, or fetch_collection may be provided at a time.
    - itemID/collection/fetch_collection filters must be used alone (no other filters allowed).
    - sensorID can be combined with a single stateID/episodeID/eventID filter.
    - Filters set to None or empty are ignored.

    Parameters
    ----------
    _01C_masterIndex_includes_fp : str or Path
        Pickled GeoDataFrame with the STAC master index.
    filters : Mapping[str, Optional[list]]
        Keys/values used to subset the index.
    index_keys : list[str], optional
        Column names that uniquely describe the STAC hits. Defaults to the filter keys.
    cache_dir : Path-like, optional
        Directory used to store cached filtered pickles. Defaults to a temp dir.
    overwrite : bool, optional
        When True, ignore any cached entry and rebuild the filtered index.
    logger : logging.Logger, optional
        Logger used for cache/build progress messages.

    Notes
    -----
    Emits verbose warnings when requested filter values are absent from the
    final filtered result. This warning is evaluated for both cache hits and
    cache misses.
    """
    log = logger or logging.getLogger(__name__)
    if filters is None:
        raise ValueError("filters must be a mapping of column names to values.")

    filters = dict(filters)
    group_a = {"stateID", "episodeID", "eventID"}
    group_b = {"sensorID"}
    group_c = {"itemID", "collection", "fetch_collection"} #used alone, no other filters allowed

    def _has_values(val):
        if val is None:
            return False
        if isinstance(val, (list, tuple, set)):
            return len(val) > 0
        return True

    active_keys = {k for k, v in filters.items() if _has_values(v)}

    # Enforce grouping rules eagerly
    active_group_c = group_c & active_keys
    if active_group_c and (active_keys - active_group_c):
        raise ValueError(
            "itemID/collection/fetch_collection filters must be used alone; "
            "remove other filters before calling load_01C_masterIndex_includes_filtered."
        )
    if len(active_group_c) > 1:
        raise ValueError(
            f"Provide only one of {sorted(group_c)}; received filters for {sorted(active_group_c)}."
        )

    active_group_a = group_a & active_keys
    if len(active_group_a) > 1:
        raise ValueError(
            f"Provide only one of {sorted(group_a)}; received filters for {sorted(active_group_a)}."
        )

    index_fp = Path(_01C_masterIndex_includes_fp)
    if not index_fp.exists():
        raise FileNotFoundError(f"STAC master index not found at {index_fp}")

    if index_keys is None:
        if not filters:
            raise ValueError("Cannot infer index_keys without any filters.")
        index_keys = list(filters.keys())

    # Normalize filters once for hashing and filtering
    normalized_filters = {}
    for key in index_keys:
        values = filters.get(key)
        if values is None:
            normalized_filters[key] = None
        else:
            normalized_filters[key] = list(values)

    cache_root = Path(cache_dir) if cache_dir is not None else Path(tempfile.gettempdir()) / "usf_stac_index_cache"
    cache_root.mkdir(parents=True, exist_ok=True)

    hash_payload = {
        "index_fp": str(index_fp.resolve()),
        "mtime": index_fp.stat().st_mtime,
        "index_keys": index_keys, #rebuilds when the index keys are newer
        "filters": {key: None if vals is None else sorted(vals) for key, vals in normalized_filters.items()},
    }
    cache_key = hashlib.sha256(
        json.dumps(hash_payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    cache_fp = cache_root / f"filtered_stac_index_{cache_key}.pkl"

    # -----------------------
    # ----- CACHE CHECK -----
    # -----------------------
    # Load cached filtered index when available; otherwise build and cache it.
    if cache_fp.exists() and not overwrite:
        log.info("Loading filtered STAC index from cache")
        filtered_df = pd.read_pickle(cache_fp)
    else:
        # ---------------------
        # ----- full load -----
        # ---------------------
        log.info("Building filtered STAC index (cache miss or overwrite requested)")
        index_df = load_01C_masterIndex_includes(index_fp, index_keys)
        """
        index_df.info()
        index_df.head()
        """

        mask = index_df['include']  #start with only included records
        for key, values in normalized_filters.items():
            if key not in index_df.columns:
                raise KeyError(f"Index key '{key}' not found in STAC master index")
            if values is None:
                continue
            mask_new = index_df[key].isin(values)
            
            if not mask_new.any():
                raise ValueError(f"No rows matched filter for {key} with ids: {values}")
            
            #check that all the values are in the index
            assert set(values).issubset(set(index_df[key].unique())), f"Some filter values for {key} were not found in the index: {values}"
 
            

            mask &= mask_new

        """
        test_itemID='S2A_MSIL2A_20230711T153821_N0510_R011_T18TYN_20241019T212830'
        index_df.loc[index_df['itemID']==test_itemID]
        mask.sum()
        """
 

        filtered_df = (index_df
                       .loc[mask].copy()
                       .reset_index(drop=True)
                       .drop(columns=['include'])
                       )
 
        if filtered_df.empty:
            raise ValueError("Filtering removed all rows from the STAC master index.")

        if index_keys:
            sort_cols = [c for c in index_keys if c in filtered_df.columns]
            if sort_cols:
                filtered_df = filtered_df.sort_values(sort_cols).reset_index(drop=True)

        # -----------------
        # ----- write -----
        # -----------------
        assertions.assert_01C_masterIndex(filtered_df)

        filtered_df.to_pickle(cache_fp)
        log.info(f"Cached filtered STAC index at\n  {cache_fp}")

    # Always warn when requested filter values were clipped from final output.
    for key, values in normalized_filters.items():
        if values is None or key not in filtered_df.columns:
            continue
        requested_l = list(values)
        requested_set = set(requested_l)
        kept_set = set(filtered_df[key].dropna().unique().tolist())
        dropped_l = [value for value in requested_l if value not in kept_set]
        if dropped_l:
            log.warning(
                f"Requested filter values were removed from filtered STAC index for {key}: "
                f"requested={len(requested_set):,}, kept={len(kept_set & requested_set):,}, dropped={len(set(dropped_l)):,}.\n"
                f"    dropped_{key}={sorted(set(dropped_l))}"
            )
 

    return filtered_df


# Backwards compatibility alias for earlier function name
#get_sd_huc_event_index_dx = get_sd_huc_event_index


def collect_common_inputs(repo_root, search_dirs, extensions):
    """Gather relative file paths for matching extensions under search_dirs."""
    repo_root = Path(repo_root)
    paths = []
    for rel_dir in search_dirs:
        base = repo_root / rel_dir
        if not base.exists():
            warnings.warn(f"collect_common_inputs: missing dir: {base}")
            continue
        for ext in extensions:
            for fp in base.rglob(f"*{ext}"):
                if fp.is_file():
                    # WARNING. this means if .tsv changes, a re-run will not be automatically triggered
                    paths.append(ancient(str(fp.relative_to(repo_root))))
    return sorted(set(paths))
