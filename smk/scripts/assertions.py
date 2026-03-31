"""Assertions for the local Fathom Snakemake workflow."""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio

from conf import fathom_scenario_dimensions


def assert_load_index_df(index_df, logger=None):
    """Validate the normalized load-index dataframe."""
    if not __debug__:
        return

    log = logger or logging.getLogger(__name__)
    required_cols = {
        "tileID",
        "return_period",
        "protection",
        "hazard_type",
        "tile_fp",
        "index_fp",
        "layer_name",
    }

    assert isinstance(index_df, pd.DataFrame), f"expected DataFrame, got {type(index_df)!r}"
    assert not index_df.empty, "load_index returned an empty dataframe"
    missing_cols = required_cols.difference(index_df.columns)
    assert not missing_cols, f"missing load_index columns: {sorted(missing_cols)}"
    assert "geometry" not in index_df.columns, "load_index dataframe must be flat and geometry-free"
    assert index_df.index.is_unique, "load_index dataframe index must be unique"
    assert not index_df[list(required_cols)].isna().any().any(), "load_index contains nulls in required columns"

    duplicate_mask = index_df.duplicated(subset=["return_period", "hazard_type", "protection", "tileID"])
    assert not duplicate_mask.any(), "scenario + tileID combinations must be unique"

    for key, allowed_l in fathom_scenario_dimensions.items():
        bad_values = sorted(set(index_df[key].tolist()).difference(allowed_l))
        assert not bad_values, f"unexpected values for {key}: {bad_values}"

    tile_path_l = [Path(fp) for fp in index_df["tile_fp"].tolist()]
    missing_tile_l = [str(fp) for fp in tile_path_l if not fp.exists()]
    assert not missing_tile_l, f"missing tile paths in load_index: {missing_tile_l[:3]}"

    index_path_l = [Path(fp) for fp in index_df["index_fp"].tolist()]
    missing_index_l = [str(fp) for fp in index_path_l if not fp.exists()]
    assert not missing_index_l, f"missing index paths in load_index: {missing_index_l[:3]}"

    log.debug(f"load_index assertion passed for {len(index_df):,} rows")


def assert_01_prep_input_raster(depth_lr_fp):
    """Validate one raw low-resolution Fathom depth tile."""
    if not __debug__:
        return

    depth_lr_fp = Path(depth_lr_fp)
    assert depth_lr_fp.exists(), f"missing prep input raster:\n    {depth_lr_fp}"

    with rasterio.open(depth_lr_fp) as ds:
        assert ds.count == 1, f"expected one band, got {ds.count}"
        assert ds.dtypes[0] == "int16", f"expected int16 input, got {ds.dtypes[0]}"
        assert ds.crs is not None and ds.crs.to_epsg() == 4326, f"expected EPSG:4326, got {ds.crs}"
        assert ds.nodata in {None, -32768.0, -32767.0}, f"unexpected nodata value: {ds.nodata}"
        assert 0.0 < abs(ds.res[0]) < 0.001 and 0.0 < abs(ds.res[1]) < 0.001, f"unexpected resolution: {ds.res}"
        raw_ar = ds.read(1)

    assert raw_ar.min() >= -32768, f"unexpected minimum value: {raw_ar.min()}"
    assert raw_ar.max() <= 1000, f"unexpected maximum value: {raw_ar.max()}"


def assert_01_prep_inputs(tile_fp, min_depth, manual_window_size):
    """Validate the main_01_prep runtime inputs before processing."""
    if not __debug__:
        return

    tile_fp = Path(tile_fp)
    assert tile_fp.is_file(), f"missing prep input raster:\n    {tile_fp}"
    assert min_depth >= 0.0, f"min_depth must be non-negative, got {min_depth}"
    if manual_window_size is not None:
        assert manual_window_size > 0, f"manual_window_size must be positive, got {manual_window_size}"


def assert_01_prep_output_raster(depth_pp_fp, min_depth):
    """Validate one preprocessed low-resolution depth raster."""
    if not __debug__:
        return

    depth_pp_fp = Path(depth_pp_fp)
    assert depth_pp_fp.exists(), f"missing prep output raster:\n    {depth_pp_fp}"

    with rasterio.open(depth_pp_fp) as ds:
        assert ds.count == 1, f"expected one band, got {ds.count}"
        assert ds.dtypes[0] == "float32", f"expected float32 output, got {ds.dtypes[0]}"
        assert ds.crs is not None and ds.crs.to_epsg() == 4326, f"expected EPSG:4326, got {ds.crs}"
        assert ds.nodata is None, f"expected nodata=None, got {ds.nodata}"
        out_ar = ds.read(1)

    assert np.isfinite(out_ar).all(), "prep output contains non-finite values"
    assert out_ar.min() >= 0.0, f"prep output contains negative values: {out_ar.min()}"
    assert out_ar.max() < 15.0, f"prep output exceeds expected max depth: {out_ar.max()}"

    wet_mask = out_ar >= np.float32(min_depth)
    assert wet_mask.any(), "prep output contains no wet pixels at or above min_depth"
    assert not wet_mask.all(), "prep output is fully wet, which violates the expected contract"


def assert_02_hrdem_output_raster(dem_hr_fp):
    """Validate one fetched HRDEM raster or VRT."""
    if not __debug__:
        return

    dem_hr_fp = Path(dem_hr_fp)
    assert dem_hr_fp.exists(), f"missing HRDEM output raster:\n    {dem_hr_fp}"

    with rasterio.open(dem_hr_fp) as ds:
        assert ds.count == 1, f"expected one band, got {ds.count}"
        assert ds.width > 0 and ds.height > 0, f"unexpected raster shape: {(ds.height, ds.width)}"
        assert ds.crs is not None, "expected HRDEM CRS to be present"
        assert abs(ds.res[0]) > 0.0 and abs(ds.res[1]) > 0.0, f"unexpected HRDEM resolution: {ds.res}"
        sample_ar = ds.read(1, out_shape=(min(ds.height, 64), min(ds.width, 64)), masked=True)

    assert sample_ar.count() > 0, "HRDEM output sample is fully masked"


def assert_03_tohr_output_raster(r03_tohr_fp):
    """Validate one ToHR output raster or VRT."""
    if not __debug__:
        return

    r03_tohr_fp = Path(r03_tohr_fp)
    assert r03_tohr_fp.exists(), f"missing ToHR output raster:\n    {r03_tohr_fp}"
    assert r03_tohr_fp.suffix.lower() in {".tif", ".vrt"}, f"unexpected ToHR suffix:\n    {r03_tohr_fp}"

    with rasterio.open(r03_tohr_fp) as ds:
        assert ds.count == 1, f"expected one band, got {ds.count}"
        assert ds.width > 0 and ds.height > 0, f"unexpected raster shape: {(ds.height, ds.width)}"
        assert ds.crs is not None, "expected ToHR CRS to be present"
        sample_ar = ds.read(1, out_shape=(min(ds.height, 64), min(ds.width, 64)), masked=True)

    assert sample_ar.count() > 0, "ToHR output sample is fully masked"
    assert np.isfinite(sample_ar.compressed()).all(), "ToHR output sample contains non-finite values"


def assert_02_hrdem_inputs(r01_prep_fp, r02_hrdem_fp):
    """Validate the main_02_hrdem runtime inputs before fetch."""
    if not __debug__:
        return

    r01_prep_fp = Path(r01_prep_fp)
    r02_hrdem_fp = Path(r02_hrdem_fp)
    assert r01_prep_fp.is_file(), f"missing HRDEM input raster:\n    {r01_prep_fp}"
    assert r02_hrdem_fp.suffix.lower() in {".tif", ".vrt"}, f"HRDEM output must be a GeoTIFF or VRT:\n    {r02_hrdem_fp}"
