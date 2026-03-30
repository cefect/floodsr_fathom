

from __future__ import annotations
from pprint import pprint
import json
import tempfile
from pathlib import Path
from typing import Any, Iterable
from numbers import Integral, Real
import warnings


# example assertion
# def assert_02_fetch_index_gdf(fetch_index_gdf: Any, source_fp: Path | None = None):
#     """Validate r02 fetch index input used by `main_03_prep`."""
#     if not __debug__:
#         return

#     assert isinstance(fetch_index_gdf, gpd.GeoDataFrame), (
#         f"Expected GeoDataFrame, got {type(fetch_index_gdf)!r}"
#     )
#     assert len(fetch_index_gdf) > 0, f"No rows found in fetch tile index: {source_fp}"

#     required_cols = {"fetch_tile_id", "dest_fp", "geometry"}
#     missing_cols = required_cols.difference(fetch_index_gdf.columns)
#     assert not missing_cols, (
#         f"Missing required columns in fetch tile index: {sorted(missing_cols)}"
#     )
#     assert not fetch_index_gdf["fetch_tile_id"].isna().any(), (
#         "Null values found in 'fetch_tile_id' column of fetch tile index"
#     )
#     assert fetch_index_gdf["fetch_tile_id"].is_unique, (
#         "fetch_tile_id values must be unique in fetch tile index"
#     )
#     assert not fetch_index_gdf["dest_fp"].isna().any(), (
#         "Null values found in 'dest_fp' column of fetch tile index"
#     )
#     assert not fetch_index_gdf.geometry.isna().any(), (
#         "Null geometries found in fetch tile index"
#     )

#     return