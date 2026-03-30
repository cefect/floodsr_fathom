"""Workflow-B prep step for fetch outputs.

ADR
---
Context:
    S2 and S1 prep share tile I/O and mean infill mechanics.
Decision:
    Keep `main_03_prep` as a thin orchestrator.
    Dispatch to sensor-specific assertion workers.
    Keep the shared item/tile iteration workflow in `BasePrepRunner.run()`.
    Use the shared `_2_prep_fetch_tile()` implementation for both S2 and S1.
Consequences:
    Prep outputs normalize masked pixels the same way across both sensors and
    always write a trailing `masked` band for downstream inference.
    MODIS prep merges GA onto the GQ grid and derives the explicit prep mask
    from the GQ 250 m `FILL_MASK` band on the final output grid while leaving
    raw resampled pixel values untouched.
"""

import json, logging, os, traceback
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.enums import Resampling
from rasterio.warp import reproject
from tqdm import tqdm

import smk.scripts.assertions as assertions
import smk.scripts.assertions_sensors as assertions_sensors
import smk.scripts.sensors as sensors
from smk.contracts import get_fetch_contract, get_inference_contract, get_prep_contract
from smk.scripts.coms import build_tiles_vrt
from parameters import SENSORS



def main_03_prep(
    _02_fetch_tiles_gpkg_fp_l,
    _02_fetch_assets_od_l,
    _03_prep_index_ofp,
    _03_prep_assets_od,
    _03_prep_metadata_ofp,
    _03_prep_vrt_ofp,
    sensorID,
    itemID,
    prep_stripe_score=False,
    logger=None,
    debug=False,
):
    """Prepare fetched GEE tiles for inference via sensor-specific prep workers."""
    log = logger or logging.getLogger(__name__)
    if debug:
        log.setLevel(logging.DEBUG)

    # Normalize all Snakemake/filepath inputs to lists of `Path`s.
    if isinstance(_02_fetch_tiles_gpkg_fp_l, (str, Path)):
        _02_fetch_tiles_gpkg_fp_l = [_02_fetch_tiles_gpkg_fp_l]
    if isinstance(_02_fetch_assets_od_l, (str, Path)):
        _02_fetch_assets_od_l = [_02_fetch_assets_od_l]
    _02_fetch_tiles_gpkg_fp_l = [Path(v) for v in _02_fetch_tiles_gpkg_fp_l]
    _02_fetch_assets_od_l = [Path(v) for v in _02_fetch_assets_od_l]
    assert len(_02_fetch_tiles_gpkg_fp_l) > 0, "No fetch_tiles inputs"
    assert len(_02_fetch_assets_od_l) > 0, "No fetch assets dirs"

    _03_prep_index_ofp = Path(_03_prep_index_ofp)
    _03_prep_assets_od = Path(_03_prep_assets_od)
    _03_prep_metadata_ofp = Path(_03_prep_metadata_ofp)
    _03_prep_vrt_ofp = Path(_03_prep_vrt_ofp)
    _03_prep_assets_od.mkdir(parents=True, exist_ok=True)
    _03_prep_metadata_ofp.parent.mkdir(parents=True, exist_ok=True)
    _03_prep_vrt_ofp.parent.mkdir(parents=True, exist_ok=True)

    # Build one merged prep config: workflow routing from `parameters.py`,
    # boundary schema from contracts, and model input band names from inference contracts.
    prep_cfg = dict(SENSORS[sensorID]["_03_prep"])
    prep_cfg.update(get_prep_contract(sensorID))
    prep_cfg["prep_stripe_score"] = bool(prep_stripe_score)
    band_model_map = get_inference_contract(sensorID)["band_model_map"]
    assert isinstance(band_model_map, dict) and len(band_model_map) > 0, (
        f"{sensorID} expects non-empty _04_inference.band_model_map"
    )
    prep_cfg["band_names_inference"] = list(band_model_map.keys())
    prep_fetch_collection_l = prep_cfg.get("fetch_collections", [])
    assert isinstance(prep_fetch_collection_l, list) and len(prep_fetch_collection_l) > 0, (
        f"{sensorID} expects non-empty _03_prep.fetch_collections"
    )
    fetch_collection = str(prep_cfg.get("target_fetch_collection", prep_fetch_collection_l[0]))

    # Index the passed fetch asset directories by fetch_collection for runner lookup.
    fetch_assets_od_by_collection = {}
    for _assets_od in _02_fetch_assets_od_l:
        fetch_collection_key = _assets_od.parent.parent.name
        if fetch_collection_key not in prep_fetch_collection_l:
            continue
        fetch_assets_od_by_collection[fetch_collection_key] = _assets_od

    # MODIS dispatches by sensor because it consumes two fetch collections; the
    # single-fetch sensors dispatch directly by fetch_collection.
    if len(prep_fetch_collection_l) > 1:
        runner_cls = _PREP_SENSOR_RUNNERS.get(sensorID)
    else:
        runner_cls = _PREP_COLLECTION_RUNNERS.get(fetch_collection)
    assert runner_cls is not None, f"No prep runner found for fetch_collection {fetch_collection!r}"

    runner = runner_cls(
        sensorID=sensorID,
        fetch_collection=fetch_collection,
        prep_cfg=prep_cfg,
        logger=log,
        debug=debug,
    )
    prep_index_gdf = runner.run(
        _02_fetch_tiles_gpkg_fp_l=_02_fetch_tiles_gpkg_fp_l,
        fetch_assets_od_by_collection=fetch_assets_od_by_collection,
        prep_assets_od=_03_prep_assets_od,
        itemID=itemID,
        debug=debug,
    )

    # Persist the prep index and derived VRT after the runner has written all tiles.
    _03_prep_index_ofp.parent.mkdir(parents=True, exist_ok=True)
    prep_index_gdf.to_file(_03_prep_index_ofp, driver="GPKG")
    assertions.assert_03_prep_index_gdf(gpd.read_file(_03_prep_index_ofp), source_fp=_03_prep_index_ofp)
    build_tiles_vrt(
        tile_fp_l=prep_index_gdf["dest_fp"].astype(str).tolist(),
        out_vrt_fp=_03_prep_vrt_ofp,
        logger=log,
        src_nodata=-9999.0,
        vrt_nodata=-9999.0,
        stage_label="prep",
    )
    # Write a lightweight sidecar JSON for quick inspection of stripe diagnostics.
    stripe_metric_l = ["stripe_score_peak"]
    metadata_d = {
        "sensorID": sensorID,
        "itemID": itemID,
        "prep_stripe_score": bool(prep_stripe_score),
        "tile_count": int(len(prep_index_gdf)),
        "tiles": [],
    }
    for row in prep_index_gdf.sort_values("fetch_tile_id").itertuples():
        tile_d = {
            "fetch_tile_id": str(row.fetch_tile_id),
            "dest_fp": str(row.dest_fp),
            "geometry_wkt": None if row.geometry is None else row.geometry.wkt,
            "stripe_worst_score": (
                None
                if pd.isna(getattr(row, "stripe_worst_score", np.nan))
                else round(float(getattr(row, "stripe_worst_score")), 3)
            ),
            "band_scores": {},
        }
        for band_name in runner.band_names_inference:
            band_score_d = {}
            for metric_name in stripe_metric_l:
                col_name = f"{metric_name}_{band_name}"
                if hasattr(row, col_name):
                    col_val = getattr(row, col_name)
                    band_score_d[metric_name] = None if pd.isna(col_val) else round(float(col_val), 3)
            if band_score_d:
                tile_d["band_scores"][band_name] = band_score_d
        metadata_d["tiles"].append(tile_d)
    with open(_03_prep_metadata_ofp, "w", encoding="utf-8") as f:
        json.dump(metadata_d, f, indent=2)
    log.info(f"Wrote prep item index with {len(prep_index_gdf)} rows to {_03_prep_index_ofp}")
    return prep_index_gdf


class BasePrepRunner:
    """Shared prep workflow and helpers for per-sensor assertion workers."""

    sensorID = None
    fetch_collection = None

    def __init__(self, sensorID, fetch_collection, prep_cfg, logger=None, debug=False):
        """Store prep configuration and validate sensor/collection wiring."""
        assert isinstance(sensorID, str) and sensorID, "sensorID must be a non-empty string"
        assert isinstance(fetch_collection, str) and fetch_collection, "fetch_collection must be a non-empty string"
        assert isinstance(prep_cfg, dict), f"prep_cfg must be a dict, got {type(prep_cfg)!r}"

        # Validate runtime selectors against worker class contracts.
        self.assert_expected_sensor(sensorID)
        self.assert_expected_fetch_collection(fetch_collection)

        self.sensorID = sensorID
        self.fetch_collection = fetch_collection
        self.prep_cfg = prep_cfg.copy()
        self.logger = logger or logging.getLogger(__name__)
        if debug:
            self.logger.setLevel(logging.DEBUG)

        self.prep_fetch_collection_l = self.prep_cfg.get("fetch_collections", [])
        assert isinstance(self.prep_fetch_collection_l, list) and len(self.prep_fetch_collection_l) > 0, (
            f"Missing _03_prep.fetch_collections for {self.sensorID}"
        )
        assert self.fetch_collection in self.prep_fetch_collection_l, (
            f"{self.sensorID} missing {self.fetch_collection} in _03_prep.fetch_collections"
        )

        self.profile_update = self.prep_cfg.get("profile", {})
        self.mask_band_name = str(self.prep_cfg.get("mask_band_name", "FILL_MASK")).strip()
        assert self.mask_band_name, f"{self.sensorID} _03_prep.mask_band_name must be non-empty"
        self.mask_band_masked_val = self.prep_cfg.get("mask_band_masked_val", 0)
        self.band_names_inference = list(self.prep_cfg.get("band_names_inference", []))
        assert len(self.band_names_inference) > 0, (
            f"{self.sensorID} missing _03_prep.band_names_inference"
        )
        self.band_names_load = list(self.band_names_inference) + [self.mask_band_name]
        self.prep_stripe_score = bool(self.prep_cfg.get("prep_stripe_score", False))
        self.prep_stripe_score_window = int(self.prep_cfg.get("prep_stripe_score_window", 31))
        self.prep_stripe_score_min_valid_frac = float(self.prep_cfg.get("prep_stripe_score_min_valid_frac", 0.25))
        self.allow_unmasked_nonfinite = bool(self.prep_cfg.get("allow_unmasked_nonfinite", False))

 

    def run(
        self,
        _02_fetch_tiles_gpkg_fp_l,
        fetch_assets_od_by_collection,
        prep_assets_od,
        itemID,
        debug=False,
    ):
        """Run shared prep workflow for one item."""
        assert isinstance(_02_fetch_tiles_gpkg_fp_l, list) and len(_02_fetch_tiles_gpkg_fp_l) > 0, (
            f"Expected non-empty fetch tiles list, got {_02_fetch_tiles_gpkg_fp_l!r}"
        )
        assert isinstance(fetch_assets_od_by_collection, dict), (
            f"fetch_assets_od_by_collection must be dict, got {type(fetch_assets_od_by_collection)!r}"
        )
        assert isinstance(itemID, str) and itemID, "itemID must be a non-empty string"

        prep_assets_od = Path(prep_assets_od)
        prep_assets_od.mkdir(parents=True, exist_ok=True)
        fetch_tiles_gdf = self._1_load_fetch_tiles_gdf(_02_fetch_tiles_gpkg_fp_l)

        # Iterate the primary fetch tile index and let the worker-specific row-prep
        # method handle any per-row sensor logic.
        records = []
        for row in tqdm(
            fetch_tiles_gdf.itertuples(),
            total=len(fetch_tiles_gdf),
            desc="03_prep load rasters",
            disable=not debug,
        ):
            records.append(
                self._2_prep_row(
                    row=row,
                    itemID=itemID,
                    fetch_assets_od_by_collection=fetch_assets_od_by_collection,
                    prep_assets_od=prep_assets_od,
                )
            )

        return self._4_build_prep_index_gdf(records=records, fetch_tiles_gdf=fetch_tiles_gdf)

    def _resolve_fetch_tiles(self, _02_fetch_tiles_gpkg_fp_l, fetch_collection=None):
        """Return the fetch tiles geopackage matching this fetch collection."""
        fetch_collection = fetch_collection or self.fetch_collection
        for _02_fetch_tiles_gpkg_fp in _02_fetch_tiles_gpkg_fp_l:
            _02_fetch_tiles_gpkg_fp = Path(_02_fetch_tiles_gpkg_fp)
            if _02_fetch_tiles_gpkg_fp.parent.parent.name == fetch_collection:
                return _02_fetch_tiles_gpkg_fp

        raise AssertionError(
            f"Missing fetch tiles gpkg for {self.sensorID}/{fetch_collection}; "
            f"candidates={_02_fetch_tiles_gpkg_fp_l}"
        )

    def _1_load_fetch_tiles_gdf(self, _02_fetch_tiles_gpkg_fp_l):
        """Load and validate the primary fetch tile index for this runner."""
        _02_fetch_tiles_gpkg_fp = self._resolve_fetch_tiles(_02_fetch_tiles_gpkg_fp_l)
        fetch_tiles_gdf = gpd.read_file(_02_fetch_tiles_gpkg_fp)
        assertions.assert_02_fetch_index_gdf(fetch_tiles_gdf, source_fp=_02_fetch_tiles_gpkg_fp)
        self.logger.info(f"Loaded {len(fetch_tiles_gdf)} fetch tile records from {_02_fetch_tiles_gpkg_fp}")
        return fetch_tiles_gdf

    def _2_prep_row(self, row, itemID, fetch_assets_od_by_collection, prep_assets_od):
        """Prep one row from the primary fetch tile index."""
        fetch_assets_od = fetch_assets_od_by_collection.get(self.fetch_collection)
        assert fetch_assets_od is not None, (
            f"Missing assets dir for {self.sensorID}/{itemID}/{self.fetch_collection}"
        )
        return self._2_prep_fetch_tile(
            row=row,
            itemID=itemID,
            fetch_assets_od=fetch_assets_od,
            prep_assets_od=prep_assets_od,
        )

    def _2_prep_fetch_tile(
        self,
        row,
        itemID,
        fetch_assets_od,
        prep_assets_od,
        logger=None,
    ):
        """Prep one fetch tile raster and return a metadata record."""
        logger = logger or self.logger
        log = logger.getChild("_02_prep_fetch_tile")
        # Localize expected load-band list for this tile-prep call.
        band_names_load = self.band_names_load
        dest_fp = Path(row.dest_fp)
        tile_fp = dest_fp if dest_fp.exists() else (fetch_assets_od / dest_fp.name)
        assert tile_fp.exists(), f"Missing raster for {row.fetch_tile_id}: {tile_fp}"

        # Validate source tile conformance before prep.
        self.contract_02_fetch(tile_fp)

        with rasterio.open(tile_fp) as src:
            # Resolve required load bands from source descriptions so prep is driven
            # by band names rather than positional assumptions.
            band_tuples = self._resolve_band_tuples(src=src)
            src_band_d = {band_key: idx0 for band_key, idx0 in band_tuples}
 

            # Load only the inference bands plus configured source mask band.
            arr = src.read(indexes=[src_band_d[band_name] + 1 for band_name in band_names_load])
            band_d = {band_name: idx0 for idx0, band_name in enumerate(band_names_load)}

            # get meta
            src_meta_d = {key: int(getattr(src, key)) for key in ["width", "height"]}
            src_meta_d["count"] = int(arr.shape[0])
            src_meta_d.update({"dtype": str(src.dtypes[0]), "crs": str(src.crs)})
            log.debug(f"loaded raster with shape={arr.shape} for {tile_fp.name}")

            # Resolve the explicit prep mask from the configured source mask band.
            masked_band_arr, mask_band_idx0 = self._resolve_fill_masks_for_tile(
                src_name=str(src.name),arr=arr,band_names=band_names_load,)
            
            assert mask_band_idx0 == len(band_names_load) - 1 

            # Normalize masked inference pixels before write for the single-fetch sensors.
            fill_band_tuples = [(band_name, band_d[band_name]) for band_name in self.band_names_inference]
            band_fill_meta_d = self._mean_fill_masked_pixels(
                arr=arr,
                masked_band_arr=masked_band_arr,
                band_tuples=fill_band_tuples,
            )
            stripe_meta_d = self._build_stripe_metric_d(
                arr=arr,
                masked_band_arr=masked_band_arr,
                band_tuples=fill_band_tuples,
            )


            # Write only inference bands plus the shared explicit `masked` band.
            out_arr = np.concatenate(
                [
                    arr[[band_d[band_name] for band_name in self.band_names_inference]],
                    masked_band_arr[np.newaxis, :, :].astype(arr.dtype, copy=False),
                ],
                axis=0,
            )
            # Force one prep output dtype so `nodata=-9999` is valid for every band.
            out_arr = out_arr.astype(np.float32, copy=False)
            out_band_names = list(self.band_names_inference) + ["masked"]

            # -----------------
            # ----- WRITE -----
            # -----------------
            # Write prepped raster with updated band count.
            out_tile_fp = prep_assets_od / tile_fp.name
            log.debug(f"Writing raster with profile updates:\n   {self.profile_update}\n to {out_tile_fp}")

            src_dataset_mask = np.where(masked_band_arr == 1, 0, 255).astype(np.uint8)
            src_dataset_masked_pixels = int(np.count_nonzero(src_dataset_mask == 0))

            self._3_write_prep_tile(
                out_tile_fp=out_tile_fp,
                out_arr=out_arr,
                out_band_name_l=out_band_names,
                src_profile=src.profile,
                src_dataset_mask=src_dataset_mask,
                out_dtype=str(out_arr.dtype),
                out_nodata=-9999.0,
                logger=log,
            )

        # Sanity-check that the GDAL dataset mask matches the explicit `masked` band.
        with rasterio.open(out_tile_fp) as dst:
            out_dataset_mask = dst.dataset_mask()
        out_dataset_masked_pixels = int(np.count_nonzero(out_dataset_mask == 0))
        assert out_dataset_masked_pixels == src_dataset_masked_pixels, (
            f"Dataset masked-pixel count changed: in={src_dataset_masked_pixels} out={out_dataset_masked_pixels}"
        )

        # Validate prepped output with sensor/collection assertions.
        self.contract_03_prep(out_tile_fp)

        return {
            "sensorID": self.sensorID,
            "itemID": itemID,
            "fetch_collection": self.fetch_collection,
            "fetch_tile_id": row.fetch_tile_id,
            "dest_fp": str(out_tile_fp),
            **src_meta_d,
            **band_fill_meta_d,
            **stripe_meta_d,
            "geometry": row.geometry,
        }

    def _resolve_fill_masks_for_tile(
        self,
        src_name,
        arr,
        band_names,
    ):
        """Resolve and return the normalized binary mask band and its index."""
        descriptions = [str(desc).strip() for desc in list(band_names)]
        desc_to_idx0 = self._build_desc_to_idx0(descriptions=descriptions, src_name=src_name)
        assert self.mask_band_name in desc_to_idx0, (
            f"{self.sensorID} prep expects mask band {self.mask_band_name!r} in {src_name}; "
            f"found {descriptions}"
        )
        mask_band_idx0 = int(desc_to_idx0[self.mask_band_name])
        raw_mask_arr = np.asarray(arr[mask_band_idx0], dtype=np.float32)
        masked_bx = ~np.isfinite(raw_mask_arr) | np.isclose(raw_mask_arr, float(self.mask_band_masked_val))
        masked_band_arr = np.where(masked_bx, 1, 0).astype(np.uint8)
        self.logger.debug(
            f"Resolved mask for {Path(src_name).name} from {self.mask_band_name!r}: "
            f"masked_pixels={int(np.count_nonzero(masked_bx)):,}"
        )
        return masked_band_arr, mask_band_idx0

    def _mean_fill_masked_pixels(
        self,
        arr,
        masked_band_arr,
        band_tuples,
    ):
        """Mean-fill masked pixels for each requested band and return fill metadata.
        TODO: upgrade to nearest-neighbor
        """
        masked_bx = masked_band_arr == 1
        band_fill_meta_d = {"masked_pixels_filled": 0}
        if not np.any(masked_bx):
            return band_fill_meta_d

        valid_bx = ~masked_bx
        for band_key, idx0 in band_tuples:
            band_arr = arr[idx0]
            band_non_finite_unmasked = (~np.isfinite(band_arr)) & valid_bx
            non_finite_unmasked_cnt = int(np.count_nonzero(band_non_finite_unmasked))
            if self.allow_unmasked_nonfinite:
                if non_finite_unmasked_cnt > 0:
                    self.logger.debug(
                        f"Leaving {non_finite_unmasked_cnt:,} unmasked NaN/Inf pixels unchanged in band {band_key!r}"
                    )
            else:
                assert non_finite_unmasked_cnt == 0, (
                    f"Found {non_finite_unmasked_cnt:,} NaN/Inf values in unmasked raw pixels "
                    f"for band {band_key!r}"
                )

            invalid_count = int(np.count_nonzero(masked_bx))
            if invalid_count == 0:
                continue

            self.logger.debug(
                f"Filling {invalid_count:,}/{band_arr.size:,} masked pixels in band {band_key!r}"
            )
            valid_values = band_arr[valid_bx]
            finite_values = valid_values[np.isfinite(valid_values)]
            assert finite_values.size > 0, "No finite valid pixels available for masked-band infill"

            fill_value = float(finite_values.mean())
            if np.issubdtype(band_arr.dtype, np.integer):
                fill_value = int(np.round(fill_value))

            band_arr[masked_bx] = fill_value
            arr[idx0] = band_arr
            band_fill_meta_d["masked_pixels_filled"] += invalid_count

        return band_fill_meta_d

    def _build_stripe_metric_d(
        self,
        arr,
        masked_band_arr,
        band_tuples,
    ):
        """Build compact row-median striping diagnostics for inference bands."""
        if not self.prep_stripe_score:
            return {}

        masked_bx = masked_band_arr == 1
        meta_d = {"stripe_worst_score": np.nan}
        worst_score = -np.inf
        for band_key, idx0 in band_tuples:
            band_arr = np.asarray(arr[idx0], dtype=np.float64)
            min_valid_cnt = max(1, int(np.ceil(self.prep_stripe_score_min_valid_frac * band_arr.shape[1])))
            row_median_l = []
            for row_idx in range(band_arr.shape[0]):
                valid_bx = ~masked_bx[row_idx]
                if int(np.count_nonzero(valid_bx)) < min_valid_cnt:
                    row_median_l.append(np.nan)
                    continue
                row_vals = band_arr[row_idx, valid_bx]
                row_vals = row_vals[~np.isnan(row_vals)]
                row_median_l.append(np.median(row_vals) if row_vals.size > 0 else np.nan)

            row_median_arr = np.asarray(row_median_l, dtype=np.float64)
            finite_row_median = row_median_arr[np.isfinite(row_median_arr)]
            if finite_row_median.size < 3:
                meta_d[f"stripe_score_peak_{band_key}"] = np.nan
                continue

            scale = max(float(np.nanstd(finite_row_median)), 1.0)
            extreme = max(float(np.nanmax(np.abs(finite_row_median))), 1.0) + 10.0 * scale
            row_median_work = np.where(
                np.isposinf(row_median_arr),
                extreme,
                np.where(np.isneginf(row_median_arr), -extreme, row_median_arr),
            )
            row_median_work = np.where(
                np.isnan(row_median_work),
                float(np.nanmedian(finite_row_median)),
                row_median_work,
            )

            window = int(self.prep_stripe_score_window)
            if window % 2 == 0:
                window -= 1
            window = min(window, len(row_median_work) if len(row_median_work) % 2 == 1 else len(row_median_work) - 1)
            if window < 3:
                trend_arr = row_median_work.copy()
            else:
                pad = window // 2
                trend_arr = np.convolve(
                    np.pad(row_median_work, (pad, pad), mode="edge"),
                    np.full(window, 1.0 / float(window), dtype=np.float64),
                    mode="valid",
                )
            resid_arr = row_median_work - trend_arr
            peak_score = float(np.max(np.abs(resid_arr)) / scale)
            meta_d[f"stripe_score_peak_{band_key}"] = peak_score
            worst_score = max(worst_score, peak_score)
            self.logger.debug(
                f"Stripe score for {self.sensorID} band {band_key!r}: "
                f"peak={peak_score:.3f}, valid_rows={int(np.count_nonzero(np.isfinite(row_median_arr))):,}, "
                f"masked_pixels={int(np.count_nonzero(masked_bx)):,}"
            )

        meta_d["stripe_worst_score"] = np.nan if not np.isfinite(worst_score) else float(worst_score)
        self.logger.debug(
            f"Stripe scoring complete for {self.sensorID}: worst_peak="
            f"{meta_d['stripe_worst_score'] if pd.notna(meta_d['stripe_worst_score']) else None}"
        )
        return meta_d

    def _3_write_prep_tile(
        self,
        out_tile_fp,
        out_arr,
        out_band_name_l,
        src_profile,
        src_dataset_mask,
        out_dtype,
        out_nodata,
        logger=None,
    ):
        """Write one prep tile with common profile, mask, and descriptions."""
        logger = logger or self.logger
        # Start from the source profile, then overwrite only the prep-stage fields.
        if out_tile_fp.exists():
            out_tile_fp.unlink()
        out_profile = src_profile.copy()
        out_profile.update(self.profile_update)
        out_profile.update(count=int(out_arr.shape[0]), dtype=str(out_dtype), nodata=float(out_nodata))
        with rasterio.open(out_tile_fp, "w", **out_profile) as dst:
            dst.write(out_arr.astype(np.dtype(out_dtype), copy=False))
            dst.write_mask(src_dataset_mask)
            for band_i, desc in enumerate(out_band_name_l, start=1):
                dst.set_band_description(band_i, desc)

    def _4_build_prep_index_gdf(self, records, fetch_tiles_gdf):
        """Build the final prep index GeoDataFrame from per-tile records."""
        prep_index_df = pd.DataFrame.from_records(records).sort_values("fetch_tile_id").reset_index(drop=True)
        assert len(prep_index_df) > 0, "No prep rows built"
        prep_index_gdf = gpd.GeoDataFrame(
            prep_index_df.drop(columns=["geometry"]),
            geometry=prep_index_df["geometry"],
            crs=fetch_tiles_gdf.crs,
        )
        assertions.assert_03_prep_index_gdf(prep_index_gdf)
        return prep_index_gdf


class S2PrepRunner(sensors.S2SensorWorker, BasePrepRunner, assertions.S2PrepAssertions):
    """Prep worker for `S2/gee_S2_SR_HARMONIZED` assertions."""

    # Uses BasePrepRunner.run workflow directly.


class S1PrepRunner(sensors.S1SensorWorker, BasePrepRunner, assertions.S1PrepAssertions):
    """Prep worker for `S1/gee_S1_GRD` assertions."""

    # def _2_prep_fetch_tile(...):
    #     Disabled to revert S1 prep back to the shared BasePrepRunner behavior.


class MODISPrepRunner(sensors.MODISSensorWorker, BasePrepRunner):
    """Prep worker for merged MODIS GA + GQ assets on the GQ grid."""

    def __init__(self, sensorID, fetch_collection, prep_cfg, logger=None, debug=False):
        """Store MODIS prep configuration for the GA/GQ merge path."""
        super().__init__(sensorID=sensorID, fetch_collection=fetch_collection, prep_cfg=prep_cfg, logger=logger, debug=debug)
        self.prep_fetch_collection_l = list(self.prep_cfg.get("fetch_collections", []))
        assert len(self.prep_fetch_collection_l) == 2, (
            f"{self.sensorID} expects exactly two _03_prep.fetch_collections entries for GA/GQ merge"
        )
        self.target_fetch_collection = str(self.prep_cfg.get("target_fetch_collection", "gee_MODIS_GQ"))
        assert self.target_fetch_collection in self.prep_fetch_collection_l, (
            f"{self.sensorID} target_fetch_collection={self.target_fetch_collection!r} "
            f"must be one of {self.prep_fetch_collection_l!r}"
        )
        aux_fetch_collection_l = [fc for fc in self.prep_fetch_collection_l if fc != self.target_fetch_collection]
        assert len(aux_fetch_collection_l) == 1, f"Expected one auxiliary MODIS fetch collection, got {aux_fetch_collection_l!r}"
        self.aux_fetch_collection = aux_fetch_collection_l[0]

        self.output_band_order = list(self.prep_cfg.get("output_band_order", self.band_names_inference))
        assert set(self.band_names_inference).issubset(set(self.output_band_order)), (
            f"{self.sensorID} expects inference bands to be a subset of output_band_order"
        )
        self.output_mask_band_name = str(self.prep_cfg.get("output_mask_band_name", "masked")).strip()
        assert self.output_mask_band_name, f"{self.sensorID} _03_prep.output_mask_band_name must be non-empty"
        self.output_dtype = str(self.prep_cfg.get("output_dtype", "float32"))
        self.output_nodata = float(self.prep_cfg.get("output_nodata", -9999.0))
        self.allow_unmasked_nonfinite = True

    def _1_load_fetch_tiles_gdf(self, _02_fetch_tiles_gpkg_fp_l):
        """Load the primary GQ tile index and cache the aligned GA tile lookup."""
        # The GQ tile index defines the output grid; GA rows are looked up by fetch_tile_id.
        gq_fetch_tiles_gpkg_fp = self._resolve_fetch_tiles(_02_fetch_tiles_gpkg_fp_l, self.target_fetch_collection)
        ga_fetch_tiles_gpkg_fp = self._resolve_fetch_tiles(_02_fetch_tiles_gpkg_fp_l, self.aux_fetch_collection)
        gq_fetch_tiles_gdf = gpd.read_file(gq_fetch_tiles_gpkg_fp)
        ga_fetch_tiles_gdf = gpd.read_file(ga_fetch_tiles_gpkg_fp)
        assertions.assert_02_fetch_index_gdf(gq_fetch_tiles_gdf, source_fp=gq_fetch_tiles_gpkg_fp)
        assertions.assert_02_fetch_index_gdf(ga_fetch_tiles_gdf, source_fp=ga_fetch_tiles_gpkg_fp)
        self.logger.info(f"Loaded {len(gq_fetch_tiles_gdf)} fetch tile records from {gq_fetch_tiles_gpkg_fp}")
        self._ga_row_d = {str(row.fetch_tile_id): row for row in ga_fetch_tiles_gdf.itertuples()}
        return gq_fetch_tiles_gdf

    def _2_prep_row(self, row, itemID, fetch_assets_od_by_collection, prep_assets_od):
        """Prep one MODIS GQ row together with its matched GA row."""
        # Use the GQ row as the primary tile record and join the matching GA row.
        ga_row = self._ga_row_d.get(str(row.fetch_tile_id))
        assert ga_row is not None, (
            f"Missing GA fetch tile for MODIS itemID={itemID} fetch_tile_id={row.fetch_tile_id!r}"
        )
        return self._2_prep_fetch_tile_pair(
            gq_row=row,
            ga_row=ga_row,
            gq_fetch_assets_od=fetch_assets_od_by_collection[self.target_fetch_collection],
            ga_fetch_assets_od=fetch_assets_od_by_collection[self.aux_fetch_collection],
            prep_assets_od=prep_assets_od,
            itemID=itemID,
        )

    def _assert_fetch_asset(self, fp: Path, fetch_collection: str):
        """Validate one MODIS fetch asset against the fetch-stage contract."""
        fetch_cfg = get_fetch_contract(self.sensorID, fetch_collection)
        expected_band_l = list(fetch_cfg["band_set"])
        expected_dtype = str(fetch_cfg["dtype"])
        expected_resolution = float(fetch_cfg["resolution_m"])
        with rasterio.open(fp) as src:
            assert src.height > 0 and src.width > 0, f"Image dimensions must be positive, found {src.width}x{src.height}"
            assert src.crs is not None, f"Input image at {fp} must have a CRS"
            assert all(dtype_name == expected_dtype for dtype_name in src.dtypes), (
                f"Expected all MODIS bands to be {expected_dtype}, found {src.dtypes} in {fp}"
            )
            assert np.isclose(src.res[0], expected_resolution, rtol=0.1) and np.isclose(src.res[1], expected_resolution, rtol=0.1), (
                f"Expected {expected_resolution} m pixels for {fetch_collection}, found {src.res} in {fp}"
            )
            band_name_l = [str(desc).strip() for desc in list(src.descriptions or ()) if desc and str(desc).strip()]
            missing_band_l = sorted(set(expected_band_l) - set(band_name_l))
            assert not missing_band_l, f"Missing MODIS bands {missing_band_l} in {fp}; found {band_name_l}"
            dataset_valid_mask = src.dataset_mask() > 0
            assert dataset_valid_mask.any(), f"No valid pixels (fully masked tile) in {fp}"

    def _resolve_src_band_d(self, src, expected_band_l):
        """Map expected band names to zero-based source band indexes."""
        src_name = str(getattr(src, "name", "<unknown-src>"))
        desc_to_idx0 = self._build_desc_to_idx0(
            descriptions=[str(desc).strip() for desc in list(src.descriptions or ())],
            src_name=src_name,
        )
        missing_band_l = [band_name for band_name in expected_band_l if band_name not in desc_to_idx0]
        assert not missing_band_l, f"Missing expected bands {missing_band_l} in {src_name}"
        return {band_name: int(desc_to_idx0[band_name]) for band_name in expected_band_l}

    def _2_prep_fetch_tile_pair(
        self,
        gq_row,
        ga_row,
        gq_fetch_assets_od,
        ga_fetch_assets_od,
        prep_assets_od,
        itemID,
    ):
        """Merge one GQ tile with the matching GA tile on the GQ grid."""
        gq_dest_fp = Path(gq_row.dest_fp)
        ga_dest_fp = Path(ga_row.dest_fp)
        gq_tile_fp = gq_dest_fp if gq_dest_fp.exists() else (Path(gq_fetch_assets_od) / gq_dest_fp.name)
        ga_tile_fp = ga_dest_fp if ga_dest_fp.exists() else (Path(ga_fetch_assets_od) / ga_dest_fp.name)
        assert gq_tile_fp.exists(), f"Missing GQ raster for {gq_row.fetch_tile_id}: {gq_tile_fp}"
        assert ga_tile_fp.exists(), f"Missing GA raster for {ga_row.fetch_tile_id}: {ga_tile_fp}"
        self._assert_fetch_asset(gq_tile_fp, self.target_fetch_collection)
        self._assert_fetch_asset(ga_tile_fp, self.aux_fetch_collection)

        gq_band_l = list(get_fetch_contract(self.sensorID, self.target_fetch_collection)["band_set"])
        ga_band_l = list(get_fetch_contract(self.sensorID, self.aux_fetch_collection)["band_set"])

        with rasterio.open(gq_tile_fp) as gq_src, rasterio.open(ga_tile_fp) as ga_src:
            gq_band_d = self._resolve_src_band_d(gq_src, gq_band_l)
            ga_band_d = self._resolve_src_band_d(ga_src, ga_band_l)
            gq_desc_to_idx0 = self._build_desc_to_idx0(
                descriptions=[str(desc).strip() for desc in list(gq_src.descriptions or ())],
                src_name=str(gq_src.name),
            )

            # Read native GQ bands directly on the target 250 m grid.
            gq_arr = np.stack(
                [np.asarray(gq_src.read(gq_band_d[band_name] + 1), dtype=np.float32) for band_name in gq_band_l],
                axis=0,
            )
            # Reproject each GA band onto the GQ tile grid before concatenation.
            ga_resampled = np.full((len(ga_band_l), gq_src.height, gq_src.width), np.nan, dtype=np.float32)
            for idx0, band_name in enumerate(ga_band_l):
                reproject(
                    source=np.asarray(ga_src.read(ga_band_d[band_name] + 1), dtype=np.float32),
                    destination=ga_resampled[idx0],
                    src_transform=ga_src.transform,
                    src_crs=ga_src.crs,
                    dst_transform=gq_src.transform,
                    dst_crs=gq_src.crs,
                    src_nodata=ga_src.nodata,
                    dst_nodata=np.nan,
                    resampling=Resampling.nearest if band_name == "state_1km" else Resampling.bilinear,
                )

            merged_arr = np.concatenate([gq_arr, ga_resampled], axis=0)
            # Build the explicit prep mask from the target-grid GQ FILL_MASK only.
            # MODIS intentionally does not expand this mask using GA non-finite pixels.
            assert self.mask_band_name in gq_desc_to_idx0, (
                f"Missing MODIS GQ mask band {self.mask_band_name!r} in {gq_tile_fp}"
            )
            gq_mask_idx0 = int(gq_desc_to_idx0[self.mask_band_name])
            gq_mask_arr = np.asarray(gq_src.read(gq_mask_idx0 + 1), dtype=np.float32)
            masked_band_arr = np.where(
                np.isclose(gq_mask_arr, float(self.mask_band_masked_val)) | ~np.isfinite(gq_mask_arr),
                1,
                0,
            ).astype(np.uint8)
            band_tuples = [(band_name, idx0) for idx0, band_name in enumerate(self.band_names_inference)]
            # MODIS keeps raw merged values unchanged after resample, so no infill is applied.
            band_fill_meta_d = {"masked_pixels_filled": 0}
            stripe_meta_d = self._build_stripe_metric_d(
                arr=merged_arr,
                masked_band_arr=masked_band_arr,
                band_tuples=band_tuples,
            )

            out_arr = np.concatenate(
                [merged_arr, masked_band_arr[np.newaxis, :, :].astype(np.float32, copy=False)],
                axis=0,
            ).astype(np.float32, copy=False)
            out_band_name_l = list(self.output_band_order) + [self.output_mask_band_name]
            out_tile_fp = Path(prep_assets_od) / gq_tile_fp.name
            src_dataset_mask = np.where(masked_band_arr == 1, 0, 255).astype(np.uint8)
            out_width = int(gq_src.width)
            out_height = int(gq_src.height)
            out_crs = str(gq_src.crs)

            self._3_write_prep_tile(
                out_tile_fp=out_tile_fp,
                out_arr=out_arr,
                out_band_name_l=out_band_name_l,
                src_profile=gq_src.profile,
                src_dataset_mask=src_dataset_mask,
                out_dtype=self.output_dtype,
                out_nodata=self.output_nodata,
                logger=self.logger,
            )

        self.contract_03_prep(out_tile_fp)
        return {
            "sensorID": self.sensorID,
            "itemID": itemID,
            "fetch_collection": self.target_fetch_collection,
            "fetch_tile_id": gq_row.fetch_tile_id,
            "dest_fp": str(out_tile_fp),
            "width": out_width,
            "height": out_height,
            "count": int(len(out_band_name_l)),
            "dtype": str(self.output_dtype),
            "crs": out_crs,
            **band_fill_meta_d,
            **stripe_meta_d,
            "geometry": gq_row.geometry,
        }

    def contract_03_prep(self, fp: str | Path):
        """Validate MODIS prep-stage assets written on the GQ grid."""
        path = Path(fp)
        prep_cfg = get_prep_contract(self.sensorID)
        expected_band_l = list(prep_cfg.get("output_band_order", []))
        expected_resolution = float(get_fetch_contract(self.sensorID, self.target_fetch_collection)["resolution_m"])
        with rasterio.open(path) as src:
            assert src.count == len(expected_band_l) + 1, (
                f"Expected {len(expected_band_l) + 1} bands, found {src.count} in {path}"
            )
            assert src.dtypes[0] == str(prep_cfg.get("output_dtype", "float32")), (
                f"Expected dtype {prep_cfg.get('output_dtype', 'float32')}, found {src.dtypes[0]} in {path}"
            )
            assert float(src.nodata) == float(prep_cfg.get("output_nodata", -9999.0)), (
                f"Expected nodata {prep_cfg.get('output_nodata', -9999.0)}, found {src.nodata} in {path}"
            )
            assert np.isclose(src.res[0], expected_resolution, rtol=0.1) and np.isclose(src.res[1], expected_resolution, rtol=0.1), (
                f"Expected {expected_resolution} m pixels, found {src.res} in {path}"
            )
            band_name_l = [str(desc).strip() for desc in list(src.descriptions or ()) if desc and str(desc).strip()]
            assert band_name_l == expected_band_l + [str(prep_cfg.get("output_mask_band_name", "masked"))], (
                f"Unexpected band order in {path}: {band_name_l}"
            )
            assertions_sensors._assert_masked_band_semantics(
                src,
                path,
                mask_band_name=str(prep_cfg.get("output_mask_band_name", "masked")),
            )
            assert (src.dataset_mask() > 0).any(), f"No valid pixels in {path}"


# Dispatch table: fetch_collection -> prep runner class.
_PREP_COLLECTION_RUNNERS = {
    "gee_S2_SR_HARMONIZED": S2PrepRunner,
    "gee_S1_GRD": S1PrepRunner,
}

_PREP_SENSOR_RUNNERS = {
    "MODIS": MODISPrepRunner,
}


if __name__ == "__main__":
    from smk.scripts.coms import get_logger

    try:
        sensorID = snakemake.wildcards.sensorID
        itemID = snakemake.wildcards.itemID
        DEBUG = snakemake.params.DEBUG

        # Disable tqdm progress bars for non-debug runs.
        if not DEBUG:
            os.environ["TQDM_DISABLE"] = "1"

        log = get_logger(
            snakemake.log[0],
            logger_name=f"03_prep.{sensorID}",
            level=logging.DEBUG if DEBUG else logging.WARNING,
            add_stream_handler=True,
        )
        log.debug(
            f"Resolved r03_prep inputs for sensorID={sensorID}, itemID={itemID}:\n"
            f"    fetch_tiles={list(snakemake.input._02_fetch_tiles_gpkg_fp_l)}\n"
            f"    fetch_assets={list(snakemake.input._02_fetch_assets_od_l)}\n"
            f"    out_index={snakemake.output._03_prep_index_ofp}\n"
            f"    out_metadata={snakemake.output._03_prep_metadata_ofp}\n"
            f"    out_vrt={snakemake.output._03_prep_vrt_ofp}\n"
            f"    out_assets={snakemake.output._03_prep_assets_od}\n"
            f"    prep_stripe_score={bool(getattr(snakemake.params, 'prep_stripe_score', False))}\n"
            f"    debug={DEBUG}"
        )

        main_03_prep(
            snakemake.input._02_fetch_tiles_gpkg_fp_l,
            snakemake.input._02_fetch_assets_od_l,
            snakemake.output._03_prep_index_ofp,
            snakemake.output._03_prep_assets_od,
            snakemake.output._03_prep_metadata_ofp,
            snakemake.output._03_prep_vrt_ofp,
            sensorID=sensorID,
            itemID=itemID,
            prep_stripe_score=bool(getattr(snakemake.params, "prep_stripe_score", False)),
            logger=log,
            debug=DEBUG,
        )
    except Exception as e:
        with open(snakemake.log[0], "a") as logf:
            traceback.print_exc(file=logf)
        raise e
