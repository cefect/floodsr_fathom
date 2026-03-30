"""Preprocess one Fathom low-resolution tile for downstream HRDEM fetch."""

import logging
from pathlib import Path

import numpy as np
import rasterio
from parameters import min_depth as MIN_DEPTH
from rasterio.windows import Window
from tqdm import tqdm

import smk.scripts.assertions as assertions
from smk.scripts.coms import get_logger


def main_01_prep(
    tile_fp,
    r01_prep_fp,
    min_depth=MIN_DEPTH,
    manual_window_size=None,
    show_progress=False,
    logger=None,
):
    """Write one float32 meters raster with dry and invalid cells set to zero."""
    # Normalize the runtime inputs before any I/O or validation.
    log = logger or logging.getLogger(__name__)
    tile_fp = Path(tile_fp)
    r01_prep_fp = Path(r01_prep_fp)
    min_depth = float(min_depth)
    manual_window_size = None if manual_window_size is None else int(manual_window_size)

    # Validate the major inputs and the upstream raster contract.
    assert tile_fp.is_file(), f"missing prep input raster:\n    {tile_fp}"
    assert min_depth >= 0.0, f"min_depth must be non-negative, got {min_depth}"
    if manual_window_size is not None:
        assert manual_window_size > 0, f"manual_window_size must be positive, got {manual_window_size}"

    assertions.assert_01_prep_input_raster(tile_fp)
    r01_prep_fp.parent.mkdir(parents=True, exist_ok=True)
    log.info(f"starting 01_prep with input=\n    {tile_fp}")
    log.debug(
        f"01_prep parameters: r01_prep_fp={r01_prep_fp}, min_depth={min_depth}, manual_window_size={manual_window_size}"
    )

    # Read the source raster and choose either native blocks or manual windows.
    with rasterio.open(tile_fp) as src_ds:
        profile = src_ds.profile.copy()
        profile.update(dtype="float32", nodata=None)

        if manual_window_size is None:
            window_l = [window for _, window in src_ds.block_windows(1)]
        else:
            window_l = []
            for row_off in range(0, src_ds.height, manual_window_size):
                for col_off in range(0, src_ds.width, manual_window_size):
                    window_l.append(
                        Window(
                            col_off=col_off,
                            row_off=row_off,
                            width=min(manual_window_size, src_ds.width - col_off),
                            height=min(manual_window_size, src_ds.height - row_off),
                        )
                    )

        wet_pixel_count = 0
        valid_pixel_count = 0
        iterator = tqdm(window_l, desc="01_prep windows", disable=not show_progress)

        # Convert centimeters to meters and zero out dry or invalid cells window by window.
        with rasterio.open(r01_prep_fp, "w", **profile) as dst_ds:
            for i, window in enumerate(iterator, start=1):
                raw_block_ar = src_ds.read(1, window=window)
                invalid_mask = (raw_block_ar == -32768) | (raw_block_ar == -32767)
                out_block_ar = np.zeros(raw_block_ar.shape, dtype=np.float32)

                if (~invalid_mask).any():
                    depth_block_ar = raw_block_ar.astype(np.float32) / np.float32(100.0)
                    wet_mask = (~invalid_mask) & (depth_block_ar >= np.float32(min_depth))
                    out_block_ar[wet_mask] = depth_block_ar[wet_mask]
                    wet_pixel_count += int(wet_mask.sum())
                    valid_pixel_count += int((~invalid_mask).sum())

                dst_ds.write(out_block_ar, 1, window=window)

                if i == 1 or i == len(window_l) or i % 100 == 0:
                    log.debug(f"processed window {i}/{len(window_l)}")

    # Fail hard on fully dry tiles and remove any partial output artifact.
    if wet_pixel_count == 0:
        r01_prep_fp.unlink(missing_ok=True)
        raise RuntimeError(
            f"No wet pixels remain after min_depth={min_depth:.3f} for tile:\n"
            f"    {tile_fp}\n"
            "Remove this tile from the test set before attempting r02_hrdem."
        )

    # Validate the output contract and report a concise completion summary.
    assertions.assert_01_prep_output_raster(r01_prep_fp, min_depth=min_depth)
    log.info(
        f"finished 01_prep with wet_pixels={wet_pixel_count:,}, valid_pixels={valid_pixel_count:,}, output=\n    {r01_prep_fp}"
    )
    return r01_prep_fp


if __name__ == "__main__":
    if "snakemake" in globals():
        # Initialize the rule logger and execute the rule entrypoint once.
        rule_name = snakemake.params.rule_name
        logger = get_logger(snakemake.log[0], level=logging.INFO, logger_name=rule_name, add_stream_handler=True)
        try:
            main_01_prep(
                tile_fp=snakemake.input.tile_fp,
                r01_prep_fp=snakemake.output.r01_prep_fp,
                min_depth=snakemake.params.min_depth,
                manual_window_size=snakemake.params.manual_window_size,
                show_progress=snakemake.params.show_progress,
                logger=logger,
            )
        except Exception as e:
            # Attach rule context at the boundary, then log and re-raise once.
            e.add_note(f"{rule_name} failed for input:\n    {snakemake.input.tile_fp}")
            logger.exception(f"{rule_name} failed")
            raise
