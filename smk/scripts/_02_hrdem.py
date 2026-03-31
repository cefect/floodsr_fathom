"""Fetch one HRDEM raster for a preprocessed low-resolution tile."""

import logging
from pathlib import Path

from floodsr.dem_sources.hrdem_mosaic import main_fetch_hrdem_for_lowres_tile
from parameters import min_depth as MIN_DEPTH

import smk.scripts.assertions as assertions
from smk.scripts.coms import get_logger, resolve_cache_dir


def main_02_hrdem(
    r01_prep_fp,
    r02_hrdem_fp,
    cache_dir=None,
    asset_key="dtm",
    use_cache=True,
    force_tiling=False,
    show_progress=False,
    logger=None,
):
    """Fetch one HRDEM raster aligned to a preprocessed low-resolution tile."""
    # Normalize the runtime inputs before validation and fetch setup.
    log = logger or logging.getLogger(__name__)
    r01_prep_fp = Path(r01_prep_fp)
    r02_hrdem_fp = Path(r02_hrdem_fp)
    cache_dir = resolve_cache_dir(cache_dir, rule_name="r02_hrdem")

    # Validate the prepared depth input and the expected output target.
    assertions.assert_02_hrdem_inputs(r01_prep_fp, r02_hrdem_fp)
    r02_hrdem_fp.parent.mkdir(parents=True, exist_ok=True)

    assertions.assert_01_prep_output_raster(r01_prep_fp, min_depth=MIN_DEPTH)
    log.info(f"starting 02_hrdem with input=\n    {r01_prep_fp}")
    log.debug(
        f"02_hrdem parameters: r02_hrdem_fp={r02_hrdem_fp}, cache_dir={cache_dir}, asset_key={asset_key}, use_cache={use_cache}, force_tiling={force_tiling}"
    )

    # Delegate the aligned DEM fetch to the shared HRDEM source helper.
    fetch_result = main_fetch_hrdem_for_lowres_tile(
        depth_lr_fp=r01_prep_fp,
        output_fp=r02_hrdem_fp,
        cache_dir=cache_dir,
        asset_key=asset_key,
        use_cache=bool(use_cache),
        force_tiling=bool(force_tiling),
        show_progress=bool(show_progress),
        logger=log,
    )

    # Validate the fetched raster contract and report the final output path.
    dem_fp = Path(fetch_result.dem_fp)
    assertions.assert_02_hrdem_output_raster(dem_fp)
    log.info(f"finished 02_hrdem with output=\n    {dem_fp}")
    return dem_fp


if __name__ == "__main__":
 
    # Initialize the rule logger and execute the rule entrypoint once.
    rule_name = snakemake.params.rule_name
    logger = get_logger(snakemake.log[0], level=logging.INFO, logger_name=rule_name, add_stream_handler=True)
    try:
        main_02_hrdem(
            r01_prep_fp=snakemake.input.r01_prep_fp,
            r02_hrdem_fp=snakemake.output.r02_hrdem_fp,
            cache_dir=snakemake.params.cache_dir,
            asset_key="dtm",
            use_cache=snakemake.params.use_cache,
            force_tiling=snakemake.params.force_tiling,
            show_progress=snakemake.params.show_progress,
            logger=logger,
        )
    except Exception as e:
        # Attach rule context at the boundary, then log and re-raise once.
        e.add_note(f"{rule_name} failed for input:\n    {snakemake.input.r01_prep_fp}")
        logger.exception(f"{rule_name} failed")
        raise
