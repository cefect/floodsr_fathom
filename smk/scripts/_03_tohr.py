"""Run one ToHR inference pass from prepared low-res depth and HRDEM inputs."""

import logging
from pathlib import Path

from floodsr.model_registry import fetch_model
from floodsr.tohr import tohr
from parameters import min_depth as MIN_DEPTH

import smk.scripts.assertions as assertions
from smk.scripts.coms import get_logger, resolve_cache_dir, resolve_logging_level


def main_03_tohr(
    r01_prep_fp,
    r02_hrdem_fp,
    r03_tohr_fp,
    cache_dir=None,
    model_version="ResUNet_16x_DEM",
    max_depth=10,
    min_depth_threshold=MIN_DEPTH,
    crs_policy="use-dem",
    window_method="hard",
    show_progress=False,
    logger=None,
):
    """Run one ToHR model pass and write the high-resolution output raster."""
    # Normalize the runtime inputs before validation and model resolution.
    log = logger or logging.getLogger(__name__)
    r01_prep_fp = Path(r01_prep_fp)
    r02_hrdem_fp = Path(r02_hrdem_fp)
    r03_tohr_fp = Path(r03_tohr_fp)
    cache_dir = resolve_cache_dir(cache_dir, rule_name="r03_tohr")
    max_depth = None if max_depth is None else float(max_depth)
    min_depth_threshold = None if min_depth_threshold is None else float(min_depth_threshold)

    # Validate the two upstream workflow contracts and the expected output target.
    assertions.assert_01_prep_output_raster(r01_prep_fp, min_depth=MIN_DEPTH)
    assertions.assert_02_hrdem_output_raster(r02_hrdem_fp)
    assert r03_tohr_fp.suffix.lower() in {".tif", ".vrt"}, f"unexpected ToHR suffix:\n    {r03_tohr_fp}"
    r03_tohr_fp.parent.mkdir(parents=True, exist_ok=True)
    log.info(f"starting 03_tohr with input=\n    {r01_prep_fp}")
    log.debug(
        f"03_tohr parameters: r02_hrdem_fp={r02_hrdem_fp}, r03_tohr_fp={r03_tohr_fp}, cache_dir={cache_dir}, model_version={model_version}, max_depth={max_depth}, min_depth_threshold={min_depth_threshold}, window_method={window_method}"
    )

    # Resolve the model path through the package registry using the rule cache.
    model_fp = fetch_model(model_version, cache_dir=cache_dir, show_progress=bool(show_progress))
    log.info(f"resolved model_fp=\n    {model_fp}")

    # Run the model wrapper with the notebook-aligned defaults for the workflow rule.
    result = tohr(
        model_version=model_version,
        model_fp=model_fp,
        depth_lr_fp=r01_prep_fp,
        dem_hr_fp=r02_hrdem_fp,
        output_fp=r03_tohr_fp,
        max_depth=max_depth,
        min_depth_threshold=min_depth_threshold,
        crs_policy=crs_policy,
        window_method=window_method,
        show_progress=bool(show_progress),
        logger=log,
    )

    # Validate the emitted raster contract and return the resolved output path.
    result_fp = Path(result["output_fp"]).resolve()
    assertions.assert_03_tohr_output_raster(result_fp)
    log.info(f"finished 03_tohr with output=\n    {result_fp}")
    return result_fp


if __name__ == "__main__":
    rule_name = snakemake.params.rule_name
    logger = get_logger(
        snakemake.log[0],
        level=resolve_logging_level(snakemake.params.logging_level, snakemake.params.DEBUG),
        logger_name=rule_name,
        add_stream_handler=True,
    )
    try:
        main_03_tohr(
            r01_prep_fp=snakemake.input.r01_prep_fp,
            r02_hrdem_fp=snakemake.input.r02_hrdem_fp,
            r03_tohr_fp=snakemake.output.r03_tohr_fp,
            cache_dir=snakemake.params.cache_dir,
            model_version=snakemake.params.model_version,
            max_depth=snakemake.params.max_depth,
            min_depth_threshold=snakemake.params.min_depth_threshold,
            crs_policy=snakemake.params.crs_policy,
            window_method=snakemake.params.window_method,
            show_progress=snakemake.params.show_progress,
            logger=logger,
        )
    except Exception as e:
        e.add_note(f"{rule_name} failed for input:\n    {snakemake.input.r01_prep_fp}")
        logger.exception(f"{rule_name} failed")
        raise
