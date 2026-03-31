"""helper functions for snakemake scripts"""
import json, logging, os, tempfile




from datetime import datetime
from pathlib import Path

import numpy as np
 
#from rasterio.warp import transform_bounds



 

# NOTE: keep plotting helpers out of this module.
 

# Use explicit GDAL exception behavior before GDAL 4.0 default changes.
# gdal.UseExceptions()
 

def get_logger(log_file, level=logging.WARNING, logger_name="snake", add_stream_handler=True):
    """Create a stream and file logger.

    Parameters:
        log_file (str): Path to the log file.
        level (int): Stream handler level when enabled.
        logger_name (str): Name for the logger.
        add_stream_handler (bool): Attach stderr stream handler when requested.

    Returns:
        logging.Logger: Configured logger.
    """
    # Get a logger instance with the given name.
    logger = logging.getLogger(logger_name)
    logger.propagate = False

    # Set the logger level.
    logger.setLevel(logging.DEBUG)
    
    # Create a formatter for both handlers.
    file_formatter = logging.Formatter("%(levelname)s-%(asctime)s-%(name)s:  %(message)s", datefmt="%H:%M:%S")

    # Create and configure the file handler.
    if not os.path.exists(os.path.dirname(log_file)):
        print(f"[WARNING] log directory does not exist.. creating {os.path.dirname(log_file)}")
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(file_formatter)

    # Avoid duplicate writes when this logger name is reused in-process.
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    logger.addHandler(file_handler)

    # Optional stderr logging for local interactive runs.
    if add_stream_handler:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level)
        stream_formatter = logging.Formatter("[%(levelname)s]%(name)s:  %(message)s")
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"\n\nLogger {logger_name} initialized at {current_time}.\n{'='*50}\n")



    return logger


def resolve_cache_dir(cache_dir, rule_name):
    """Return one cache directory path for a rule, with a system-temp fallback."""
    cache_dir = Path(cache_dir) if cache_dir is not None else Path(tempfile.gettempdir()) / "floodsr" / ".cache" / rule_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir.resolve()

 
 
