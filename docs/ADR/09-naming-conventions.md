# ADR 09: Python Naming Conventions

## Context

Workflow code already uses a recognizable naming scheme across rule scripts, runner classes, sensor mixins, and assertion workers, but the conventions are only implied and a few ADR examples drift from the implemented style.

This ADR codifies the project naming conventions so new workflow code and future ADRs use the same vocabulary.

## Decision

Use the following naming conventions for Python workflow code.

### Rule-script entrypoints

- Rule-script entrypoints use `main_<stage>_<action>(...)`.
- `<stage>` should match the workflow step identifier used in the script filename and Snakemake rule family.
- Keep the stage prefix numeric and zero-padded when the workflow step is numeric.
- Preserve an existing stage-letter suffix when the workflow already uses one, such as `main_01B_stac_concat(...)` or `main_01C_index(...)`.

Examples:
- `main_02_fetch_gee(...)`
- `main_03_prep(...)`
- `main_04_inference(...)`

### Internal ordered workflow methods

- Ordered worker methods use a private numeric prefix: `_<n>_<verb_phrase>(...)`.
- Use these only for major ordered phases inside a class-based workflow.
- Numbering is local to the worker and communicates execution order, not global workflow stage.

Examples:
- `_1_select_image(...)`
- `_2_resolve_native_crs(...)`
- `_3_build_cache_payload(...)`
- `_4_infer_batch_probs(...)`

### Runner and worker classes

- Shared abstract or common runner classes use `Base_<Domain>`.
- Sensor-level shared specializations use `<Sensor>_<Domain>`.
- Collection- or model-specific concrete runners append the specific product/model name 
- Sensor mixins use `<Sensor>_Worker`.
- Assertion workers use `<ProductOrSensor>_Assertions` or `<ProductOrSensor>_Assertion_Base`.

 

### Module-level helper functions

- General helper functions use `snake_case`.
- Private helpers should begin with `_`.
- Context managers and small utility functions should not use staged numeric prefixes unless they are part of an ordered worker lifecycle.

Examples:
- `build_fetch_tiles(...)`
- `init_ee(...)`
- `_link_to_out_dir(...)`
- `_suppress_geedim_output(...)`

### Constants and registries

- Module constants and dispatch registries use `UPPER_SNAKE_CASE`.

Examples:
- `SENSORS`
- `CONFIG_DIR`
- `_FETCH_COLLECTION_RUNNERS`

 