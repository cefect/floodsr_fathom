# ADR 02: SMK Snakefile structure 


Workflow-specific ADRs for the Snakemake pipelines live under:
- `docs/ADR/smk/`


## general
- driven by `workflow_outdir/00_tile_index`, with optional config/CLI selection for  `return_period`, `protection`, `hazard_type`. these are the main wildcards. 
- use the below wildcards. use a collector _all rule (after each rule) to invoke all comabtions in the index_df. 
    - hazard_type: FLUVIAL, PLUVIAL, COASTAL
    - protection: DEFENDED, UNDEFENDED
    - return_period: 1in5, 1in10, 1in20
    - tileID: the tileID from the index (e.g., 360_360)
- workflow should invoke and use `snakemake.logging`. this should be passed through and wired into scripts and script runner functions. 
 

## cache and tmp
- use python for retriving the platform agnostic tempdir
- default cache location should be `{out_dir}'/.cache/{ruleName}` where out_dir comes from config and ruleName is the name of the rule (or `00_load_index` for the case of the preabmel load_index f unction)

## rules
- use the folowing rule types
    - normal DAG rules (e.g., r01_hrdem) for the main processing steps.
    - collection/concat _all rules to invoke all combinations of their main rule. should be super short. primarily for dev. 

### normal rules
- always include `tile_index_chunk` in the params as the row from the tile_index_bx for that invocation. 
- include params.DEBUG

### scripts
- use the main function and `if __name__ == "__main__"` entrypoint patterns from `smk/scripts/example.py`