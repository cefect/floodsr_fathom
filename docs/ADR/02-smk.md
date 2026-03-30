# ADR 02: SMK Snakefile structure 


Workflow-specific ADRs for the Snakemake pipelines live under:
- `docs/ADR/smk/`


## general
- driven by `workflow_outdir/00_tile_index`, with optional config/CLI selection for  `return_period`, `protection`, `hazard_type`
- workflow should invoke and use `snakemake.logging`. this should be passed through and wired into scripts and script runner functions. 


### scripts
- use the main function and `if __name__ == "__main__"` entrypoint patterns from `smk/scripts/example.py`