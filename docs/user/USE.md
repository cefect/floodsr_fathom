# fathom floodsr pipeline

## background
- background/definition of super-resolution of flood hazard layers (resolution enhancement, and what we're trying to achieve)
- intro to regional flood hazard data sets, like fathom, and challenges at working in this scale
- intro to HRDEM (coverage, mosaic project, resolutino, collection, etc.)
- snakemake and how it is useful for this application
- objective of the project

### floodsr
- philosophy and objective of the tool
- explain why we need a snakemake pipeline to execute on large fathom dataset. 
- explain cli vs. python api and why we make use of pythnon api here (to integrate with snakemake)
- link to docs and homepage

## USE

### data acquidition and environment setup
see [readme.md](../readme.md)

### running the pipeline
- introduce snakemake cli, configuration file, and profile
- bash command to prove the environment
- bash command to `-n` dry-run the pipeline (and how to interpret the output)
- running a selection (with config and with cli overrides)
- where to find inputs/outputs/logs

## FAQ/support
post an issue