 
## setup test data set
actual data is large and slow
want to build a test data set that mirrors the structure, but has a much smaller area/path for innitial wiring/proving. 
- create a bash script in `misc` to:
    - for each `hazard_type` and `protection` combination, create a new tile set, 1/10th the size (360x360) cropped from the center from the 1in1000 return period.  (use gdal cli... ensure the releavant raster metadata is preserved... jsut want a spatial crop... no change in projection or resolution). 
    - should result in 3x2=6 new indexes. name with the same pattern, but change the return_period patttern to 1in9999
    - output indexes to `workflow_outdir/00_tile_index` and tiles to `workflow_outdir/00_tiles`
    - have some progress reporting. 
run to prove (conda -n deploy). 
summarize resulting file size

## implement smk workflow (on `workflow_outdir_test`)
- follow the poc from `dev/proof_of_concept/fathom_tohr.ipynb`
- update config (set out_dir to `workflow_outdir_test`) and profile 
- simplify/prune snakefile copy for local run only 
    - clean out EP/resources, COMMON_INPUTS, etc
    - keep other patterns
    - revise `TILE_CNT` so it is used only when debug=True (should limit the number of tiles processed per scenario dimension). applied after `load_index`

create the `load_index` function in `smk/snakefile_coms.py` to return the `index_df`:
    - ingeests the the `fathom_index_dir` from config, and tile_cnt, and optional cli args for subsetting on scenario dimensions (return_period, protection, hazard_type)    
    - concats all the index files files (report on counts per scenario dimension combination)
    - normalizes column names to match the `fathom_scenario_dimensions` keys (and 'tileID')
    - checks all scenario dimension values are found in `fathom_scenario_dimensions`
    - if a subset was specified, check it is valid, then filter the index accordingly (report on counts per scenario dimension combination after filtering). this happens regardless of debug
    - return a dataframe to drive the workflow (i.e, one row per linear chain (of jobs/rules) invocation)
    - use liberal caching to avoid re-building the index if identical filters are passed. 
    - flat dataframe with arbiraty index (no geometry)

r01_hrdem
    - complete the rule showing the existing template. 
    - for each wildcard combination in the index_df
    - use api_calls (and set in profile) so we only fetch 4 tiles at a time. 
    - make a best guess at a good `threads` value. 
    - create a script matching the style of `smk/scripts/example.py`
        - mostly a wrapper around `main_fetch_hrdem_for_lowres_tile`
        - ensure everything is pointed/wired correctly for the snakemake flow
        - supress progress reporting.
    - complete the `r01_hrdem_all` to give us all invocations of teh `r01_hrdem` rule (contained in the filtered index_df)

tests
    - complete `tests/test_smk.py` for the new rule. (see commented out tests for example templates)
    - complete `tests/test_workflow.py` for the new main_ function in for the new script
    - complete `tests/test_snakefile_coms.py` for  `load_index` (should mirror some invocation patterns from teh snakefile. i.e., full coverage). 

smk/readme.md
    - add the bash command to invoke a single tile
    - add the bash command to invoke all tiles (with the _all rule)

no fallbacks. hard errors. 
be critical of existing code.. it is a mix of copy/paste and partial migration from another project (ADRs are fully migrated though and should be trusted). 
