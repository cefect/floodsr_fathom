 
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

## implement smk workflow (on `workflow_outdir_test`) r01_prep + r02_hrdem
- follow the poc from `dev/proof_of_concept/fathom_tohr.ipynb`
- match where possible and replace with a fresh minimal local Snakefile using current ADRs,
    - clean out EP/resources, COMMON_INPUTS, etc
    - keep other patterns
    - revise `TILE_CNT` so it is used only when debug=True (should limit the number of tiles processed per scenario dimension). applied after `load_index` and stable sorting. 

create the `load_index` function in `smk/snakefile_coms.py` to return the `index_df`:
    - ingeests the the `fathom_index_dir` from config, and tile_cnt, and optional cli args for subsetting on scenario dimensions (return_period, protection, hazard_type)    
    - concats all the index files files (report on counts per scenario dimension combination)
    - make the schema contract explicit from the `.gpkg` contents: each layer currently gives only `geometry` plus `location`, so `return_period`, `hazard_type`, and `protection` should be imputed from the layer/file name parts `FLOOD_MAP-1ARCSEC-NW_OFFSET-<return_period>-<hazard_type>-<protection>-DEPTH-2020-PERCENTILE50-v3.1`, `tileID` should come from the tile stem in `location` (e.g. `n70w108`), and the low-res tile filepath should come directly from `location`
    - normalizes column names to match the `fathom_scenario_dimensions` keys (and 'tileID')
    - checks all scenario dimension values are found in `fathom_scenario_dimensions`
    - if a subset was specified, check it is valid, then filter the index accordingly (report on counts per scenario dimension combination after filtering). this happens regardless of debug
    - return a dataframe to drive the workflow (i.e, one row per linear chain (of jobs/rules) invocation)
    - use liberal caching to avoid re-building the index if identical filters are passed. 
    - flat dataframe with arbiraty index (no geometry)
    - create an assertion function (`smk/scripts/assertions.py`) to codify/validate the contract for the index_df schema and contents. use this in `load_index` and in tests.

r01_prep
    - add the rule for preprocessing
    - for each wildcard combination in the index_df
    - create a script matching the style of `smk/scripts/example.py`
        - mostly implements the `_write_preprocessed_lores` from the POC notebook. 
            - wire in some more control for block_windows and tuning the memory usage. 
            - add logging and progress bar for the blocking loop. 
        - also applies the `min_depth' parameter (masking out all values less than this depth).
            - fail elegantly when all pixels are maxed (i.e. , no wet pixels). make a note here for me so we can remove these test cases. dont try to run these through r02_hrdem.
        - add an assertion function to perform basic checks and codify the contract on the output (values >=0, nonulls, <15, dtype, crs, some wet cells, not all wet cells)
        - also add a separate one for the input (probe metadata from a few tiles in `_inputs/full_2tile/00_tiles` to establish the contract). contract should not include shape.

 

r02_hrdem
    - complete the rule per the existing template. 

    - use api_calls (and set in profile) so we only fetch 4 tiles at a time. 
    - make a best guess at a good `threads` value. 
    - create a script matching the style of `smk/scripts/example.py`
        - mostly a wrapper around `main_fetch_hrdem_for_lowres_tile`
        - call the assertion on the inputs to validate the contract. 
        - ensure everything is pointed/wired correctly for the snakemake flow
        - supress progress reporting.
        - elegant skipping/warning if there is no HRDEM coverage or 
        - call the custom assertion on the outputs to validate teh contract. 
    - complete the `r01_hrdem_all` to give us all invocations of teh `r01_hrdem` rule (contained in the filtered index_df)
    - develop a custom assertion on the output to validate the contract. 

tests
    - complete `tests/test_smk.py` for the new rule. (see commented out tests for example templates)
        - point to a single tile for the test (you may have to change tiles a few times to get a nice one). 
    - complete `tests/test_workflow.py` for the new main_ function in for the new script
    - complete `tests/test_snakefile_coms.py` for  `load_index` (should mirror some invocation patterns from teh snakefile. i.e., full coverage). 

smk/readme.md
    - add the bash command to invoke a single tile
    - add the bash command to invoke all tiles (with the _all rule)

no fallbacks. hard errors. 
be critical of existing code.. it is a mix of copy/paste and partial migration from another project (ADRs are fully migrated though and should be trusted). 
smk/config.yaml as the only place for out_dir, fathom_index_dir, fathom_tiles_dir, and scenario selectors. Keep the profile strictly for executor/cores/resources.


## implement tohr
- roughly following the poc from `dev/proof_of_concept/fathom_tohr.ipynb`, extend the workflow to implement the `tohr` function. 

r03_tohr
- start with skeleton rule I put in the snakefile. 
- add a new assertion to prove the output contract (use existing assertion to prove input)
- see notebook for contract and patterns

tests
- add a test for the new rule in `tests/test_smk.py` and `tests/test_workflow.py` (pointing to a single tile).