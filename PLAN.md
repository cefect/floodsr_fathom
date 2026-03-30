 
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