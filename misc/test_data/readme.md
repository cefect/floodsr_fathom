# test data helpers

## pipeline

```bash
# rebuild the cropped 300x300 tiles from the full local source set
conda run -n deploy /workspace/misc/test_data/build_test_tile_dataset.sh /workspace/_inputs/300x300_2tile/00_tiles /workspace/_inputs/full_2tile/00_tiles

# rebuild the gpkg tile indexes from the cropped output tree
conda run -n deploy bash /workspace/misc/test_data/build_grid.sh --out-dir /workspace/_inputs/300x300_2tile/00_tile_index
```
