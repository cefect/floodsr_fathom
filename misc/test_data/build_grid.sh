#!/usr/bin/env bash
#
# Build one GDAL tile index per top-level download directory.
# Usage: build_grid.sh [--target-dir <download_dir>] [--out-dir <tile_index_dir>]
#
# tested against GDAL 3.12.2 "Chicoutimi"
set -euo pipefail

target_dir="${TARGET_DIR:-_inputs/300x300_2tile/00_tiles}"
out_dir=""
dev_break_cnt=""
gdaltindex_args=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target-dir)
      target_dir="$2"
      shift 2
      ;;
    --out-dir)
      out_dir="$2"
      shift 2
      ;;
    --dev-break-cnt)
      dev_break_cnt="$2"
      shift 2
      ;;
    *)
      gdaltindex_args+=("$1")
      shift
      ;;
  esac
done

if [[ -n "$dev_break_cnt" ]] && ! [[ "$dev_break_cnt" =~ ^[0-9]+$ ]]; then
  echo "--dev-break-cnt must be an integer"
  exit 1
fi

# default output location for all generated tile indexes
out_dir="${out_dir:-$(pwd)/tile_index}"
mkdir -p "$out_dir"

# reduce GDAL debug chatter (including GDALOpen/GDALClose messages)
export CPL_DEBUG=OFF

# dont care about sidecars
export GDAL_DISABLE_READDIR_ON_OPEN=TRUE

# find each top-level scenario folder to index
mapfile -d '' dirs < <(find "$target_dir" -mindepth 1 -maxdepth 1 -type d -print0 | sort -z)
total="${#dirs[@]}"

if [[ "$total" -eq 0 ]]; then
  echo "No top-level directories found in: $target_dir"
  exit 0
fi

echo "Building tile indexes for $total directories"
echo "target_dir: $target_dir"
echo "out_dir:    $out_dir"

i=0
for d in "${dirs[@]}"; do
  i=$((i + 1))
  name="$(basename "$d")"
  ofp="${out_dir}/${name}.gpkg"

  # skip empty scenario directories with no tif payload
  if ! find "$d" -type f -name "*.tif" -print -quit | grep -q .; then
    echo "[WARNING] [$i/$total] skipping $name (no *.tif files)"
    continue
  fi

  # lightweight progress output while delegating all heavy lifting to GDAL
  echo "[$i/$total] indexing $name"
  echo "    output: $ofp"

  gdaltindex "${gdaltindex_args[@]}" -overwrite -recursive -of GPKG "$ofp" "$d"
  echo "    wrote:  $ofp"

  if [[ -n "$dev_break_cnt" ]] && [[ "$i" -ge "$dev_break_cnt" ]]; then
    echo "Dev break reached at count=$i"
    break
  fi

done

echo "Done. Built $total tile indexes."
