#!/usr/bin/env bash

set -euo pipefail

# Build a compact Fathom test dataset by center-cropping the 1in1000 source tiles.
# Reminder: the current local source tree is missing PLUVIAL-UNDEFENDED, so the full 3x2 grid is not available here.
OUTPUT_TILES_DIR="${1:-_inputs/300x300_2tile/00_tiles}"
SOURCE_ROOT="${2:-_inputs/full_2tile/00_tiles}"
SRC_RETURN_PERIOD="${SRC_RETURN_PERIOD:-1in1000}"
CROP_SIZE="${CROP_SIZE:-300}"
CROP_OVERRIDE_FP="${CROP_OVERRIDE_FP:-$(dirname "$0")/test_crop_override.json}"

HAZARD_TYPES=("COASTAL" "FLUVIAL" "PLUVIAL")
PROTECTIONS=("DEFENDED" "UNDEFENDED")

OUTPUT_ROOT="$(dirname "${OUTPUT_TILES_DIR}")"

for cmd in gdalinfo gdal_translate; do
    command -v "${cmd}" >/dev/null 2>&1 || {
        echo "missing required command: ${cmd}" >&2
        exit 1
    }
done

[[ "${CROP_SIZE}" =~ ^[0-9]+$ ]] || {
    echo "CROP_SIZE must be an integer, got '${CROP_SIZE}'" >&2
    exit 1
}

mkdir -p "${OUTPUT_TILES_DIR}"

combo_total=$(( ${#HAZARD_TYPES[@]} * ${#PROTECTIONS[@]} ))
combo_i=0
tile_total=0
tile_written_total=0
missing_combo_total=0
summary_fp="${OUTPUT_ROOT}/build_test_tile_dataset_summary.tsv"

printf "combo\ttiles_in\ttiles_out\ttiles_size\n" > "${summary_fp}"

echo "output_tiles_dir: ${OUTPUT_TILES_DIR}"
echo "source_root: ${SOURCE_ROOT}"
echo "src_return_period: ${SRC_RETURN_PERIOD}"
echo "crop_size: ${CROP_SIZE} x ${CROP_SIZE}"
echo "crop_override_fp: ${CROP_OVERRIDE_FP}"
echo "note: local source data is missing PLUVIAL-UNDEFENDED, so this run may produce 5 tile folders instead of 6"

for hazard_type in "${HAZARD_TYPES[@]}"; do
    for protection in "${PROTECTIONS[@]}"; do
        combo_i=$((combo_i + 1))
        src_name="FLOOD_MAP-1ARCSEC-NW_OFFSET-${SRC_RETURN_PERIOD}-${hazard_type}-${protection}-DEPTH-2020-PERCENTILE50-v3.1"
        src_dir="${SOURCE_ROOT}/${src_name}"
        dst_dir="${OUTPUT_TILES_DIR}/${src_name}"

        echo
        echo "[${combo_i}/${combo_total}] ${hazard_type} ${protection}"
        echo "  source: ${src_dir}"

        if [[ ! -d "${src_dir}" ]]; then
            echo "  warning: missing source directory, skipping"
            missing_combo_total=$((missing_combo_total + 1))
            continue
        fi

        mapfile -t tif_files < <(find "${src_dir}" -maxdepth 1 -type f -name "*.tif" | sort)
        tif_count="${#tif_files[@]}"
        tile_total=$((tile_total + tif_count))

        if [[ "${tif_count}" -eq 0 ]]; then
            echo "  warning: no tif files found, skipping"
            missing_combo_total=$((missing_combo_total + 1))
            continue
        fi

        read -r raster_w raster_h < <(
            gdalinfo -json "${tif_files[0]}" | python -c 'import json, sys; data = json.load(sys.stdin); print(data["size"][0], data["size"][1])'
        )

        if (( raster_w < CROP_SIZE || raster_h < CROP_SIZE )); then
            echo "  crop size ${CROP_SIZE} exceeds raster size ${raster_w}x${raster_h}" >&2
            exit 1
        fi

        echo "  tiles: ${tif_count}"

        rm -rf "${dst_dir}"
        mkdir -p "${dst_dir}"

        tile_i=0
        for src_fp in "${tif_files[@]}"; do
            tile_i=$((tile_i + 1))
            dst_fp="${dst_dir}/$(basename "${src_fp}")"
            rel_key="${src_name}/$(basename "${src_fp}")"
            override_key="${hazard_type}/$(basename "${src_fp}")"
            xoff=$(( (raster_w - CROP_SIZE) / 2 ))
            yoff=$(( (raster_h - CROP_SIZE) / 2 ))
            crop_source="center"

            if [[ -f "${CROP_OVERRIDE_FP}" ]]; then
                override_vals="$(python - "${CROP_OVERRIDE_FP}" "${override_key}" <<'PY'
import json, sys
from pathlib import Path

override_fp = Path(sys.argv[1])
tile_key = sys.argv[2]
override_d = json.loads(override_fp.read_text())
row = override_d.get(tile_key, {})
if row:
    print(f"{int(row['col'])} {int(row['row'])}")
PY
)"
                if [[ -n "${override_vals}" ]]; then
                    read -r xoff yoff <<< "${override_vals}"
                    crop_source="override"
                fi
            fi

            if (( xoff < 0 || yoff < 0 || xoff + CROP_SIZE > raster_w || yoff + CROP_SIZE > raster_h )); then
                echo "  crop window out of bounds for ${rel_key}: xoff=${xoff}, yoff=${yoff}, width=${CROP_SIZE}, height=${CROP_SIZE}, raster=${raster_w}x${raster_h}" >&2
                exit 1
            fi

            echo "    srcwin (${crop_source}): ${rel_key} [override_key=${override_key}] xoff=${xoff}, yoff=${yoff}, width=${CROP_SIZE}, height=${CROP_SIZE}"

            # Crop the center window while keeping projection, resolution, and georeferencing intact.
            gdal_translate \
                -q \
                -of GTiff \
                -srcwin "${xoff}" "${yoff}" "${CROP_SIZE}" "${CROP_SIZE}" \
                -co TILED=YES \
                -co BLOCKXSIZE=256 \
                -co BLOCKYSIZE=256 \
                -co COMPRESS=DEFLATE \
                -co PREDICTOR=2 \
                "${src_fp}" \
                "${dst_fp}"

            if (( tile_i % 200 == 0 || tile_i == tif_count )); then
                echo "    wrote ${tile_i}/${tif_count}"
            fi
        done

        tile_written_total=$((tile_written_total + tif_count))
        tiles_size="$(du -sh "${dst_dir}" | awk '{print $1}')"
        dst_count="$(find "${dst_dir}" -maxdepth 1 -type f -name "*.tif" | wc -l | tr -d ' ')"
        [[ "${dst_count}" == "${tif_count}" ]] || {
            echo "  output count ${dst_count} does not match input count ${tif_count}" >&2
            exit 1
        }
        printf "%s\t%s\t%s\t%s\n" "${src_name}" "${tif_count}" "${dst_count}" "${tiles_size}" >> "${summary_fp}"

        echo "  output tiles: ${dst_dir}"
        echo "  sizes: tiles=${tiles_size}"
    done
done

echo
echo "finished"
echo "  source combos scanned: ${combo_total}"
echo "  missing combos: ${missing_combo_total}"
echo "  source tiles scanned: ${tile_total}"
echo "  cropped tiles written: ${tile_written_total}"
echo "  tile output dir: ${OUTPUT_TILES_DIR}"
echo "  summary: ${summary_fp}"
echo "  total output size: $(du -sh "${OUTPUT_ROOT}" | awk '{print $1}')"
