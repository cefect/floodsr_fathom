#!/usr/bin/env bash

set -euo pipefail

# Build a compact Fathom test dataset by center-cropping the 1in1000 source tiles.
# Reminder: the current local source tree is missing PLUVIAL-UNDEFENDED, so the full 3x2 grid is not available here.
WORKFLOW_OUTDIR="${1:-workflow_outdir_test}"
SOURCE_ROOT="${2:-/home/cefect/LS/10_IO/2407_FHIMP/fathom}"
SRC_RETURN_PERIOD="${SRC_RETURN_PERIOD:-1in1000}"
CROP_SIZE="${CROP_SIZE:-300}"

HAZARD_TYPES=("COASTAL" "FLUVIAL" "PLUVIAL")
PROTECTIONS=("DEFENDED" "UNDEFENDED")

TILES_DIR="${WORKFLOW_OUTDIR}/00_tiles"
INDEX_DIR="${WORKFLOW_OUTDIR}/00_tile_index"

for cmd in gdalinfo gdal_translate gdaltindex; do
    command -v "${cmd}" >/dev/null 2>&1 || {
        echo "missing required command: ${cmd}" >&2
        exit 1
    }
done

[[ "${CROP_SIZE}" =~ ^[0-9]+$ ]] || {
    echo "CROP_SIZE must be an integer, got '${CROP_SIZE}'" >&2
    exit 1
}

mkdir -p "${TILES_DIR}" "${INDEX_DIR}"

combo_total=$(( ${#HAZARD_TYPES[@]} * ${#PROTECTIONS[@]} ))
combo_i=0
tile_total=0
tile_written_total=0
missing_combo_total=0
summary_fp="${WORKFLOW_OUTDIR}/build_test_tile_dataset_summary.tsv"

printf "combo\ttiles_written\ttiles_size\tindex_size\n" > "${summary_fp}"

echo "workflow_outdir: ${WORKFLOW_OUTDIR}"
echo "source_root: ${SOURCE_ROOT}"
echo "src_return_period: ${SRC_RETURN_PERIOD}"
echo "crop_size: ${CROP_SIZE} x ${CROP_SIZE}"
echo "note: local source data is missing PLUVIAL-UNDEFENDED, so this run may produce 5 indexes instead of 6"

for hazard_type in "${HAZARD_TYPES[@]}"; do
    for protection in "${PROTECTIONS[@]}"; do
        combo_i=$((combo_i + 1))
        src_name="FLOOD_MAP-1ARCSEC-NW_OFFSET-${SRC_RETURN_PERIOD}-${hazard_type}-${protection}-DEPTH-2020-PERCENTILE50-v3.1"
        src_dir="${SOURCE_ROOT}/${src_name}"
        dst_dir="${TILES_DIR}/${src_name}"
        index_fp="${INDEX_DIR}/${src_name}.gpkg"

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

        xoff=$(( (raster_w - CROP_SIZE) / 2 ))
        yoff=$(( (raster_h - CROP_SIZE) / 2 ))

        echo "  tiles: ${tif_count}"
        echo "  srcwin: xoff=${xoff}, yoff=${yoff}, width=${CROP_SIZE}, height=${CROP_SIZE}"

        rm -rf "${dst_dir}"
        mkdir -p "${dst_dir}"

        tile_i=0
        for src_fp in "${tif_files[@]}"; do
            tile_i=$((tile_i + 1))
            dst_fp="${dst_dir}/$(basename "${src_fp}")"

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

        rm -f "${index_fp}"

        # Build one spatial index per combo with absolute tile paths for downstream wiring.
        gdaltindex \
            --quiet \
            -overwrite \
            -of GPKG \
            -tileindex location \
            -write_absolute_path \
            "${index_fp}" \
            "${dst_dir}"

        tile_written_total=$((tile_written_total + tif_count))
        tiles_size="$(du -sh "${dst_dir}" | awk '{print $1}')"
        index_size="$(du -sh "${index_fp}" | awk '{print $1}')"
        printf "%s\t%s\t%s\t%s\n" "${src_name}" "${tif_count}" "${tiles_size}" "${index_size}" >> "${summary_fp}"

        echo "  output tiles: ${dst_dir}"
        echo "  output index: ${index_fp}"
        echo "  sizes: tiles=${tiles_size}, index=${index_size}"
    done
done

echo
echo "finished"
echo "  source combos scanned: ${combo_total}"
echo "  missing combos: ${missing_combo_total}"
echo "  source tiles scanned: ${tile_total}"
echo "  cropped tiles written: ${tile_written_total}"
echo "  tile output dir: ${TILES_DIR}"
echo "  index output dir: ${INDEX_DIR}"
echo "  summary: ${summary_fp}"
echo "  total output size: $(du -sh "${WORKFLOW_OUTDIR}" | awk '{print $1}')"
