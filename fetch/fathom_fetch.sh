#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  ./fathom_fetch.sh /home/cefect/LS/10_IO/2407_FHIMP/fathom

Arguments:
  OUT_DIR   Directory to download into (default: current directory)

Environment:
  AWS_ACCESS_KEY_ID         required
  AWS_SECRET_ACCESS_KEY     required
  AWS_SESSION_TOKEN         optional (only for temporary creds)
  AWS_DEFAULT_REGION        optional (default: eu-west-2)

Examples:
  export AWS_ACCESS_KEY_ID="..."
  export AWS_SECRET_ACCESS_KEY="..."
  export AWS_DEFAULT_REGION="eu-west-2"
  ./fathom_fetch.sh ./data
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

OUT_DIR="${1:-.}"

command -v aws >/dev/null 2>&1 || { echo "ERROR: aws CLI not found on PATH"; exit 1; }

: "${AWS_ACCESS_KEY_ID:?ERROR: Need AWS_ACCESS_KEY_ID}"
: "${AWS_SECRET_ACCESS_KEY:?ERROR: Need AWS_SECRET_ACCESS_KEY}"
: "${AWS_DEFAULT_REGION:=eu-west-2}"

mkdir -p "$OUT_DIR"

S3_URIS=(
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in5-PLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in10-PLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in20-PLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in50-PLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in75-PLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in100-PLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in200-PLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in500-PLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in1000-PLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"

  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in5-PLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in10-PLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in20-PLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in50-PLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in75-PLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in100-PLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in200-PLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in500-PLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in1000-PLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"

  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in5-FLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in10-FLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in20-FLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in50-FLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in75-FLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in100-FLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in200-FLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in500-FLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in1000-FLUVIAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"

  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in5-FLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in10-FLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in20-FLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in50-FLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in75-FLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in100-FLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in200-FLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in500-FLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in1000-FLUVIAL-UNDEFENDED-DEPTH-2020-PERCENTILE50-v3.1"

  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in5-COASTAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in10-COASTAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in20-COASTAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in50-COASTAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in75-COASTAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in100-COASTAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in200-COASTAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in500-COASTAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
  "s3://fathom-products-flood/flood-map-3/FLOOD_MAP-1ARCSEC-NW_OFFSET-1in1000-COASTAL-DEFENDED-DEPTH-2020-PERCENTILE50-v3.1"
)

echo "Downloading to: $OUT_DIR"
echo "Region: $AWS_DEFAULT_REGION"
echo "Datasets: ${#S3_URIS[@]}"

# Track aggregate outcomes across all sync calls.
success_count=0
access_denied_count=0
failure_count=0
dataset_count="${#S3_URIS[@]}"
started_epoch="$(date +%s)"
declare -a access_denied_uris=()
declare -a failed_uris=()

# Human-readable byte formatter for diagnostics.
human_bytes() {
  local b="${1:-0}"
  awk -v bytes="$b" '
    BEGIN {
      split("B KB MB GB TB PB", u, " ");
      i=1;
      while (bytes >= 1024 && i < 6) { bytes/=1024; i++; }
      printf "%.2f %s", bytes, u[i];
    }'
}

# Count files under a target path for sync diagnostics.
count_files() {
  local p="$1"
  find "$p" -type f 2>/dev/null | wc -l
}

for i in "${!S3_URIS[@]}"; do
  uri="${S3_URIS[$i]}"
  idx=$((i + 1))
  remaining=$((dataset_count - idx))
  elapsed=$(( $(date +%s) - started_epoch ))
  base="${uri##*/}"
  dest="${OUT_DIR%/}/${base}"
  mkdir -p "$dest"
  before_bytes="$(du -sb "$dest" 2>/dev/null | awk '{print $1+0}')"
  before_files="$(count_files "$dest")"

  echo
  echo "[$idx/$dataset_count] sync: ${uri}"
  echo "  target: $dest"
  echo "  elapsed: ${elapsed}s, remaining datasets after this: ${remaining}"
  # Run per-dataset sync without stopping the whole script on a single failure.
  set +e
  sync_output="$(aws s3 sync --only-show-errors "$uri" "$dest" 2>&1)"
  sync_rc=$?
  set -e
  after_bytes="$(du -sb "$dest" 2>/dev/null | awk '{print $1+0}')"
  after_files="$(count_files "$dest")"
  delta_bytes=$((after_bytes - before_bytes))
  delta_files=$((after_files - before_files))
  progress_pct=$((100 * idx / dataset_count))

  if [[ "$sync_rc" -eq 0 ]]; then
    ((success_count+=1))
    echo "  status: success"
    echo "  added: $(human_bytes "$delta_bytes"), files: +${delta_files}"
    echo "  totals: success=${success_count}, denied=${access_denied_count}, failed=${failure_count}, progress=${progress_pct}%"
    continue
  fi

  # Silently tolerate partial-permission object fetch failures.
  if grep -qi "AccessDenied" <<<"$sync_output" && grep -qi "GetObject" <<<"$sync_output"; then
    ((access_denied_count+=1))
    access_denied_uris+=("$uri")
    echo "  status: access-denied skipped"
    echo "  added before denial: $(human_bytes "$delta_bytes"), files: +${delta_files}"
    echo "  totals: success=${success_count}, denied=${access_denied_count}, failed=${failure_count}, progress=${progress_pct}%"
    continue
  fi

  # Surface unexpected failures after capture so the loop can continue.
  ((failure_count+=1))
  failed_uris+=("$uri")
  echo "  status: failed"
  echo "  added before failure: $(human_bytes "$delta_bytes"), files: +${delta_files}"
  echo "  totals: success=${success_count}, denied=${access_denied_count}, failed=${failure_count}, progress=${progress_pct}%"
  echo "ERROR: sync failed for ${uri}" >&2
  echo "$sync_output" >&2
done

# Print a concise final summary.
echo
echo "Summary:"
echo "  successful syncs: ${success_count}"
echo "  access-denied skipped: ${access_denied_count}"
echo "  failed syncs: ${failure_count}"
echo "  attempted syncs: ${dataset_count}"
echo "  completion: $((100 * (success_count + access_denied_count + failure_count) / dataset_count))%"
echo "  total runtime: $(( $(date +%s) - started_epoch ))s"

if ((access_denied_count > 0)); then
  echo
  echo "AccessDenied/GetObject prefixes (suppressed during run):"
  for uri in "${access_denied_uris[@]}"; do
    echo "  - ${uri}"
  done
fi

if ((failure_count > 0)); then
  echo
  echo "Unexpected failures:"
  for uri in "${failed_uris[@]}"; do
    echo "  - ${uri}"
  done
  exit 1
fi

echo "Done."
