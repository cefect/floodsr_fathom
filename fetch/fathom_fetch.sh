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

# Track aggregate outcomes across all bucket/item copy calls.
success_count=0
access_denied_count=0
failure_count=0
dataset_count="${#S3_URIS[@]}"
started_epoch="$(date +%s)"
declare -a access_denied_uris=()
declare -a failed_uris=()
declare -a bucket_item_stats=()
item_attempted_count=0
item_success_count=0
item_denied_count=0
item_failed_count=0

# Enable AWS progress bars when attached to a terminal.
aws_progress_args=()
if [[ -t 1 ]]; then
  aws_progress_args=(--progress-multiline)
fi

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
  echo "[$idx/$dataset_count] bucket: ${uri}"
  echo "  target: $dest"
  echo "  elapsed: ${elapsed}s, remaining datasets after this: ${remaining}"
  # Enumerate objects first, then copy each object so every item is attempted.
  bucket_and_prefix="${uri#s3://}"
  bucket="${bucket_and_prefix%%/*}"
  prefix="${bucket_and_prefix#*/}"
  object_total=0
  object_attempted=0
  object_success=0
  object_denied=0
  object_failed=0
  object_failed_sample=""
  progress_pct=$((100 * idx / dataset_count))

  set +e
  list_output="$(aws s3 ls --recursive "${uri}/" 2>&1)"
  list_rc=$?
  set -e

  if [[ "$list_rc" -ne 0 ]]; then
    after_bytes="$(du -sb "$dest" 2>/dev/null | awk '{print $1+0}')"
    after_files="$(count_files "$dest")"
    delta_bytes=$((after_bytes - before_bytes))
    delta_files=$((after_files - before_files))
    if grep -qi "AccessDenied" <<<"$list_output"; then
      ((access_denied_count+=1))
      access_denied_uris+=("$uri (list failed)")
      echo "  status: access-denied (failed to list prefix)"
    else
      ((failure_count+=1))
      failed_uris+=("$uri (list failed)")
      echo "  status: failed (failed to list prefix)"
    fi
    bucket_item_stats+=("${uri}|0|0|0|0")
    echo "  objects: total=0, attempted=0, copied=0, denied=0, failed=0"
    echo "  added: $(human_bytes "$delta_bytes"), files: +${delta_files}"
    echo "  totals: success=${success_count}, denied=${access_denied_count}, failed=${failure_count}, progress=${progress_pct}%"
    echo "ERROR: list failed for ${uri}" >&2
    echo "$list_output" >&2
    continue
  fi

  mapfile -t key_l < <(awk 'NF >= 4 { $1=$2=$3=""; sub(/^ +/, ""); print }' <<<"$list_output")
  object_total="${#key_l[@]}"
  for key in "${key_l[@]}"; do
    rel="${key#${prefix}/}"
    out_fp="${dest}/${rel}"
    mkdir -p "$(dirname "$out_fp")"
    ((object_attempted+=1))
    ((item_attempted_count+=1))

    cp_err_fp="$(mktemp)"
    set +e
    aws s3 cp "${aws_progress_args[@]}" "s3://${bucket}/${key}" "$out_fp" 2> >(tee "$cp_err_fp" >&2)
    cp_rc=$?
    set -e

    if [[ "$cp_rc" -eq 0 ]]; then
      rm -f "$cp_err_fp"
      ((object_success+=1))
      ((item_success_count+=1))
      continue
    fi

    cp_output="$(cat "$cp_err_fp")"
    rm -f "$cp_err_fp"
    # Remove any potentially incomplete local output after failed object copy.
    rm -f "$out_fp"
    if grep -qi "AccessDenied" <<<"$cp_output" && grep -qi "GetObject" <<<"$cp_output"; then
      ((object_denied+=1))
      ((item_denied_count+=1))
      continue
    fi

    ((object_failed+=1))
    ((item_failed_count+=1))
    if [[ -z "$object_failed_sample" ]]; then
      object_failed_sample="$cp_output"
    fi
  done

  after_bytes="$(du -sb "$dest" 2>/dev/null | awk '{print $1+0}')"
  after_files="$(count_files "$dest")"
  delta_bytes=$((after_bytes - before_bytes))
  delta_files=$((after_files - before_files))

  if ((object_denied > 0)); then
    ((access_denied_count+=1))
    access_denied_uris+=("$uri (${object_denied}/${object_attempted} denied)")
  fi
  if ((object_failed > 0)); then
    ((failure_count+=1))
    failed_uris+=("$uri (${object_failed}/${object_attempted} failed)")
  fi
  if ((object_denied == 0 && object_failed == 0)); then
    ((success_count+=1))
  fi

  bucket_item_stats+=("${uri}|${object_total}|${object_attempted}|${object_denied}|${object_failed}")
  echo "  status: per-item copy complete"
  echo "  objects: total=${object_total}, attempted=${object_attempted}, copied=${object_success}, denied=${object_denied}, failed=${object_failed}"
  echo "  added: $(human_bytes "$delta_bytes"), files: +${delta_files}"
  echo "  totals: success=${success_count}, denied=${access_denied_count}, failed=${failure_count}, progress=${progress_pct}%"
  if ((object_failed > 0)); then
    echo "ERROR: sample object failure for ${uri}" >&2
    echo "$object_failed_sample" >&2
  fi
done

# Print a concise final summary.
echo
echo "Summary:"
echo "  successful buckets: ${success_count}"
echo "  access-denied buckets: ${access_denied_count}"
echo "  failed buckets: ${failure_count}"
echo "  attempted buckets: ${dataset_count}"
echo "  item attempts: ${item_attempted_count}"
echo "  item copied: ${item_success_count}"
echo "  item access-denied: ${item_denied_count}"
echo "  item failed: ${item_failed_count}"
echo "  completion: $((100 * (success_count + access_denied_count + failure_count) / dataset_count))%"
echo "  total runtime: $(( $(date +%s) - started_epoch ))s"

echo
echo "Per-bucket item failure counts:"
for stat in "${bucket_item_stats[@]}"; do
  IFS='|' read -r stat_uri stat_total stat_attempted stat_denied stat_failed <<<"$stat"
  echo "  - ${stat_uri}"
  echo "    total=${stat_total}, attempted=${stat_attempted}, denied=${stat_denied}, failed=${stat_failed}"
done

if ((access_denied_count > 0)); then
  echo
  echo "AccessDenied/GetObject buckets:"
  for uri in "${access_denied_uris[@]}"; do
    echo "  - ${uri}"
  done
fi

if ((failure_count > 0)); then
  echo
  echo "Buckets with unexpected failures:"
  for uri in "${failed_uris[@]}"; do
    echo "  - ${uri}"
  done
  exit 1
fi

echo "Done."
