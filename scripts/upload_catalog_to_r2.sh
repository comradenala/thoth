#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  scripts/upload_catalog_to_r2.sh --input <records.jsonl|records.jsonl.zst> --bucket <bucket> [options]

Required:
  --input PATH            Path to source JSONL (optionally .zst compressed)
  --bucket NAME           Target R2/S3 bucket

Optional:
  --prefix PREFIX         Catalog prefix in bucket (default: catalog/gbooks)
  --shard-size N          Records per shard file (default: 10000)
  --source-key KEY        Object key for uploading source file (default: <prefix>/source/<basename>)
  --skip-source-upload    Do not upload source file to object storage
  --cleanup-on-failure    Delete temp shard data if the script fails (default: keep for retry)
  --endpoint-url URL      S3-compatible endpoint (for Cloudflare R2)
  --aws-profile PROFILE   AWS CLI profile name

Notes:
  - Requires: aws, python3, and zstd (if input ends with .zst)
  - Output objects:
      <prefix>/manifest.json
      <prefix>/shards/shard-XXXXXXXXXX.ndjson
USAGE
}

INPUT=""
BUCKET=""
PREFIX="catalog/gbooks"
SHARD_SIZE=10000
SOURCE_KEY=""
SKIP_SOURCE_UPLOAD=0
CLEANUP_ON_FAILURE=0
ENDPOINT_URL=""
AWS_PROFILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --input)
      INPUT="$2"
      shift 2
      ;;
    --bucket)
      BUCKET="$2"
      shift 2
      ;;
    --prefix)
      PREFIX="$2"
      shift 2
      ;;
    --shard-size)
      SHARD_SIZE="$2"
      shift 2
      ;;
    --source-key)
      SOURCE_KEY="$2"
      shift 2
      ;;
    --skip-source-upload)
      SKIP_SOURCE_UPLOAD=1
      shift
      ;;
    --cleanup-on-failure)
      CLEANUP_ON_FAILURE=1
      shift
      ;;
    --endpoint-url)
      ENDPOINT_URL="$2"
      shift 2
      ;;
    --aws-profile)
      AWS_PROFILE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$INPUT" || -z "$BUCKET" ]]; then
  usage
  exit 1
fi

if [[ ! -f "$INPUT" ]]; then
  echo "Input file not found: $INPUT" >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "Missing required command: aws" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "Missing required command: python3" >&2
  exit 1
fi
if [[ "$INPUT" == *.zst ]] && ! command -v zstd >/dev/null 2>&1; then
  echo "Missing required command for .zst input: zstd" >&2
  exit 1
fi

if [[ ! "$SHARD_SIZE" =~ ^[0-9]+$ || "$SHARD_SIZE" -le 0 ]]; then
  echo "--shard-size must be a positive integer" >&2
  exit 1
fi

PREFIX="${PREFIX%/}"
if [[ -z "$SOURCE_KEY" ]]; then
  SOURCE_KEY="${PREFIX}/source/$(basename "$INPUT")"
fi

SUCCESS=0
TMPDIR="$(mktemp -d)"
trap '
if [[ "${SUCCESS:-0}" -eq 1 ]]; then
  rm -rf "$TMPDIR"
elif [[ "${CLEANUP_ON_FAILURE:-0}" -eq 1 ]]; then
  rm -rf "$TMPDIR"
else
  echo "Upload failed; preserved temp data at: $TMPDIR" >&2
fi
' EXIT
SHARDS_DIR="$TMPDIR/shards"
MANIFEST_PATH="$TMPDIR/manifest.json"
mkdir -p "$SHARDS_DIR"

AWS_ARGS=()
if [[ -n "$ENDPOINT_URL" ]]; then
  AWS_ARGS+=(--endpoint-url "$ENDPOINT_URL")
fi
if [[ -n "$AWS_PROFILE" ]]; then
  AWS_ARGS+=(--profile "$AWS_PROFILE")
fi

echo "Running storage preflight against s3://$BUCKET/$PREFIX ..."
PREFLIGHT_FILE="$TMPDIR/preflight.txt"
PREFLIGHT_KEY="${PREFIX}/_preflight/$(date +%s)-$$.txt"
printf "ok\n" > "$PREFLIGHT_FILE"
aws "${AWS_ARGS[@]}" s3 cp "$PREFLIGHT_FILE" "s3://$BUCKET/$PREFLIGHT_KEY" --content-type text/plain >/dev/null
# Best-effort cleanup; harmless if delete permission is unavailable.
aws "${AWS_ARGS[@]}" s3 rm "s3://$BUCKET/$PREFLIGHT_KEY" >/dev/null 2>&1 || true

echo "Preparing catalog shards in $TMPDIR ..."

TOTAL_INPUT_BYTES=0
if [[ "$INPUT" == *.zst ]]; then
  # Best effort: parse decompressed size from zstd metadata so we can show ETA.
  ZSTD_LIST="$(zstd -lv -- "$INPUT" 2>/dev/null || true)"
  if [[ -n "$ZSTD_LIST" ]]; then
    TOTAL_INPUT_BYTES="$(printf "%s\n" "$ZSTD_LIST" | awk -F'[()]' '/Decompressed Size:/ {gsub(/[^0-9]/, "", $2); print $2; exit}')"
  fi
else
  TOTAL_INPUT_BYTES="$(wc -c < "$INPUT" | tr -d " ")"
fi

if [[ ! "$TOTAL_INPUT_BYTES" =~ ^[0-9]+$ ]]; then
  TOTAL_INPUT_BYTES=0
fi

if [[ "$TOTAL_INPUT_BYTES" -gt 0 ]]; then
  echo "ETA enabled (input bytes: $TOTAL_INPUT_BYTES)"
else
  echo "ETA unavailable (could not determine total input bytes)"
fi

PYTHON_SCRIPT='
import json
import sys
import time
from pathlib import Path

shard_size = int(sys.argv[1])
shards_dir = Path(sys.argv[2])
manifest_path = Path(sys.argv[3])
total_input_bytes = int(sys.argv[4])
progress_every = 250000

def pick_source_id(obj):
    if isinstance(obj, dict):
        # Common flat schemas.
        for key in ("source_id", "primary_id", "gbooks_id", "id"):
            val = obj.get(key)
            if val is not None and str(val).strip():
                return str(val).strip()
        # Common nested schemas (including Annas Archive gbooks_records).
        for nested_key in ("record", "metadata"):
            nested = obj.get(nested_key)
            if isinstance(nested, dict):
                for key in ("source_id", "primary_id", "gbooks_id", "id"):
                    val = nested.get(key)
                    if val is not None and str(val).strip():
                        return str(val).strip()
        # Last-resort fallback for AA records when only AACID is present.
        val = obj.get("aacid")
        if val is not None and str(val).strip():
            return str(val).strip()
    return None

total_records = 0
skipped = 0
started = time.time()
line_no = 0
current_shard_id = None
current_handle = None
processed_bytes = 0

def fmt_eta(seconds):
    if seconds < 0:
        seconds = 0
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"

for line_no, raw_line in enumerate(sys.stdin, start=1):
    processed_bytes += len(raw_line)
    line = raw_line.strip()
    if not line:
        continue
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        skipped += 1
        continue
    source_id = pick_source_id(obj)
    if not source_id:
        skipped += 1
        continue

    book_id = total_records + 1
    shard_id = (book_id - 1) // shard_size
    shard_path = shards_dir / f"shard-{shard_id:010d}.ndjson"
    if current_shard_id != shard_id:
        if current_handle is not None:
            current_handle.close()
        current_handle = open(shard_path, "a", encoding="utf-8")
        current_shard_id = shard_id
    current_handle.write(json.dumps({"book_id": book_id, "source_id": source_id}, ensure_ascii=True) + "\n")
    total_records += 1

    if line_no % progress_every == 0:
        elapsed = max(time.time() - started, 0.001)
        rate = line_no / elapsed
        if total_input_bytes > 0 and processed_bytes > 0:
            bytes_rate = processed_bytes / elapsed
            remaining = max(total_input_bytes - processed_bytes, 0)
            eta = remaining / max(bytes_rate, 1e-9)
            pct = min((processed_bytes / total_input_bytes) * 100.0, 100.0)
            print(
                f"[progress] lines={line_no} kept={total_records} skipped={skipped} "
                f"rate={rate:.0f} lines/s pct={pct:.2f}% eta={fmt_eta(eta)}",
                file=sys.stderr,
                flush=True,
            )
        else:
            print(
                f"[progress] lines={line_no} kept={total_records} skipped={skipped} rate={rate:.0f} lines/s",
                file=sys.stderr,
                flush=True,
            )

if line_no > 0:
    elapsed = max(time.time() - started, 0.001)
    rate = line_no / elapsed
    print(
        f"[done] lines={line_no} kept={total_records} skipped={skipped} rate={rate:.0f} lines/s elapsed={fmt_eta(elapsed)}",
        file=sys.stderr,
        flush=True,
    )

if current_handle is not None:
    current_handle.close()

if total_records == 0:
    raise SystemExit("No usable records were produced from input")

manifest = {
    "version": 1,
    "created_at_unix": int(time.time()),
    "total_records": total_records,
    "total_shards": (total_records + shard_size - 1) // shard_size,
    "shard_size": shard_size,
    "skipped_records": skipped,
}
manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
print(json.dumps(manifest))
'

if [[ "$INPUT" == *.zst ]]; then
  zstd -dc -- "$INPUT" | python3 -c "$PYTHON_SCRIPT" "$SHARD_SIZE" "$SHARDS_DIR" "$MANIFEST_PATH" "$TOTAL_INPUT_BYTES"
else
  cat "$INPUT" | python3 -c "$PYTHON_SCRIPT" "$SHARD_SIZE" "$SHARDS_DIR" "$MANIFEST_PATH" "$TOTAL_INPUT_BYTES"
fi

echo "Uploading manifest + shards to s3://$BUCKET/$PREFIX ..."
aws "${AWS_ARGS[@]}" s3 cp "$MANIFEST_PATH" "s3://$BUCKET/$PREFIX/manifest.json" --content-type application/json
aws "${AWS_ARGS[@]}" s3 cp "$SHARDS_DIR/" "s3://$BUCKET/$PREFIX/shards/" --recursive --exclude "*" --include "shard-*.ndjson" --content-type application/x-ndjson

if [[ "$SKIP_SOURCE_UPLOAD" -eq 0 ]]; then
  echo "Uploading source file to s3://$BUCKET/$SOURCE_KEY ..."
  aws "${AWS_ARGS[@]}" s3 cp "$INPUT" "s3://$BUCKET/$SOURCE_KEY"
fi

echo "Catalog upload complete."
echo "Manifest: s3://$BUCKET/$PREFIX/manifest.json"
echo "Shards:   s3://$BUCKET/$PREFIX/shards/"
SUCCESS=1
