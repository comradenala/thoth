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

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT
SHARDS_DIR="$TMPDIR/shards"
MANIFEST_PATH="$TMPDIR/manifest.json"
mkdir -p "$SHARDS_DIR"

echo "Preparing catalog shards in $TMPDIR ..."

PYTHON_SCRIPT='
import json
import sys
import time
from pathlib import Path

shard_size = int(sys.argv[1])
shards_dir = Path(sys.argv[2])
manifest_path = Path(sys.argv[3])

def pick_source_id(obj):
    if isinstance(obj, dict):
        for key in ("source_id", "primary_id", "gbooks_id"):
            val = obj.get(key)
            if val is not None and str(val).strip():
                return str(val).strip()
        nested = obj.get("record")
        if isinstance(nested, dict):
            for key in ("source_id", "primary_id", "gbooks_id"):
                val = nested.get(key)
                if val is not None and str(val).strip():
                    return str(val).strip()
    return None

total_records = 0
skipped = 0
handles = {}

for line_no, raw_line in enumerate(sys.stdin, start=1):
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
    handle = handles.get(shard_id)
    if handle is None:
        handle = open(shard_path, "a", encoding="utf-8")
        handles[shard_id] = handle
    handle.write(json.dumps({"book_id": book_id, "source_id": source_id}, ensure_ascii=True) + "\n")
    total_records += 1

for h in handles.values():
    h.close()

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
  zstd -dc -- "$INPUT" | python3 -c "$PYTHON_SCRIPT" "$SHARD_SIZE" "$SHARDS_DIR" "$MANIFEST_PATH"
else
  cat "$INPUT" | python3 -c "$PYTHON_SCRIPT" "$SHARD_SIZE" "$SHARDS_DIR" "$MANIFEST_PATH"
fi

AWS_ARGS=()
if [[ -n "$ENDPOINT_URL" ]]; then
  AWS_ARGS+=(--endpoint-url "$ENDPOINT_URL")
fi
if [[ -n "$AWS_PROFILE" ]]; then
  AWS_ARGS+=(--profile "$AWS_PROFILE")
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
