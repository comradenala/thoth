# thoth

`thoth` is a peer-to-peer Rust crawler/archiver for large book datasets.
Multiple peers coordinate work through S3-compatible object storage (AWS S3, Cloudflare R2, etc.) without a central coordinator.

## What It Does

- Splits a large corpus into deterministic shards.
- Lets each peer claim unowned shards using object-storage claim files.
- Crawls book pages, discovers downloadable formats, and uploads files to object storage.
- Writes local checkpoints for restart safety.
- Writes remote manifests/heartbeats so peers can see cluster progress.
- Packages checkpointed metadata into compressed, hashed distribution artifacts.

## How It Coordinates Peers

- Shard claims are stored under `peers/claims/`.
- Peer heartbeats are stored under `peers/directory/`.
- Invite codes are stored under `peers/invites/`.
- Per-shard completion manifests are stored under `manifests/shards/`.

As long as peers share the same `[storage]` configuration and credentials, they can cooperate.

## Prerequisites

- A Unix-like shell (macOS/Linux recommended).
- Access to an S3-compatible bucket.
- Network access to the target corpus URLs.

## Install Rust

Install Rust with `rustup` (recommended by the Rust project):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Reload your shell and verify:

```bash
rustc --version
cargo --version
```

On Windows, use PowerShell:

```powershell
winget install Rustlang.Rustup
```

Then open a new terminal and verify:

```powershell
rustc --version
cargo --version
```

## Install thoth

From this repository root:

```bash
cargo build
```

For optimized binaries:

```bash
cargo build --release
```

Binary path:

- Debug: `target/debug/thoth`
- Release: `target/release/thoth`

## Configure

1. Copy the example config:

```bash
cp config.toml.example config.toml
```

2. Edit `config.toml`:

- `[peer].identity_path`: local file storing this machine's peer ID.
- `[storage].bucket`: shared bucket name.
- `[storage].endpoint_url`: set for S3-compatible backends (required for R2/MinIO).
- `[crawler].book_url_template`: must contain `{book}` placeholder.
- `[crawler].checkpoint_dir`: local directory for shard checkpoint files.
- `[catalog].enabled`: set to `true` to crawl IDs from catalog shards in object storage.
- `[catalog].manifest_key`: object key for catalog manifest JSON.
- `[catalog].shard_prefix`: object prefix containing `shard-XXXXXXXXXX.ndjson` files.

3. Set storage credentials in your environment.

Typical variables:

```bash
export AWS_ACCESS_KEY_ID="..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_REGION="us-east-1"
# Optional:
# export AWS_SESSION_TOKEN="..."
```

## Quick Start (Single Peer)

Initialize local identity + checkpoint directories:

```bash
cargo run -- --config config.toml init
```

Check cluster status:

```bash
cargo run -- --config config.toml status
```

Start crawling:

```bash
RUST_LOG=info cargo run -- --config config.toml crawl
```

## Catalog Mode (R2-Hosted ID Frontier)

Use this when crawling from a prebuilt ID catalog (for example AA `gbooks_records`) instead of numeric `total_books` ranges.

1. Build/upload catalog shards to your bucket:

```bash
scripts/upload_catalog_to_r2.sh \
  --input /path/to/annas_archive_meta__aacid__gbooks_records__*.jsonl.seekable.zst \
  --bucket open-books-archive \
  --prefix catalog/gbooks
```

2. Enable `[catalog]` in `config.toml`:

```toml
[catalog]
enabled = true
manifest_key = "catalog/gbooks/manifest.json"
shard_prefix = "catalog/gbooks/shards"
```

In catalog mode, peers claim by catalog shard ID and crawl IDs from shard files.

## Invite and Join (Multi-Peer)

### Coordinator: create invite

On a machine already configured for the shared storage:

```bash
cargo run -- --config config.toml invite --expiry-hours 24
```

This writes an invite record to object storage and prints a shortcode like `ABCD-1234`.

### New peer: join cluster

1. Create a local config with the same `[storage]` and local `[peer]` path.
2. Redeem invite:

```bash
cargo run -- --config config.toml join ABCD-1234 --output joined.toml
```

3. If the command prints credential exports, run them in your shell.
4. Start the worker:

```bash
cargo run -- --config joined.toml crawl
```

Notes:

- `join` fails if the invite is expired or already redeemed.
- Use `--force` to overwrite an existing output config file.
- If no scoped credentials are minted, provide credentials manually in the peer environment.

## Packaging Output Dataset

After crawling has produced checkpoint files:

```bash
cargo run -- --config config.toml package --output dist --chunk-mib 512
```

Generated artifacts:

- `dist/corpus.jsonl`
- `dist/chunks/chunk-*.jsonl.zst`
- `dist/seek_index.json`
- `dist/manifest.json`

Optional via `[package]` config:

- `generate_torrent = true` creates `dist/corpus.torrent`.
- `pin_ipfs = true` uploads the output directory to IPFS and prints CID.

## CLI Commands

```text
thoth init
thoth crawl
thoth status
thoth package [--output dist] [--chunk-mib 512]
thoth invite [--expiry-hours 24]
thoth join <SHORTCODE> [--output joined.toml] [--force]
```

Global option:

- `-c, --config <PATH>` (default `config.toml`)

## Data Layout

Object storage keys used by `thoth`:

- `books/{shard_id}/{book_id}.{format}`
- `peers/claims/shard-XXXXXXXXXX.json`
- `peers/directory/{peer_id}.json`
- `peers/invites/{invite_code}.json`
- `manifests/shards/shard-XXXXXXXXXX.json`
- `catalog/gbooks/manifest.json` (when catalog mode is enabled)
- `catalog/gbooks/shards/shard-XXXXXXXXXX.ndjson` (when catalog mode is enabled)

Local checkpoint files:

- `{checkpoint_dir}/shard-XXXXXXXXXX.ndjson`

## Development

Run checks/tests:

```bash
cargo check
cargo test
```

## Troubleshooting

- `invite code not found`: joining peer is pointed at the wrong bucket/endpoint, or code was mistyped.
- `invite ... is expired`: create a new invite with a longer `--expiry-hours`.
- `No unclaimed shards`: all shards are currently claimed or completed.
- S3 auth/region errors: verify credential env vars and `AWS_REGION`.
- Empty package output: ensure checkpoint files exist in `[crawler].checkpoint_dir`.
