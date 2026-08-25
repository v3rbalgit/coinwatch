_default:
  @just --list

# Format all Rust code.
fmt:
  cargo fmt --all

# Fast compile + type checks across the workspace, using checked-in SQLx metadata.
check:
  SQLX_OFFLINE=true cargo check --locked --workspace --all-targets

# Clippy with warnings denied (the lint policy lives in Cargo.toml [workspace.lints]).
lint:
  SQLX_OFFLINE=true cargo clippy --locked --workspace --all-targets -- -D warnings

# Run the full test suite.
test:
  SQLX_OFFLINE=true cargo test --locked --workspace

# Prepare/check SQLx query metadata against a fresh database built from migrations.
_sqlx mode:
  #!/usr/bin/env bash
  set -euo pipefail
  work="$(mktemp -d)"
  trap 'rm -rf "$work"' EXIT
  export DATABASE_URL="sqlite://$work/prepare.db"
  cargo sqlx database create
  cargo sqlx migrate run --source crates/storage/migrations
  args=(--workspace)
  if [[ "{{mode}}" == "check" ]]; then args+=(--check); fi
  cargo sqlx prepare "${args[@]}"

# Regenerate checked-in .sqlx metadata after migrations or query changes.
sqlx-prepare: (_sqlx "prepare")

# Verify checked-in .sqlx metadata against all migrations and queries.
sqlx-check: (_sqlx "check")

# Install frontend dependencies (run once, and after dependency changes).
setup:
  cd screener && pnpm install

# Run the desktop app in dev (Vite on :1420 + the Rust app). Needs a display and
# system WebKitGTK; run `just setup` first.
run:
  cd screener && pnpm tauri dev

# Full local release gate; clippy subsumes cargo check.
quality:
  cargo fmt --all -- --check
  SQLX_OFFLINE=true cargo clippy --locked --workspace --all-targets -- -D warnings
  SQLX_OFFLINE=true cargo test --locked --workspace
  SQLX_OFFLINE=true RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps
  cargo deny check
  git diff --check
