_default:
  @just --list

# Format all Rust code.
fmt:
  cargo fmt --all

# Fast compile + type checks across the workspace.
check:
  cargo check --locked --workspace --all-targets

# Clippy with warnings denied (the lint policy lives in Cargo.toml [workspace.lints]).
lint:
  cargo clippy --locked --workspace --all-targets -- -D warnings

# Run the full test suite.
test:
  cargo test --locked --workspace

# Ingest the Hyperliquid universe, or run offline research: `just run analyze`.
run *args:
  cargo run --locked -p cw-app -- {{args}}

# Full local release gate; clippy subsumes cargo check.
quality:
  cargo fmt --all -- --check
  cargo clippy --locked --workspace --all-targets -- -D warnings
  cargo test --locked --workspace
  RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps
  cargo deny check
  git diff --check

# Run the criterion benchmarks.
bench:
  cargo bench -p cw-harness
