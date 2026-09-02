.PHONY: run build release check test clippy clippy-strict fmt clean

# Run the orchestrator (dev profile; reads config/default.toml + env overrides from .env)
run:
	./scripts/run-with-gcp.sh cargo run

# Build debug binary
build:
	cargo build

# Build release binary
release:
	cargo build --release

# Fast compilation check (no codegen)
check:
	cargo check

# Run clippy lints
clippy:
	cargo clippy

# Run clippy lints (strict, fails on warnings)
clippy-strict:
	cargo clippy -- -D warnings

# Format code
fmt:
	cargo fmt

# Clean build artifacts
clean:
	cargo clean
