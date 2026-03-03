.PHONY: build test check fmt clippy

build:
	cargo build

test:
	cargo test

fmt:
	cargo fmt -- --check

clippy:
	cargo clippy -- -D warnings

check: fmt clippy test
