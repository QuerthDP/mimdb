# MIMDB Makefile
# Build and run commands for the MIMDB database system

.PHONY: all build release test clean docker docker-run server help

# Default target
all: build

# Build debug version
build:
	cargo build

# Build release version
release:
	cargo build --release

# Build only the server binary
server:
	cargo build --release --bin server

# Run tests and API validation
test:
	cargo test
	python scripts/validate_api.py

# Clean build artifacts
clean:
	cargo clean

# Build Docker image
docker:
	docker build -t mimdb:latest --load .

# Run Docker container
docker-run:
	docker run -d \
		--name mimdb \
		-p 3000:3000 \
		-v $(PWD)/data:/data \
		-v mimdb_data:/app/mimdb_data \
		mimdb:latest

# Stop and remove Docker container
docker-stop:
	docker stop mimdb || true
	docker rm mimdb || true

# Run the server locally
run:
	cargo run --release --bin server

# Format code
fmt:
	cargo fmt  --all

# Check code
check:
	cargo check

# Run clippy
clippy:
	cargo clippy --all-targets --workspace -- -Dwarnings

# Help target
help:
	@echo "MIMDB - Columnar Analytical Database"
	@echo ""
	@echo "Available targets:"
	@echo "  build       - Build debug version"
	@echo "  release     - Build release version"
	@echo "  server      - Build only the server binary (release)"
	@echo "  test        - Run tests and API validation"
	@echo "  clean       - Clean build artifacts"
	@echo "  docker      - Build Docker image"
	@echo "  docker-run  - Run Docker container"
	@echo "  docker-stop - Stop and remove Docker container"
	@echo "  run         - Run the server locally (release)"
	@echo "  fmt         - Format code"
	@echo "  check       - Check code"
	@echo "  clippy      - Run clippy"
	@echo "  help        - Show this help message"
