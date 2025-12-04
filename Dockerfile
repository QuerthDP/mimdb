# MIMDB - Columnar Analytical Database
# Multi-stage Docker build

# Build stage
FROM rust:1.90-bookworm AS builder

WORKDIR /app

# Copy only mimdb crate and api spec
COPY mimdb/ ./mimdb/
COPY api/ ./api/

# Build release binary
RUN cargo build --release --bin server --manifest-path mimdb/Cargo.toml

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Copy the built binary
COPY --from=builder /app/mimdb/target/release/server /app/server

# Create data directories
RUN mkdir -p /app/mimdb_data /data

# Set environment variables
ENV RUST_LOG=info

# Expose the default port
EXPOSE 3000

# Volume for external data (CSV files for COPY operations)
VOLUME ["/data"]

# Volume for database storage
VOLUME ["/app/mimdb_data"]

# Run the server
CMD ["/app/server", "--port", "3000", "--data-dir", "/app/mimdb_data"]
