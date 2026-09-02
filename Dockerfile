# ------------------------------
# Build stage
# ------------------------------
FROM rust:bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y git python3-pip protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Install rustfmt (required by mediasoup-sys build)
RUN rustup component add rustfmt

# Copy dependency manifests
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo

# Copy source code
COPY src ./src

# Build the application
RUN cargo build --release

# ------------------------------
# Runtime stage
# ------------------------------
FROM debian:bookworm-slim

WORKDIR /app

# Install CA certificates for outbound TLS (Deepgram, Cartesia, OpenAI, etc.)
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/target/release/saasy-orchestrator ./saasy-orchestrator

# Copy and set up entrypoint script for GCP credentials handling
COPY scripts/run-with-gcp.sh ./run-with-gcp.sh
RUN chmod +x ./run-with-gcp.sh

# Expose HTTP port (health endpoints)
EXPOSE 8081

# Use entrypoint script to handle GCP_SA_JSON -> GOOGLE_APPLICATION_CREDENTIALS
ENTRYPOINT ["./run-with-gcp.sh"]
CMD ["./saasy-orchestrator"]
