# syntax=docker/dockerfile:1

# ---- Build stage ----
FROM rust:1.83-bookworm AS builder
WORKDIR /app

# Copy the whole workspace and build the server binary in release mode.
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release -p queueflow-server && \
    cp target/release/queueflow /usr/local/bin/queueflow

# ---- Runtime stage ----
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user.
RUN useradd --system --uid 10001 queueflow
USER queueflow

COPY --from=builder /usr/local/bin/queueflow /usr/local/bin/queueflow

EXPOSE 8000 9090

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O- http://localhost:8000/health || exit 1

ENTRYPOINT ["queueflow"]
CMD ["serve"]
