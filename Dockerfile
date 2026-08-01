# syntax=docker/dockerfile:1

# PlayOnAir multi-stage image (glibc). Used by Home Assistant OS via GHCR.
# Build args: BUILD_VERSION, BUILD_ARCH (HA: amd64 | aarch64)

ARG BUILD_VERSION=0.1.3
ARG BUILD_ARCH=amd64

# -----------------------------------------------------------------------------
# Builder
# -----------------------------------------------------------------------------
FROM rust:1.88-bookworm AS builder

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    clang \
    cmake \
    libclang-dev \
    pkg-config \
    perl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY Cargo.toml Cargo.lock rustfmt.toml clippy.toml deny.toml ./
COPY crates/play-on-air crates/play-on-air

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release -p play-on-air \
    && cp /src/target/release/play-on-air /tmp/play-on-air

# -----------------------------------------------------------------------------
# Runtime
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim

ARG BUILD_VERSION
ARG BUILD_ARCH

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/play-on-air /usr/local/bin/play-on-air

# Home Assistant Supervisor labels + OCI metadata.
LABEL org.opencontainers.image.title="PlayOnAir" \
  org.opencontainers.image.description="Chromecast devices as AirPlay 2 speakers on the local network" \
  org.opencontainers.image.source="https://github.com/felipefdl/play-on-air" \
  org.opencontainers.image.url="https://github.com/felipefdl/play-on-air" \
  org.opencontainers.image.licenses="MIT" \
  org.opencontainers.image.version="${BUILD_VERSION}" \
  io.hass.name="PlayOnAir" \
  io.hass.description="Chromecast devices as AirPlay 2 speakers on the local network" \
  io.hass.type="app" \
  io.hass.version="${BUILD_VERSION}" \
  io.hass.arch="${BUILD_ARCH}"

ENV PLAY_ON_AIR_CONFIG=/config/play-on-air.toml

# Host network is required at runtime (set by HAOS config.yaml host_network).
CMD ["play-on-air"]
