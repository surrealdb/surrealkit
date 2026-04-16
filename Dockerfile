# syntax=docker/dockerfile:1.7

FROM rust:1.94-slim-bookworm AS builder

WORKDIR /src

# Pre-fetch dependencies to maximize layer caching.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build \
        --manifest-path crates/surrealkit/Cargo.toml \
        --locked \
        --release \
    && cp target/release/surrealkit /surrealkit

FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=builder /surrealkit /usr/local/bin/surrealkit

ENTRYPOINT ["/usr/local/bin/surrealkit"]
