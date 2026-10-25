FROM rust:1.99-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked \
    && rm -rf src target/release/aurorium target/release/deps/aurorium*
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim@sha256:7ff6ba31527b9e336162fa061746c366c4b2032f4d2cff2078bea43f78efe3f8
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --home-dir /app aurorium \
    && mkdir -p /app/data \
    && chown aurorium /app/data
WORKDIR /app
COPY --from=build /src/target/release/aurorium /app/aurorium
COPY deploy/config.docker.toml /app/config.toml
USER aurorium
VOLUME ["/app/data"]
EXPOSE 12369
ENTRYPOINT ["/app/aurorium"]
