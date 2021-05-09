FROM debian:buster-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates wget gcc g++ make protobuf-compiler-grpc && \
    rm -rf /var/lib/apt/lists/*

# Install rust.
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH
RUN wget "https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init" && \
    chmod +x rustup-init && \
    ./rustup-init \
        -y \
        --no-modify-path \
        --default-toolchain nightly && \
    rm rustup-init

# Install cmake.
RUN wget -qO- "https://cmake.org/files/v3.20/cmake-3.20.0-linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local

# Install sccache.
RUN VERSION="v0.2.16-fork-5" && \
    wget -qO- "https://github.com/zeedb/sccache/releases/download/$VERSION/sccache-$VERSION-x86_64-unknown-linux-musl.tar.gz" | tar --strip-components=1 -xz -C /usr/local/bin && \
    chmod +x /usr/local/bin/sccache

# Fetch crates.io index.
RUN cargo search