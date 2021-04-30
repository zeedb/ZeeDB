
# This is the Docker image we use to build ZDB.
# Ran once:
#   gcloud artifacts repositories create zeedb-containers --repository-format=docker --location=us-west1 --description="ZeeDB Docker Containers"
# Run for each new version:
#   gcloud builds submit --tag us-west1-docker.pkg.dev/zeedeebee/zeedb-containers/zeedb-builder:latest
FROM rustlang/rust:nightly

# Install cmake, required to build grpcio-sys.
RUN \
    set -eux; \
    wget -qO- "https://cmake.org/files/v3.20/cmake-3.20.0-linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local; \
    cmake --version

# Install sccache, implements remote caching.
RUN \
    VERSION="v0.2.16-fork-5"; \
    wget -qO- "https://github.com/zeedb/sccache/releases/download/$VERSION/sccache-$VERSION-x86_64-unknown-linux-musl.tar.gz" | tar -xz -C .; \
    chmod +x sccache-$VERSION-x86_64-unknown-linux-musl/sccache; \
    mv sccache-$VERSION-x86_64-unknown-linux-musl/sccache /usr/bin/sccache; \
    rm -rf sccache-$VERSION-x86_64-unknown-linux-musl; \
    sccache --version