# Install dependencies.
# apk add --no-cache \
#     ca-certificates \
#     gcc \
#     g++ \
#     make \
#     cmake \
#     protoc \
#     libclang
apt-get update
apt-get install -y wget gcc g++ make

# Install cmake.
wget -O- "https://cmake.org/files/v3.20/cmake-3.20.0-linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local

# Install sccache
VERSION="v0.2.16-fork-5"
wget -O- "https://github.com/zeedb/sccache/releases/download/$VERSION/sccache-$VERSION-x86_64-unknown-linux-musl.tar.gz" | tar --strip-components=1 -xz -C /usr/local/bin
chmod +x /usr/local/bin/sccache

# Install rust.
PATH=$HOME/.cargo/bin:$PATH
CARGO_BUILD_TARGET=x86_64-unknown-linux-gnu
wget "https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init"
chmod +x rustup-init
./rustup-init \
    -y \
    --no-modify-path \
    --default-toolchain nightly
rm rustup-init

# Run tests.
cargo build -p e2e_tests