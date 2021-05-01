FROM rustlang/rust:nightly

# Install cmake, required to build grpcio-sys.
RUN \
    set -eux; \
    wget -qO- "https://cmake.org/files/v3.20/cmake-3.20.0-linux-x86_64.tar.gz" | tar --strip-components=1 -xz -C /usr/local; \
    cmake --version;

# Install sccache, implements remote caching.
RUN \
    VERSION="v0.2.16-fork-5"; \
    wget -qO- "https://github.com/zeedb/sccache/releases/download/$VERSION/sccache-$VERSION-x86_64-unknown-linux-musl.tar.gz" | tar -xz -C .; \
    chmod +x sccache-$VERSION-x86_64-unknown-linux-musl/sccache; \
    mv sccache-$VERSION-x86_64-unknown-linux-musl/sccache /usr/bin/sccache; \
    rm -rf sccache-$VERSION-x86_64-unknown-linux-musl; \
    sccache --version;

# Install prefetch
RUN cargo install cargo-prefetch

# TODO this list should be automatically generated from Cargo.lock at runtime.
# Prefetch deps
RUN cargo prefetch \
    addr2line@0.14.1 \
    adler@0.2.3 \
    ahash@0.6.3 \
    aho-corasick@0.7.15 \
    anyhow@1.0.38 \
    arrayvec@0.4.12 \
    atty@0.2.14 \
    autocfg@1.0.1 \
    backtrace@0.3.56 \
    bincode@1.3.1 \
    bindgen@0.57.0 \
    bitflags@1.2.1 \
    boringssl-src@0.2.0 \
    bytemuck@1.5.1 \
    byteorder@1.4.2 \
    bytes@1.0.1 \
    cc@1.0.67 \
    cexpr@0.4.0 \
    cfg-if@1.0.0 \
    chrono@0.4.19 \
    clang-sys@1.1.1 \
    cmake@0.1.45 \
    const_fn@0.4.5 \
    cpp_demangle@0.3.2 \
    crossbeam-channel@0.5.0 \
    crossbeam-deque@0.8.0 \
    crossbeam-epoch@0.9.1 \
    crossbeam-utils@0.8.1 \
    debugid@0.7.2 \
    derive-new@0.5.9 \
    either@1.6.1 \
    fixedbitset@0.2.0 \
    futures-channel@0.3.13 \
    futures-core@0.3.13 \
    futures-executor@0.3.13 \
    futures-io@0.3.13 \
    futures-macro@0.3.13 \
    futures-sink@0.3.13 \
    futures-task@0.3.13 \
    futures-util@0.3.13 \
    futures@0.3.13 \
    getrandom@0.2.2 \
    gimli@0.23.0 \
    glob@0.3.0 \
    grpcio-compiler@0.8.0 \
    grpcio-sys@0.8.1 \
    grpcio@0.8.2 \
    hashbrown@0.9.1 \
    heck@0.3.2 \
    hermit-abi@0.1.18 \
    hyperloglogplus@0.2.2 \
    indexmap@1.6.1 \
    inferno@0.10.3 \
    instant@0.1.9 \
    itertools@0.9.0 \
    itoa@0.4.7 \
    lazy_static@1.4.0 \
    lazycell@1.3.0 \
    libc@0.2.82 \
    libloading@0.7.0 \
    libm@0.1.4 \
    libz-sys@1.1.2 \
    lock_api@0.4.2 \
    log@0.4.13 \
    memchr@2.3.4 \
    memmap@0.7.0 \
    memoffset@0.6.1 \
    miniz_oxide@0.4.3 \
    multimap@0.8.2 \
    nix@0.20.0 \
    nodrop@0.1.14 \
    nom@5.1.2 \
    num_cpus@1.13.0 \
    num-format@0.4.0 \
    num-integer@0.1.44 \
    num-traits@0.2.14 \
    object@0.23.0 \
    once_cell@1.5.2 \
    packed_simd_2@0.3.4 \
    parking_lot_core@0.8.3 \
    parking_lot@0.11.1 \
    peeking_take_while@0.1.2 \
    petgraph@0.5.1 \
    pin-project-lite@0.2.4 \
    pin-utils@0.1.0 \
    pkg-config@0.3.19 \
    pprof@0.4.3 \
    ppv-lite86@0.2.10 \
    proc-macro-hack@0.5.19 \
    proc-macro-nested@0.1.7 \
    proc-macro2@1.0.24 \
    prost-build@0.7.0 \
    prost-derive@0.7.0 \
    prost-types@0.7.0 \
    prost@0.7.0 \
    protobuf@2.22.1 \
    quick-xml@0.20.0 \
    quote@1.0.8 \
    rand_chacha@0.3.0 \
    rand_core@0.6.1 \
    rand_hc@0.3.0 \
    rand@0.8.2 \
    rayon-core@1.9.0 \
    rayon@1.5.0 \
    redox_syscall@0.2.4 \
    regex-syntax@0.6.22 \
    regex@1.4.5 \
    remove_dir_all@0.5.3 \
    rgb@0.8.27 \
    rustc-demangle@0.1.18 \
    rustc-hash@1.1.0 \
    same-file@1.0.6 \
    scopeguard@1.1.0 \
    serde_derive@1.0.123 \
    serde@1.0.123 \
    shlex@0.1.1 \
    slab@0.4.2 \
    smallvec@1.6.1 \
    stable_deref_trait@1.2.0 \
    static_assertions@1.1.0 \
    str_stack@0.1.0 \
    symbolic-common@8.0.5 \
    symbolic-demangle@8.0.5 \
    syn@1.0.60 \
    tempfile@3.2.0 \
    thiserror-impl@1.0.23 \
    thiserror@1.0.23 \
    time@0.1.43 \
    twox-hash@1.6.0 \
    unicode-segmentation@1.7.1 \
    unicode-xid@0.2.1 \
    uuid@0.8.2 \
    vcpkg@0.2.11 \
    version_check@0.9.2 \
    walkdir@2.3.2 \
    wasi@0.10.1+wasi-snapshot-preview1 \
    which@4.0.2 \
    winapi-i686-pc-windows-gnu@0.4.0 \
    winapi-util@0.1.5 \
    winapi-x86_64-pc-windows-gnu@0.4.0 \
    winapi@0.3.9