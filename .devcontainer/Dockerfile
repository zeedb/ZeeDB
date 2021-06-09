FROM mcr.microsoft.com/vscode/devcontainers/rust:1

# [Optional] Uncomment this section to install additional OS packages.
# RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
#     && apt-get -y install --no-install-recommends <packages> 

RUN rustup toolchain install nightly-2021-05-17 && \
    rustup default nightly-2021-05-17 && \
    rustup component add llvm-tools-preview rustfmt rust-src

USER vscode

RUN cargo install grcov

COPY --from=gcr.io/zeedeebee/zetasql /usr/local/bin/zetasql_server /usr/local/bin/zetasql_server