FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04

RUN apt-get update && apt-get install --yes \
    build-essential \
    bzip2 \
    ceph \
    curl \
    jq \
    librados-dev \
    systemd

ARG GO_VERSION=1.25.4
RUN curl --fail --silent --show-error --location https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz | \
    tar --extract --gzip --directory /usr/local && \
    ln --symbolic /usr/local/go/bin/go /usr/local/bin/go && \
    ln --symbolic /usr/local/go/bin/gofmt /usr/local/bin/gofmt

ARG RESTIC_VERSION=0.18.1
RUN curl --fail --location --show-error --silent \
        "https://github.com/restic/restic/releases/download/v${RESTIC_VERSION}/restic_${RESTIC_VERSION}_linux_amd64.bz2" \
        --output /tmp/restic.bz2 && \
    bunzip2 /tmp/restic.bz2 && \
    install --mode=0755 /tmp/restic /usr/local/bin/restic && \
    rm /tmp/restic

ARG GOLANGCI_LINT_VERSION=2.1.6
RUN curl --silent --show-error --fail --location \
    https://github.com/golangci/golangci-lint/releases/download/v${GOLANGCI_LINT_VERSION}/golangci-lint-${GOLANGCI_LINT_VERSION}-linux-amd64.tar.gz | \
    tar --extract --gzip --directory /usr/local/bin --strip-components=1 golangci-lint-${GOLANGCI_LINT_VERSION}-linux-amd64/golangci-lint

USER vscode
WORKDIR /workspace
