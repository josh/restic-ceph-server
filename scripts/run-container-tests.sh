#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

detect_container_runtime() {
  if command -v podman >/dev/null 2>&1; then
    echo "podman"
  elif command -v docker >/dev/null 2>&1; then
    echo "docker"
  elif command -v container >/dev/null 2>&1; then
    echo "container"
  else
    echo ""
  fi
}

CONTAINER_RUNTIME=$(detect_container_runtime)

if [ -z "$CONTAINER_RUNTIME" ]; then
  echo "error: no container runtime available" >&2
  exit 1
fi

rm -rf ./tmp
mkdir ./tmp

set -o xtrace
$CONTAINER_RUNTIME build --file Dockerfile-dev --tag restic-ceph-server:latest .
$CONTAINER_RUNTIME run --rm --name restic-ceph-server --volume "$PWD/tmp:/tmp/host" restic-ceph-server:latest go test "$@"
