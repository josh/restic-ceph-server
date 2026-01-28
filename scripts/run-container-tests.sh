#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

CEPH_RELEASE="${CEPH_RELEASE:-tentacle}"

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

# Optional: Create go cache volume with correct permissions
# $ docker volume create restic-rados-server-go-cache
# $ docker run --rm -v restic-rados-server-go-cache:/go --user root alpine chown -R 1000:1000 /go
GO_CACHE_ARGS=()
if $CONTAINER_RUNTIME volume inspect restic-rados-server-go-cache >/dev/null 2>&1; then
	GO_CACHE_ARGS=(--volume "restic-rados-server-go-cache:/go:z" --env GOCACHE=/go/cache --env GOMODCACHE=/go/pkg/mod)
fi

set -o xtrace
$CONTAINER_RUNTIME build --file .devcontainer/Dockerfile --build-arg "CEPH_RELEASE=${CEPH_RELEASE}" --tag "restic-rados-server:${CEPH_RELEASE}" .
$CONTAINER_RUNTIME run --rm --name restic-rados-server --volume "$PWD:/workspace:z" "${GO_CACHE_ARGS[@]}" "restic-rados-server:${CEPH_RELEASE}" go test "$@"
