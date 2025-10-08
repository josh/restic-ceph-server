#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

if command -v container >/dev/null 2>&1; then
	set -o xtrace
	container build --file Dockerfile-dev --tag restic-ceph-server:latest .
	container run --rm --name restic-ceph-server restic-ceph-server:latest go test
elif command -v docker >/dev/null 2>&1; then
	set -o xtrace
	docker build --file Dockerfile-dev --tag restic-ceph-server:latest .
	docker run --rm --name restic-ceph-server restic-ceph-server:latest go test
else
	echo "error: no container runtime available" >&2
	exit 1
fi
