#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

RESTIC_VERSION="${RESTIC_VERSION:-0.18.1}"

if [[ $# -ne 1 ]]; then
  printf 'Usage: %s <target-directory>\n' "$0" >&2
  exit 1
fi

target="$1"

mkdir --parents "${target}"

curl --fail --location --show-error --silent \
  "https://github.com/restic/restic/releases/download/v${RESTIC_VERSION}/restic_${RESTIC_VERSION}_linux_amd64.bz2" \
  --output "${target}/restic_${RESTIC_VERSION}_linux_amd64.bz2"

bunzip2 "${target}/restic_${RESTIC_VERSION}_linux_amd64.bz2"
chmod +x "${target}/restic_${RESTIC_VERSION}_linux_amd64"
install --mode=0755 "${target}/restic_${RESTIC_VERSION}_linux_amd64" "${target}/restic"
