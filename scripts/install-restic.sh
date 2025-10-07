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

case "$(uname -m)" in
x86_64)
	arch="amd64"
	;;
aarch64)
	arch="arm64"
	;;
*)
	printf 'Unsupported arch: %s\n' "$(uname -m)" >&2
	exit 1
	;;
esac

filename="restic_${RESTIC_VERSION}_linux_${arch}"

mkdir --parents "$target"

curl --fail --location --show-error --silent \
	"https://github.com/restic/restic/releases/download/v$RESTIC_VERSION/$filename.bz2" \
	--output "$target/$filename.bz2"

bunzip2 "$target/$filename.bz2"
chmod +x "$target/$filename"
install --mode=0755 "$target/$filename" "$target/restic"
