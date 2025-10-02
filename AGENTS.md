# Agents Guide

This project is written in Go and uses Go modules for dependency management. The repository assumes Go 1.24 or newer.

## Setup

1. Install Go 1.24 or later.
2. Install Restic by running the helper script (requires sudo if installing to a system path). The script installs the required Restic version, which may be newer than the one provided by apt:

```sh
./scripts/install-restic.sh /usr/local/bin
```

3. Download dependencies with:

```sh
go mod download
```

## Testing

Before running the tests, ensure system dependencies for Ceph and Restic are
installed. On Debian or Ubuntu systems you can install them with:

```sh
sudo apt-get update
sudo apt-get install ceph librados-dev
```

> **Note:** Do not install Restic from apt; the repository version may be too old. Always use `scripts/install-restic.sh` to ensure the correct release is available.

Run all tests with:

```sh
go test ./...
```

To generate a coverage report:

```sh
go test -cover ./...
```

The test suite uses [testscript](https://pkg.go.dev/github.com/rogpeppe/go-internal/testscript). Test files live in the `testdata` directory and contain scripts that execute commands and compare their output. These tests differ from standard Go tests because they drive the program via shell-like scripts instead of calling functions directly.

Set the environment variable `UPDATE_SCRIPTS=true` when running tests to enable automatic updates of testscript output files.

## Formatting

Format code with:

```sh
go fmt ./...
```

## Code Quality

Run vet and static analysis tools before committing:

```sh
go vet ./...
```

Optionally run `golangci-lint` for additional checks:

```sh
golangci-lint run ./...
```

## Building

Build the project with:

```sh
go build ./...
```

## Comments

Keep comments concise. Only add them when they clarify non-obvious logic.
