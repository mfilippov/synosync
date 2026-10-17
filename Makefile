# synosync Makefile - build, test, lint and coverage targets.

GO         ?= go
BINARY     := synosync
CMD_PKG    := ./cmd/synosync
COVERAGE   := coverage.out

# Pin the golangci-lint version local `make lint` expects to the exact version CI
# runs (.github/workflows/ci.yml), so a local lint can't pass against a different
# linter build than the gate. Override on the command line if you knowingly want a
# different one: `make lint GOLANGCI_VERSION=...`.
GOLANGCI_VERSION ?= 2.14.0

# Pin govulncheck to the exact version CI runs (.github/workflows/ci.yml) so the
# security gate is reproducible: `@latest` would silently change the analyzer and
# its vuln DB schema between runs. Bump deliberately. Override on the command line
# if needed: `make vulncheck GOVULNCHECK_VERSION=...`.
GOVULNCHECK_VERSION ?= v1.4.0

DIST       := dist

# Host-local tool binaries (govulncheck) installed by the *-cross targets. A
# cross-GOOS analysis cannot go through `go run`: GOOS applies to building the
# TOOL as well, so `GOOS=windows go run …/govulncheck` would build a Windows
# binary and fail to exec it on the host. Install once for the host, then invoke
# it with GOOS set for the analysis.
TOOLS_DIR  := $(CURDIR)/.tools
EXE        :=
ifeq ($(OS),Windows_NT)
EXE        := .exe
endif

CROSS_OS   := linux darwin windows
CROSS_ARCH := amd64 arm64

.PHONY: all build crossbuild vet-cross test test-integration test-integration-build lint lint-version lint-install lint-cross vulncheck vulncheck-cross cover cover-check cover-gate-selftest license-check clean tidy fmt release release-clean

all: build

## build: compile all packages and the synosync binary.
build:
	$(GO) build ./...
	$(GO) build -o $(BINARY) $(CMD_PKG)

## crossbuild: no-cgo compile for every release GOOS/GOARCH (catches per-OS and
## per-arch build breaks that the Linux/amd64-only test run would otherwise miss).
crossbuild:
	@for goos in $(CROSS_OS); do \
		for goarch in $(CROSS_ARCH); do \
			echo "== GOOS=$$goos GOARCH=$$goarch =="; \
			CGO_ENABLED=0 GOOS=$$goos GOARCH=$$goarch $(GO) build ./... || exit 1; \
		done; \
	done

## vet-cross: go vet every package for each release GOOS. Unlike crossbuild,
## vet type-checks TEST code too, so a per-OS compile break in an untagged
## _test.go (e.g. a unix-only syscall like Mkfifo) is caught locally instead of
## only on the native windows/macos CI runner. GOARCH is
## left at the host default: the break class is per-OS symbols, and crossbuild
## already covers per-arch production code.
vet-cross:
	@for goos in $(CROSS_OS); do \
		echo "== GOOS=$$goos vet =="; \
		CGO_ENABLED=0 GOOS=$$goos $(GO) vet ./... || exit 1; \
	done

## test: run all unit tests with the race detector.
test:
	$(GO) test -race ./...

## test-integration: run the live-DSM integration harness (needs SYNOSYNC_IT_* env
## and a reachable test DSM; self-skips when the env is absent - see docs/INTEGRATION.md).
test-integration:
	$(GO) test -tags integration -race -timeout 40m ./test/integration/...

## test-integration-build: type-check and vet the integration harness under the
## integration build tag without running it (no live DSM needed) - guards the
## tagged tree against silently rotting on a signature refactor (matches the CI gate).
test-integration-build:
	$(GO) build -tags integration ./test/integration/...
	$(GO) vet -tags integration ./test/integration/...

## lint: run golangci-lint over the whole module (version-checked against CI).
lint: lint-version
	golangci-lint run ./...

## lint-version: assert the locally installed golangci-lint matches the CI pin.
## A prerequisite of both lint targets so the check lives in one place.
lint-version:
	@have=$$(golangci-lint version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1); \
	if [ "$$have" != "$(GOLANGCI_VERSION)" ]; then \
		echo "golangci-lint $(GOLANGCI_VERSION) expected (CI pin), found '$$have'."; \
		echo "Install it: make lint-install"; \
		echo "Or override: make lint GOLANGCI_VERSION=$$have"; \
		exit 1; \
	fi

## lint-install: install the pinned golangci-lint into GOPATH/bin. Used by the
## CI cross-GOOS lint step so the version pin lives here, not inline in the
## workflow (the linux arm's pin stays with the golangci-lint-action).
lint-install:
	$(GO) install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v$(GOLANGCI_VERSION)

## lint-cross: run golangci-lint once per release GOOS. A single (linux) lint run
## sees only the unix-tagged surface: every `_windows.go`/`_darwin.go` file is
## outside the analysis, so it is never linted at all - the same single-GOOS
## enforcement gap as vet-cross closes for compilation.
lint-cross: lint-version
	@for goos in $(CROSS_OS); do \
		echo "== GOOS=$$goos lint =="; \
		GOOS=$$goos golangci-lint run ./... || exit 1; \
	done

## vulncheck: run govulncheck over the module (matches the CI vulnerability scan).
vulncheck:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...

## vulncheck-cross: run govulncheck once per release GOOS (symmetry with
## vet-cross). Reachability is computed against ONE build context, so a linux-only
## scan never analyses the windows/darwin-tagged surface: go-winio (imported solely
## by listen_windows.go - the DACL'd named pipe that IS the Windows IPC security
## mechanism), go-keyring's wincred arm, service_windows.go and the x/sys/windows
## syscalls. A CVE there scored exit 0 and a v* tag shipped signed, vulnerable
## Windows archives.
vulncheck-cross:
	GOBIN=$(TOOLS_DIR) $(GO) install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)
	@for goos in $(CROSS_OS); do \
		echo "== GOOS=$$goos govulncheck =="; \
		GOOS=$$goos $(TOOLS_DIR)/govulncheck$(EXE) ./... || exit 1; \
	done

## cover: run tests with coverage and print a per-function summary.
cover:
	$(GO) test -coverprofile=$(COVERAGE) ./...
	$(GO) tool cover -func=$(COVERAGE)

## cover-check: run tests once with -race and enforce the per-package coverage
## floor for THIS build context - LINUX_THRESHOLD (80, the authoritative DESIGN
## §12 floor) on linux, CROSS_THRESHOLD (75) on a native windows/darwin run,
## which is the only context where the per-OS files enter a denominator at all.
## Fails the build on a regression (see scripts/coverage-gate.sh).
cover-check:
	bash scripts/coverage-gate.sh

## cover-gate-selftest: run the coverage-gate parser self-test on synthetic
## fixtures (no go test run) - guards scripts/coverage-gate.sh against parser regressions.
cover-gate-selftest:
	bash scripts/coverage-gate_test.sh

## license-check: assert every archive in dist/ bundles LICENSE and NOTICE
## (Apache-2.0 §4(d)) and its platform's service unit; run after a release build
## (see scripts/license-check.sh).
license-check:
	bash scripts/license-check.sh

## fmt: format the codebase.
fmt:
	$(GO) fmt ./...

## tidy: tidy the module dependencies.
tidy:
	$(GO) mod tidy

## release: cross-build the release matrix into dist/ (see scripts/release.sh).
release:
	bash scripts/release.sh

## release-clean: remove the release output directory.
release-clean:
	rm -rf $(DIST)

## clean: remove build, coverage, tool and release artifacts.
clean:
	$(GO) clean
	rm -f $(BINARY) $(COVERAGE)
	rm -rf $(DIST) $(TOOLS_DIR)
