#!/usr/bin/env bash
#
# coverage-gate.sh - enforce the DESIGN §12 per-package coverage floor.
#
# Parses the per-package coverage summary `go test -cover` prints (the
# `ok <pkg> 0.1s coverage: NN.N% of statements` lines) and fails the build if
# any gated internal/* package dropped below THRESHOLD, or if a gated package is
# missing from the output entirely (e.g. tests did not run for it - "no tests"
# must not silently pass the gate).
#
# By default the script runs the test suite itself; pass a file argument to parse
# a pre-captured coverage summary instead (used by the bash self-test fixtures).
#
# The floor is per build context. A Linux run compiles only the unix-tagged
# surface, so before the per-GOOS floor existed the whole windows/darwin-tagged tree
# (listen_windows.go's DACL'd named pipe, pipesecret, rename_noreplace_{windows,
# darwin}, syncdir/setmeta/openflags/statdev/nametoolong/diskfull_windows,
# lock_windows) sat in NO gated denominator: a regression there reached a release
# with no red signal. The native windows/macOS jobs now run this gate over their
# OWN profile at CROSS_THRESHOLD - a floor that is deliberately its own constant
# so it can start below the linux one and be ratcheted up, never so the
# authoritative linux floor gets pulled down to meet it.
#
# Configuration (env):
#   THRESHOLD        explicit floor in percent; overrides the per-GOOS choice
#   LINUX_THRESHOLD  floor for a linux build context (default 80 - DESIGN §12)
#   CROSS_THRESHOLD  floor for a native windows/darwin run (default 75)
#   GATE_GOOS        build context to pick the floor for (default `go env GOOS`)
#   GATED_PACKAGES   space-separated internal/* package names to gate
#                    (default - the 14 packages from DESIGN §12)
#   GOTEST_FLAGS     extra flags for the test run (default `-race -timeout 25m`;
#                    see the note beside the assignment for why the timeout is
#                    part of the default rather than left to `go test`)
#
# Usage:
#   scripts/coverage-gate.sh                 # run the suite and enforce the gate
#   scripts/coverage-gate.sh coverage.txt    # parse a captured summary instead
#   THRESHOLD=85 scripts/coverage-gate.sh    # raise the floor
#
set -euo pipefail

# Resolve the repository root from this script's location so the script works
# regardless of the caller's working directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# -timeout is part of the default, not a knob a caller has to remember.
#
# `go test` defaults to 10 minutes PER PACKAGE, and internal/engine is a 24k-line
# package whose suite is measured (24.08.2026, native windows-latest runner) at
# ~200 s with -race alone and roughly three times that once -coverprofile
# instruments it - which is what this script always adds. There was no margin
# left: master crossed the default and failed with `panic: test timed out after
# 10m0s`, having actually completed the same suite in 200 s when run without
# coverage.
#
# So the ceiling is raised to a length that catches a genuine hang (a wedged
# drain, a walk that does not terminate) while leaving the instrumented run room
# on the slowest platform. It is deliberately NOT unbounded: a hung test must
# still fail rather than hold a runner for its whole allowance.
GOTEST_FLAGS="${GOTEST_FLAGS:--race -timeout 25m}"

# Resolve the build context whose floor applies. `go env GOOS` is the truth when
# a toolchain is present (it honours a GOOS override too); the uname fallback
# keeps the parser self-test runnable without one.
gate_goos() {
	if [[ -n "${GATE_GOOS:-}" ]]; then
		printf '%s' "${GATE_GOOS}"
		return
	fi
	if command -v go >/dev/null 2>&1; then
		go env GOOS
		return
	fi
	case "$(uname -s 2>/dev/null || echo unknown)" in
	Linux) printf 'linux' ;;
	Darwin) printf 'darwin' ;;
	MINGW* | MSYS* | CYGWIN* | Windows_NT) printf 'windows' ;;
	*) printf 'unknown' ;;
	esac
}

LINUX_THRESHOLD="${LINUX_THRESHOLD:-80}"
# A native run measures the per-OS files listed in the header, which no linux run
# compiles, so it needs its own floor. Evidence when it was introduced: a native
# windows/amd64 `go test -race` put every gated package at or above 84.1% (lowest:
# ipc 84.1 - the package carrying the most windows-only code; engine 86.5,
# fsutil 86.5, store 86.8), i.e. the linux floor would already hold there. It
# starts at 75 anyway because no darwin run had been observed yet and a first-PR
# red on an unmeasured platform is worse than a cushion. This is a RATCHET: raise
# it (next step 80, once a green macOS run is on record) - never lower
# LINUX_THRESHOLD to meet it.
CROSS_THRESHOLD="${CROSS_THRESHOLD:-75}"

GATE_GOOS="$(gate_goos)"
if [[ -n "${THRESHOLD:-}" ]]; then
	: # an explicit floor from the caller wins over the per-GOOS choice
elif [[ "${GATE_GOOS}" == "windows" || "${GATE_GOOS}" == "darwin" ]]; then
	THRESHOLD="${CROSS_THRESHOLD}"
else
	# linux - and any context we could not identify: fail toward the STRICT floor,
	# so a broken GOOS probe can never silently downgrade the gate.
	THRESHOLD="${LINUX_THRESHOLD}"
fi

# The gated packages. When adding a new internal/* package with its own tests,
# update both (this list and the enumeration in docs/INVARIANTS.md's
# "Coverage and cross-GOOS gates" section) so they
# stay in sync and the package can't drift in without coverage enforcement.
#
# The second sync point used to be named as "DESIGN §12", which does not
# enumerate packages at all - it refers only to "the listed internal/* packages"
# - so the checklist pointed at a file that could not go stale, and the file that
# could went unupdated. Override via env for ad-hoc runs; the default is the
# authoritative set.
DEFAULT_GATED_PACKAGES="md4 sproto secrets transport auth actions store engine fsutil watcher ipc daemon normpath netctx backoff service servertree"
# Record whether the caller overrode the package set via env (an ad-hoc run);
# the completeness guard below only applies to the authoritative default set.
if [[ -n "${GATED_PACKAGES:-}" ]]; then
	GATED_PACKAGES_OVERRIDDEN=1
fi
GATED_PACKAGES="${GATED_PACKAGES:-${DEFAULT_GATED_PACKAGES}}"

# internal/* packages intentionally NOT gated: thin wiring/serialization helpers
# without logic that warrants a coverage floor. Any internal/* package that is in
# neither GATED_PACKAGES nor this list fails the guard below, so a new package
# can't drift in without a deliberate coverage decision. Keep in sync with
# DESIGN §12 / docs/INVARIANTS.md.
UNGATED_PACKAGES="config logging"

MODULE="github.com/mfilippov/synosync"

# Obtain the coverage summary: from the file argument if given, otherwise by
# running the suite ourselves (teeing the go test output so we can both display
# and parse it).
if [[ $# -gt 0 ]]; then
	summary_file="$1"
	if [[ ! -f "${summary_file}" ]]; then
		echo "coverage-gate.sh: summary file not found: ${summary_file}" >&2
		exit 1
	fi
	output="$(cat "${summary_file}")"
else
	cd "${ROOT_DIR}"

	# Guard: every internal/* package must be either gated or explicitly
	# allowlisted as ungated, so a new package can't land without a coverage
	# decision. Only runs in the real gating mode - a captured-file run is the
	# parser self-test, driven with a synthetic GATED_PACKAGES set. Skipped when
	# GATED_PACKAGES was overridden via env (an ad-hoc run, not the full §12 set).
	if [[ -z "${GATED_PACKAGES_OVERRIDDEN:-}" ]]; then
		known=" ${GATED_PACKAGES} ${UNGATED_PACKAGES} "
		unlisted=()
		while IFS= read -r imp; do
			[[ -z "${imp}" ]] && continue
			pkg="${imp#*/internal/}"
			if [[ "${known}" != *" ${pkg} "* ]]; then
				unlisted+=("${pkg}")
			fi
		done < <(go list ./internal/...)
		if [[ ${#unlisted[@]} -gt 0 ]]; then
			echo >&2
			echo "coverage-gate: internal/* package(s) not listed in GATED_PACKAGES or UNGATED_PACKAGES:" >&2
			for pkg in "${unlisted[@]}"; do
				echo "  internal/${pkg}" >&2
			done
			echo >&2
			echo "Add each to GATED_PACKAGES (with tests + DESIGN §12 / docs/INVARIANTS.md) or to UNGATED_PACKAGES if intentionally ungated." >&2
			echo "coverage-gate: FAIL" >&2
			exit 1
		fi
	fi

	echo "Running tests with coverage (${GOTEST_FLAGS}); GOOS=${GATE_GOOS} floor ${THRESHOLD}%..."
	# Tee to a FILE, never to /dev/stderr: Git-for-Windows bash (the shell the
	# windows-latest job runs this under) has no openable /dev/stderr, so the
	# former `tee /dev/stderr` died with "No such file or directory" and took the
	# gate with it under `set -e`. That meant the Windows coverage floor, when first
	# added, NEVER ran once - the job always failed earlier, in the
	# PowerShell self-test, so the dead gate was invisible behind it. `tee FILE`
	# is POSIX and still streams the run live, which matters for a suite that
	# takes minutes on that runner.
	run_log="$(mktemp)"
	trap 'rm -f "${run_log}"' EXIT
	# `set -e` + `pipefail` still abort here when the suite fails, after the
	# output has streamed - the pre-existing behaviour, deliberately kept.
	# shellcheck disable=SC2086
	go test ${GOTEST_FLAGS} -coverprofile=coverage.out ./... 2>&1 | tee "${run_log}"
	output="$(cat "${run_log}")"
fi

below=()   # "pkg NN.N%" entries below the threshold
missing=() # gated packages with no coverage line in the output

# Disable globbing while word-splitting the package list: GATED_PACKAGES is
# deliberately unquoted to split on whitespace, but without `set -f` a value
# containing a glob char (e.g. an env override like `engine*`) would also be
# expanded against the working directory's filenames.
set -f
for pkg in ${GATED_PACKAGES}; do
	full="${MODULE}/internal/${pkg}"
	# Match the `ok <full> ... coverage: NN.N% of statements` line for this exact
	# package. The trailing space after the path avoids matching a longer path
	# that shares this one as a prefix.
	line="$(printf '%s\n' "${output}" | grep -E "^ok[[:space:]]+${full}[[:space:]].*coverage:" || true)"
	if [[ -z "${line}" ]]; then
		missing+=("${pkg}")
		continue
	fi
	pct="$(printf '%s\n' "${line}" | sed -nE 's/.*coverage:[[:space:]]*([0-9]+(\.[0-9]+)?)%.*/\1/p' | head -n1)"
	if [[ -z "${pct}" ]]; then
		missing+=("${pkg}")
		continue
	fi
	if awk -v p="${pct}" -v t="${THRESHOLD}" 'BEGIN { exit !(p < t) }'; then
		below+=("${pkg} ${pct}%")
	fi
done
set +f

fail=0

if [[ ${#below[@]} -gt 0 ]]; then
	fail=1
	echo >&2
	echo "coverage-gate: packages below the ${THRESHOLD}% threshold (GOOS=${GATE_GOOS}):" >&2
	for entry in "${below[@]}"; do
		echo "  ${entry}" >&2
	done
fi

if [[ ${#missing[@]} -gt 0 ]]; then
	fail=1
	echo >&2
	echo "coverage-gate: gated packages missing from coverage output (no tests ran?):" >&2
	for pkg in "${missing[@]}"; do
		echo "  internal/${pkg}" >&2
	done
fi

if [[ ${fail} -ne 0 ]]; then
	echo >&2
	echo "coverage-gate: FAIL" >&2
	exit 1
fi

echo
echo "coverage-gate: PASS - all gated packages ≥ ${THRESHOLD}% (GOOS=${GATE_GOOS})"
