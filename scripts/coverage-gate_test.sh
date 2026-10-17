#!/usr/bin/env bash
#
# coverage-gate_test.sh - self-test for the coverage-gate.sh parser.
#
# Drives scripts/coverage-gate.sh against synthetic per-package coverage
# summaries (no real `go test` run) covering:
#   1. every gated package ≥ THRESHOLD          -> expect exit 0, "PASS"
#   2. one gated package below THRESHOLD         -> expect exit 1, package named
#   3. a gated package missing from the output   -> expect exit 1, "missing"
#   4. a line with no parseable percent          -> expect exit 1, "missing"
#   5. the per-GOOS floor selection: the same summary is red
#      under the authoritative linux floor and green under the lower native
#      windows/darwin floor, and dropping a package below the native floor turns
#      the native run red too - the forced-red check for the per-OS profile gate.
#
# The script fails (exit 1) on the first mismatch between the gate's actual
# behaviour and the expectation, and prints an OK summary otherwise.
#
# Usage:
#   scripts/coverage-gate_test.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Overridable so the same cases can be pointed at another copy of the gate - how
# the per-GOOS floor cases were pinned red against the pre-fix script:
#   git show HEAD~1:scripts/coverage-gate.sh >/tmp/gate.sh
#   GATE=/tmp/gate.sh scripts/coverage-gate_test.sh
GATE="${GATE:-${SCRIPT_DIR}/coverage-gate.sh}"
MODULE="github.com/mfilippov/synosync"

# A small, fixed package set keeps the fixtures readable; we drive the gate with
# this set via GATED_PACKAGES rather than the full §12 list.
PACKAGES="md4 sproto store"

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

failures=0

# fail records a self-test failure and keeps going so all cases are reported.
fail() {
	echo "  FAIL: $*" >&2
	failures=$((failures + 1))
}

# write_summary emits a `go test`-style coverage summary line per "pkg=pct" pair
# given as arguments (pct empty -> omit the package's line entirely).
write_summary() {
	local file="$1"
	shift
	: >"${file}"
	local pair pkg pct
	for pair in "$@"; do
		pkg="${pair%%=*}"
		pct="${pair#*=}"
		if [[ -n "${pct}" ]]; then
			printf 'ok  \t%s/internal/%s\t0.10s\tcoverage: %s%% of statements\n' \
				"${MODULE}" "${pkg}" "${pct}" >>"${file}"
		fi
	done
}

# The environment each case drives the gate with. Cases that exercise the
# per-GOOS floor selection replace it (see reset_gate_env) - they must NOT pass
# THRESHOLD, since an explicit floor deliberately overrides that choice.
GATE_ENV=()

# reset_gate_env restores the default case environment: the small fixed package
# set at an explicit floor of 80.
reset_gate_env() {
	GATE_ENV=("GATED_PACKAGES=${PACKAGES}" "THRESHOLD=80")
}
reset_gate_env

# run_case runs the gate against a summary file and checks the exit code and that
# every expected substring is present in the combined output.
#   $1 case name, $2 summary file, $3 expected exit code, rest: required substrings
run_case() {
	local name="$1" summary="$2" want_code="$3"
	shift 3
	local out code
	set +e
	out="$(env "${GATE_ENV[@]}" "${GATE}" "${summary}" 2>&1)"
	code=$?
	set -e
	if [[ "${code}" -ne "${want_code}" ]]; then
		fail "${name}: exit ${code}, want ${want_code} (output: ${out})"
		return
	fi
	local needle
	for needle in "$@"; do
		if [[ "${out}" != *"${needle}"* ]]; then
			fail "${name}: output missing '${needle}' (output: ${out})"
			return
		fi
	done
	echo "  ok: ${name}"
}

# Case 1: every gated package at or above the threshold -> PASS, exit 0.
write_summary "${tmpdir}/pass.txt" "md4=100.0" "sproto=85.5" "store=80.0"
run_case "all-above-threshold" "${tmpdir}/pass.txt" 0 "PASS"

# Case 2: one gated package below the threshold -> FAIL, exit 1, package named.
write_summary "${tmpdir}/below.txt" "md4=100.0" "sproto=79.9" "store=83.0"
run_case "one-below-threshold" "${tmpdir}/below.txt" 1 "below" "sproto" "FAIL"

# Case 3: a gated package missing from the output entirely -> FAIL, exit 1.
write_summary "${tmpdir}/missing.txt" "md4=100.0" "sproto=" "store=83.0"
run_case "missing-package" "${tmpdir}/missing.txt" 1 "missing" "sproto" "FAIL"

# Case 4: a gated package's line is present but carries no parseable percent
# (real `go test` prints `coverage: [no statements]` for a statement-free
# package) -> the gate must route it to "missing", FAIL, exit 1.
{
	printf 'ok  \t%s/internal/md4\t0.10s\tcoverage: 100.0%% of statements\n' "${MODULE}"
	printf 'ok  \t%s/internal/sproto\t0.10s\tcoverage: [no statements]\n' "${MODULE}"
	printf 'ok  \t%s/internal/store\t0.10s\tcoverage: 83.0%% of statements\n' "${MODULE}"
} >"${tmpdir}/no-statements.txt"
run_case "no-statements-package" "${tmpdir}/no-statements.txt" 1 "missing" "sproto" "FAIL"

# Cases 5a-5c: the per-GOOS floor. One summary sits between the two floors (77%
# - under the linux 80, over the native 75). No THRESHOLD is passed: these cases
# assert the floor the gate PICKS from the build context, which an explicit
# THRESHOLD would mask.
write_summary "${tmpdir}/between.txt" "md4=100.0" "sproto=77.0" "store=83.0"

# 5a: the authoritative linux floor still fails it - the native arm must not be
# reachable from a linux build context (this is what "not weakening the linux
# gate" means operationally).
GATE_ENV=("GATED_PACKAGES=${PACKAGES}" "GATE_GOOS=linux")
run_case "between-floors-linux-fails" "${tmpdir}/between.txt" 1 "below" "sproto" "GOOS=linux" "FAIL"

# 5b: the same summary passes the native windows/darwin run at its own floor.
for goos in windows darwin; do
	GATE_ENV=("GATED_PACKAGES=${PACKAGES}" "GATE_GOOS=${goos}")
	run_case "between-floors-${goos}-passes" "${tmpdir}/between.txt" 0 "PASS" "GOOS=${goos}"
done

# 5c: forced red - a per-OS profile that drops BELOW the native floor fails the
# native run. Without this the lower floor would read as "no gate at all".
write_summary "${tmpdir}/below-native.txt" "md4=100.0" "sproto=74.9" "store=83.0"
GATE_ENV=("GATED_PACKAGES=${PACKAGES}" "GATE_GOOS=windows")
run_case "below-native-floor-fails" "${tmpdir}/below-native.txt" 1 "below" "sproto" "GOOS=windows" "FAIL"

# 5d: a raised native floor is honoured, so the constant can be ratcheted up.
GATE_ENV=("GATED_PACKAGES=${PACKAGES}" "GATE_GOOS=windows" "CROSS_THRESHOLD=80")
run_case "raised-native-floor-fails" "${tmpdir}/between.txt" 1 "below" "sproto" "FAIL"

# 5e: an unidentifiable build context takes the STRICT floor, not the lenient
# one - a broken GOOS probe must not silently downgrade the gate.
GATE_ENV=("GATED_PACKAGES=${PACKAGES}" "GATE_GOOS=nosuchos")
run_case "unknown-goos-uses-strict-floor" "${tmpdir}/between.txt" 1 "below" "sproto" "FAIL"

reset_gate_env

echo
if [[ "${failures}" -ne 0 ]]; then
	echo "coverage-gate_test: ${failures} case(s) FAILED" >&2
	exit 1
fi
echo "coverage-gate_test: OK - all cases passed"
