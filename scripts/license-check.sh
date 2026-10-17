#!/usr/bin/env bash
#
# license-check.sh - assert every release archive in dist/ carries what it must:
# LICENSE and NOTICE (Apache-2.0 §4(d) requires shipping them with the
# derivative, not just leaving them in the repo tree), and the service unit for
# its platform.
#
# The service-unit check joined this script rather than getting one of its own so
# the release archives keep a single content gate; the filename and the
# `make license-check` target stay as they are because CI, the Makefile and the
# docs all name them.
#
# Called from both CI jobs so the per-platform checks live in one place. Both
# now build the FULL matrix before calling this (the PR/branch job stopped
# smoke-checking linux/amd64 alone when it started uploading the archives), and
# this gate gets whatever is in dist/ either way.
#
# Usage:
#   scripts/license-check.sh            # check dist/ (default)
#   DIST=out scripts/license-check.sh   # check a different output dir
#
set -euo pipefail
shopt -s nullglob

DIST="${DIST:-dist}"

archives=("${DIST}"/synosync-*.tar.gz "${DIST}"/synosync-*.zip)
if [[ ${#archives[@]} -eq 0 ]]; then
	echo "license-check: no release archives found in ${DIST}/" >&2
	exit 1
fi

fail=0
for a in "${archives[@]}"; do
	echo "== ${a} =="
	case "${a}" in
	*.tar.gz) manifest="$(tar tzf "${a}")" ;;
	*.zip) manifest="$(unzip -Z1 "${a}")" ;;
	*)
		echo "license-check: unknown archive type: ${a}" >&2
		fail=1
		continue
		;;
	esac
	if ! printf '%s\n' "${manifest}" | grep -q '/LICENSE$'; then
		echo "license-check: ${a} missing LICENSE" >&2
		fail=1
	fi
	if ! printf '%s\n' "${manifest}" | grep -q '/NOTICE$'; then
		echo "license-check: ${a} missing NOTICE" >&2
		fail=1
	fi
	# The service unit for the archive's own platform, matched off the archive
	# name. Without it a downloaded release cannot install the daemon, which is
	# the state every archive shipped in until now - while README and
	# `synosync daemon --help` both pointed at packaging/.
	case "${a}" in
	*-linux-*) unit='packaging/synosync\.service$' ;;
	*-darwin-*) unit='packaging/me\.filippov\.synosync\.plist$' ;;
	*-windows-*) unit='packaging/install-service\.ps1$' ;;
	*)
		echo "license-check: ${a} has no recognisable platform in its name" >&2
		fail=1
		continue
		;;
	esac
	if ! printf '%s\n' "${manifest}" | grep -qE "${unit}"; then
		echo "license-check: ${a} missing its service unit (want ${unit})" >&2
		fail=1
	fi
done

if [[ ${fail} -ne 0 ]]; then
	echo "license-check: FAIL" >&2
	exit 1
fi

echo "license-check: PASS - every archive carries LICENSE, NOTICE and its service unit"
