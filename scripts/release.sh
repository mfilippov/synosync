#!/usr/bin/env bash
#
# release.sh - reproducible cross-platform release build for synosync.
#
# Builds a pure-Go (CGO disabled), version-stamped binary for each target in the
# platform matrix, packages it into a per-platform archive (tar.gz, or .zip for
# Windows) under dist/, and emits a SHA256SUMS manifest covering every archive.
#
# The archives - not just the binaries - are reproducible: two builds of the
# same commit yield byte-identical archives and therefore an identical
# SHA256SUMS. The binary is reproducible via -trimpath + pinned ldflags; the
# archive layer pins every filesystem-derived input into the archive bytes
# (member order, owner/group, mtimes, compressor header) - see the packaging
# step below. All embedded timestamps come from the commit date
# (SOURCE_DATE_EPOCH convention; the env overrides).
#
# Version is derived from `git describe --tags --always --dirty`, falling back to
# 0.0.0-dev outside a git checkout, and injected via -ldflags -X main.version.
#
# A release built from a dirty working tree is refused: a published artifact must
# correspond to a committed, reproducible tree, so an uncommitted change can never
# be silently baked into a release under a `-dirty` version stamp. Set ALLOW_DIRTY=1
# to override for local experimentation; an explicit VERSION= bypasses the git
# derivation entirely and so is not subject to the check.
#
# Usage:
#   scripts/release.sh                 # build the full matrix
#   scripts/release.sh linux/amd64     # build a single platform
#   scripts/release.sh linux/amd64 darwin/arm64
#   PLATFORMS="linux/amd64" scripts/release.sh
#   VERSION=1.2.3 scripts/release.sh   # override the derived version
#   ALLOW_DIRTY=1 scripts/release.sh   # permit a dirty tree (local builds only)
#   BIN_ONLY=1 scripts/release.sh windows/amd64   # binaries only, as dist/bin/synosync-<os>-<arch>[.exe]
#   PREBUILT_DIR=dir scripts/release.sh           # package dir/synosync-<os>-<arch>[.exe] where present
#
# The last two split the release for Windows code signing: the release workflow builds the
# Windows binaries with BIN_ONLY, has them Authenticode-signed (mfilippov/github-codesign), and
# packages the signed files with PREBUILT_DIR. Every other platform is built as usual.
#
set -euo pipefail

# Resolve the repository root from this script's location so the script works
# regardless of the caller's working directory.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

# Pin the umask so the file modes baked into the archives do not depend on the
# caller's environment. `cp` (staging the compliance files) and `mkdir` (the
# staging dir) derive the created mode as `source_mode & ~umask`, so a build run
# under umask 077 would produce 0600/0700 members while umask 022 produces
# 0644/0755 - different archive bytes and a different SHA256SUMS for the same
# commit, defeating the reproducibility contract above. The tar `--mode` and the
# pre-zip `chmod` below re-normalize modes belt-and-braces, but pinning the umask
# keeps the whole staging tree deterministic regardless of how it is invoked.
umask 022

BINARY="synosync"
CMD_PKG="./cmd/synosync"
DIST_DIR="${ROOT_DIR}/dist"

# The default build matrix. Override with positional args or the PLATFORMS env.
DEFAULT_PLATFORMS=(
	"linux/amd64"
	"linux/arm64"
	"darwin/amd64"
	"darwin/arm64"
	"windows/amd64"
	"windows/arm64"
)

# Determine which platforms to build: positional args win, then $PLATFORMS, then
# the default matrix.
if [[ $# -gt 0 ]]; then
	PLATFORMS=("$@")
elif [[ -n "${PLATFORMS:-}" ]]; then
	# shellcheck disable=SC2206
	PLATFORMS=(${PLATFORMS})
else
	PLATFORMS=("${DEFAULT_PLATFORMS[@]}")
fi

# Derive the version string. An explicit VERSION env overrides the git-derived
# value; a non-git tree falls back to a dev marker.
if [[ -n "${VERSION:-}" ]]; then
	version="${VERSION}"
elif version="$(git describe --tags --always --dirty 2>/dev/null)"; then
	# Refuse to publish from a dirty tree: a build that does not correspond to any
	# committed state cannot be reproduced from the attested commit. ALLOW_DIRTY=1
	# overrides for local experimentation.
	#
	# `git describe --dirty` only flags TRACKED modifications - it ignores
	# untracked files. But `go build` compiles every .go file in a package
	# directory, including an untracked one, so a stray untracked source would
	# slip into the artifact while the `-dirty` suffix stayed absent. Use
	# `git status --porcelain --untracked-files=all` as the authoritative dirty
	# check: it lists untracked, non-.gitignored files too (the build output dir
	# `/dist/` is gitignored, so it never trips this), catching what the describe
	# suffix misses.
	if [[ "${ALLOW_DIRTY:-}" != "1" ]]; then
		dirty="$(git status --porcelain --untracked-files=all 2>/dev/null)"
		if [[ -n "${dirty}" ]]; then
			echo "release.sh: refusing to build from a dirty working tree:" >&2
			echo "${dirty}" >&2
			echo "release.sh: commit, stash, or remove the above changes (including untracked files), or set ALLOW_DIRTY=1 for a local build." >&2
			exit 1
		fi
	fi
else
	version="0.0.0-dev"
fi

# All timestamps embedded in the archives are pinned to the commit date so the
# archive bytes depend only on the commit, not on when the build ran. Honour an
# explicit SOURCE_DATE_EPOCH (the reproducible-builds convention); otherwise use
# HEAD's commit timestamp; outside a git checkout fall back to the DOS epoch
# (1980-01-01, the oldest mtime a zip can represent - plain 0/1970 would be
# clamped by zip and warned about).
SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(git log -1 --format=%ct 2>/dev/null || echo 315532800)}"

# Pick a sha256 tool: sha256sum (Linux) or `shasum -a 256` (macOS/BSD).
if command -v sha256sum >/dev/null 2>&1; then
	sha256() { sha256sum "$@"; }
elif command -v shasum >/dev/null 2>&1; then
	sha256() { shasum -a 256 "$@"; }
else
	echo "release.sh: need sha256sum or shasum to emit checksums" >&2
	exit 1
fi

# Windows archives are zipped, so require `zip` up front when a windows target is
# in the matrix - otherwise the run would abort partway through (under set -e)
# with a cryptic error after already building several artifacts.
for platform in "${PLATFORMS[@]}"; do
	if [[ -z "${BIN_ONLY:-}" && "${platform%%/*}" == "windows" ]] && ! command -v zip >/dev/null 2>&1; then
		echo "release.sh: need zip to package Windows archives" >&2
		exit 1
	fi
done

# tar.gz targets need GNU tar: the deterministic-archive flags below
# (--sort=name --owner/--group --numeric-owner --mtime) are GNU extensions that
# bsdtar (the macOS system tar) does not support. Prefer `gtar` (the Homebrew
# install name) when the system tar is not GNU, and fail up front - mirroring
# the zip guard - when a non-Windows target is in the matrix without one.
TAR=tar
if ! tar --version 2>/dev/null | grep -q 'GNU tar'; then
	if command -v gtar >/dev/null 2>&1 && gtar --version 2>/dev/null | grep -q 'GNU tar'; then
		TAR=gtar
	else
		for platform in "${PLATFORMS[@]}"; do
			if [[ -z "${BIN_ONLY:-}" && "${platform%%/*}" != "windows" ]]; then
				echo "release.sh: need GNU tar for deterministic tar.gz archives (on macOS: brew install gnu-tar)" >&2
				exit 1
			fi
		done
	fi
fi

# Compliance/attribution files bundled into every archive. Apache-2.0 §4(d)
# requires shipping a copy of the license and the NOTICE text with derivative
# works, so each archive must carry them alongside the binary; PROVENANCE and
# README round out the DESIGN §9 attribution set. Fail fast if a source file is
# missing so a packaging regression breaks the release instead of shipping a
# non-compliant archive (mirrors the CI "Compliance files present" gate, which
# only checks the repo, not the artifact).
COMPLIANCE_FILES=("LICENSE" "NOTICE" "PROVENANCE" "README.md")
for f in "${COMPLIANCE_FILES[@]}"; do
	if [[ ! -f "${ROOT_DIR}/${f}" ]]; then
		echo "release.sh: missing required file ${f} in repo root" >&2
		exit 1
	fi
done

LDFLAGS="-s -w -X main.version=${version}"

echo "Building synosync ${version}"
echo "Output dir: ${DIST_DIR}"
echo

rm -rf "${DIST_DIR}"
mkdir -p "${DIST_DIR}"

ARTIFACTS=()

for platform in "${PLATFORMS[@]}"; do
	goos="${platform%%/*}"
	goarch="${platform##*/}"
	if [[ "${goos}" == "${platform}" || -z "${goarch}" ]]; then
		echo "release.sh: invalid platform ${platform} (want GOOS/GOARCH)" >&2
		exit 1
	fi

	ext=""
	if [[ "${goos}" == "windows" ]]; then
		ext=".exe"
	fi

	name="${BINARY}-${version}-${goos}-${goarch}"
	stage_dir="${DIST_DIR}/${name}"
	mkdir -p "${stage_dir}"
	bin_path="${stage_dir}/${BINARY}${ext}"

	echo "==> ${goos}/${goarch}"
	# GOWORK=off disables Go workspace mode: go.work / go.work.sum are gitignored
	# (so the dirty guard above never sees them), but `go build` would otherwise
	# auto-detect an active go.work and build against its local `replace`
	# directives - bytes not represented by the attested commit. Pinning GOWORK=off
	# forces the build to resolve dependencies from the committed go.mod/go.sum
	# alone, keeping the artifact reproducible from the tagged tree.
	prebuilt=""
	if [[ -n "${PREBUILT_DIR:-}" ]]; then
		prebuilt="${PREBUILT_DIR}/${BINARY}-${goos}-${goarch}${ext}"
	fi
	if [[ -n "${prebuilt}" && -f "${prebuilt}" ]]; then
		echo "    using prebuilt ${prebuilt}"
		install -m 0755 "${prebuilt}" "${bin_path}"
	else
		CGO_ENABLED=0 GOWORK=off GOOS="${goos}" GOARCH="${goarch}" \
			go build -trimpath -ldflags "${LDFLAGS}" -o "${bin_path}" "${CMD_PKG}"
	fi

	if [[ -n "${BIN_ONLY:-}" ]]; then
		mkdir -p "${DIST_DIR}/bin"
		mv "${bin_path}" "${DIST_DIR}/bin/${BINARY}-${goos}-${goarch}${ext}"
		rm -rf "${stage_dir}"
		ARTIFACTS+=("bin/${BINARY}-${goos}-${goarch}${ext}")
		echo "    bin/${BINARY}-${goos}-${goarch}${ext}"
		continue
	fi

	# Bundle the compliance/attribution files next to the binary so every
	# tar.gz/zip carries the license and NOTICE (Apache-2.0 §4(d)).
	for f in "${COMPLIANCE_FILES[@]}"; do
		cp "${ROOT_DIR}/${f}" "${stage_dir}/${f}"
	done

	# Ship THIS platform's service unit under packaging/ in the ARCHIVE. The
	# sources live in internal/service/templates/, where `synosync service
	# install` embeds them too, so the file a user edits by hand and the file the
	# installer renders are the same bytes. The archives carried
	# only the binary and the compliance files, so a user who downloaded a
	# release had no way to install the daemon as a service at all: the units
	# existed solely in the git tree, and README/`synosync daemon --help` pointed
	# at a packaging/ directory the archive did not contain.
	#
	# One unit per archive, not all three - a Linux tarball has no use for a
	# PowerShell installer. install-service_test.ps1 stays out: it is a
	# development self-test, not an operator tool.
	#
	# The subdirectory name is load-bearing on Windows: install-service.ps1
	# resolves its default BinPath as ..\synosync.exe relative to its own
	# directory, so packaging/install-service.ps1 finds the sibling binary with
	# no -BinPath argument. That layout is what the script was written for.
	service_unit=""
	case "${goos}" in
	linux) service_unit="internal/service/templates/synosync.service" ;;
	darwin) service_unit="internal/service/templates/me.filippov.synosync.plist" ;;
	windows) service_unit="internal/service/templates/install-service.ps1" ;;
	esac
	if [[ -n "${service_unit}" ]]; then
		if [[ ! -f "${ROOT_DIR}/${service_unit}" ]]; then
			echo "release.sh: missing service unit ${service_unit}" >&2
			exit 1
		fi
		mkdir -p "${stage_dir}/packaging"
		cp "${ROOT_DIR}/${service_unit}" "${stage_dir}/packaging/"
	fi

	# Normalize the staging tree's file modes so the modes stored in the archive
	# do not depend on the caller's umask (belt-and-braces with the `umask 022`
	# above): `u=rwX,go=rX` gives 0755 to the binary and every directory (the
	# capital X only adds execute where execute already exists, i.e. the built
	# binary and dirs) and 0644 to the text/compliance files, deterministically.
	# tar re-applies the same modes via --mode; zip has no such flag and stores
	# the on-disk mode verbatim, so the chmod is the load-bearing step there.
	chmod -R u=rwX,go=rX "${stage_dir}"

	# Package: zip for Windows, tar.gz everywhere else. Both are built
	# deterministically so the archive bytes depend only on the commit:
	#   tar - members sorted by name, owner/group forced to numeric 0, modes
	#   pinned via --mode (umask-independent), every member mtime clamped to
	#   SOURCE_DATE_EPOCH, and gzip -n so the gzip header carries no
	#   filename/timestamp.
	#   zip - staged files chmod'd (above) and touched to SOURCE_DATE_EPOCH first
	#   (zip has no --mode/--mtime), the member list fed in sorted via find|sort
	#   (readdir order is filesystem-dependent), -X to strip the uid/gid/timestamp
	#   extra fields, and TZ pinned to UTC because zip stores DOS timestamps in
	#   LOCAL time.
	# (`touch -d @epoch` is GNU coreutils syntax; Windows targets are packaged
	# on a GNU userland - the Linux CI release job.)
	if [[ "${goos}" == "windows" ]]; then
		archive="${name}.zip"
		find "${stage_dir}" -exec touch -d "@${SOURCE_DATE_EPOCH}" {} +
		( cd "${DIST_DIR}" && find "${name}" | LC_ALL=C sort | TZ=UTC zip -q -X "${archive}" -@ )
	else
		archive="${name}.tar.gz"
		"${TAR}" --sort=name --owner=0 --group=0 --numeric-owner \
			--mode='u=rwX,go=rX' \
			--mtime="@${SOURCE_DATE_EPOCH}" \
			-cf - -C "${DIST_DIR}" "${name}" | gzip -n >"${DIST_DIR}/${archive}"
	fi

	# Drop the staging directory; keep only the archive.
	rm -rf "${stage_dir}"
	ARTIFACTS+=("${archive}")
	echo "    ${archive}"
done

if [[ -n "${BIN_ONLY:-}" ]]; then
	echo
	echo "Binaries in ${DIST_DIR}/bin (no archives, no SHA256SUMS):"
	for a in "${ARTIFACTS[@]}"; do
		echo "  ${a}"
	done
	exit 0
fi

# Emit a single SHA256SUMS manifest over every archive, with paths relative to
# dist/ so the file is portable.
(
	cd "${DIST_DIR}"
	sha256 "${ARTIFACTS[@]}" >SHA256SUMS
)

echo
echo "Artifacts in ${DIST_DIR}:"
for a in "${ARTIFACTS[@]}"; do
	echo "  ${a}"
done
echo "  SHA256SUMS"
