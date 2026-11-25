# Changelog

Notable changes to synosync. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- `status` names an operation that failed and waits for its retry, with the reason,
  from its first failure, and shows a held change poll and failed polls instead of an
  ageing `last reply`; stuck entries always carry a reason line (#6).
- A `go install` build spells its version as a release build does (`v0.1.0`, not
  `0.1.0`).

## [0.1.0] - 2026-10-17

First public release: a two-way Synology Drive sync client for Linux, macOS and
Windows, TLS only, in pure Go. Diverged changes are kept as `_Conflict` copies;
selective sync starts from the official client's defaults; a daemon with service
units; signed, attested release archives. See README.md.
