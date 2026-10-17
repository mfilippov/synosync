# Contributing to synosync

Bug reports, fixes and measurements against a real DSM are all welcome. A sync client
deletes and overwrites files, so changes are held to a high bar; this page says what
that bar is.

## Reporting

- **Bugs:** open an issue with the bug report template. [docs/TESTING.md](docs/TESTING.md)
  lists what a useful report contains (version, OS and file system, steps, the log).
- **Security issues:** never in a public issue; see [SECURITY.md](SECURITY.md).

## Clean-room rule

The Synology Drive protocol is implemented from observation of the wire and of the
behaviour of a running server only; [PROVENANCE](PROVENANCE) states how. A contribution
must not be derived from Synology source code, from decompiled or disassembled Synology
binaries, or from leaked or private specifications. Observations you made yourself -
captured traffic, a server's replies, what the official client visibly does - are fine;
say how you made them in the pull request.

## Building and testing

Go 1.27.2 or newer. Before opening a pull request:

```sh
make test         # go test -race ./...
make lint         # golangci-lint at the pinned version (make lint-install fetches it)
make lint-cross   # the linter per release OS: per-OS files are linted nowhere else
make vet-cross    # go vet, test code included, per release OS
```

CI also runs `make cover-check` (a per-package coverage floor), `make vulncheck-cross`
and a full release build with a license check.

Changes that touch protocol or sync behaviour should also pass the live-DSM harness:
`make test-integration` against a DSM you control, set up as in
[docs/INTEGRATION.md](docs/INTEGRATION.md). A virtual DSM in a container works; on
Windows see [docs/stand-wslc.md](docs/stand-wslc.md).

## How the code is held together

Read these before changing the code they cover:

- [AGENTS.md](AGENTS.md): the one-page summary - packages, commands, the invariants as
  one-line rules with links. Coding agents load it automatically; it is a good start for
  people too.
- [docs/INVARIANTS.md](docs/INVARIANTS.md): the rules every change must keep. The
  central one is never-lose: a destructive apply must prove the target is the last
  agreed version, and anything else is kept as a `_Conflict` copy.
- [docs/ENGINE.md](docs/ENGINE.md): the sync engine file by file, with the trap each
  file sets for an editor.
- [docs/DESIGN.md](docs/DESIGN.md): the architecture; §13 lists accepted limits.

Conventions the review will ask about:

- **Pure Go, no cgo**, few dependencies, TLS only.
- **Every unit of network or disk work is bounded** in time.
- **Per-OS behaviour lives in build-tagged files**, never in a `runtime.GOOS` branch,
  with a host-native test for each OS.
- **Tests probe the host** (`t.Skipf` when a capability is missing) instead of
  branching on GOOS, and **a fake is never kinder than the measured server**.
- **A change to a persisted config or store field bumps the schema version**
  (`store.CurrentSchemaVersion`), with a migration step.
- **Comments explain why.** Where a rule exists because of a measured server
  behaviour or a past bug, the comment says so.

## Pull requests

- One topic per pull request, with tests for the change.
- Say what you measured and on which DSM / Drive Server version if the change rests on
  server behaviour.
- Commits should be signed.
- Add a line to [CHANGELOG.md](CHANGELOG.md) under Unreleased for user-visible changes.

By contributing you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE).
