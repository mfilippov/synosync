# AGENTS.md

Guidance for coding agents (and people) working in the synosync repository. This file is the always-on
summary: statements only; every "why" (measurement, errno history, the bug a rule
encodes) lives in `docs/` - `PACKAGES.md`, `ENGINE.md` (the sync engine),
`INVARIANTS.md`, `DESIGN.md`. Read the linked section before changing code a
statement guards.

## What this is

synosync is a cross-platform, console two-way sync client for Synology Drive,
written in pure Go (no cgo). The wire protocol (`sproto`) is reimplemented from
scratch. The client speaks **only over HTTPS/TLS** - plaintext HTTP and the
obfuscated login encryption are intentionally not implemented.

Deletion policy: deletions apply directly (no local trash staging); recovery
relies on the server-side recycle bin, and divergent-content deletes/overwrites
funnel into keep-both `_Conflict` copies (DESIGN §6). Synology and Synology
Drive are trademarks of Synology Inc.; this is an independent, unaffiliated
client.

Full design: `docs/DESIGN.md` (implementation order §11, testing §12, accepted limits §13).

## Package structure

Detail and rationale: `docs/PACKAGES.md`.

- `cmd/synosync` - CLI dispatcher + subcommands (incl. `service`); settings are
  one JSON blob in `state.db`; Windows SCM (**`Execute` never sends
  `svc.Stopped`**).
- `internal/config` - `Config`, defaults, validated `Limits`, per-OS paths;
  `ValidatePartial` (`config set`) vs `Validate`.
- `internal/sproto` - frame header, OOM-guarded `PStream` codec, `Conn` (TLS
  upgrade, pin hook), Legacy-TLV.
- `internal/transport` - handshake, SPKI pinning, `Session`, `pool.go` (event
  session + 2-4 workers, terminal pause on 8193/20481).
- `internal/actions` - the typed action turn (capped reads, a mutate needs an
  errno, idle watchdog), sentinels, `List` → `(nodes, listed, err)`, streamed MD4
  transfers (`resume_upload` is WHOLE-FILE).
- `internal/store` - bbolt buckets, composite ops atomic per transaction, `Batch`/`Writer`
  batch writes (**a store call inside a batch DEADLOCKS; it panics instead**),
  migrations (v13), `ErrCorruptRecord`/`ErrLocked`, `QuarantinedOps()`.
- `internal/fsutil` - atomic apply helpers (DESIGN §7): durable mkdir/rename/
  remove, `RenameNoReplace`/`RenameReplace`, `SetMeta`, `SyncDir`.
- `internal/engine` - the two-way sync core (DESIGN §6). **Read
  `docs/ENGINE.md` first** - per-file breakdown and traps.
- `internal/daemon` - lock → IPC → serve → sync loop; keychain connector; cached
  `status.go` where only the O(tree) count may fail (degrades).
- `internal/service` - embedded systemd/launchd/SCM units rendered with the real
  exe path, escaped per format; **Windows `Install` REFUSES** (needs a run-as
  account), printing the elevated command.
- `internal/ipc` JSON-RPC over 0600 socket / DACL pipe, bounded ·
  `internal/watcher` fsnotify (latency only; a missed event is no correctness
  gap) · `internal/auth` `Pair`/`Reuse` (needs evidence) + `device_uuid` ·
  `internal/secrets` keyring token+pin; password never persisted ·
  `internal/backoff` jittered exp delays · `internal/netctx` ctx cancel → past
  deadline · `internal/servertree` top-down `list` walk = cold start
  catalog (an unevidenced listing is never empty) · `internal/normpath` NFC
  `Normalize`+case-folded `Key` · `internal/md4` MD4 · `internal/logging` slog.

## Commands

```sh
make build    # go build ./... and the ./synosync binary
make crossbuild # no-cgo compile for every release GOOS
make vet-cross # go vet per release GOOS
make test     # go test -race ./...
make lint     # golangci-lint run ./... (pinned GOLANGCI_VERSION)
make lint-cross # golangci-lint per release GOOS
make lint-install # pinned golangci-lint into /.tools
make vulncheck # govulncheck over the module
make vulncheck-cross # per release GOOS
make cover    # coverage profile
make cover-check # per-package coverage gate
make test-integration-build # type-check the harness
make fmt      # go fmt ./...
make tidy     # go mod tidy
make release       # dist/ archives + SHA256SUMS
make release-clean # remove dist/
make test-integration # live-DSM harness (needs SYNOSYNC_IT_*; else skips)
```

The integration harness lives under `test/integration/` (build-tagged
`integration`, env-gated on `SYNOSYNC_IT_*`, excluded from `make test`/CI); env
contract in `docs/INTEGRATION.md`. Packaging: `scripts/release.sh` +
`internal/service` (units shipped per archive under `packaging/`; only the
systemd unit passes `--log-stderr`).

Always run `go test ./...` and `make lint` before considering a task done.
`make cover-check` is a blocking gate on 17 packages: ≥80% on linux, ≥75% on the
native windows/macOS jobs; `lint-cross`/`vulncheck-cross` run per release GOOS.
(→ docs/INVARIANTS.md#coverage-and-cross-goos-gates)

## Key invariants

- **Never-lose doctrine (DESIGN §6).** A destructive apply must prove the target
  is the last-agreed base; diverged, untracked or unprovable content is kept as a
  `_Conflict` copy. (→ docs/INVARIANTS.md#never-lose-doctrine)
- **The server is NOT the arbiter of divergence - a push must prove it itself.**
  `compare_mtime` overwrites on any not-older mtime and ignores base version, so
  24579 never arrives: `OpUpload` probes the target first (`guardUploadTarget`),
  funnels divergence (`BaseLocalHash` vs `ServerHash`) into keep-both, and stays
  queued if inconclusive. (→ docs/INVARIANTS.md#upload-guard)
- **`remove` of a directory is RECURSIVE on the server**, so an outbound delete
  enumerates the subtree first (`verifyServerSubtreeAgrees`) and refuses on an
  unknown child, a moved hash or kind, or a foreign `file_id`; an unlisted or
  path-error reply is no proof. (→ docs/INVARIANTS.md#recursive-remove)
- **`markSynced` is the one place NEW agreement is recorded.** Seven other sites
  assign `StateSynced` (self-echo overlays, reverts, a repair); all tabulated.
  (→ docs/INVARIANTS.md#marksynced)
- **Enqueue before baseline mutation** (both passes); a create/modify commits
  BOTH in ONE transaction, as ordering alone cannot keep its gate alive.
  (→ docs/INVARIANTS.md#enqueue-first)
- **A cleared subtree drops its DESCENDANT baselines too**, or the next scan
  pushes a remove per child.
- **An emptied directory still recording contents is not a deletion** unless it is
  the object this run saw populated (an unmount swaps it); else its removes wait
  for corroboration (`emptieddir.go`).
- **Serialization invariant.** Two queue ops conflict iff their resolved on-disk
  paths are equal or ancestor/descendant (`serialize.go`); a rename serialises on
  both endpoints. (→ docs/INVARIANTS.md#serialization)
- **Reply evidence.** A mutating reply must carry a VERDICT - a nested
  `error{action,code,reason}` map, or its absence on a `type:"response"` frame; a
  bare `{}` is never proof. `List` reports it as `listed`; an absence needs
  repetition: a pass count AND a wall-clock floor.
  (→ docs/INVARIANTS.md#reply-evidence)
- **Schema rule.** Any change to a persisted `Config`/store record field bumps
  `store.CurrentSchemaVersion`; additive tolerant fields are exempt.
  (→ docs/INVARIANTS.md#schema-rule)
- **The server interleaves unsolicited `keep_alive` frames** before any reply;
  step over them. (→ docs/INVARIANTS.md#keep-alive-frames)
- **A `move` event's endpoints are inverted**: top-level `path` is the SOURCE,
  the new location is `destination.path`. (→ docs/INVARIANTS.md#move-endpoints)
- **`pull_event` is a LOG, not a snapshot**, and only the INCREMENTAL poll reads it
  - cold start walks `list` instead (`internal/servertree`). The DSM compacts the log
  per node and pages it by a fixed `sync_id` WINDOW over a log holed by that
  compaction, so an **empty page proves one window, not the log**; the poll advances
  past it. (→ docs/INVARIANTS.md#pull_event-is-a-log)
- **`long_poll` holds only with the last reply's `profile_digest` and a non-zero `lock_id`**;
  otherwise it answers at once and an idle daemon spins.
  (→ docs/INVARIANTS.md#long_poll-must-be-able-to-hold)
- **Server result codes are measured, not guessed**, and differ per verb:
  32769/40961/28672 → `ErrPathError` (absence is always corroborated), 40962
  (stale cursor) is RETRIED, 24580/24581/24584 are NAME verdicts, 32772 is TRANSIENT.
  (→ docs/INVARIANTS.md#result-codes)
- **The mutating verbs are asymmetric - do not "unify" them.** `remove`:
  leading-slash path, NO cursor. `rename`: `src_path`/`dst_path` plus the source
  node's own `node.max_id`; both pinned by wire-shape tests.
  (→ docs/INVARIANTS.md#mutating-verbs)
- **Path safety.** Untrusted server paths are containment-checked
  (`filepath.IsLocal`), NFC-resolved to the on-disk spelling, and applied with
  no-follow resolution refusing any redirecting component; Windows also refuses
  non-verbatim names and 8.3 aliases. (→ docs/INVARIANTS.md#path-safety)
- **Streaming.** Never fully buffer large binary values - stream them.
  (→ docs/INVARIANTS.md#streaming)

## Conventions

- **Pure Go, stdlib-first.** No cgo; deps `go-keyring`, `x/term`, `bbolt`,
  `x/sys`, `fsnotify`, `x/text` (NFC), `go-winio`. **TLS only** - never add a
  plaintext-HTTP fallback.
- **Bounded work.** Every network or disk unit of work carries a time bound: an
  idle/progress budget where the length is server-controlled, a bounded
  cancellation latency for a long walk+hash.
- **Per-OS error classification** lives in build-tagged files, never a GOOS
  branch, each with a host-native test fixture pair; direct Win32 calls route
  every path argument through `fsutil.ExtendedPath`.
- **Testing.** Each task ships with tests; table-driven, in-memory pipes; host
  behaviour PROBED with `t.Skipf`, never branched on GOOS. A fake must never be
  kinder than the measured server; never-lose shapes need two-engine scenarios.
- **Docs budget.** `AGENTS.md` ≤ 10 KB (agents load it on every prompt), other
  `docs/*.md` ≤ 60 KB, a `CHANGELOG` entry ≤ 400 B; new "why" goes to `docs/`.
- **Style.** Doc comments on exported symbols, big-endian on the wire, wrapped
  errors. Go 1.27, module `github.com/mfilippov/synosync`.
