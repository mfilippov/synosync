# synosync - Design

A cross-platform, console **two-way** sync client for Synology Drive, written in
Go, covering the core scenarios of bidirectional file synchronization.

> Status: the reference **architecture** document. Phases 0-10 are implemented (§11).
> This file records the target architecture and the design decisions behind it; the
> detailed, current state of packages and subcommands lives in `README.md` and the
> documents below. When they disagree, fix whichever side is stale.

## Document map

Section numbers are stable: code comments cite them as "DESIGN §6",
"DESIGN §13", and so on.

| Section | Content |
|---|---|
| §1 | Goals and constraints |
| §2 | The wire protocol, as implemented |
| §3 | Architecture (packages) |
| §4 | Connection model |
| §5 | State store (bbolt), §5.1 schema migrations |
| §6 | Sync core (reconcile) - summary |
| §7 | Watcher and the local side - summary |
| §8 | Daemon, IPC and CLI - summary |
| §9 | Secrets and security |
| §10 | Dependencies |
| §11 | Implementation order (phases 0-10) |
| §12 | Testing |
| §13 | Open questions and accepted residuals |

Companion documents:

- [`ENGINE.md`](ENGINE.md) - per-file breakdown of `internal/engine` and its traps.
- [`INVARIANTS.md`](INVARIANTS.md) - the full text of the key invariants: the
  never-lose doctrine, the upload guard, recursive remove, `markSynced`, reply
  evidence, result codes, the schema rule, and the two-client findings H1-H7.
- [`PACKAGES.md`](PACKAGES.md) - per-package detail (watcher backends, daemon,
  IPC, store, transport).
- [`INTEGRATION.md`](INTEGRATION.md) - the live-DSM harness and its measurements.
- [`TESTING.md`](TESTING.md) - test strategy and conventions.

## 1. Goals and constraints

- A cross-platform (Linux/macOS/Windows) console two-way sync client.
- A background daemon with a minimal idle footprint (idle = one blocked
  `long_poll`, ~0 bytes / ~0 CPU).
- Secrets live in the system keychain. 2FA is supported (the user enters the OTP
  at pairing / re-login).
- A system file watcher for local changes.
- **TLS only.** No plaintext fallback and no custom login encryption - the
  password travels in clear text *inside* TLS, as the protocol intends. The
  server certificate is pinned at the application level; the pin format and its
  rotation strategy are described in §9.
- Minimal dependencies. An independent implementation of the protocol.
- Language: **Go** (pure Go, no cgo → trivial cross-compilation).

> **Trademark.** Synology and Synology Drive are trademarks of Synology Inc.
> synosync is an independent, unofficial client, not affiliated with or endorsed
> by Synology Inc.; the names are used only to identify the service the client
> talks to.

## 2. Protocol (what we implement)

- **Transport:** TCP `:6690`. The first frames are plaintext, then the socket is
  upgraded to TLS.
- **Handshake:** `TEST_CONN` (yields `ds_id`) → `encrypt_channel` (the reply is
  a bare PObject without the 8-byte header) → TLS 1.2/1.3 on the same socket.
- **Encoding:** an 8-byte header - `magic:uint32 0x25521814`,
  `version:uint8 0x46`, `cmd:uint8`, `pkt_len:uint16` (a hint, 0 in practice;
  the body is self-delimiting, so it must never be used for framing) - followed
  by a recursive PStream/PObject (Null/Int/String/Binary/Array/Map/End,
  big-endian). Keys with `_` lose the underscore on the wire; the `@proto`
  envelope is preserved. The server reports errors as a nested map
  `error{action,code,reason}`, not as a flat `errno`; success is a
  `type:"response"` frame without `error`. After TLS, **server replies are bare
  PObjects**, while client requests carry the header. `cmd` after TLS is almost
  always `0x01` - dispatch is by the `action` string. The decoder is guarded
  against OOM not only by a per-value cap on inline Binary values but also by an
  aggregate per-frame payload budget (`MaxFrameBytes`, default 64 MiB): a flat
  array of many under-cap Binary/String values whose sum exceeds the budget fails
  with a typed `ErrFrameTooLarge` before allocation (large streamed Binary values
  are drained, not buffered, and do not count against the budget). Each decoded
  node is also charged a floor cost (see §13, frame budget).
- **Auth:** mode C (password + OTP) at first pairing → the server issues a
  `session` (32 hex chars) → afterwards mode A (the session is reused on every
  connection). Mode B (RSA) is not implemented - it was never verified end to
  end, and the Linux client does not use it.
- **Real-time sync:** a loop of `long_poll(subscribe[], timeout≥300)` → on
  `changes:[view_id]` → `pull_event(view_id, sync_id)` → apply `event_list`,
  advance `next_sync_id`.
- **File transfer:** `download` (binary inline, tag 0x30, the whole file in one
  PObject - streamed to disk), `upload` (two frames: metadata with
  `body-continue:1`, then the content as a bare PObject with
  `@proto.type:"body"`). Large files use `resume_get_token`/`resume_upload`/
  `resume_download` with an offset. The content hash is **MD4(content)**,
  lowercase hex.
- **Structural actions:** `query_user_info` (yields `view_list`), `list`,
  `query_node`, `rename` (view-relative paths), `remove`, and directory creation
  via `upload` with `is_dir:1`. `remove` of a directory is recursive on the
  server (INVARIANTS.md#recursive-remove).
- **Addressing:** human-readable paths (`/dir/file`) for
  `list`/`query_node`/`rename`/`remove`; `id:<file_id>` for `download`/
  `resume_download`; `id:<parent_id>/<name>` for `upload`.
- **Keep-alive:** there is no dedicated client keep-alive message; liveness is
  held by the in-flight `long_poll` itself (any real request counts as activity;
  the `alive` window is 150 s). The long poll is reopened immediately between
  cycles. The server does interleave unsolicited `keep_alive` frames before
  replies, which the client steps over (INVARIANTS.md#keep-alive-frames).
- **Conflicts:** resolved by the client (the server returns no special error in
  the normal flow). The policy is **keep-both**: the server version wins the
  canonical path (downloaded over the local file), and the divergent local
  version is uploaded as a new file
  `<base>_<hostname>_<Month-DD-HHMMSS-YYYY>_Conflict.<ext>` (Go layout
  `Jan-02-150405-2006` - the month, not the weekday). Error `24579` on uploading
  over a stale base means "resync and retry / keep-both", not a fatal error.
  **But `24579` is NOT a base-version check and must not be relied upon.**
  Measured on a live DSM (`TestIntegration_UploadConflictSemantics`,
  INTEGRATION.md): `conflict_policy: "compare_mtime"` accepts a write whose mtime
  is NOT OLDER than the server's and refuses only an older one, and stale
  `max_id`/`sync_id` values are not rejected at all
  (`TestIntegration_UploadBaseVersionProbe`) - `upload` has no optimistic
  locking. Divergence of an outbound upload is proved by the CLIENT, with a
  separate probe before the push (§6, `guardUploadTarget`); `24579` stays a
  second line of defence.
- **Error codes:** `8193` session vanished → re-auth; `4104/4105` OTP; `20481`
  client_version (as a string!); `28672` timeout/path; `36870` hash mismatch;
  `24579` conflict version; `24580`/`24581`
  (`ErrNameCaseCollision`/`ErrNameTypeCollision`) - verdicts about the NAME (a
  sibling differing only by case; the name is held by a node of ANOTHER type),
  terminal for a push since waiting changes nothing; `32772` (`ErrNodeBusy`) is
  the only measurably TRANSIENT code (reading a node the server is still
  settling; it reads normally a second later), so no run of it proves
  permanence. The full table is in INVARIANTS.md#result-codes.
  `8193` and `20481` are SESSION-level verdicts, not per-operation ones, so both
  map to sentinels shared with `auth` (`auth.ErrSessionVanished`,
  `auth.ErrClientVersion`) on BOTH axes - at connect (the pool pauses with
  `ErrPoolPaused`/`ErrPoolClientRejected`) and on an action turn of a live
  session (`isTerminal` stops `Run`, and the daemon prints the same hint -
  "re-login" / "update synosync"). A `20481` received on an action turn used to
  be an anonymous `*ServerError` consumed by the per-op escalation of
  deterministic refusals: every outbound op in the streak was marked
  `StateConflict` and dead-lettered (one-way - `localReconcile` never re-derives
  such a push), while the operator's only remedy (update the client) was reachable
  only through a reconnect.

## 3. Architecture (packages)

A single `synosync` binary with subcommands (the CLI and the daemon live in one
executable).

```
cmd/synosync/            - entrypoint; parses AND implements the subcommands
                           (the CLI is an IPC client; there is no internal/cli)
internal/sproto/         - pure wire codec: header, pstream (streaming enc/dec),
                           Conn + TLS upgrade, Legacy-TLV (TEST_CONN)
internal/actions/        - type-safe action client on top of Session
                           (catalog/transfer/resume/events). Split OUT of sproto
                           so sproto stays a pure codec with no import cycle on
                           auth/md4
internal/md4/            - own MD4 implementation (stdlib style, hash.Hash)
internal/transport/      - handshake, Session, connection pool, reconnect/backoff,
                           certificate pinning
internal/netctx/         - shared deadline/cancellation helper over net.Conn
                           (Watch/Err); the single owner of logic once duplicated
                           across auth/actions/transport
internal/auth/           - pairing (mode C + OTP), session reuse (mode A),
                           device_uuid, agent/version
internal/secrets/        - go-keyring wrapper (session token, pinned cert)
internal/store/          - bbolt: schema, buckets, baseline model, migrations,
                           durable queue, conflicts; also the settings blob
internal/servertree/     - top-down `list` walk of a view (the cold-start catalog)
internal/normpath/       - path normalization: NFC + probe-based case folding (§13)
internal/watcher/        - fsnotify + debounce + path filters
internal/engine/         - sync core: event stream, reconcile, conflicts,
                           operation queues, scheduler, cursors, local
                           scanner+hasher (scanner.go - no separate package)
internal/fsutil/         - atomic writes (temp+rename+fsync), perms, mtime,
                           no-replace rename
internal/daemon/         - lifecycle, single-instance lock, supervisor, assembly
                           of pool+engine+watcher, IPC handlers
internal/service/        - embedded systemd/launchd/Windows SCM units
internal/ipc/            - daemon↔CLI IPC (UDS; on Windows a named pipe with a
                           DACL, see §8), JSON-RPC
internal/config/         - settings type + validation, resolution of the state
                           directory (XDG_DATA_HOME/AppData) and of the daemon's
                           separate log directory (XDG_STATE_HOME /
                           ~/Library/Logs / LocalAppData); the settings
                           themselves are a blob in state.db, not a file
internal/backoff/        - jittered exponential delays
internal/logging/        - slog setup + size-based log file rotation
```

> There is no shared `internal/model` package: domain types live where they are
> used - `Node`/`Event`/`View`/`ResumeToken` in `internal/actions`,
> `Baseline`/the `View` record/`Op`/`Conflict` in `internal/store`, `Config` in
> `internal/config`. This is a deliberate choice against an empty aggregator
> package. Per-package detail: PACKAGES.md.

## 4. Connection model

sproto carries one request/response per socket at a time, and `long_poll` holds a
socket for up to 300 s. Hence several connections:

- **Event connection** - dedicated; runs `long_poll`/`pull_event`.
- **Worker pool** (N = 2-4 connections) - `download`/`upload`/`rename`/`remove`/
  `list`. The official client sends deletes and copies in parallel, hence the
  pool.
- Every connection is independent: TCP → `TEST_CONN` → `encrypt_channel` → TLS →
  `connect(session)` (mode A). Pairing (mode C) happens once; the token is reused
  by all connections.
- Reconnect with exponential backoff; `8193 session vanished` (and `20481`
  client version rejected) pauses the daemon and requires operator action -
  because of 2FA, a silent re-authentication without a fresh OTP is impossible.

## 5. State store (bbolt)

A single file, `state.db` (in `~/.local/share/synosync/` / `%LocalAppData%`), is
the only state file for everything: the durable sync state and all non-secret
settings. Secrets (session token, pinned-certificate fingerprint) live in the
system keychain. Buckets:

- `meta` - the key `config` (a JSON blob of all settings: `sync_root`, server
  host/port/user/`ds_id`, `device_uuid`, `log_level`, `log_format`, the
  `limits.*` substructure for resource tuning, and the `views` substructure -
  view selection, flat layout, excluded paths and excluded names; a missing key
  means "not configured / not paired"), plus `schema_version` (currently 12).
  Two additive, tolerantly decoded keys sit beside them: `paused` (the
  operator's durable pause state, so `synosync pause` survives a restart) and
  `last_stop` (the reason the daemon last stopped for a cause the operator must
  act on). The blob is decoded strictly (`DisallowUnknownFields`): unknown keys
  are rejected, so removing a field from the schema requires a store migration
  that rewrites the blob (§5.1).
- `views` - `view_id → {node_id, root_node_id, name, privilege,
  last_seen_sync_id, initial_sync_done, root_device, rescan}`.
  `initial_sync_done` records whether the cold-start catalog pull for the view
  has completed (absent = false; the v2→v3 back-fill set it true on existing
  views). `root_device` is the device id (Unix `st_dev` / Windows volume serial)
  of the view root observed in a healthy state, so the empty-scan mount-swap
  guard survives a daemon restart (absent/0 = not recorded yet). `root_node_id`
  is the view root's small integer node id (distinct from the file id in
  `node_id`). `rescan` lists subtrees queued for a narrowed cold start (an
  exclusion lifted, or a server move into the synced scope).
- `files` (the baseline, key `view_id\0path`) - `{file_id, file_type,
  server_hash (MD4), server_mtime, server_sync_id, local_hash, local_mtime,
  local_size, unix_perm, state}`.
- `byid` (index `view_id\0file_id → path`) - for move/rename by event.
- `queue` - the durable queue of operations (survives restarts).
- `conflicts` (key `view_id\0canonical_path`) - the durable list of keep-both
  copies `{canonical_path, copy_path, copy_file_id, detected_at}` (added by
  schema v2).
- `deadletters` - the durable trace of dropped (dead-lettered) operations: when
  the scheduler permanently discards an inapplicable op (a broken path, a
  pre-existing symlink, a doomed rename, a deterministic server refusal, …), it
  writes a record with the reason before the `Ack`, so a silent skip still leaves
  an observable trace (`RecordDeadLetter`/`DeadLetterLen`/`ListDeadLetters`; the
  count is shown in `status`; added by schema v6).

bbolt provides ACID transactions and B+tree indexes with zero cgo; mmap makes
reads cheap. Composite operations are atomic per transaction; batched writes go
through `store.Batch`/`Writer` (PACKAGES.md).

### 5.1. Schema migrations

`meta.schema_version` is the single version counter. An empty database is
initialised directly at `CurrentSchemaVersion`; an older database is stepped
through the ordered migrations; a version newer than the binary understands is
rejected (`ErrSchemaTooNew`). A version below the oldest known migration is
reported as `ErrCorruptRecord` with an `unlink` hint. Steps:

- **v1→v2** - records the bump (the `conflicts` bucket is created by bootstrap).
- **v2→v3** - back-fills `View.InitialSyncDone=true` on every existing view
  (views that predate the flag already sync incrementally and must not repeat
  the cold start).
- **v3→v4** - removes the dead `selective_sync` placeholder from the `config`
  blob. The step reads the blob generically (not through the strict
  `config.Unmarshal`, which would reject the unknown key under the new schema),
  strips `selective_sync` if present and rewrites the blob. It is idempotent and
  a no-op when the key is absent. This is the pattern of the **mandatory** rule:
  every removal or rename of a field in a persisted blob ships with a migration -
  an old blob must never fail on `DisallowUnknownFields`.
- **v4→v5** - drops the dead `profile` bucket (the profile-digest backend of the
  removed selective-sync placeholder, with no remaining readers) via
  `tx.DeleteBucket`. Idempotent: `bolt.ErrBucketNotFound` is ignored.
- **v5→v6** - records the bump for the new `deadletters` bucket (created by
  bootstrap).
- **v6→v7** - records the bump for the new persisted config field
  `limits.op_turn_timeout_s` (the per-turn deadline of engine actions), per the
  bump rule below. The field defaults to 0 (disabled) and is `omitempty`; no data
  is rewritten.
- **v7→v8** - adds `View.RootNodeID`. Nothing is rewritten: an existing view
  decodes it as 0 and the next `query_user_info` (run every sync cycle)
  re-resolves it.
- **v8→v9** - REMOVES the persisted `limits.upload_chunk_size` knob. The protocol
  has no sproto-level chunking for uploads (`resume_upload` carries the whole
  file in one content frame), so nothing read it, and its cross-check against
  `upload_resume_threshold` refused legitimate lower thresholds. The step strips
  the key from the blob so the first `Open` of an upgraded database repairs it;
  a blob that does not decode as an object is left for `config.Unmarshal` to
  report.
- **v9→v10** - changes no data. It adds `Config.Views` (view selection and flat
  layout), which decides every on-disk path: a binary that does not know it would
  compute `syncRoot/<view>/…` for a flat tree, find nothing, and push every file
  as a local deletion. The bump makes an older binary refuse the database with
  `ErrSchemaTooNew` instead of a misleading "corrupt config" that would send the
  operator to `unlink`.
- **v10→v11** - changes no data. It adds `Config.Views.Excluded` and
  `View.Rescan`; an older binary would sync - and upload into - exactly the
  subtrees the user excluded, so the version gate makes it refuse the database.
- **v11→v12** - changes no data. It adds `Config.Views.ExcludedNames` (name
  rules), for the same reason: an older binary would upload the names the user
  left out.
- **v12→v13** - raises a stored `limits.poll_deadline_slack_s` of exactly 15 (the
  old default) to 300; any other value is the operator's and is kept. A DSM answers
  an idle 300 s `long_poll` 20 to over 60 s late, so the old 315 s deadline cut idle
  polls short.

**Bump rule for changes to the `config` blob (downgrade hazard).** The blob is
read strictly (`config.Unmarshal` with `DisallowUnknownFields`), so the rule
applies not only to removing/renaming a field but also to **adding** one: a blob
written by a new binary with a new persisted field would fail on an old binary
with an opaque "unknown field" error rather than a clear `ErrSchemaTooNew`. To
make an old binary reject the database cleanly at the version check in `Open`
(before the strict decode), every addition/removal/rename of a persisted
`Config` field **must** bump `CurrentSchemaVersion`. Only additive fields that
every reader tolerates (separate meta keys such as `paused`) are exempt
(INVARIANTS.md#schema-rule).

## 6. Sync core (reconcile)

Summary; the per-file engine breakdown is in [`ENGINE.md`](ENGINE.md) and the
full text of every invariant in [`INVARIANTS.md`](INVARIANTS.md).

**Three sources of truth.** *Remote* (the event stream plus a per-view cursor),
*local* (watcher plus periodic scan) and the *baseline* (the store's record of the
last agreed state). A remote change whose local side still equals the baseline is
applied (download/mkdir/rename/remove); a local change whose remote side still
equals the baseline is pushed (upload/mkdir/rename/remove); a change on both
sides is a conflict. A self-echo (an event committed by our own session) is not
re-applied - it only advances baseline facts and the cursor, and never stamps
`StateSynced` over a record a queued op still owns. Only the apply path records
new agreement, through `markSynced` (INVARIANTS.md#marksynced).

**Durable queue.** Both reconcile passes enqueue the operation durably BEFORE
mutating the baseline, and a create/modify commits both in one transaction, so a
crash leaves a replayable op rather than an orphaned pending record
(INVARIANTS.md#enqueue-first). Permanently inapplicable ops are dead-lettered
with a durable trace atomically with their `Ack`; a corrupt queue record is
quarantined and a repair sweep re-derives its owner.

**Never-lose doctrine.** A destructive apply (an overwrite, a local or server
delete, a rename over an occupant) must prove that its target is the last-agreed
base: a synced FILE record with a non-empty `local_hash` is the only trusted
base. Diverged, untracked or unprovable content is kept as a keep-both
`_Conflict` copy instead; a duplicate is an accepted cost, silent loss is not.
Deletions apply directly (no local trash); recovery relies on the server-side
recycle bin. In a rename-vs-delete race, the delete wins - whichever
event the server accepted first.

**The server is not the arbiter of divergence.** `upload` with
`compare_mtime` overwrites on any not-older mtime and ignores base versions, so
an outbound `OpUpload` first probes its target (`guardUploadTarget`), funnels
divergence into keep-both, and stays queued when the probe is inconclusive
(finding H1, INVARIANTS.md#upload-guard). A server `remove` of a directory is
recursive, so an outbound delete first enumerates the server subtree
(`verifyServerSubtreeAgrees`) and refuses on any unknown child, moved hash or
kind, or foreign `file_id` (finding H7). Absence on the server is never inferred
from one reply: a mutating reply must carry a verdict, and an absence verdict
needs repetition across passes plus a wall-clock floor
(INVARIANTS.md#reply-evidence).

**Cold start** (first pairing or lost store) walks the view top-down with `list`
(`internal/servertree`), one turn per directory, instead of replaying the event
log. The walk runs on its own goroutine and streams listings to the loop, which
plans each directory against the local scan+hash and drains as it goes
(`coldstartstream.go`); a local file whose hash matches is adopted without a
transfer, missing content is downloaded, locally-unique content uploaded, and
nothing is deleted without a baseline. The cursor is the log head read BEFORE
the first listing and is persisted with the completion flag at the end, so
replaying forward re-asserts what was seen and delivers what changed mid-walk.
An unevidenced listing is never read as an empty directory.

**Incremental sync** reads `pull_event` forward from the cursor. The log is
compacted per node and paged by a fixed `sync_id` window, so an empty page proves
one window, not the end of the log (INVARIANTS.md#pull_event-is-a-log). Events
are applied in ascending `sync_id`; a `move` event carries its source in `path`
and its destination in `destination.path`.

**Executor and serialization.** Two queued ops conflict iff their resolved
on-disk paths are equal or ancestor/descendant; conflicting ops serialise (a
rename on both endpoints), independent ops run in parallel over the worker pool
(`serialize.go`). Directories are created parent-first and removed
children-first. Server paths are untrusted: they are containment-checked,
NFC-resolved to the on-disk spelling, and applied with no-follow resolution;
names the host cannot represent are skipped with a trace
(INVARIANTS.md#path-safety). Selective sync is implemented as path exclusions
and name rules (`synosync exclude`, ENGINE.md#excludego).

## 7. Watcher and the local side

The watcher is a latency optimization over the authoritative periodic scan: a
missed event is never a correctness gap, because the scan re-derives the truth.
On Linux and macOS/BSD it uses fsnotify (inotify; kqueue, not FSEvents) with
manual recursive registration, a debounced coalescing wake, a per-GOOS watch cap
(darwin: 4096 distinct directories) and a system-limit fallback to scanning. On
Windows a dedicated backend runs ONE recursive `ReadDirectoryChangesW` on the
root, since per-directory watches cost a 64 KiB buffer each. A filter drops
self-induced noise (staging temps, `_Conflict` copies, editor temp files), and
symlinks are never registered.

The local scanner is cheap by default - it short-circuits on `(size, mtime)` and
rehashes in full only periodically - and guards against an unmounted or swapped
root: an empty scan over existing baselines is debounced (a count of scans AND a
wall-clock spacing), and the root's device id is remembered so a changed device
suppresses the scan until content evidence confirms the tree. Local applies go
through `internal/fsutil`: staged temp files, durable mkdir/rename/remove,
no-replace and replace renames, and fd-bound metadata. Detail: PACKAGES.md
(`internal/watcher`, `internal/fsutil`) and ENGINE.md (`scanner.go`,
`localreconcile.go`, `emptieddir.go`).

## 8. Daemon, IPC and CLI

`synosync daemon` runs in the foreground (or as a systemd/launchd/Windows
service unit rendered by `internal/service`): single-instance lock → IPC bind →
serve → sync loop with backoff. Settings are read from `state.db` and secrets
from the keychain; there is no configuration file. Because `state.db` holds an
exclusive bbolt lock, `pair`/`login` under a running daemon get `ErrLocked` and
print a "stop the daemon first" hint.

The IPC is local-only newline-delimited JSON-RPC with bounded requests and a
versioned envelope: a `0600` Unix socket, or on Windows a named pipe with a
current-user-only DACL whose name is derived from the state path and a per-user
secret (against pipe squatting). `status` is cached and never touches the
network; the O(tree) counts are the only part allowed to fail, and they degrade
(`CountsStale`) rather than fail the verb. It reports pending and stuck ops,
`Unresolved` divergences, the `Dropped` dead-letter count, disk-full, pause
state and exclusions. The daemon logs through slog to a size-rotated file in the
per-OS log directory. The CLI subcommands (`pair`, `login`, `daemon`, `status`,
`sync`, `pause`, `resume`, `conflicts`, `drops`, `exclude`, `config`, `log`,
`service`, `unlink`, `version`) are described in README.md; package detail is in
PACKAGES.md (`cmd/synosync`, `internal/daemon`, `internal/ipc`,
`internal/service`).

## 9. Secrets and security

- Split storage: non-secret settings live in `state.db` (bucket `meta`, key
  `config`); secrets live in the system keychain. There is neither a config file
  nor any clear-text secret on disk.
- `secrets` (go-keyring): the session token (key = server+user) and the pinned
  certificate fingerprint (SPKI). The password is NOT persisted: `pair`/`login`
  use the entered password in memory for one auth turn (2FA would require an OTP
  anyway), so the keychain holds exactly two durable secrets per (host, user).
  The password accessors remain in the wrapper, and `unlink` calls
  `DeletePassword` as a belt-and-braces no-op.
- Certificate pinning: at pairing, the server certificate is taken from the TLS
  handshake and the fingerprint of its public key (SPKI) is stored in the
  keychain; on every connect, `InsecureSkipVerify:true` plus a manual check of
  the leaf against the pin (a MITM with a different certificate fails). The pin
  check hangs on `VerifyConnection`, not `VerifyPeerCertificate`: `crypto/tls`
  skips `VerifyPeerCertificate` on a resumed handshake, so only
  `VerifyConnection` covers it - otherwise session resumption would be a latent
  pin bypass. TLS only, no plaintext actions, no custom login crypto.
- **What is pinned, and pin rotation.** The pin targets the **public key of the
  leaf certificate (SPKI, SHA-256)**, not a fingerprint of the full DER: this
  survives a routine certificate renewal with the same keypair (common for DSM
  behind Let's Encrypt with 90-day rotation) without re-pairing. Implemented in
  `internal/transport`: a new pairing pins the SPKI directly (`spki-sha256:`
  prefix), while the format-aware verifier still accepts legacy full-DER pins
  (`sha256:` prefix) and migrates them to SPKI transparently on the first
  successful reconnect with an unchanged certificate.
- **Recovery from a pin mismatch.** A mismatch is either a MITM or a legitimate
  certificate change. Silently trusting the new certificate is not allowed, so
  the daemon pauses, logs an explicit diagnostic and requires a confirmed
  re-pairing (`pair`); `status` shows the reason. There is no "TOFU update"
  without the user.
- **Protocol provenance (clean room).** `sproto` is an independent
  implementation based on observing the wire; no private specification or its
  artifacts (names, constants, code tables) are copied into the code. This
  compliance requirement is recorded in `NOTICE` (dependency attribution) and
  `PROVENANCE` (the clean-room method and the non-derivation statement) and is
  reviewed alongside the Apache-2.0 license; CI additionally enforces that all
  three files (`LICENSE`/`NOTICE`/`PROVENANCE`) exist.
- **License in release artifacts (Apache-2.0 §4(d)).** Apache-2.0 requires a copy
  of the license and `NOTICE` to accompany derivative works, so
  `scripts/release.sh` copies `LICENSE`/`NOTICE`/`PROVENANCE`/`README.md` into
  each platform's staging directory next to the binary before packaging - every
  `tar.gz`/`zip` carries the license and `NOTICE`; a missing source file at the
  repository root fails the build. A CI step builds one platform's archive and
  checks that `LICENSE`/`NOTICE` are in its manifest (checking only the
  repository would not cover the artifact itself).
- **Signing and provenance of release artifacts (supply chain).** `SHA256SUMS`
  next to the archives protects only against accidental corruption, not
  substitution: whoever replaces an asset can replace the manifest too. So a tag
  release also signs SLSA build provenance for every artifact (all platform
  archives plus `SHA256SUMS`) with GitHub-native
  `actions/attest-build-provenance` - a keyless Sigstore signature using the
  job's OIDC token (`permissions: id-token: write` + `attestations: write`,
  least-privilege, on the release job only). The attestation binds each artifact
  to the repository, workflow and commit, which a hash manifest alone cannot.
  GitHub-native attestation was chosen over hand-rolled cosign/Sigstore: zero Go
  dependencies, a few lines of workflow, one-command verification. Users verify
  an artifact with
  `gh attestation verify <archive> --repo mfilippov/synosync` (instructions in
  README.md). The Windows binaries are additionally Authenticode-signed.
  Releasing from a dirty tree is forbidden: a `git describe --dirty` suffix fails
  `scripts/release.sh` (override `ALLOW_DIRTY=1`, for local builds only), so a
  published artifact always matches committed state.

## 10. Dependencies

`go.etcd.io/bbolt`, `github.com/fsnotify/fsnotify`,
`github.com/zalando/go-keyring`, plus `golang.org/x/{sys,term,text}` (no-follow
`openat`, no-echo password input, `unicode/norm` NFC path normalization) and
`github.com/Microsoft/go-winio` (pure Go, no cgo; imported only by
`listen_windows.go` on Windows for the DACL-restricted named-pipe IPC, §8 - not
compiled on Linux/macOS). Everything else is the standard library (JSON
settings, `crypto/tls`, `crypto/rand` for UUIDs, our own MD4). Pure Go without
cgo → trivial cross-compilation for linux/darwin/windows × amd64/arm64.

## 11. Implementation order

The full architecture was designed up front and implemented in a deliberate
sequence. **All phases 0-10 are implemented.**

0. **Skeleton** - go.mod, dependencies, config, logging, lint/CI.
1. **sproto core** - header, streaming PStream encoder/decoder, TLS upgrade,
   `Conn`, MD4, round-trip tests.
2. **Auth/transport** - handshake, pairing (mode C + OTP), session reuse
   (mode A), `device_uuid`, keychain, certificate pinning, connection pool and
   reconnect.
3. **Catalog and transfer actions** - `query_user_info`, `list`, `query_node`,
   `download`, small `upload`, mkdir, `rename`, `remove`.
4. **Large files** - `resume_get_token`/status, `resume_upload`,
   `resume_download` with streaming.
5. **Store** - bbolt schema, baseline model, migrations.
6. **Sync engine** - event stream (`long_poll`/`pull_event`), remote apply,
   reconcile, keep-both, self-echo, cursors, durable queue, cold start.
7. **Watcher/scanner** - fsnotify + debounce + filters, real-time local diff.
8. **Daemon/IPC/CLI** - lifecycle, single instance, UDS/named pipe, all
   subcommands, daemon logging.
9. **Hardening** - reconnect/backoff, error mapping, perms/mtime, Unicode and
   case normalization, validated resource limits (`Config.Limits`), the cheap
   `(size, mtime)` scan, SPKI pinning.
10. **Packaging** - cross-build, release archives with `SHA256SUMS` and
    provenance, service units, documentation, integration tests against a test
    DSM.

## 12. Testing

- Unit: PStream round-trips, MD4 vectors, the header, the reconcile state
  machine (property tests).
- A mock sproto server for engine tests (golden scenarios: modify/move/remove,
  conflict, self-echo).
- Integration against a real DSM stand (behind a build tag and env gate),
  replaying the happy path end to end (INTEGRATION.md).
- **A fake must never be kinder than the measured server.** The root cause found
  by the two-client round: EVERY keep-both funnel had full unit coverage -
  against a fake that returned `ErrConflictVersion`, a verdict a live DSM gives
  only for an OLDER mtime, i.e. practically never. Wherever a test relies on
  server semantics, the fake models what was MEASURED (recursive `remove`, the
  mtime policy of `upload`).
- **The never-lose invariants of §6 are truly exercised only by scenarios with
  TWO live engines** (`test/integration/twoclient*_test.go`): a single-client
  harness has no second base that could diverge, so the relevant shapes are
  structurally unreachable for it.
- **The coverage threshold is an automated gate.** The ≥80% target for the listed
  `internal/*` packages is enforced: CI **fails** below the threshold (the
  `make cover-check` step → `scripts/coverage-gate.sh` parses the per-package
  `coverage: NN.N%` lines from `go test -cover` for the package list), rather
  than merely producing `coverage.out`. Quality checks block delivery and are not
  bypassed.
- **The threshold is per build context, not merely per package.** A run measures
  only the files its GOOS compiles, so a Linux-only gate never covers
  `_windows.go`/`_darwin.go` at all - they are in no denominator. The native
  Windows/macOS jobs run THE SAME gate over their own profile with a separate
  `CROSS_THRESHOLD` (75) - separate precisely so it can be ratcheted up toward 80
  rather than lowering the authoritative `LINUX_THRESHOLD` to meet it. The same
  principle applies to the two other gates: `lint-cross` and `vulncheck-cross`
  run per release GOOS, since neither linting nor reachability analysis sees
  files of another build context (INVARIANTS.md#coverage-and-cross-goos-gates).
- **Reproducibility and reach of the CI gates.** `govulncheck` is pinned to a
  specific version (`GOVULNCHECK_VERSION`, like golangci-lint), not `@latest`, so
  the security gate is deterministic, and it runs for all three release GOOSes
  (`make vulncheck-cross`): go-winio is imported only by `listen_windows.go` -
  exactly the DACL mechanism of IPC security - and a Linux-only scan never
  analysed it. The OS matrix (Windows/macOS) runs `go test -race` plus its
  context's coverage gate, so per-OS code (winio, `lock_windows`, `openflags`)
  is checked by the race detector and the threshold natively, not only on Linux.
  A separate `go build`/`go vet -tags integration ./test/integration/...` step
  (no live DSM) type-checks the env-gated integration harness, so a signature
  refactor in `engine`/`actions`/`store` cannot silently rot it.

## 13. Open questions and accepted residuals

Each bullet is an open question or a consciously accepted limitation. Code
comments that cite "DESIGN §13" refer to one of these by subject.

**Protocol and server semantics**

- **Pagination of `list`/`view_list` on a live DSM is unconfirmed.** Neither
  `node_list` nor `view_list` carries a count, end marker or cursor, so "absent
  from the reply" is not "absent on the server" if the DSM ever pages or
  truncates. Every absence prover therefore corroborates with a path-addressed
  `QueryNode` of the node's OWN path (pagination-proof): for id-less callers by
  occupancy (any answering node refutes absence, `ErrPathError` confirms, any
  other failure is inconclusive), and a verdict counts only after
  `absentNodeConfirmations` agreeing probes, at most one per drain pass, at least
  `absenceStreakMinAge` (60 s) apart, within `absenceStreakMaxSpan` (10 min);
  `markSynced` clears a path's streak and a verdict older than
  `absenceVerdictMaxAge` passes restarts it (`absencestreak.go`). This is because
  28672 conflates "no such path" with a server-side timeout. Still open: a probe
  cannot prove that a node moved out of a page slice, and whether the DSM pages
  at all needs a live assert.
- **Completeness of a `list` enumeration is not provable (finding H7,
  continued).** `verifyServerSubtreeAgrees` reads a successful non-empty listing
  as the directory's COMPLETE contents before a recursive `remove`. A page that
  lost a child freshly created by another client would make the subtree look
  agreed, and there is no path-addressed probe for "nothing else exists". Closing
  it needs a server cursor/count (unmeasured) or two agreeing walks separated by a
  pass and `absenceStreakMinAge`, whose cost is not yet justified. Every other
  shape (unlisted reply, failed descendant listing, unknown name, cap exceeded)
  stays transient or divergent; only an incomplete but syntactically successful
  page could lose data.
- **An inbound op rejected forever by an unmapped server code gets no terminal
  exit.** The "deterministic server refusal" escalation is deliberately
  outbound-only: dropping a push deletes nothing, while dropping an inbound op
  would discard a server change whose event is already consumed. Such an op stays
  transient but observable (`Stuck` + a Warn with `server_errno`). The live DSM's
  quota/ACL error vocabulary is unconfirmed.
- **Per-file content hash on `event_list`/`query_node` - measured present.** On
  DSM 7.2.2 / Drive Server 4.0.3 the MD4 is present on `query_node`, on
  `pull_event` events and in the `upload` reply (directories carry none). The
  hash-less degradations are kept only as insurance for other builds: the
  cold-start match falls back to size+kind with a Warn, `probeUploadedNode` to
  identity+size only on the lost-reply arc, and the resolver's false-conflict fast
  path has no degradation (an honest keep-both). On such a server an equal-size
  divergence could be recorded as agreement.
- **Positional self-echo adoption with no server position (finding H3,
  continued).** `selfEchoSupersedesRecord` compares `ev.SyncID` with
  `Baseline.ServerSyncID`, but `markSynced` does not write a server position, so a
  record created by our own push on a fresh path adopts ANY positioned echo. The
  measured H3 shape carries a real stamp; the narrow failing window needs two
  pushes to one path with the node replaced in between and a batch break between
  their echoes. A real fix needs the outbound apply to record its node's server
  position; whether the `upload` reply carries one is unmeasured.
- **Fold-equal siblings are impossible on the server (errno 24580).** DSM lookup
  is case-SENSITIVE but uniqueness is case-INSENSITIVE: creating `notes.txt` next
  to `Notes.txt` is refused with 24580. On a case-sensitive local host where both
  spellings coexist, one of them can never be pushed. No data is lost; 24580 and
  24581 are typed name verdicts, terminal for a push after the short
  `nameRejectionDrains`/`nameRejectionMinAge` confirmation, with a worded
  dead-letter trace.
- **The root name `id:*` aliases id-addressing - protected conservatively.** The
  same `path` parameter carries both view-relative paths and `id:<file_id>`
  addressing, so a legal Linux/macOS name `id:12345` at a view ROOT could have
  addressed another node. `scanView` (`isRootIDAlias`) protects such paths and
  reconcile skips server events with a root `id:*` path. Measured
  (`TestIntegration_IDAliasProbe`): this server parses the path literally, so the
  class is unreachable there; lifting the cheap, fail-safe gate is a separate
  decision.
- **Moves without a reported KIND are not rescued.** `rescueUncontainableSource`
  rewrites a move from an uncontainable source into a create, and a create
  dispatches on `ev.IsDir`, which a sparse move does not report. Rescue therefore
  requires `Event.KindReported`; otherwise the event is skipped with a Warn
  (a download of a directory id would wedge forever). Measured: this DSM always
  reports kind and `from_path` on moves.
- **Moves without a reported SOURCE are not rescued.** An empty `from_path` says
  nothing about whether the node is tracked here, and rewriting such a rename
  into a create would duplicate a tracked directory. Rescue requires a non-empty
  `FromPath`.
- **Descendants of an unrepresentable DIRECTORY do not arrive with it.** When a
  move out of an unrepresentable source is rewritten into a create, a directory
  arrives empty: its children's creates were skipped earlier and a server rename
  emits nothing for them. Workaround: touch the contents on the server/peer, or
  `unlink` + re-pair to re-run the cold start. A full fix would need durable
  memory of unrepresentable subtrees.
- **Unrepresentable names are reachable from the server (measured).** The live
  DSM accepts and stores verbatim names with a trailing dot or space (`dot.`,
  `notes.txt.`), so the class is real and the remedy ("rename on the
  server/peer") is a working scenario. The server does not alias the trimmed
  spelling, but the `hostRepresentablePath` gate stays, since a Windows client
  would otherwise address something other than what it creates.

**Transfers and codec**

- **Close-before-join on an errno/offset-less upload reply; bounded join on
  success.** `Upload`/`ResumeUpload` write the body frame from a goroutine. A
  reply without `errno`/`offset` is checked BEFORE the join and handled by
  poison+close, so a body writer blocked on backpressure is released rather than
  wedging the worker (with `limits.op_turn_timeout_s` = 0 by default). On a valid
  success reply the join is bounded by `bodyJoinGrace` (30 s), after which the
  connection is poisoned and the reply treated as `ErrUnexpectedReply`
  (transient).
- **The frame budget counts NODES, not only payload bytes.** Each decoded node
  costs `max(wire, nodeCostBytes=128)` of the 64 MiB budget, so a frame of ~1M
  empty maps fails mid-frame with `ErrFrameTooLarge` before any preallocation in
  `actions`; the node ceiling drops to 512Ki (~45k records per reply, four times
  the largest observed page). The client cannot request smaller pages, so an
  over-budget `pull_event` page sticks at its `sync_id` and a directory with more
  than ~55k children is permanently unlistable (its absence provers stay
  inconclusive); `warnIfDecoderCap` makes this visible. Discarded bytes are capped
  too (`Decoder.MaxDrainBytes`, default 64 MiB, `ErrDrainTooLarge`), at the cost
  of one re-dial.

**Local filesystem and platforms**

- **`SetMeta` on Linux depends on a mounted `/proc`.** `unix.Futimes` is
  implemented as `Utimes("/proc/self/fd/N")`, so without `/proc` (chroot, minimal
  container) every download promote fails permanently. Only the diagnostic was
  fixed (an `ENOENT` names `/proc`); a raw `utimensat` syscall would need `unsafe`,
  and a path-based call would reopen the TOCTOU window the fd binding closes.
- **`ERROR_SHARING_VIOLATION` on a Windows promote.** `fsutil.RenameReplace`
  retries once only on `FILE_ATTRIBUTE_READONLY`; a sharing violation from an
  indexer or antivirus stays an ordinary transient, healed by the next drain. A
  dedicated short retry is a candidate for separate work.
- **A Windows replace loses the destination's read-only attribute.**
  `RenameReplace` clears `FILE_ATTRIBUTE_READONLY` so `MoveFileEx` stops failing
  with ACCESS_DENIED, and the result carries the staging temp's attributes.
  `SetMeta` applies wire permissions only on Unix, so the user's read-only mark is
  lost on each such inbound modify; data is not at risk.
- **Windows no-follow upload verification is fail-closed on filesystems without
  `GetFinalPathNameByHandle`.** Windows has no per-component `O_NOFOLLOW`, so the
  only barrier against a symlink redirect is comparing the open handle's
  canonical path with the literal path. On filesystems that do not implement the
  call (SMB/WebDAV redirectors, some user-mode mounts) the open is refused, one
  Warn per view is logged and that view's uploads are disabled until the sync
  root moves to a supporting filesystem.
- **Extended forms of Windows reserved names are not gated.** `filepath.IsLocal`
  rejects bare `CON`/`NUL`/`COM1` but passes `con.txt`, `LPT1.log`, `conin$.txt`,
  which create ordinary files on current Windows. Gating them would reject
  legitimate names everywhere; if a build ever aliases them to devices, the fix is
  extending the same gate.
- **8.3 short-name aliasing is detected only on CREATING verbs.**
  `rejectShortNameAlias` guards the materializing resolver; verbs that address an
  existing record (`applyRemove`, the source of `applyRename`) do not carry it.
  For anything this client synced the class is closed transitively; a record
  created by an older build (or a hand-edited store) could still address a short
  name, where the divergence gate would almost certainly keep-both. Whether the
  DSM accepts literal 8.3-shaped names is open.
- **Case-fold detection probes only the sync ROOT, and the flag is per process.**
  `probeSyncRootCaseFold` overrides the GOOS default with the root's empirical
  behaviour, but a view directory on a separate mount with a different fold
  behaviour uses the root's flag. Operational contract: do not mix case-fold
  behaviours under one root; a per-view fold state is deferred.
- **NFD/NFC name collisions in one directory leave both files unsynced (F4).** On
  byte-preserving filesystems two on-disk spellings of the same NFC string fold to
  one key; both files are protected (no upload, remove or move) with a Warn and a
  durable trace in `status` (Dropped), until the user renames one. Full byte-level
  matching is deferred.
- **Case folding approximates `strings.ToLower`, not the OS tables.** Known edge
  cases (U+0130 İ, U+1E9E ẞ, Cherokee case pairs) give either a false collision
  (caught by the F4 guard) or a false distinction (a spurious delete+create for
  those rare characters); neither loses data.
- **The `Lstat`→`rename` window in `renameNoReplaceGeneric` on network
  filesystems.** The fallback is reached only on filesystems that reject the
  atomic no-replace flag (NFS < 4.2, CIFS/SMB, some FUSE), where the window is a
  network round-trip; a file created at the target inside it is replaced without
  a keep-both copy. Local filesystems (the supported sync-root configuration)
  support the flag.
- **A selective fsync failure of a non-leaf created directory is not retried.**
  `mkdirAllDurable` attempts one flush per created ancestor and always re-flushes
  the leaf parent on retry, but an intermediate ancestor whose flush failed while
  the leaf's succeeded is not re-flushed later. Fixing it needs memory of failed
  directories across calls; the window is narrow.
- **The watcher's `watched` map is maintained only when a cap is active.** The
  map exists solely for the watch cap, so insertion is gated on `maxWatches > 0`;
  on uncapped platforms nothing is recorded, avoiding a slow leak in a
  long-running daemon, rather than pruning on Remove/Rename.
- **The `st_dev` guard is blind to same-device mounts.** The device id identifies
  the filesystem, not the mount, so unmounting a bind mount or a btrfs subvolume
  from the same device is not seen by it; the empty-scan debounce still covers
  that case. Linux `statx(STATX_MNT_ID)` would fix it but has no macOS
  equivalent.
- **A single view: device renumbering under an EMPTY root finds no
  corroboration.** Leaving suppression requires a neighbouring view on the same
  filesystem that adopted the same `old→new` transition by content evidence;
  with one view (or all views empty) there is no witness and the scan stays
  suppressed. Adopting without evidence could delete the whole server view. The
  Warn names the remedies: restore the mount, let one tracked file reappear, or
  `unlink` + re-pair.
- **A view whose device AND entire content changed stays suppressed until the
  operator acts.** Adopting a new device (or a server rename of the view) needs
  positive content evidence - a matching hash (or non-zero size) of at least one
  synced file. A migration in which every tracked file changed gives none, nor
  does a view tracking only empty files. The failure direction is deliberate: a
  stopped sync rather than a mass delete.
- **A network `$HOME` with lock-breaking mount options defeats the
  single-instance lock.** Both lock implementations are advisory and live under
  the state directory. Default NFS/SMB mounts propagate them, but `local_lock` or
  `nolock` makes each host lock locally, so two machines could run daemons over
  one `state.db`. Durable fencing is not built; keeping the state directory on a
  local filesystem is the operational contract.
- **Windows SCM behaviour on a second `SetServiceStatus` after `SERVICE_STOPPED`
  is not verified live.** `Execute` no longer sends `Stopped` itself, so the only
  `SERVICE_STOPPED` carries `ERROR_SERVICE_SPECIFIC_ERROR` plus the daemon exit
  code that `sc.exe failure` recovery relies on. MSDN calls a second status after
  STOPPED undefined; the fix avoids the case rather than relying on it. The
  manual check is in INTEGRATION.md.
- **Permission-only changes are out of scope.** A modify op is derived only from
  a content difference; `unix_perm` is stored and sent but is not a trigger. The
  cheap `(size, mtime)` scan cannot see a `chmod` (it changes ctime), permission
  sync was never a goal, and it cannot lose data. Pinned by
  `TestLocalReconcilePermOnlyChangeNotSynced`.

**Engine: deletes, renames and recovery**

- **Deleting a directory costs two server turns per file.** `diffLocal` orders
  deletes children-before-parents, so `rm -rf` of N files gives N removes, each
  costing a `QueryNode` precondition plus the `remove` - 2N turns occupying the
  whole pool (about ten minutes for 10,000 files at 50 ms RTT). A single recursive
  `remove` after a full `verifyServerSubtreeAgrees` walk was not adopted: the
  walk is cheap only because of this order, per-op deletes keep partial progress
  and per-file recoverability, and the gain needs measuring on a live stand.
- **An H7 precondition refusal freezes the directory record.** When
  `verifyServerSubtreeAgrees` refuses a directory delete, the record becomes
  `StateConflict` with a dead-letter naming the disputed child. Nothing moves it
  out of that state until an inbound apply runs the path through `markSynced`, so
  the divergence is visible (`Unresolved`) but does not heal itself. Data is
  intact on both replicas.
- **An id-less directory record left by a refused inbound delete stays
  `StatePending`.** Raising it to `StateConflict` would make `resolveParentID`
  treat it as terminal (`errParentConflicted`) and dead-letter every op beneath
  it; such a record means "a mkdir that will yield an id has not applied yet", so
  it stays pending with a Warn.
- **Compensating a dropped inbound DIRECTORY rename repairs the node but not its
  subtree (finding H2).** The drop cascades descendant records away and the
  compensation queues one inbound `OpMkdir` at the destination; children the
  server still holds there are recorded nowhere until a forced cold start. The
  dead-letter reason and a Warn declare it. The proper fix - listing the node at
  drop time and queuing an op per child - is separate work.
- **Cross-batch rescue of a deferred inbound download.** A download for a path
  with a queued outbound `OpUpload` is deferred so the upload's verdict arbitrates
  the path. A server move of that node in the SAME batch is rescued
  (`rescueDeferredMove`); a move arriving in a later page or cycle is not, since
  linking them needs durable state. The dropped rename's compensating download
  (H2) still materializes the node; only keep-both arbitration is lost.
- **Rescuing a deferral of a TRACKED node yields a duplicate instead of
  keep-both.** The rescue leaves the user's edit untracked at the source (the next
  scan pushes it as a separate node) and lands server content at the destination:
  duplication and churn rather than silent divergence. Keep-both at the
  destination would need the op and its baseline on one path, which the byid
  redirect in `markUpsertPending` splits.
- **Crash replay of an applied inbound rename: "source present" is not "move did
  not run".** A crash between `fsutil.Rename` and the baseline writes, plus a new
  file the user created at the old path, makes the replay move that new file and
  give it the node's identity. Telling the cases apart would need a full hash on
  every rename replay. The outcome is non-destructive: the next scan pushes the
  difference, and the server overwrite is recoverable from the recycle bin.
- **Re-tracking a dead-lettered rename freezes the path for outbound.**
  `resolveRejectedRename` re-tracks the still-live file as `StatePending`, but
  `localReconcile` drives only `StateSynced` records, so the path drops out of
  outbound sync until the next server event (or until the blocking symlink is
  removed). Neither stamping synced nor re-enqueuing an unexecutable upload is
  acceptable; an honest fix needs a "re-verify" baseline state.
- **The cross-pass doom mark lives in process memory.** `carriedDooms` carries a
  doom that a drain proved but could not record (a failing store) across a pass
  boundary; a daemon restart in exactly that window loses it. A durable marker
  would need a persisted field for a window that opens only when the store is
  already failing.
- **The repair-sweep trigger is in memory.** The sweep that restores
  "enqueue before baseline mutation" after a corrupt queue record is quarantined
  is armed by this store handle's `QuarantinedOps` counter; a daemon death between
  the (durable) quarantine and the sweep leaves the path frozen. The sweep infers
  direction from the record: a local edit re-derives a push, a
  `markUpsertPending` shape re-derives a download (even with nothing on disk), and
  anything ambiguous stays frozen with a Warn. A server directory whose mkdir was
  quarantined is untracked; if empty, the divergence persists until the next
  event or cold start.
- **Corrupt records skipped by list/scan store readers do not mark the result
  degraded.** `ScanConflicts`, `ScanBaselinesForView` and `ScanPending` skip an
  undecodable record with a Warn and report success, so `conflicts` and `status`
  counts can be silently short. Failing the whole verb would break them exactly on
  a damaged store; `unlink` is the documented exit from systemic corruption.

**Exclusions (selective sync)**

- **Exclusions are by PATH.** Selective sync is implemented as path exclusions
  and name rules (`synosync exclude`, ENGINE.md#excludego). Renaming an excluded
  path's ancestor (on the server, or locally, which pushes the rename) carries
  the subtree out of exclusion without a rescan; a sparse move with no
  `from_path` out of an excluded path is not recognised as one; a file moved
  locally INTO an excluded folder reads as deleted and its server copy goes to
  the recycle bin. Deleting a folder locally whose server copy holds a filtered
  file is refused by `verifyServerSubtreeAgrees` (an unknown child), so it stays.

**Testing and architecture**

- **The cold-start integration scenario scales with view history and size.**
  Scenarios that do not test cold start mark the view as already synced at the
  current log tail (`skipColdStart`) and run on the incremental path, so they no
  longer depend on accumulated history; `KeepBothColdStart` still runs a real cold
  start and scales with the view. The 10,000-child capture
  (`SYNOSYNC_IT_BIG_LIST`) leaves 10,000 tombstones in the log permanently, so it
  should run against a dedicated view.
- **Cohesion of the `Engine` type.** The engine is split into files by axis
  (`inbound_*`, `outbound_*`, reconcile/executor/scanner), but all ~270 methods
  hang on one `*Engine` with ~69 fields, so cross-file invariants ("only
  `markSynced` records agreement", "enqueue before baseline mutation") are held
  by discipline and review, not the compiler. Splitting into subpackages with
  narrow interfaces was rejected on cost/risk given the size of the pinned test
  corpus; new state clusters go into owner substructures with limited visibility
  (like `absenceMemo`), and the decision is to be revisited if active engine
  development resumes.
