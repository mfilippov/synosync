# Package details

One section per package, including the bug history each rule encodes. `internal/engine` has its own file,
`docs/ENGINE.md`.

## Contents

- [`cmd/synosync`](#cmdsynosync)
- [`internal/config`](#internalconfig)
- [`internal/logging`](#internallogging)
- [`internal/md4`](#internalmd4)
- [`internal/normpath`](#internalnormpath)
- [`internal/sproto`](#internalsproto)
- [`internal/secrets`](#internalsecrets)
- [`internal/transport`](#internaltransport)
- [`internal/auth`](#internalauth)
- [`internal/netctx`](#internalnetctx)
- [`internal/actions`](#internalactions)
- [`internal/store`](#internalstore)
- [`internal/fsutil`](#internalfsutil)
- [`internal/watcher`](#internalwatcher)
- [`internal/ipc`](#internalipc)
- [`internal/daemon`](#internaldaemon)
- [`internal/service`](#internalservice)
- [`internal/engine`](ENGINE.md)

## `cmd/synosync`

- `cmd/synosync` - CLI entrypoint. `run(args, stdout, stderr) int` is the
  testable dispatcher; subcommands register in `commands()` and are all
  functional: `version`, `pair`, `login`, `daemon`, `status`, `sync`, `pause`,
  `resume`, `conflicts`, `config`, `log`, `service`, `unlink`. Settings live as one JSON
  blob in `state.db` (`store.GetConfig`/`PutConfig`) - there is no
  `config.json`. Side effects route through injection vars (`openKeyring`,
  `openConfigStore`, `newPrompter`, `resolveDaemonPaths`, `openDaemonStore`,
  `runDaemon`, `ipcDial`, `openLogWriter`) so flows are unit-tested with mocks.
  Every subcommand rejects extra positional args (exit 2). A busy store
  (`store.ErrLocked`) prints a "stop the daemon first" hint; `config get` falls
  back to the daemon's read-only `config` IPC method. `unlink` is the recovery
  path for a corrupt config blob or corrupt `state.db` (best-effort keychain
  cleanup, then `store.Reset` or delete). Changing `server.*` or re-pairing to
  a new identity best-effort erases the old identity's keychain secrets
  (`cleanupIdentitySecrets`) - always AFTER a successful config write. On
  Windows `daemon` speaks the SCM protocol when service-launched (event-log
  fallback for pre-logger failures) and runs foreground otherwise; a stop
  re-reports `StopPending` with an incremented checkpoint every
  `stopCheckpointInterval` until the run loop returns, so an unbounded teardown
  reads as progress instead of parking the service in `STOP_PENDING`. **`Execute`
  must never send `svc.Stopped` itself** - the svc package publishes the ONE
  `SERVICE_STOPPED` from `Execute`'s return values, so it carries
  `ERROR_SERVICE_SPECIFIC_ERROR` plus the daemon exit code that the installer's
  `sc.exe failure` recovery keys on; re-adding a `changes <- Stopped` silently
  reports every terminal failure as a clean stop and no restart fires.

## `internal/config`

- `internal/config` - `Config` type, defaults/validation and byte
  serialization (strict decode; **downgrade rule: adding/removing/renaming a
  persisted `Config` field MUST bump `store.CurrentSchemaVersion`**), the
  validated `Limits` sub-struct (workers, scan/poll intervals, resume
  threshold, watcher debounce, cold-start cap, full-rehash interval,
  op-turn timeout - hardcoded constants promoted to `limits.*` keys with safe
  back-filled defaults and overflow-guarding upper bounds), and per-OS path
  resolution (`paths.go`): state dir, `DaemonSock()`, `LockFile()`,
  `PipeSecret()`, `LogDir()`/`LogFile()`. Relative XDG/`%LocalAppData%` values are
  ignored (treated as unset). `ValidatePartial` = per-field checks without the
  host/user/sync_root completeness trio (used by `config set`); `Validate`
  layers completeness on top (the `pair`/`login`/`daemon` gate).

## `internal/logging`

- `internal/logging` - `log/slog` init plus `RotatingWriter` (size-based
  rotation, ~10 MiB / 5 backups, self-healing on transient failures, degraded
  rotation is observable).

## `internal/md4`

- `internal/md4` - pure-Go MD4 (`hash.Hash`) + lowercase-hex content-hash
  helper; required by the protocol.

## `internal/normpath`

- `internal/normpath` - path canonicalization: `Normalize` folds to Unicode
  NFC; `Fold`/`Key` additionally lower-case on a case-insensitive FS. The GOOS
  default is a fallback - the engine probes the actual sync-root behaviour and
  calls `UseCaseInsensitive`. Store keys keep NFC with original case; folding
  applies only to in-memory matching keys.

## `internal/sproto`

- `internal/sproto` - the wire protocol: `header.go` (8-byte frame header),
  `pstream.go` (recursive `PStream`/`PObject` codec, OOM-guarded on three
  orthogonal axes - depth/nodes, per-value inline-binary cap, aggregate
  frame-byte budget where every decoded node costs `max(wire, nodeCostBytes)`
  so a flood of 2-byte empty containers cannot outrun the byte budget;
  buffered per-frame encoder; `Decoder.MaxDrainBytes` bounds what one `Decode`
  DISCARDS - a streamed `Binary` pays only the node floor, so an absurd claimed
  length is refused with `ErrDrainTooLarge` before its body is read, while bytes
  a `BinaryStream` handler consumes are not charged), `conn.go` (`Conn`: request/
  reply, in-place TLS upgrade with `VerifyConnection` pin hook, shared
  `ReadCappedResponse`, `DecoderWith` for a read-observing decoder),
  `legacy.go` (Legacy-TLV `TEST_CONN`). `(*Map).Int`/
  `.String` are the single owners of typed map decoding.

## `internal/secrets`

- `internal/secrets` - `go-keyring` wrapper for the session token and cert
  pin; distinguishes "not found" from backend failure. The account password is
  NEVER persisted. Tests use `keyring.MockInit()`.

## `internal/transport`

- `internal/transport` - connection setup: `handshake.go` (`TestConn`,
  `encrypt_channel`), `pinning.go` (SPKI pinning - `spki-sha256:` with legacy
  full-DER `sha256:` compatibility, enforced via `VerifyConnection` so resumed
  handshakes are covered; `ErrPinMismatch`), `session.go` (`Session`, the
  single authenticated connect primitive; stdlib TLS 1.2 floor is intentional),
  `pool.go` (event session + 2-4 workers over an injected
  `Connector`; `Acquire`/`Event`/`Replace`/`ReplaceEvent`; terminal pauses:
  `ErrPoolPaused` on 8193, `ErrPoolClientRejected` on 20481, reason exposed via
  `PauseReason`; per-streak reconnect logging and `Reconnecting()` for status).
  `Replace` accepts only a **leased** worker session (`ErrPoolNotLeased`) - the
  non-blocking `free` send under the mutex depends on it, so replacing an
  unleased session deadlocked the whole pool (`ReplaceEvent` takes no session:
  the event session is never leased); `Acquire`
  before `Start` is `ErrPoolNotStarted` rather than a block, and a transiently
  failed `Start` releases the started flag so the bring-up is retryable (a
  terminal one still pauses).

## `internal/auth`

- `internal/backoff` - context-aware exponential backoff with jitter and a
  ceiling (`Backoff`, `New`; 500 ms → ×2 → 30 s, up to 50% downward jitter).
  Shared by the transport pool's reconnect loop, the daemon's sync-loop
  bring-up retry and the IPC server's accept retry - it lives below all three
  so `ipc` need not import the network-session package for a delay sequence.
- `internal/auth` - mode-C `Pair` and mode-A `Reuse`, `device_uuid`
  generation, typed errors for 4104/4105 (OTP), 20481 (client version), 8193
  (session vanished). `Reuse` requires success evidence (errno or `view_list`).

## `internal/netctx`

- `internal/netctx` - shared `Watch(ctx, conn)`/`Err(ctx, err)`: force a past
  deadline (read AND write) on ctx cancel, map deadline I/O errors back to
  `ctx.Err()`. Single owner of logic formerly duplicated in auth/actions/
  transport.

## `internal/actions`

- `internal/actions` - the typed action client over an authenticated session.
  - `client.go` - the `do` turn: capped reply reads keep the connection
    byte-aligned, `doMutate` requires an errno (a mutating reply's errno is its
    only success evidence), and every reply read runs under the idle/progress
    watchdog `actionIdleTimeout` - `limits.op_turn_timeout_s` ships disabled, so a
    server going silent mid-reply must fail as the transient `ErrTransferStalled`
    rather than wedge the op. `long_poll` is the one turn it is disarmed for.
  - `errors.go` - typed sentinels (28672 path, 36870 hash, 24579 stale base,
    45057 resume token, `ErrUnexpectedReply`/`ErrConnBroken`/`ErrTransferStalled`).
    8193 and 20481 map to the SHARED `auth` sentinels, so a session-level verdict
    on an action turn is terminal exactly as on connect; every other non-zero
    errno is a `*ServerError` the engine escalates on a deterministic streak -
    except 32772, which is a `*ServerError` it deliberately NEVER escalates. Three
    codes are typed through `(*ServerError).Unwrap` rather than `errorForCode`:
    24580/24581 (`ErrNameCaseCollision`/`ErrNameTypeCollision`, terminal for a push)
    and 32772 (`ErrNodeBusy`, measured transient). They match a sentinel while
    `ServerErrorCode` still answers their number, and the number is what feeds BOTH
    the engine's escalation streak and its retry pacing; an `errorForCode` mapping
    would zero it.
  - `catalog.go`/`types.go` - view-scoped `QueryNode`/`List`/`QueryUserInfo`.
    `List` returns `(nodes, listed, err)`: an errno-only reply is a third state,
    inconclusive rather than "empty". Server hashes are folded to lowercase at
    this single ingestion point.
  - `events.go` - `LongPoll`, `PullEvent` (cursor clamp), `PullEventEvidenced`.
  - `download.go`/`upload.go`/`resume.go` - streamed MD4-verified transfers; large
    bodies are never fully buffered. `resume_upload` is a WHOLE-FILE two-frame
    turn: the protocol has no sproto-level chunking and carries no offset, so an
    interrupted push restarts from zero and there is no chunk-size knob (removed
    in schema v9); only `resume_download` takes an offset (inside `file.offset`).
    Download reply decode runs under `downloadIdleTimeout` (`idle.go`) re-armed on
    every byte, and only ever pulls the socket deadline earlier, so it composes
    with `netctx`. Two-frame uploads poison-and-close before joining the body
    writer on an errno-less reply, and join under `bodyJoinGrace` on success
    (`joinBodyFrame`).
  - `mutate.go` (`Rename`/`Remove`), `unlink.go` (`UnlinkConnection`).

## `internal/servertree`

Enumerates a view's CURRENT tree by walking `list` top-down, one turn per
directory. It is the source a cold start reads instead of replaying the event
log: the log is history, so nothing it says is final until all of it is folded,
which forced a batch - read everything, plan everything, only then transfer.

Two properties are the contract rather than implementation detail.

**Order.** A directory is visited before any of its children, so a caller that
enqueues as it walks gets "the parent's create is planned before its contents"
with no global sort over the tree. Siblings descend in name order, which is what
makes a failed walk resumable by simply running it again.

**An unevidenced listing is not an empty directory.** An errno-only reply
iterates as a zero-length list, and reading that as emptiness hands the caller a
subtree that looks server-side deleted - for a first sync, every local file under
it then reads as local-only and is pushed. The walk stops there instead
(`ErrUnevidenced`), leaving the subtree unknown rather than wrong. A listing that
ERRORS is a different claim, delegated to `Options.OnDirError`, and this package
takes no view on what it means. Cold start corroborates before acting: a path
error conflates absence with a server-side timeout, so it re-lists the PARENT and
skips the subtree only when the parent no longer reports it. Skipping on the bare
errno would be a silent permanent hole - the incremental poll reads FORWARD from
the cursor, so it never goes back for a subtree the walk failed to record.

Bounded like every other unit of work here: `MaxDirs` and `MaxDepth` are
backstops against a structure that does not terminate, not tuning knobs.

## `internal/store`

- `internal/store` - typed transactional bbolt store over `state.db`. Buckets:
  `meta`/`views`/`files`/`byid`/`queue`/`conflicts`/`deadletters`; composite
  ops are atomic in one transaction so `byid` never drifts. Migration runner
  with `CurrentSchemaVersion` (v13) and `ErrSchemaTooNew`; a missing/corrupt
  schema marker on a non-empty DB is `ErrCorruptRecord` with an `unlink` hint.
  `ErrLocked` maps bbolt's lock timeout. `Open` creates durably (dir-entry
  fsyncs). Strict readers (`GetBaseline`/`GetConfig`/`GetView`) tag
  undecodable values `ErrCorruptRecord`; list/scan readers skip-and-warn
  corrupt records; queue readers quarantine them, counting each committed drop
  in the per-handle monotonic `QuarantinedOps()` - the engine's cue that a record
  left the queue without its view or path ever being known, which arms
  `engine/queuerepair.go`. Key files: `config.go`
  (settings blob), `meta.go` (schema version, persisted pause), `views.go`
  (`View` incl. `InitialSyncDone`, `RootDevice`), `files.go` (`Baseline` +
  `byid` index, streaming `ScanBaselinesForView` plus the cursor-seeked
  `ScanBaselinesUnderPrefix` every subtree sweep uses - it decodes only the
  records under the directory instead of snapshotting the whole view),
  `conflicts.go` (durable
  keep-both list, streaming `ScanConflicts`), `queue.go` (durable FIFO `Op`
  queue - single consumer, bounded readers `PendingOpsN`/`PendingOpsNFrom`,
  streaming `ScanPending`/`ScanPendingFrom`; `Op` carries the divergence-
  capture fields the engine's never-lose gates read), `deadletter.go` (capped
  FIFO trace; `RecordDeadLetterAndAck` writes trace + optional compensating
  enqueue + optional `TrackedBaseline` + queue delete atomically - a drop that
  leaves work undone cannot commit without its repair, nor the repair without the
  drop, nor the repair without the pending record that ARMS its never-lose gate;
  plus the bare `RecordDeadLetter` for the ONE drop class that never had a queued
  op to ack - two on-disk names folding to one sync key, which the path-keyed queue
  cannot even represent, so its caller owns the idempotence
  (`engine.collisionNoted`) and the record carries the queue-less `OpFoldCollision`
  type that names it).

### Batched writes

`Store.Batch` runs many writes in ONE bolt transaction, and `store.Writer` is the
interface both granularities satisfy: `*Store` gives every call its own
transaction, `*Batch` shares one. A planned op's apply step takes a `Writer`, so
one implementation serves the incremental path (a handful of ops, latency over
throughput) and a cold start (hundreds of thousands, the reverse).

The durable-queue rule - a queue record written BEFORE the baseline mutation it
owns - is about ORDER, not transaction granularity, and a batch is strictly
stronger: a transaction is all-or-nothing, so an interrupted chunk is a chunk
that never happened and the caller re-derives it.

**The trap: a call that reaches the store directly from inside a batch opens a
NESTED transaction, and bolt allows one writer - it deadlocks the daemon.** That
is why the apply step takes a `Writer` rather than closing over the engine's
store. `markLocalPending` still wrote through `e.store` when batching landed, and
the engine suite hung until it was routed through the parameter.

The `Writer` parameter makes the right call convenient; it does **not** make the
wrong one impossible, and this used to claim it did. Every apply closure captures
the engine, so `e.store.PathByID(...)` inside a batch compiles perfectly well. So
the deadlock is detected instead (`reentrancy.go`): `Batch` records the calling
goroutine, and a `*Store` call from that same goroutine panics with the remedy.
Identity is what is compared, not merely "a batch is open" - other goroutines
legitimately touch the store while one runs (the status scan, an IPC handler) and
blocking on the writer lock is correct for them. A nested READ is guarded too,
and is the more dangerous half: it only deadlocks when the batch's commit has to
remap a grown file, so it survives testing and surfaces under load.


## `internal/fsutil`

- `internal/fsutil` - atomic local-apply helpers (DESIGN §7): durable `Mkdir`/
  `Rename`/`Remove` (parent dir-entry fsyncs, cross-directory renames flush
  both parents), `RenameNoReplace` (per-OS atomic no-clobber move),
  `RenameReplace` (the replacing promote primitive: on Windows a write-through
  `MoveFileEx` plus a one-shot retry that clears a REGULAR destination's
  `FILE_ATTRIBUTE_READONLY` and restores it if the retry still fails - the
  attribute never clears itself, so the pre-fix promote failed every drain
  forever after re-streaming the whole body),
  `SetMeta` (mtime + perm; fd-based no-follow on Unix; malformed wire perm or
  out-of-range mtime warns and skips instead of wedging the op -
  `MTimeOutOfRange` is shared with the engine), `SyncDir` (per-OS directory
  fsync - a real `FlushFileBuffers` on Windows, not a no-op; CIFS/FUSE
  capability gaps degrade to one warn through a budget shared with the Unix arm,
  genuine I/O errors propagate), `ExtendedPath` (Windows-only, exported for the
  direct Win32 calls in `engine`).

## `internal/watcher`

- `internal/watcher` - the fsnotify watcher (a latency optimization over the
  authoritative periodic scan - a missed event is never a correctness gap):
  recursive registration, debounced coalescing wake channel, overflow forces a
  wake, per-GOOS watch cap (darwin 4096 distinct dirs), `filter.go` drops
  self-induced noise (staging temps, `_Conflict` copies, editor temp files).
  A 30s `recheckRoot` tick re-registers the tree when the sync root drops out of
  fsnotify's `WatchList` - a deleted-and-recreated root takes every watch with
  it, its parent is never watched, so no event can report the replacement, and
  identity comparison cannot see it either (Windows tunneling restores the NTFS
  index and both timestamps).
- Backends are per-OS (`newFSWatcher`). Windows uses its own
  `backend_windows.go`: ONE `ReadDirectoryChangesW` with `bWatchSubtree` on the
  root, because fsnotify's watch per directory owns a 64 KiB buffer each -
  338 MB of a 352 MB heap on a 5.5k-folder view - and fsnotify v1.10 keeps its
  recursive mode test-only. The first read is issued inside `Add` (the kernel
  records nothing before one is queued); a kernel overflow (`ERROR_NOTIFY_ENUM_DIR`
  or a zero-byte completion) is `ErrEventOverflow`, and the watch survives it.
  Elsewhere fsnotify watches per directory; a SYSTEM limit (`isWatchLimit`:
  inotify's `ENOSPC`, `EMFILE`/`ENFILE`) is the cap, so the walk stops once
  instead of logging a Warn per remaining directory.

## `internal/ipc`

- `internal/ipc` - local-only newline-delimited JSON-RPC between daemon and
  CLI: `ipc.go` (server with bounded in-flight conns, 1 MiB request/response
  caps, panic-isolated handlers, retrying accept loop; a `Handler` takes a
  `ctx` and `dispatch` bounds it with `handlerTimeout` derived from the serve
  context, so neither a wedged handler nor a saturated in-flight cap can hold
  up shutdown), per-OS listeners
  (`listen_unix.go` 0600 socket; `listen_windows.go` DACL-restricted named
  pipe with a per-user secret against pipe squatting; `IsDaemonAbsent`/
  `IsDaemonBusy`), `types.go` (the `StatusReport`/`ConflictReport`/
  `ConfigReport`/`ControlAck` contracts), `protocol.go` (a version on both
  envelopes: a package upgrade leaves a new CLI talking to the still-running
  old daemon, and a method the two disagree about used to surface only as
  "unknown method"; a pre-versioning peer sends 0 and is answered normally).

## `internal/daemon`

- `internal/daemon` - lifecycle assembly: `daemon.go` (`New`/`Run`: lock →
  IPC bind → serve → sync loop with backoff on transient bring-up failures;
  durable pause via `store.PutPaused`; terminal stop reasons logged to the
  rotating file), per-OS single-instance lock, `connector.go` (keychain-backed
  `transport.Connector` with a 60s per-attempt timeout and legacy→SPKI pin
  migration - the keychain write runs off the dial path with a bounded wait,
  since go-keyring is not ctx-aware and a prompting backend would otherwise
  hold the pool's connect forever), `status.go`
  (`buildStatus`/`buildConflicts`/`buildConfig` - bounded, cached,
  single-flighted; never touches the network). Everything in `status.go` follows
  one rule: **the O(tree) count is the only part allowed to fail.** The IPC
  handler budget gates only *entering* a cache-filling scan; the scan then runs on
  `scanBudget` off the daemon's `baseCtx`, since a scan cancelled at the handler's
  clock caches nothing and would be restarted and re-aborted by every later call -
  breaking the verb forever on exactly the large trees the cache exists for. A
  refill pass is committed once it has scanned its first view and stamps every
  view as one cohort (`viewCounts`): sequential per-view stamps can never all be
  fresh when the walk outlasts the TTL. Counts a pass cannot produce DEGRADE
  (`ipc.ViewStatus.CountsStale`) instead of failing the verb, so
  `Paused`/`DiskFull`/`Stuck`/`PendingOps` survive. Only the BUDGET failure class
  is remembered (`stickyScanFailure`/`scanFailTTL`, re-stamped as a cohort by
  `stampFailureCohort`), so a doomed walk is not restarted by every call while a
  transient store error is retried by the next one. Waiting on another call's
  in-flight scan is bounded by the caller's context (`acquireFlight`, a cap-1
  channel rather than a mutex) - a blocked waiter would otherwise be abandoned by
  the IPC layer and never reach the degrade path.

## `internal/service`

Installs, removes and reports the daemon under the host's service manager: a
systemd **user** unit on Linux, a launchd LaunchAgent on macOS, and the Windows
SCM. Per-OS files behind one `Install`/`Uninstall`/`Status` API, with the binary
path, home, user name and command execution arriving through `Deps`, so every
flow is tested against a recording runner and a temp home rather than a live
service manager.

**The templates are the single source.** `templates/` holds the unit files; they
are embedded here AND staged by `scripts/release.sh` into each archive under
`packaging/`, so the file a user edits by hand and the file the installer renders
are the same bytes. Rendering substitutes what the operator would otherwise fill
in - the systemd unit hardcodes `ExecStart=%h/.local/bin/synosync`, the plist a
literal `USERNAME` because launchd expands neither `~` nor `$HOME` - and
**refuses** a template whose shape changed (zero or several `ExecStart` lines, a
missing plist placeholder): installing a unit that starts nothing, or logs into
somebody else's home, is worse than refusing. Only the directives are rewritten;
the templates' own prose survives into the installed file.

Paths are escaped for the format they land in: XML-escaped text in the plist, and
a quoted `ExecStart` word with `%` doubled, since systemd splits the value on
whitespace and expands `%` as a specifier. A path carrying a line break is
refused outright - neither format can represent it.

**Linger is reported, not assumed** (Linux): a user manager is torn down at the
last logout unless lingering is on, so a service installed from a terminal stops
syncing when the session ends, silently and long after the install looked
successful. It is the one prerequisite the installer cannot satisfy itself, so it
asks `loginctl` and names the exact command. `Uninstall` tolerates only a
positively-identified absent unit; any other `disable` failure is surfaced rather
than followed by removing the unit and claiming the service is gone.

**Windows `Install` deliberately refuses** and prints the elevated
`install-service.ps1 -User … -Password …` invocation instead. A service there
runs under an account of its own, and the default LocalSystem reaches neither the
paired user's state database and keychain nor the control pipe, whose DACL names
that user's SID; registering one would look installed, never sync, and restart on
a loop under the script's failure actions. Collecting the password here is
deliberately not done - a password passed as a process argument is visible to
every other process. `Uninstall` drives the shipped script; `Status` goes
straight to `sc.exe query`, so a state query still works where the PowerShell
execution policy blocks a staged script.
