# synosync

A cross-platform, console two-way sync client for Synology Drive, written in
pure Go (no cgo). It connects to Synology only over HTTPS/TLS and implements the
`sproto` wire protocol from scratch.

**Status: 0.1.0, an early release.** It deletes and overwrites files as a sync client must,
and it has been run against few servers so far. Keep a backup of anything you point it at.
[docs/TESTING.md](docs/TESTING.md) covers protecting your data, what to try, and how to
report a problem.

## Install

Download the archive for your system from
[Releases](https://github.com/mfilippov/synosync/releases): `.tar.gz` for Linux and
macOS, `.zip` for Windows, amd64 or arm64. Each archive holds the `synosync` binary,
the license files and a service unit under `packaging/`.

Every archive carries build provenance, so you can check it was built by this
repository's release workflow from a tagged commit:

```sh
gh attestation verify synosync-<version>-linux-amd64.tar.gz --repo mfilippov/synosync
sha256sum -c SHA256SUMS --ignore-missing
```

The Windows binary is also Authenticode-signed:

```powershell
Get-AuthenticodeSignature .\synosync.exe   # Status: Valid, signer: Mikhail Filippov
```

Or build it with Go 1.27.2 or newer:

```sh
go install github.com/mfilippov/synosync/cmd/synosync@latest
```

Then pair, check, and run the daemon:

```sh
synosync pair --host nas.example.com --user alice --sync-root ~/Synology   # asks for the password
synosync daemon                                                           # Ctrl-C to stop
synosync status                                                           # from another terminal
```

`synosync service install` runs it in the background; see
[Running it in the background](#running-it-in-the-background).

## Features

- **Two-way sync** of one or more Drive views (your home folder, team folders) with a
  local folder, on Linux, macOS and Windows, amd64 and arm64.
- **Nothing is silently lost.** A change is applied only over the version both sides
  last agreed on. If a file changed on both sides, or a delete meets an edit, the
  other version is kept next to it as a `_Conflict` copy (`synosync conflicts`).
  Deletions apply directly; the server's recycle bin is the way back.
- **Selective sync.** Leave server folders and name patterns out in both directions
  with `synosync exclude`; excluding never deletes anything. A new pairing starts
  with the official client's defaults: the home view's `Photos` and `Backup`, and
  names such as `desktop.ini`, `thumbs.db`, `.DS_Store`, `*.tmp` and `~*`.
- **Fast first sync.** Files start transferring while the server tree is still being
  listed, and an interrupted first sync resumes without re-hashing what it verified.
- **Prompt and quiet.** A file watcher pushes local changes within seconds, the
  server notifies the client of remote ones, and an idle client uses next to no CPU
  or network.
- **Runs as a service** under systemd, launchd or the Windows service manager
  (`synosync service install`); a local-only control channel serves `status`,
  `pause`, `resume`, `sync`, `drops` and `log`.
- **Secure by default.** TLS only, with the server's key pinned at pairing; a
  certificate this computer does not trust needs your confirmation before the
  password is sent. The session token lives in the system keychain and the password
  is never stored.
- **Honest about what it skips.** Names this platform cannot hold, and anything the
  client gave up on, are listed with the reason (`synosync drops`) instead of
  failing the sync.

How it works inside - the protocol, the engine and the rules it keeps - is in
[docs/DESIGN.md](docs/DESIGN.md) and [docs/INVARIANTS.md](docs/INVARIANTS.md);
[CHANGELOG.md](CHANGELOG.md) carries the change history.

## Pairing and login

```sh
./synosync pair --host drive.example.com --port 6690 --user alice --sync-root /home/alice/Drive
./synosync login
```

Both subcommands accept `--host`, `--port` and `--user`, which override the
values stored in `state.db` (the default port is 6690). `pair` also takes
`--sync-root`, the absolute path of the local directory to sync into; since there
is no `config.json` to edit, this is how a fresh install records its sync root,
and `pair` refuses to proceed without one (from the flag or a previously stored
config).

**What lands in the sync root depends on how many views you sync.** A Synology
Drive account sees one *view* per shared folder, plus the personal space the
server calls `home`. Sync several and each gets its own subdirectory -
`~/Drive/home/`, `~/Drive/Team/`. Sync exactly one and its contents go
**directly** in the sync root, with no subdirectory at all.

`pair` asks which ones when the account has more than one, listing them by
number; `--views` answers the same question without the prompt, taking either
the names or the numbers `pair` prints:

```sh
./synosync pair --host drive.example.com --user alice --sync-root ~/Drive --views home
./synosync pair --host drive.example.com --user alice --sync-root ~/Drive --views 1,3
./synosync pair ... --views all      # every view (a subdirectory each, unless the account has only one)
```

Selecting a single view **requires an empty sync root** on a first setup. With a
subdirectory per view the sync root is only a container, and a file sitting
beside those directories is outside every view; flattened, the sync root *is* the
view, so anything already in it would be uploaded on the first cycle. A re-pair
does not re-check this - by then the directory is legitimately full of synced
files.

A **re-pair keeps the stored selection**: `pair --host nasB` after a migration,
or a re-pair to re-capture a rotated pin, must not re-decide where an existing
tree lives. Pass `--views` explicitly to ask for a change.

Afterwards, `config set views.selected home,Team` changes the selection. It
resolves names against the views the daemon has enumerated, so run the daemon
once first - the selection is matched by the server's view id, not by the name,
which is what keeps it working when a shared folder is renamed server-side.

Deselecting a view stops syncing it and leaves its local files alone, exactly as
a view that disappeared server-side is treated. **What cannot change once files
are synced is the layout** - what occupies the sync root. Going from one view to
several, back, or swapping which single view is flat all move the whole tree, and
the engine would find it missing from its new location and report every file as
deleted, to the server. Such a change is refused; to make it, run
`synosync unlink` and pair again. Settings live entirely in `state.db`; since bbolt holds an exclusive
file lock, `pair` and `login` fail with a "stop the daemon first" hint while a
daemon is running.

`pair` performs first-time mode-C pairing: it probes the server (`TEST_CONN`),
brings up the `encrypt_channel`/TLS connection while capturing the leaf
certificate's SPKI (public-key) fingerprint, then authenticates with the account
password (read without echo) and an optional one-time password. On success it
stores the session token and the SPKI certificate pin in the system keychain,
records the `device_uuid` and server `ds_id` in the `config` blob inside
`state.db`, and prints the pinned fingerprint (`certificate pin:
spki-sha256:<hex>`) so it can be recorded for later verification. Re-pairing an
already-paired install to a different server or user (e.g. `pair --host nasB`
after a migration, without an `unlink` first) additionally best-effort clears the
OLD identity's now-orphaned session token from the keychain - plus the
certificate pin when the host or port changed (a user-only change keeps the pin,
which still serves the same server) - the same cleanup `config set server.*`
performs, so a stale server-valid credential is not left behind.

`pair` also accepts `--expect-pin <value>`: a TOFU out-of-band check that compares
the certificate the server actually presents against the supplied pin (the
`spki-sha256:<hex>` form printed by a prior pairing, a legacy `sha256:<hex>`
full-DER pin recorded before SPKI pinning - compared against the leaf's DER
fingerprint rather than its SPKI form - or the bare hex digest; case- and
whitespace-insensitive) and refuses to pair on a mismatch *before*
prompting for a password or writing anything to the keychain, so a first
connection can be pinned against a value verified through a trusted channel.

Without `--expect-pin`, `pair` still checks the certificate against the roots this
computer trusts before asking for a password, the way the official client does. A
certificate issued for the host by a trusted authority passes and is reported. One
that is not (a self-signed DSM, a name that does not match, pairing by IP address)
is shown with its key and needs a yes, or `--accept-untrusted` for scripts. Either
way the key is pinned, and every later connection is checked against the pin.

`login` re-authenticates using the saved `device_uuid`/`ds_id`: it tries
mode-A session reuse first and, when the server reports the session has vanished,
falls back to a fresh mode-C pairing prompt. Every connection verifies the
server's leaf certificate against the SPKI pin captured at pairing time,
rejecting a MITM certificate; a legacy full-DER pin from early builds is still
accepted and seamlessly migrated to its SPKI form on the next successful reconnect
(`login` prints "certificate pin upgraded to SPKI"). A genuine key change yields
a pin-mismatch error with both fingerprints and a hint to re-run `pair`. When no
`device_uuid`/`ds_id` or pinned certificate is present, `login` fails with a hint
to run `pair` first.

## Daemon and live control

```sh
./synosync daemon          # run the sync process in the foreground (Ctrl-C to stop)
./synosync status          # show the running daemon's sync status
./synosync status -v       # the same plus internals (connections, queue, sync ids)
./synosync sync [now]      # trigger an immediate sync pass
./synosync pause           # pause syncing (durable; survives restart until resume)
./synosync resume          # resume syncing
./synosync conflicts       # list outstanding keep-both conflict copies
./synosync drops           # list work the engine gave up on, and why
./synosync exclude add Photos # keep a server folder out of the sync (also: remove, list)
./synosync status --json   # status, conflicts and drops all take --json
```

`daemon` requires a paired config (run `pair` first; it stores the session the
daemon reuses, so no separate `login` is needed - run that only when the daemon
reports the session is gone). It
runs the connection pool, the two-way sync engine and the filesystem watcher in
the foreground. A second `daemon` for the same state directory fails fast: it
opens `state.db` first, and bbolt's exclusive file lock surfaces as
`store.ErrLocked`, so the duplicate exits with "the state database is in use
(another daemon instance running?); stop it and retry" before it assembles
anything. The in-process single-instance lock (`daemon.ErrAlreadyRunning`,
"another daemon instance is already running") is the backstop taken after the
store opens, before the IPC socket is bound, so a duplicate start can never
unlink a live daemon's socket. The daemon serves a local-only IPC control plane
(a `daemon.sock` `AF_UNIX` socket with perms `0600` on Unix; a current-user-DACL
named pipe on Windows) alongside `state.db`. The Windows pipe name is derived
from the case-folded state path plus a per-user secret, so a CLI and a daemon
built from opposite sides of that folding change cannot see each other - the CLI
reports "daemon is not running" while the daemon holds the `state.db` lock.
Restarting the daemon after upgrading both binaries is the whole migration. On Windows the daemon also speaks the
Service Control Manager protocol when launched as a service (`service_windows.go`),
falling back to a foreground Ctrl-C loop otherwise.

`status` dials that socket and prints the server address, connection-pool state
(including "connecting..." while the daemon is still bringing the pool up, so an
unreachable server at boot no longer reads as a false "connected", and
"reconnecting..." while a dropped worker/event connection is being re-dialed under
backoff after a successful bring-up, so a mid-run outage no longer reads as a
false "connected" either),
whether syncing is paused, pending-operation count and per-view sync state. It
also surfaces health/observability lines when they are non-clean: a per-view
count of unresolved divergences (baselines the engine flipped to a conflict state
but could not keep-both-resolve), a `Dropped` count of dead-lettered records
(work the client will not do - most of them ops the scheduler permanently
discarded: a structurally-invalid path, a name the
local filesystem cannot hold, a name the host would silently rewrite or that
aliases another node (Windows; see "Windows platform notes" below), a path
component that redirects elsewhere (a symlink, or on Windows a junction), a
directory apply blocked by an existing local
directory, or a local push - or a keep-both conflict resolution - that fails the
same deterministic way on many consecutive drains, whether the server rejects it
with one and the same error code or never reports a resumed large-file upload as
complete; that last group leaves the local file untouched on disk and lists
the path under the per-view unresolved count instead of retrying forever - all of
them leave a durable trace in the store). How long that confirmation takes depends
on what the server said: two refusals that are about the *name* itself - a sibling
differing only by case, or the name already held by a node of the other type - are
dropped after a much shorter window, since waiting cannot change either verdict,
while one code the server is known to clear on its own is never dropped however long
the streak runs. An operation that keeps failing the same way also backs off
geometrically between attempts (up to a few seconds) rather than re-sending on every
drain, so a hopeless push no longer hammers the NAS while it waits out its
confirmation. One entry in that count is not a
discarded *operation* at all: two local filenames that fold to one sync key (NFC vs NFD,
or case on a case-insensitive filesystem) never had an operation behind them, and
neither file is synced until one of them is renamed. An outgoing directory delete
refused because the server's copy of the directory holds a node this client never
agreed on, or because that copy could not be enumerated well enough to prove the
delete safe (the child, or the reason, is named in the trace), *is* a discarded
operation - but a deliberate safety refusal rather than an apply that kept failing:
the directory is left in place and the divergence is also listed under the per-view
unresolved count. `status` also has a
`Stuck` count of queued ops that have
failed many consecutive apply attempts (still retrying, not dropped - a
visibility net for permission errors and the like), and a `Disk: full` flag
latched when an apply fails because the volume is out of space or the user's quota
is exhausted - classified per-OS, so it is reported on Windows as well as Unix (the
op stays queued and retries; the flag clears on the next successful apply). When no daemon is running it prints a hint to start one and
exits non-zero. `sync`,
`pause`, `resume` and `conflicts` dial the same socket: `sync [now]` triggers an
immediate scan/poll pass, `pause`/`resume` toggle the durable pause state (a
pause takes effect at the next cycle boundary, is honoured even when issued
while the daemon is still connecting, and survives a daemon restart until an
explicit resume), and `conflicts` lists the outstanding keep-both copies the
resolver recorded (or "no conflicts"; a very large backlog is capped to the
newest entries, printed as "N conflict(s), showing the newest M").

## Running it in the background

```sh
./synosync service install     # register with the platform's service manager and start
./synosync service status      # is it installed, is it running
./synosync service stop        # halt it, leaving it registered and enabled
./synosync service start       # run it again
./synosync service restart     # after a `config set`, which the daemon reads at startup
./synosync service uninstall   # stop, disable and remove it
```

`daemon` runs in the foreground and stops when the terminal closes, so a real
install puts it under the host's service manager: a **systemd user unit** on
Linux (`~/.config/systemd/user/synosync.service`) and a **launchd LaunchAgent**
on macOS (`~/Library/LaunchAgents/me.filippov.synosync.plist`). Neither needs
root - the daemon holds per-user credentials and serves a per-user control
plane, so it belongs to the login session.

`service install` writes the unit with **this binary's own path**, which is the
part worth automating: the templates have to guess where the binary lives
(`%h/.local/bin/synosync` for systemd, `/usr/local/bin/synosync` for launchd),
and a wrong guess is the most common reason a hand-installed unit never starts.
On macOS it also substitutes the absolute log paths, since launchd expands
neither `~` nor `$HOME`. Re-running it after moving the binary rewrites the unit
in place on both.

Pair **first**, and as the same user the service will run as: the daemon reads
that user's stored credentials and serves a control plane only that user can
reach.

**Windows works differently.** A service there runs under an account of its own,
and it must be the one you paired with - the state database, the keychain
entries and the control pipe all live in that user's profile, and the default
`LocalSystem` account can reach none of them. There is no safe way for the CLI
to collect the password (one passed as a process argument is visible to every
other process on the machine), so `synosync service install` prints the command
to run instead of registering anything:

```powershell
# from an elevated PowerShell, in the directory you unpacked
.\packaging\install-service.ps1 -Action install `
    -BinPath C:\Tools\synosync\synosync.exe `
    -User you -Password (Read-Host -AsSecureString)
```

The script grants the account `SeServiceLogonRight` itself, which `New-Service`
does not - without it the SCM refuses to start the service with error 1069.
`synosync service status` and `synosync service uninstall` work normally once it
is registered; to move the binary, run `uninstall` first, since the SCM refuses
to re-create a service that already exists.

One thing no installer can do for you: on Linux a systemd **user** manager is
torn down at your last logout unless lingering is enabled, so syncing would stop
silently. `service install` checks and tells you when it applies:

```sh
sudo loginctl enable-linger "$USER"
```

The same unit files ship inside every release archive under `packaging/`, with
their install steps and the reasoning behind each supervisor's restart bounds in
the file header, for anyone who would rather install by hand.

## Configuration, logs and unlinking

```sh
./synosync config get             # print all settable settings as key=value
./synosync config get log_level   # print one setting
./synosync config set log_level debug
./synosync log                    # print the daemon log
./synosync log -n 100 -f          # tail the last 100 lines and follow
./synosync unlink                 # detach the device and clear all credentials
```

`config` reads and writes the settings stored in `state.db` (no `config.json`):
`config get` with no key prints every settable scalar - `log_level`,
`log_format`, `sync_root`, `server.host`, `server.port`, `server.user`,
`views.selected`, plus the
resource knobs `limits.workers`, `limits.local_scan_interval_s`,
`limits.drain_batch`, `limits.upload_resume_threshold`,
`limits.empty_scan_threshold`, `limits.watcher_debounce_ms`,
`limits.cold_start_catalog_cap`, `limits.event_poll_timeout_s`,
`limits.poll_deadline_slack_s`, `limits.full_rehash_interval` and
`limits.op_turn_timeout_s` (the per-turn engine action deadline in
seconds, 0 = disabled) - and `config set <key> <value>` validates the
change before persisting it (an unknown key or invalid value is rejected).
`limits.cold_start_catalog_cap` bounds two things, not one: the one-off cold-start
catalog pull *and* the server-side subtree walk an outgoing directory delete runs
first, so a low value bounds cold-start memory at the price of refusing deletes of
directories with more entries than the cap (see the deletion policy above).
Changing `server.host`/`server.port`/`server.user` on an already-paired install
prints a warning that `synosync pair` is required before the daemon will start
again (the new identity has no keychain pin or session yet) and best-effort
clears the OLD identity's now-unused session token from the keychain - plus the
certificate pin when the host or port changed (a user-only change keeps the pin,
which still serves the same server). A keyring backend failure warns but does
not fail the change.
`config set` needs an idle store like `pair`/`login`, so it prints "stop the
daemon first" while a daemon is running; `config get` works either way - when
the daemon holds the store lock it reads the settings from the daemon over the
local IPC socket instead.

## Where things live

Everything non-secret is in one file, `state.db`, and the secrets are in the
system keychain. There is no configuration file to edit.

| | State directory (`state.db`, lock, IPC socket) | Log file |
|---|---|---|
| Linux | `$XDG_DATA_HOME/synosync`, default `~/.local/share/synosync` | `$XDG_STATE_HOME/synosync/synosync.log`, default `~/.local/state/synosync/synosync.log` |
| macOS | `~/Library/Application Support/synosync` | `~/Library/Logs/synosync/synosync.log` |
| Windows | `%LocalAppData%\synosync` | `%LocalAppData%\synosync\logs\synosync.log` |

`synosync unlink` is what removes all of it, together with the keychain entries.

The daemon writes the log file rotating to `synosync.log.1`…
`.5` past ~10 MiB. It also mirrors the log to stderr when run from an interactive
terminal; `daemon --log-stderr` forces stderr-only output (no file), the right
sink under systemd, whose journal rotates. The launchd and Windows service
templates deliberately run without it - launchd never rotates a stderr redirect
file, so there the rotating file above is the log sink. `log` prints that file
(`-n N` for the last N lines, `-f`/`--follow` to tail). When the file is absent
it names the expected path and both explanations, since a missing file does not
prove the daemon never ran: a daemon started with `--log-stderr` (as the shipped
systemd unit does) logs to its supervisor's journal instead - try
`journalctl --user -u synosync`.

`unlink` detaches this device from the server (`unlink_connection`) and wipes the
local pairing: it clears the session token and certificate pin from the
keychain and resets `state.db` (the account password is never stored, so there is
none to clear). It requires a stopped daemon and confirms first
(pass `--yes` to skip the prompt); if the server is unreachable it warns but
still performs the local cleanup.

If `state.db` is corrupt beyond repair - so corrupt that it cannot even be opened
(a lost, truncated or zeroed schema-version marker) - `unlink` instead runs a
destructive recovery: after confirmation (or with `--yes`) it deletes `state.db`
outright and advises re-pairing, warning that the keychain entries cannot be
cleared because the server coordinates inside the database are unreadable
(re-pairing against the same server overwrites them).

## Deletion policy

synosync applies deletions directly - an incoming server delete removes the local
node, an outgoing local delete calls the server `remove`. There is **no local
trash/recycle staging** for deleted files (the "trash" stage was intentionally
dropped); recovery of a deleted file relies on the **Synology Drive server-side
recycle bin** on DSM, restorable through the DSM web UI. Accidental data loss from
the sync itself is guarded at the reconcile layer rather than by local staging: a
delete (or overwrite) of a node whose on-disk content has locally diverged from
the recorded baseline - including a server rename landing on a destination that
still holds an un-pushed or divergent local file - is funnelled into a keep-both
`_Conflict` copy instead of being silently clobbered, and a view scan with
surviving baselines is cross-checked against the view root's filesystem device id -
debounced when the tree scans empty, and suppressed outright when a **populated**
root's device changed without positive content evidence that it is still the tracked
tree - so a transient or swapped-out mount does not erase the mirrored tree.
That suppression is deliberately sticky: while the device does not match and nothing in
the scanned tree matches a tracked file, the view's local→server push is skipped on every
pass and the previously recorded device id is kept, so nothing is deleted or overwritten -
but local changes in that view also stop reaching the server until the condition clears.
It is reported only as a warning in the daemon log (`synosync log`); `status` has no
dedicated line for it. Restoring or remounting the correct volume clears it automatically.
A view whose tree scanned EMPTY has two further exits, both named in the warning: another
view of the same filesystem that adopted the same old→new renumbering on content evidence
corroborates it (a reboot renumbering an LVM/btrfs/network mount), and letting a single
tracked file exist on disk again lets the ordinary non-empty scan re-adopt the device id.
A genuinely intended migration in which every tracked file also changed needs
`synosync unlink` and a re-pair.
The precondition is symmetric on the outgoing side: because the queue is durable and
the server's `remove` verb is path-addressed, a local delete that has been waiting in
the queue (offline, a failing drain) first probes the server node it means to remove -
a different node now occupying the path is left alone, and a server-side edit made
after the local delete is recovered by downloading it rather than destroyed.

For a **directory** that probe goes further, because the server's `remove` is
recursive: one call on a directory succeeds and takes the whole subtree with it,
including nodes another client added since this client last looked inside. So before
deleting a directory the client enumerates its subtree **on the server** and refuses
if it finds anything it cannot account for - a child no baseline records, a tracked
path a different node now holds, a tracked file whose server content moved, a child
whose kind changed (a file where a directory was recorded, or the reverse), or a
child a not-yet-applied incoming change still owes. The same kind check covers the
directory being deleted itself: if the server now holds a *file* where the client
recorded a directory, another client replaced the node and the delete is refused
rather than issued against a path that is no longer a directory. The delete is then dropped with a
durable trace naming the offending child (visible in the `status` `Dropped` count)
and the directory is left in place. A reply that does not actually enumerate, or a
subtree listing that fails, is not treated as proof of emptiness either: the delete
stays queued and is retried with a growing pause. If the subtree still cannot be
enumerated after several minutes of identical non-answers the delete is dropped as
well, with a trace naming the reason and the path listed under the per-view
unresolved count - nothing is ever deleted on unproven emptiness, and nothing
re-walks the whole server subtree every cycle forever either.

That enumeration is bounded, and the bound is a hard ceiling rather than a retry: a
directory whose server subtree (or whose own recorded subtree) is larger than about
50 000 nodes cannot be proved agreed in one pass, so its delete is refused
permanently, with a trace naming the cap, and the directory stays. Delete such a tree
from the DSM web interface, or in smaller pieces. Lowering
`limits.cold_start_catalog_cap` lowers this ceiling too - the key bounds both the
one-off cold-start catalog pull and this walk - so a small value trades cold-start
memory against the size of directory the client is able to delete.

The same never-lose rule now covers **outgoing uploads**, which the server does not
arbitrate itself - its upload policy accepts any write whose modification time is not
older than the stored node's, and the request carries no base version at all. So
before every push the client asks the server whether the target is still the version
it last agreed on. If a peer changed it in the meantime (or took the path for a new
file), the push becomes a keep-both `_Conflict` copy instead of an overwrite; an
answer that decides nothing leaves the change queued and pushes nothing. This costs
one extra round trip per uploaded file.

One answer is deliberately slow. The server reports "no such path" and its own
operation timeouts with the same code, so if the probe says the target is *absent*
for a file the client is **modifying**, that is not taken at face value: the push
waits for a second agreeing probe at least a minute later before it re-creates the
file. Reading one such answer as "the file is gone, so there is nothing to overwrite"
would disable the whole check on a single timed-out probe. A brand-new file is
unaffected - there an absent path is the ordinary answer and the create goes straight
through.

One race is resolved by a deliberate choice rather than by keep-both: if one client
**renames** a file while another **deletes** it, the delete wins and both replicas
converge on the file being gone. Nothing had diverged - the content still matched the
last agreed version, so no copy is set aside - and recovery is the Synology Drive
recycle bin, as for any other deletion. In the opposite ordering (the rename is
applied first) the file survives under its new name. Some sync clients give the
rename priority; synosync deliberately does not, because re-creating a node the
server has already deleted is not something the never-lose rule can promise.

## Windows platform notes

Windows stores a filename under rules the sync protocol does not share, so a name a
Linux or macOS peer creates legally can be one this host cannot address. synosync
refuses such names rather than mis-applying them - a mis-apply here does not fail
loudly, it lands the content under a *different* name and can carry an unrelated,
healthy file off to a `_Conflict` copy on the way.

- **Names Windows would silently rewrite.** Win32 strips trailing dots and spaces
  from every path component, so a server file named `notes.` or `bar ` would be
  created as `notes`/`bar` with no error. Such an event is skipped: the record stays
  untouched on the server, the skip is logged (`synosync log`), and an operation
  queued by an older build is dropped with a durable trace (the `Dropped` count in
  `synosync status`).
- **Names that alias another node.** NTFS exposes an 8.3 short-name alias for every
  long name, so a server path component spelled `DOCUME~1.TXT` can resolve to a file
  the host actually calls something else. An apply whose path component is such an
  alias is refused the same way. Reserved device names (`CON`, `NUL`, `COM1`, …) and
  a component containing `:` are refused by the general containment check.
- **Getting such a file to sync.** Rename it on the server, or from a peer that can
  represent the name. The client picks the node up at its new, representable path.
  Renaming a *directory* this way only brings the directory itself across - the
  server emits no events for the descendants that were skipped under the old name,
  so move the contents (or re-create them) as well.
- **Read-only destinations.** A local file carrying the read-only attribute no
  longer blocks an inbound update forever: the attribute is cleared for the replace
  and re-applied from the server's permission bits. The replace still runs only
  behind the divergence guard, so locally diverged content is preserved as a
  `_Conflict` copy exactly as on Unix.
- **Junctions.** The containment guard refuses a junction (and any other
  name-surrogate reparse point) in an applied path, just as it refuses a symlink -
  applying through one would write outside the sync root.
- **Service failures.** A terminal daemon failure is reported to the SCM as
  `ERROR_SERVICE_SPECIFIC_ERROR` carrying the daemon's exit code, so the `sc.exe
  failure … restart` recovery configured by the installer actually fires
  (`sc.exe query synosync` shows `WIN32_EXIT_CODE : 1066`). An operator-requested
  stop reports success and does not trigger a restart.

## Requirements

- A Synology NAS running Synology Drive Server. Tested against DSM 7.2.2 with Drive
  Server 4.0; other versions may differ in ways only a run against them shows.
- To build: Go 1.27.2 or newer (`go.mod` pins the toolchain; an older Go 1.21+
  downloads it automatically). Release binaries need nothing installed.
- A system keychain for the session token and certificate pin: Windows Credential
  Manager, the macOS login keychain, or on Linux a Secret Service provider on the
  D-Bus session bus (GNOME Keyring, KWallet, KeePassXC). A headless Linux machine
  has none, so this version does not run there. Under a lingering systemd user
  service, the keyring must also be unlocked, which usually takes a login.

## Build

```sh
make build        # build all packages and the ./synosync binary
# or
go build ./...
go build -o synosync ./cmd/synosync
```

## Run

```sh
./synosync version          # print the version
./synosync help             # list available subcommands
./synosync help pair        # one subcommand's usage, flags and examples
./synosync pair --help      # the same page
```

Every subcommand carries its own help page: what it does, how to invoke it, its
flags and a few worked examples. `--help` writes to stdout and exits 0, so it
pipes and redirects cleanly; a usage error goes to stderr and exits 2.

The version string is `0.0.0-dev` by default and can be overridden at build
time:

```sh
go build -ldflags "-X main.version=1.2.3" -o synosync ./cmd/synosync
```

## Develop

`make test` runs the unit tests with `-race`, `make lint` the linters, and
`make test-integration` the live-DSM harness. [CONTRIBUTING.md](CONTRIBUTING.md) has the
full workflow and the bar a change must meet, [AGENTS.md](AGENTS.md) the package map and
every `make` target, and [docs/PACKAGES.md](docs/PACKAGES.md) how the packages fit
together.

## Documentation

- [AGENTS.md](AGENTS.md) - the one-page map for contributors and coding agents: packages,
  commands, and the invariants as one-line rules.

- [docs/DESIGN.md](docs/DESIGN.md) - architecture, protocol and design rationale;
  §13 lists the accepted limits.
- [docs/INVARIANTS.md](docs/INVARIANTS.md) - the rules the code must keep (never-lose,
  reply evidence, result codes) and why each exists.
- [docs/ENGINE.md](docs/ENGINE.md) - the sync engine, file by file.
- [docs/PACKAGES.md](docs/PACKAGES.md) - the packages and how they fit together.
- [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md) - how to contribute and how
  to report a vulnerability.
- [CHANGELOG.md](CHANGELOG.md) - notable changes and schema-version bumps per release.
- [docs/INTEGRATION.md](docs/INTEGRATION.md) - the live-DSM integration-test contract.
- [docs/TESTING.md](docs/TESTING.md) - the product-testing guide: getting a build, scenarios,
  known limitations and what to put in a bug report.

## License

Licensed under the Apache License 2.0 - see [LICENSE](LICENSE). Attribution for
bundled dependencies is in [NOTICE](NOTICE), and the clean-room origin of the
`sproto` protocol implementation is documented in [PROVENANCE](PROVENANCE).

## Trademarks

Synology and Synology Drive are trademarks or registered trademarks of Synology
Inc. synosync is an independent, unofficial client and is not affiliated with,
endorsed by, or sponsored by Synology Inc. The trademarks are used only to
identify the service this client interoperates with.
