# Integration test harness

The integration harness (`test/integration/`) runs the **real** sync stack -
`transport.Pool`, `actions.Client`, `store` and `engine`, wired exactly as the
daemon wires them - against a **live test Synology DSM**. It replays the
bidirectional happy path end-to-end: pairing/connect, local→server and
server→local create / modify / mkdir / rename / remove, plus a large-file
resume round-trip.

A second family of scenarios drives no engine at all: they PIN A REPLY SHAPE that
reading the client's own code could not settle - `ListReplyShape`
(enumeration evidence), `NodeHashes` (per-file content hashes), `CaseOnlyRename`
(case sensitivity of lookup vs uniqueness), `TrailingDotSpaceNames` (names
Windows cannot spell verbatim), `EventShapes` / `EventLogCompaction` (which
fields an event carries, and how much per-node history the log keeps),
`UnlinkConnection` (that the unlink really releases the registration) and the two
opt-in captures `BigListPagination` (whether `list` pages or truncates) and
`PathErrorUnderLoad` (whether a busy server reports a live node as absent). Their
assertions are measurements, so a failure means the server changed: record the
new answer in this document before relaxing one.

A third family runs **two full clients at once** - two paired devices, two sync roots,
two stores, two engines on the same view (`twoclient_test.go`'s `spawnPeer`). They are
the only scenarios that can reach the shapes the never-lose doctrine is written for:
both peers acting while neither has seen the other's change. `engine.Pause()` models an
offline laptop and `scenario.stop`/`start` a killed-and-restarted daemon. Nine of them
failed against the client as originally shipped - that is how findings H1-H7 were found;
all seven are closed in the code and the scenarios are expected green; what each
measured fact costs the client is recorded in `docs/INVARIANTS.md` and DESIGN §13.

This is the test half of DESIGN §11/§12 Phase 10. It is **not** part of the
default test suite: the files carry a `//go:build integration` tag and every
scenario self-skips when the `SYNOSYNC_IT_*` environment is absent, so
`make test` and CI without a DSM stand never run it.

## Running

```sh
make test-integration
# equivalently:
go test -tags integration -race -timeout 40m ./test/integration/...
```

The explicit `-timeout` is not optional: a full run against a live stand takes roughly
13-15 minutes, well past Go's 10-minute default, which aborts the binary mid-scenario.
To run just the two-client family, add `-run 'TestIntegration_TwoClients'`.

With no `SYNOSYNC_IT_*` environment set, the run skips cleanly (no failures).
To actually exercise the scenarios, point the environment at a scratch DSM:

```sh
export SYNOSYNC_IT_HOST=drive.example.com
export SYNOSYNC_IT_PORT=6690
export SYNOSYNC_IT_USER=alice
export SYNOSYNC_IT_PASS='…'
# optional:
export SYNOSYNC_IT_OTP='123456'                 # only for 2FA accounts
export SYNOSYNC_IT_VIEW='home'                  # pin a view; else the first writable one
export SYNOSYNC_IT_REMOTE_BASE='/synosync-it'   # scratch base the harness owns
make test-integration
```

**Stand requirement:** the DSM must not sit behind a userspace TCP relay
(QEMU user-mode networking - passt/slirp). Such a relay drops in-window
segments under backpressure and stalls the `-race` runs for minutes; a
Virtual DSM container must run in NAT mode (`Mode: NAT` in its boot log).
See *`-race` stalls inbound downloads* below for the measured mechanism and
the one-flag fix.

## Environment contract

All of the following are **required**; any missing → `t.Skip` with a message
listing the missing variables:

| Variable             | Meaning                                              |
| -------------------- | --------------------------------------------------- |
| `SYNOSYNC_IT_HOST`   | DSM hostname or IP                                  |
| `SYNOSYNC_IT_PORT`   | sproto TCP port (usually `6690`)                    |
| `SYNOSYNC_IT_USER`   | login account name                                  |
| `SYNOSYNC_IT_PASS`   | account password (used to pair / login)             |

Optional:

| Variable                  | Meaning                                                                 |
| ------------------------- | ---------------------------------------------------------------------- |
| `SYNOSYNC_IT_OTP`         | one-time password for a 2FA account (see *Pairing & sessions* below)   |
| `SYNOSYNC_IT_VIEW`        | view name to sync; defaults to the first view the account holds writable |
| `SYNOSYNC_IT_REMOTE_BASE` | scratch server sub-path the harness owns; defaults to `/synosync-it`   |
| `SYNOSYNC_IT_BIG_LIST`    | child count for the large-directory listing capture; unset = that one scenario skips |
| `SYNOSYNC_IT_LOAD_PROBE`  | seconds to probe a LOADED stand for false path errors; unset = that one scenario skips |
| `SYNOSYNC_IT_RESTART_PROBE` | seconds to observe a Drive Server restart (triggered out of band); unset = that one scenario skips |
| `SYNOSYNC_IT_ID_ALIAS`    | run the `id:`-aliasing probe - writes one node at the VIEW ROOT and may delete its own victim; unset = skips |
| `SYNOSYNC_IT_SCRUB`       | set to `1` to run the scratch-base scrub - deletes the WHOLE `SYNOSYNC_IT_REMOTE_BASE`, not just this run's subtree; unset = that one scenario skips |

## Pairing & sessions

The harness pairs **two** devices once for the whole package run (cached across
all scenarios, not re-paired per test):

- an **engine** device - the session the real sync engine runs under; its session
  id is the engine's `selfSessionID`, and
- a **mutator** device - a *distinct* session the server-mutation helpers commit
  under.

A run that spawns a **second sync client** (the two-client family) pairs a **third**
device lazily on first use (`ensurePeerPaired`), for the same reason the mutator exists:
the peer engine's commits must not reach the first engine as self-echoes. A run that
never spawns a peer still registers only two.

Two sessions are required because the engine recognises an event whose
`committer_session_id` equals its own session as a **self-echo** and only advances
the baseline without touching disk (DESIGN §6). If the server→local helpers
committed under the engine's own session, those changes would be self-echoed and
never materialise on disk. Caching the pairing also keeps the run from burning the
single `SYNOSYNC_IT_OTP` (or leaving a paired device) per scenario.

The engine device is paired with `SYNOSYNC_IT_OTP`; the mutator device is paired
**without** an OTP (the one-time password is already consumed), as is the peer device.
A test account that
**enforces 2FA on every new device** will reject the second (and third) pairing - run the
harness against an account **without enforced 2FA** (or with an app password) for
the server→local and large-file-download scenarios.

### Pairing failures `4096` / `4100` - bad credentials (settled)

Measured side by side on the stand and now both mapped:

| code | server reason | sentinel |
|------|---------------|----------|
| `4096` | `User not found(auth)` | `auth.ErrUserNotFound` |
| `4100` | `Invalid password` | `auth.ErrInvalidPassword` |

They are **distinct**: a nonexistent account draws 4096, a real account with a
wrong password draws 4100. Do not collapse them into one "bad credentials"
verdict on the assumption that a server would not disclose which was wrong -
this one does, and the distinction is what tells an operator whether to fix the
account name or the password.

**Read the `reason` string, not just the code.** This one cost a whole detour.
The code arrived bare, matched no sentinel, and a deliberately wrong password
drew the *same* bare number - which read as "credential-independent, therefore a
server-state problem" and sent the investigation after DSM Auto Block and
per-account device ceilings. Both passwords were simply wrong. `errorForCode`
now appends the server's own words to every auth error, which is the whole
evidence available about a verdict this client has never seen.

Two things that are NOT the cause, ruled out by measurement - don't re-derive them:

- **Not the shell.** A password containing `$` and `@` survives
  `SYNOSYNC_IT_PASS='...'` byte-for-byte; verify with a length + hash probe
  before suspecting quoting.
- **Not DSM Auto Block.** That bans the IP at the firewall for *every* service,
  so a working web UI from the same host rules it out. `TEST_CONN` succeeding on
  6690 likewise proves the Drive service itself is up and answering.

Worth fixing regardless, since it is a real harness defect the detour surfaced:
every `go test -tags integration` process pairs **fresh devices** (two, or three when
the run spawns a peer) and never
unpairs them, so a stand accumulates them per run indefinitely (visible in
Synology Drive Admin Console → Connections). **Fixed 02.08.2026:** `TestMain`
releases every registration after `m.Run` (`unlinkPairedDevices`), best-effort so
it can neither fail a passing run nor rescue a failing one. That the verb really
releases the registration - rather than merely answering OK - is itself pinned by
`TestIntegration_UnlinkConnection`: after the unlink the token is refused with
8193, and the refusal arrives at CONNECT, since mode-A reuse presents the token
during the handshake. Whether a per-account device ceiling exists remains
unmeasured, but the accumulation that would eventually find it is gone.

### `-race` stalls inbound downloads against a live stand (CLOSED: stand's userspace network relay)

**Resolved 2026-08-02.** The stall was the stand's QEMU user-mode network
relay (passt inside the vdsm container), not the client and not DSM: under
backpressure from a slow reader, passt **drops in-window TCP segments while
still advertising an open receive window**, so the DSM guest kernel treats
the drops as path loss and climbs an exponential RTO ladder - measured on the
guest's own wire as retransmission gaps of
0.201 → 0.403 → 0.807 → 1.609 → 3.221 → 6.454 → 12.893 → 25.789 → 51.516 s
(exact doubling, 69 retransmissions on one flow, 1.79 MB delivered in 180 s).
In a companion capture the guest pushed the entire 9.5 MB body in 3.27 s,
fully ACKed by passt, guest-side Send-Q/Recv-Q zero for the whole stall -
while the client sat ready with an empty buffer and only ever received
3.2 MB: the missing megabytes sat in the relay. The other relay in the chain
(podman's pasta port forward) was measured innocent - its `bytes_sent`
tracked passt's byte-for-byte with zero Send-Q and the Windows client's
window open.

The fix is on the stand: run the vdsm container with
`--sysctl net.ipv4.ip_forward=1` (plus the existing `--device=/dev/net/tun
--cap-add NET_ADMIN`) so its network.sh picks in-kernel NAT mode instead of
silently falling back to passt under rootless podman, and drop `USER_PORTS`
(a passt-only knob).

**That recipe is INCOMPLETE on an SELinux-enforcing host (measured 27.08.2026
while rebuilding the stand).** The sysctl applied (`ip_forward=1` inside the
container) and the container still came up `Mode: User (passt)`. The reason is
that `--device=/dev/net/tun` passes the device but does NOT grant `container_t`
access to a node labelled `tun_tap_device_t`:

```
avc: denied { getattr } for comm="ls" path="/dev/net/tun"
  scontext=system_u:system_r:container_t:s0:c643,c804
  tcontext=system_u:object_r:tun_tap_device_t:s0 tclass=chr_file
```

network.sh reads the denial as "no TUN here" and falls back silently, so the
symptom is only the boot-log mode line. Adding `--security-opt label=disable`
flipped the same command to `Mode: NAT` immediately. That switch turns SELinux
confinement off for THIS container only, which is the right trade for a
disposable stand and the wrong one for anything holding real data; the host-wide
alternative (`setsebool -P container_use_devices on`) changes policy for every
container on the machine. `../virtual-dsm/run.sh` carries both flags and the
reasoning.

Check the mode after every rebuild - `podman logs dsm | grep Mode:` - because
both failure modes are silent and the cost only shows up later as multi-minute
`-race` stalls. Control experiment: **8/8 consecutive full `-race` runs
green at ~20 s each** on the NAT-mode stand, where the same series previously
failed on runs 1 and 7. `make test-integration` now passes as shipped. The
boot log must say `Mode: NAT` - if it says `Mode: User (passt)` the fallback
happened again and stalls will return (passt drops TCP segments inside its own
advertised window under backpressure; measured 02.08.2026, far-side capture).

**11.10.2026:** a wslc stand (passt only, `2026_07_28` build) does NOT stall, not
even under the 2 ms slow-reader reproducer: `-race` 49/49. Recipe and data:
[stand-wslc.md](stand-wslc.md).

Everything below is the investigation record kept so its retired hypotheses
are not re-walked. The historical failure, measured back-to-back on a freshly
cleaned stand (user-mode networking), same binary, same server, minutes apart:

| run | result |
|-----|--------|
| `go test -tags integration` | **6/6 pass, 8.6 s total** (every scenario ≈1-3 s) |
| `go test -tags integration -race` | KeepBothColdStart FAIL 122 s, LargeFileResume FAIL 233 s, ServerToLocal FAIL 90 s |

What the goroutine dump shows (force one with `-timeout` below the scenario
budget - the scenario's own failure tells you nothing):

```
drainBatch → applyDownload → resumeDownloadToTemp → ResumeDownload
  → readContentReply → sproto.streamBinary → io.Copy → progressReader.Read
  → tls.Conn.Read → [IO wait]
```

The worker is blocked mid-body on the socket; `drainBatch` waits on its
WaitGroup; the engine logs nothing for minutes. The failure times are the tell:
KeepBoth fails at 122 s against a 120 s budget and `downloadIdleTimeout` is
120 s, so the idle watchdog IS firing - the transfer really does go quiet for two
solid minutes. ServerToLocal fails at 90 s, its own budget, before the watchdog
can even fire.

**NOT the cause: the codec's byte-at-a-time reads.** They were real and are now
fixed (a read buffer installed after the TLS upgrade, DESIGN §13), but the
harness fails under `-race` exactly as before - same scenarios, same 90/122/180 s
budgets. Recorded so the next investigation does not re-walk it. The measurement
that looked conclusive:

| build | reads | bytes | bytes per read |
|-------|-------|-------|----------------|
| plain | 89 280 | 314 663 | **3.5** |
| `-race` | 62 346 | 219 443 | **3.5** |

(Those are `tls.Conn.Read` calls, not syscalls - `crypto/tls` buffers whole
records underneath. Fixed by buffering the connection after the TLS upgrade;
see DESIGN §13 for the corrected cost measurement.)

Identical either way, so this is a design property, not a race artifact:
`sproto.Conn` deliberately does **not** buffer (`conn.go`: the codec must read
exactly as many bytes as each value needs, so the in-place TLS upgrade inherits
no plaintext), and the PStream decoder reads tags, lengths and short strings one
or two bytes at a time. A reply carrying a few hundred catalog nodes therefore
costs tens of thousands of `tls.Conn.Read` calls, each its own syscall.

`-race` does not cause this - it multiplies its cost until it breaks. That is
why the harness passes plain and fails under the detector, and why the failures
look like network stalls: the trace shows individual reads growing
275 ms → 804 ms → 1.6 s → 3.2 s → 6.4 s → 12.9 s → 25.8 s, clean exponential
doubling on one connection while other connections keep working - a peer backing
off from a client that drains at a few bytes per syscall, not a dead socket.

Why the inference was wrong, stated plainly so the reasoning error is visible
too: the read counts came from `progressReader`, which sits above `tls.Conn`, and
were read as syscalls. They are not - `crypto/tls` buffers whole records, so
these were function calls. Measured over a real `tls.Conn`, buffering cuts
520 465 calls to 579 but only ~1.3× of decode wall-clock (DESIGN §13). A 1.3×
decode saving was never going to explain a 120-second stall, and that arithmetic
should have been done before calling it the root cause.

**Root cause (measured): a slow-draining reader wedges the STAND's side of the
connection. It is not a client defect, and `-race` is only a way of draining
slowly.** The isolating experiment is a 2x2 - a 2 ms-per-read throttle injected
into the download path, run against the stand and against a local TLS server:

| reader | local Go server | DSM stand |
|--------|-----------------|-----------|
| normal | OK | OK (1.5 s) |
| slow (2 ms/read) | **OK - 9 437 220 B in 1.6 s, 5 ms blocked** | **STALL - fails at 186 s** |

Neither ingredient alone does it. The same throttled client that sails through
9 MiB locally in 1.6 s gets 3.26 MB from the stand and then waits.

What the client does while it waits, measured by splitting the download loop's
time into "blocked in Read" versus "doing its own work": under `-race`, **281
reads, 3 260 601 bytes, 3 m 01 s blocked in Read, 193 ms busy**. The client is
not slow - it sits ready with an empty buffer while nothing arrives, and its
reads average 11.6 KB, so this is not the byte-at-a-time path either.

At the time this could not separate DSM's own behaviour from the QEMU
user-mode network stack; the guest-side packet capture later did - it is the
user-mode stack (passt), per the header of this section. DSM itself sends the
whole body at line rate the moment the relay accepts it.

Two earlier explanations are retired. They are kept below so the next
investigation does not re-walk them.

**Retired #1: the codec's byte-at-a-time reads.** Real, now fixed (DESIGN §13),
and NOT the cause - the harness failed identically after the fix. What pointed
there:

1. **A slow reader alone reproduces the failure - no `-race` needed.** Injecting
   a 2 ms sleep per read into the download path fails `LargeFileResume` at 186 s
   in a plain build, exactly as the detector does. So the failure is driven by
   *per-read cost × read count*, and `-race` is merely one way to make reads
   expensive.
2. **Against the live DSM, reads average 3.5 bytes even WITH the connection
   buffer installed** (verified directly: `br != nil`, observer applied beneath
   it). The buffer cannot help, because `crypto/tls` returns at most ONE record
   per `Read` - if the peer emits tiny records, every read is tiny no matter what
   sits above it.
3. **The same client code against a local Go server makes 579 reads for the same
   data.** Our own encoder buffers per frame, so it emits few large records. The
   difference is entirely the *sender's* write pattern.

DSM **does** write its reply metadata in one- and two-byte TLS records - this
part is now measured directly rather than inferred. Instrumenting the read
observer to log the slice size it was asked for against the bytes it got:

```
88 x  askedFor=65536 got=1
44 x  askedFor=65536 got=2
```

Our buffer asks for 64 KiB; `crypto/tls` hands back one record, and the record
holds a single byte. On the wire that is a 5-byte TLS header and a 16-byte GCM
tag per byte of payload, and on our side one `tls.Conn.Read` (a mutex and a
record decrypt) per byte. No buffering above TLS can merge them. The file BODY is
different - the stalled transfer averaged 11.6 KB per read - so the tiny records
are a metadata-only phenomenon.

But that finding was over-extended into a cause, and the arithmetic refutes it: the failing download makes 281 reads, not tens of thousands, and the
client is idle for 99.9% of the elapsed time.

The reasoning error is worth naming. The throttle experiment showed a slow reader
CAN produce this failure, and that was taken as proof it DID - without checking
whether `-race` makes reads anywhere near 2 ms. It does not: ~10x on a read is
not the ~100 000x the elapsed time demanded. The objection that a server cannot
know the client was built with `-race` was the right one, and the arithmetic
should have been done before the claim.

**Consequence for the connection buffer (DESIGN §13):** it remains correct and
worth keeping - it removes ~900× of decoder-level calls and helps against any
peer that batches its writes - but its benefit **against this DSM is small**,
because the read count is capped by the server's records upstream of it. It was
never the fix for this failure.

The "what remains" question this record used to end on was answered by the
guest-side capture: the sender does not stop, is not window-blocked, and does
not lose packets on its own wire - its segments are dropped by the userspace
relay next to it, and the retransmission signature the client observed from
outside (275 ms → 804 ms → 1.6 s → …) was the guest kernel's RTO ladder, seen
one relay removed.

**Retired #2: packet loss in the stand's network path.** The exponentially
growing read gaps do look like a TCP retransmission signature, and the
mechanism did turn out to live in that family - but as a guess it was rightly
retired: it named loss in the *network path* where the measurement later
placed deliberate drops in the *relay*, and it could not have distinguished
loss, a slirp window deadlock, or a server write timeout. The lesson stands:
naming a mechanism without evidence is what produced both retired
explanations.

**Do not "fix" this class of failure by raising the scenario budgets.** They
are already larger than the watchdog they sit above; a budget bump would
convert a reproducible two-minute stall into a green run that hides it.

The one budget that WAS mis-sized is a different case, and it is now corrected:
`downloadIdleTimeout` (120 s) left under a minute of the 180 s large-file budget
for the retry the watchdog itself triggers, so a correctly-RECOVERING client
still failed the scenario. `lfTimeout` is 300 s as of 02.08.2026 - the watchdog
plus a full second attempt plus slack - which is not the same move as hiding a
stall: it makes the scenario able to observe the recovery it is supposed to
observe, so a timeout there now means the client did not recover.

## Scratch-base safety model

The harness never touches unrelated server data. It works only under a
**scratch base** (`SYNOSYNC_IT_REMOTE_BASE`, default `/synosync-it`), and within
it derives a **unique per-test subdirectory** (suffixed from the test name *and*
the process id) so concurrent or repeated runs - including two
`make test-integration` processes against one DSM - don't collide and one run's
pre-run cleanup never deletes another's in-flight tree. Each scenario
**recursively** tears its scratch subtree down in `t.Cleanup` (children before
parents - no longer required now that `remove` is known to be RECURSIVE, but kept
because it is the shape the harness's failure messages are written against), and its
pre-run cleanup fails loudly rather than running on a scratch base it could not clear. Local fixtures - a temp sync root and a temp
`state.db` (via an overridden `config.Paths`) - are likewise per-test and
removed on cleanup. Secrets stay in-process (the captured leaf-cert pin and the
session token); no real system keychain is touched.

The one deliberate exception is `TestIntegration_ScrubScratchBase`, opt-in via
`SYNOSYNC_IT_SCRUB=1`: it deletes the ENTIRE scratch base, including a concurrent run's
in-flight subtree. It exists because teardown is best-effort and an abandoned tree makes
the cold-start scenarios fail on their budget with nothing actually wrong in the client.
Never run it while another run is in flight.

Point the environment only at a **disposable test DSM** with a scratch share -
not at production data.

## What runs without a DSM

Even without a stand, the harness must keep compiling and skipping. CI / local
checks that always apply:

```sh
go build -tags integration ./...
go vet -tags integration ./test/integration/...
go test -tags integration ./test/integration/ -run Skips   # skips cleanly with no env
```

The default `make test` is unaffected - the integration files are invisible to
it because of the build tag.

## A disposable stand under podman

A stand does not have to be hardware. `vdsm/virtual-dsm` boots a real DSM in a
VM inside a container, which makes the harness runnable on a workstation and
makes a destructive scenario cheap: the whole NAS is one directory, and `reset`
throws it away.

```sh
podman run -d --rm --name dsm \
  -e "DISK_SIZE=64G" -e "RAM_SIZE=4G" -e "CPU_CORES=2" \
  -e "USER_PORTS=6690" \
  -p 5000:5000 -p 5001:5001 -p 6690:6690 \
  --device=/dev/kvm --device=/dev/net/tun --cap-add NET_ADMIN \
  -v "$PWD/data:/storage:Z" --stop-timeout 120 \
  docker.io/vdsm/virtual-dsm
```

Three things about that command are not decoration:

- **`USER_PORTS=6690`.** The image runs user-mode (passt) networking, so
  publishing a port reaches the CONTAINER and stops there. Without this the
  sproto port accepts a connection and then resets it, which surfaces as
  `read TEST_CONN reply: connection reset by peer` from `pair` - a wire error
  that reads like a broken server rather than a missing forward. Only the web UI
  ports are forwarded by default.
- **`:Z` on the volume.** With SELinux enforcing, an unlabelled bind mount is
  denied and DSM never gets its disk.
- **`--stop-timeout 120`.** DSM needs the grace period to shut its services down;
  a hard kill can leave the virtual disk inconsistent.

`/dev/kvm` must be readable and writable by the invoking user - the container is
rootless, so no `kvm` group membership is involved, only the device mode.

After first boot, DSM runs its setup wizard on port 5000, and the wizard offers
to install **Synology Drive Server** ("Securely Access and Share Files From
Anywhere"). Accept it: it is what opens 6690, and it enables the user home
service, which is the `home` view the harness syncs by default.

Measured on such a stand, 23.08.2026:

| | |
|---|---|
| DSM | 7.2.2-72806 (VirtualDSM) |
| Synology Drive Server | 4.0.3-27892 |
| `pull_event(sync_id=0)`, 1000-node log | one page, 1019 events, cursor reached the head |

Package and system versions are readable without the UI, which is faster than
driving the wizard's DOM:

```sh
curl -s -c jar 'http://127.0.0.1:5000/webapi/auth.cgi' \
  --data-urlencode api=SYNO.API.Auth --data-urlencode version=3 \
  --data-urlencode method=login --data-urlencode account=USER \
  --data-urlencode passwd=PASS --data-urlencode session=Core \
  --data-urlencode format=cookie
curl -s -b jar 'http://127.0.0.1:5000/webapi/entry.cgi?api=SYNO.Core.Package&version=2&method=list'
```

This stand is also what proved the identity-block requirement is not optional:
`pair` built a bare `actions.NewClient`, and Drive Server 4.0.3-27892 answered
its `QueryUserInfo` turn with 20481. An older Drive Server had tolerated the
same omission, so the bug was invisible until a current one was available.

## `list` is complete and carries hashes (settled 24.08.2026)

Two premises a tree-walk cold start needs, both measured on the podman stand
(DSM 7.2.2-72806 / Drive Server 4.0.3-27892):

| question | verdict |
|---|---|
| does `list` truncate a large directory? | **no** - 10000 children, 10000 returned in one reply (`SYNOSYNC_IT_BIG_LIST=10000`) |
| does `list` carry a per-file content hash? | **yes** - 5/5 file nodes, each matching the upload reply's hash; directories carry none, as `query_node` also reports |

Together these say the current tree is readable directly, with the evidence
needed to adopt a local file without transferring it. That matters because
they are what let cold start stop reading the event LOG. The log is history, so
nothing in it is final until the whole of it is folded, which forced a batch
shape - read everything, plan everything, only then transfer. Measured on a real
190k-node view that was 4 minutes of catalog read followed by ~70 minutes of
enqueueing with the network idle.

A walk over `list` is O(tree) rather than O(history), needs no fold, holds one
directory rather than the whole catalog, and can enqueue and transfer as it
goes.

## Unconfirmed DSM semantics to capture on a live stand

**Settled on 31.07.2026** (DSM 7.2.2-72806 / Drive Server 4.0.3-27892 - the
versions read off the stand itself, `/etc/VERSION` and
`/var/packages/SynologyDrive/INFO`) - kept here as the record of what the stand
actually answered:

- Absence is verb-specific: `32769` for query_node/list/download/upload, `40961`
  for remove/rename (ambiguous: absent target OR malformed request), `28672` for
  input validation. `40962` is a stale-cursor rejection.
- `remove` needs a leading-slash path and NO cursor; `rename` needs
  `src_path`/`dst_path` and a cursor fresh at send time, retried on 40962.
- `get_max_sync_id` lags the event log; `pull_event`'s `next_sync_id` is the real
  tail.
- The simple single-shot `upload` is accepted to at least 8 MiB, so the real
  client's 512 KiB switch point is its own choice, not a server limit.
- `resume_upload` carries the whole file in one content frame (no chunking, no
  offset) and requires a resume_token; the `dry_run:1` negotiation is advisory.
- Bad credentials are two codes, not one: `4096` "User not found(auth)" for an
  unknown account, `4100` "Invalid password" for a real account. See below.
- `list` returns its children under **`node_list`**, not `file_list`; the latter
  is kept in `catalog.go` only as a fallback for in-memory fakes written against
  the earlier assumption. An EMPTY directory is genuinely ENUMERATED - the reply
  carries `node_list: []`, so `listed == true` with zero children - which is what
  every absence prover needs; DSM does not answer it with a bare verdict.
  Pinned by `TestIntegration_ListReplyShape`. Whether the array is ever paged or
  truncated is still open (see below).

**Settled on 14.08.2026** (same stand, driven by the new TWO-CLIENT scenarios; what
each fact costs the client is recorded in `docs/INVARIANTS.md` and DESIGN §13):

- **`upload`'s `conflict_policy: "compare_mtime"` is NOT a base-version check.**
  Measured side by side (`TestIntegration_UploadConflictSemantics`): an upload whose
  mtime is NEWER than the stored node's is accepted and overwrites; one with the SAME
  mtime is accepted and overwrites; only an OLDER mtime draws `24579`. So the stale-base
  rejection the engine's whole outbound conflict funnel hangs on
  (`engine/outbound.go` `funnelUploadConflict`, `reconcile.go`'s keep-both deferral)
  fires only when the losing client's clock/mtime is BEHIND - i.e. almost never.
  *Answered in the client:* the push no longer relies on that verdict -
  `engine/outbound_uploadguard.go`'s `guardUploadTarget` proves the target is still the
  last agreed version with one `query_node` before the destructive turn, and funnels a
  divergence into the same keep-both resolver `24579` funnels into. The deferral in
  `reconcile.go` enqueues that resolve itself when the batch already proves the
  divergence.
- **There is no optimistic-concurrency knob on `upload` at all.**
  `TestIntegration_UploadBaseVersionProbe` builds the two-frame turn by hand and sends
  the node's PRE-CHANGE `max_id` (the same field `rename` uses as its cursor, where a
  stale value draws 40962) in both `max_id` and `sync_id`: the push lands anyway, on top
  of the peer's newer version. A control push with a fresh `max_id` also lands, so the
  raw shape is fine and the result is about the VERSION, not the framing.
- **`remove` of a NON-EMPTY directory succeeds and takes the children with it - the
  verb is RECURSIVE** (`TestIntegration_RemoveDirectorySemantics`). This contradicts the
  assumption this document and `removeTreeRemote` were written under ("the server rejects
  removing a non-empty directory"); the child-first teardown is still correct, just no
  longer required. The consequence for the client is severe: a directory delete also
  destroys every node a PEER added inside since this client last looked, and no
  client-side never-lose gate can see those nodes.
- **Two more rejection codes - both now MAPPED (task 4 of the H-plan):** `24580` when a
  sibling differs from the name only by case (already recorded above, and also seen from
  the engine as a push-streak `server_errno`), and **`24581` when `mkdir` targets a name a
  FILE already occupies** (`TestIntegration_TwoClientsFileDirClash`). Both are verdicts
  about the NAME, so waiting changes nothing: they carry the typed sentinels
  `actions.ErrNameCaseCollision`/`ErrNameTypeCollision` - while still reporting their
  NUMBER to `ServerErrorCode`, which is what the escalation counts (`ServerError.Unwrap`) -
  and a push refused by either is dead-lettered after the shortened
  `nameRejectionDrains` + `nameRejectionMinAge` confirmation instead of the five-minute
  unknown-code window, with the cause spelled out in its trace. Measured before the fix:
  the refused turn was re-issued ~80 times per second for the whole window (23 610
  attempts) while every op serialised behind it stayed blocked; every repeat rejection now
  arms a geometric pause before the next attempt (`engine/opbackoff.go`), capped at the
  order of the local-scan interval.
- **`download` of a just-overwritten node can answer `32772` once and succeed a second
  later** (`TestIntegration_TwoClientsRapidChurn`, which rewrites a file every 100 ms).
  It is the one code whose TRANSIENCE is measured, and it is now mapped as such: it
  carries `actions.ErrNodeBusy` while still reporting its number to `ServerErrorCode`
  (`ServerError.Unwrap`), so the repeat-rejection pacing still arms while the escalation
  refuses to count it at all (`engine.serverTransientRejection`). Before the mapping an
  outbound op unlucky enough to draw it repeatedly accumulated it into the
  deterministic-rejection streak and was eventually dead-lettered - abandoning a healthy
  local change because the server was settling a node.
- **Two on-disk names folding to one `normpath.Key` are DECLARED, not silently skipped**
  (`TestIntegration_TwoClientsUnicodeNames`, finding H6). NTFS holds `café.txt` in its NFC
  and NFD spellings as two directory entries; the client can sync only one of them, and the
  other used to sit there looking synchronised - no op, no trace, no stuck count, never
  backed up. It now leaves a durable dead-letter naming the collision and the remedy
  (`engine/collisiontrace.go`), recorded once per (view, fold key) and forgotten when the
  key stops colliding, so `status` counts it under Dropped.

**Settled on 27.08.2026** (rebuilt stand, DSM 7.2 / Synology Drive Server, NAT
mode) - the two facts the 27.08 project review made load-bearing, each captured by
its own scenario. Note what each scenario ASSERTS: the file_id one asserts presence
on the two create replies whose omission would arm the unpaced re-upload loop, so a
server that changes fails it loudly; the tombstone one asserts only that the capture
happened (the directory's own row arrived) and LOGS the descendant verdict, because
the client is written to be correct under either answer:

- **A recursive `remove` reports ONLY the directory it names - descendants get NO
  event of their own** (`TestIntegration_RecursiveRemoveTombstones`). Measured on
  a directory holding two files, a nested directory and a file inside that: the
  one `remove` produced exactly ONE row (`action:"delete"`, `is_dir=true`, the
  directory's own `file_id`) and zero rows for the four descendants, matched by
  both path and id. The absence is corroborated rather than read once: the
  scenario waits for the directory's own row, then SETTLES for ten seconds and
  re-drains the log, so a server that emits the children's tombstones a moment
  later would contradict it. The log did not grow. So `applyRemove`'s premise -
  "a server delete event names only the directory, never its descendants" - is
  CORRECT, and `dropDescendantBaselines` is the only thing that retires those
  baselines.
  The consequence is the reason this was measured, and it has since been FIXED
  (`surfaceSupersededDescendants`, 28.08.2026): the refusal path was lossy. When an
  inbound directory delete is refused because a successor node
  stands at the path (the H3 identity gate, or the positional gate),
  `repairSupersededRecord` repairs only the directory's own record and
  deliberately keeps the descendant records - and nothing ever converges them.
  The children keep a `StateSynced` record over local files the server already
  destroyed: `localReconcile` drives only `StateSynced` records and finds
  disk == `LocalHash`, so it never diffs them; the server never re-describes the
  dead nodes; cold start does not re-run. They look synced in both replicas while
  existing in one - no `_Conflict`, no dead-letter, no `Unresolved`, which is the
  silent-unsyncable class H6 exists to close. The repair has to account for the
  SUBTREE, not just the node.
- **Every reply surface carries a `file_id`** (`TestIntegration_FileIDPresence`).
  Measured across seven reply shapes - `upload` create and modify, `mkdir`,
  `resume_upload` create and modify, `query_node` of a file and of a directory -
  plus every listed child (3/3) and every event row (4/4, the delete included).
  Nothing omitted it. So the client's id-less arms are DEAD INSURANCE on this
  server, and three failure modes that rest on them are latent rather than live:
  the unpaced whole-body re-upload loop in `applyUploadOut` (a create whose reply
  omits the id leaves a bare error that `noteServerErr` cannot pace, so the body
  re-streams every drain forever) is unreachable; the id-less outbound rename's
  lost-reply probe cannot adopt a foreign occupant; and `removeTargetFacts`'
  blanked divergence base never applies. Worth the cheap fixes, not urgent - and
  the pacing arm should be fixed BEFORE that assertion is ever relaxed.
- **The completing `resume_upload` reply carries the finished node's FACTS, not
  just a verdict** - this settles the narrower question left open below under "the
  completing resume_upload reply". The reply carried both the `file_id` and the
  content `hash` (equal to the MD4 of the bytes sent), so a streamed push records
  the server's own facts rather than deriving them from its staged bytes, and
  `applyUploadOut` does not blank `ServerHash` on a streamed create.

**Settled on 02.08.2026** (same stand, after it was moved to NAT mode) - three
questions left open until then, each now pinned by a scenario:

- **Per-file content hashes are present**. `query_node` of a
  file, the `upload` reply and the `pull_event` event for that file all carry a
  `hash`, and it equals the MD4 of the uploaded bytes exactly. A DIRECTORY
  carries none, as the client assumes. So all three degrade arms -
  `coldStartContentMatch`'s size+kind fallback, `probeUploadedNode`'s
  identity+size arm, and the resolver's hash-less Warn - are dead insurance on
  this server rather than the everyday path. Pinned by
  `TestIntegration_NodeHashes`.
- **The lookup is case-SENSITIVE, the uniqueness is case-INsensitive**
 . A case-only rename (`notes.txt` → `Notes.txt`) is
  accepted, the listing reports the new spelling, and `query_node` of the OLD
  spelling answers a path error - so a stale-case turn fails loudly rather than
  silently hitting the renamed node, and the old-spelling arm is reachable.
  But re-creating the old spelling as a SIBLING is refused with **errno 24580**,
  a code with no typed sentinel: two fold-equal names cannot coexist in a view.
  A case-sensitive local host holding both therefore has one file it can never
  push (dead-letters on the unmapped-errno streak with the bare code in its
  trace - DESIGN §13). Pinned by `TestIntegration_CaseOnlyRename`.
- **Names with a trailing dot or space are ACCEPTED and stored VERBATIM**
 . `dot.`, `space ` and `notes.txt.` all round-trip: created,
  listed and addressable under the exact spelling. So the class Windows cannot
  represent really does arrive from the server. Mitigating measurement: the
  server does NOT alias the stripped spelling - `query_node("dot")` against a
  stored `dot.` answers a path error - so a Windows-mangled path fails loudly
  instead of hitting a different node. Pinned by
  `TestIntegration_TrailingDotSpaceNames`.
- **A create surfaces in the event log as `action:"modify"`, because the log is
  COMPACTED per node.** `TestIntegration_EventLogCompaction` drives three
  lifecycles and reads them back in one pull: `create+modify` leaves ONE `modify`
  row carrying the current size/hash, `create+delete` leaves ONE `delete` row,
  `create+move` leaves ONE `move` row at the destination carrying `from_path`.
  So a create is absorbed by any later row for the same node, and a `create` row
  is a shape this server essentially never emits. (A move row is NOT absorbed by
  a later delete - `TestIntegration_EventShapes` saw both survive - so the
  compaction is not simply "one row per node".) The replay converges on every
  observed shape; what it means for the client is that a `create`-keyed arm is
  dead code here, `preBatchDeletePath`'s walk terminator among them.
- **`list` neither pages nor truncates at 10 000 children**.
  `TestIntegration_BigListPagination` builds a 10k-child directory and lists it:
  all 10 000 come back, verified by NAME and not just by count, two identical
  calls return the same window, and the client's decoder budget swallows the
  frame. This was the single unconfirmed premise under every listing-derived
  absence verdict. Opt-in via `SYNOSYNC_IT_BIG_LIST=<count>` - ~4 minutes, since
  building costs one upload per child and teardown one remove per child.
  **It permanently costs the view it runs in:** deleting the children leaves 10 000
  tombstones in the event log, and cold start replays the log from sync_id 0, so
  `KeepBothColdStart` (the one scenario that must pay a real cold start) went from
  seconds to 95 s against its 120 s budget on this stand. Point it at a DEDICATED
  view (`SYNOSYNC_IT_VIEW`), not the one the rest of the suite uses.
- **A Drive Server restart DROPS the connection; the live session never sees
  `20481`**. `TestIntegration_ServiceRestartWindow` holds one
  accepted session, asks about a live node twice a second and re-dials when the
  connection dies, while `synopkg restart SynologyDrive` runs from the side.
  Measured timeline: the established session's action turn died with a TCP reset
  (not an errno of any kind), a fresh connect then failed for ~7 s at
  `encrypt_channel` - the port accepts while the service is not yet answering -
  and the first connect that succeeded, 8.5 s after the drop, answered the very
  next `query_node` correctly. So the action-turn `20481` arm is insurance that
  costs nothing on this server, exactly as the residual guessed, and a client with
  the pool's reconnect rides a package restart out in under ten seconds. Note the
  scope: this measures a RESTART. An upgrade contains one, so the connection
  dropping is the expected shape there too, but a server that rejects a client
  VERSION mid-session was not exercised - no such policy knob was found on the
  stand.
- **Coming back up, the service does not report a live node as absent**
  (second provocation). The same timeline shows zero
  `PATH-ERROR` rows: the service either refuses the connection outright or answers
  correctly - there is no window where it accepts a turn and denies a node that
  exists. Together with the starvation run (below), two different provocations
  failed to reproduce the conflated verdict; the untried one is a full share
  RE-INDEX, which churns the whole view's event log and was deliberately not run.
- **A bad member rejects the WHOLE `long_poll` subscription with errno 12289, and
  a single-view subscribe of that member is rejected on its own** (`TestIntegration_LongPollRejectionShape`). Both halves the
  poison-view quarantine assumes are therefore real: one unacceptable view does
  break the shared subscribe, and the bisect probe reproduces the rejection, so
  attribution works. A valid view alone is accepted normally. **The raw turn
  settled the rest** (`TestIntegration_LongPollRejectionRaw`, which builds the
  request by hand and dumps every reply key, nested maps included): the rejection
  does NOT name the culprit. It carries
  `error{action: 0, code: 12289, reason: "invalid view id"}` - a human-readable
  cause but no id anywhere in the frame - so direct attribution is impossible and
  the bisect probe round is the only way to name the offending view.
  That raw dump also pins two things worth knowing beyond this question. The
  reply's failure shape is exactly the nested `error{action,code,reason}` map the
  reply-evidence doctrine assumes, and **every reply carries the server's own
  versions**: `server.dsm` (major 7, minor 2, build 72806 - matching
  `/etc/VERSION` on the stand exactly) and `server.package_version` (major 4,
  minor 0, mini 3, build 17892; the package's own `INFO` says `4.0.3-27892`, so
  the build numbers are NOT the same field - do not treat the wire value as the
  package release). A frame also carries `alive: 150` and `@proto.version 7.0`.
- **The `path` parameter is parsed LITERALLY: `id:<file_id>` in a view root is
  not id-addressing** (`TestIntegration_IDAliasProbe`, opt-in
  `SYNOSYNC_IT_ID_ALIAS`). The bait file named after a real node's id is accepted
  at the view root, `query_node` of that path answers about the BAIT, and `remove`
  kills the bait while the victim node with that id is untouched. So the
  ambiguity the scan/reconcile protection guards does not exist on this server -
  which makes retiring that guard possible, though that is a design decision on
  one build's evidence, not a consequence of this measurement. The probe is
  opt-in because it writes one node at the view ROOT (the only scenario that
  leaves the scratch base) and deletes a node if the server had aliased.
- **No unpositioned events: 0 of 523 rows on a full page carry `sync_id ≤ 0`**
  (`TestIntegration_EventShapes`). Cold start's cursor clamp only covers
  positively-positioned events, so the residual stands for other builds, but this
  DSM gives it nothing to bite.
- **A `delete` event carries `file_id`; a `move` carries `file_id`, `from_path`
  and the node's kind, but NO `from_file_id` - across every move SHAPE.** File
  rename, DIRECTORY rename and a move across directories were all measured, and
  each reported `kind_reported=true` with a non-empty `from_path`, which is what
  the two "sparse move" residuals (a move missing its kind, a move missing its
  source) need in order to be reachable at all. `TestIntegration_EventShapes`
  pins it. The id on a delete is what makes `preBatchDeletePath` - which exists
  only for an ID-LESS delete - unreachable against this server, which bounds the
  dead `create` arm above to servers that omit the id.

Set `SYNOSYNC_IT_LOG=debug|info|warn` to route the engine's diagnostics to stderr -
without it a scenario that times out gives no clue why. In a TWO-CLIENT scenario both
engines log to that one stream, so every record carries a `client=` label: `client=A`
is the scenario's own client and `client=B` the spawned peer. Which side dropped an
op is the whole question in those scenarios, and without the label the interleaved
diagnostics are unreadable.


Some questions about the client's safety turn on a reply shape no one has yet
observed on a real server. The client's fix for each is written to be
safe under EITHER semantics, but the stand is where the question is actually
settled - record what the server sends when the harness next runs.

- **The completing `resume_upload` reply - the QUESTION was
  built on a premise the stand refuted.** It asked how DSM signals that a
  *chunked* upload finished; the 31.07 measurement showed there is no chunking at
  all - `resume_upload` is a whole-file two-frame turn with no offset, so the
  reply's own verdict IS the completion signal and `engine.errResumeNoCompletion`
  is unreachable. What is still worth capturing is much narrower: whether that
  single reply carries the finished node's facts (`file_id`/`hash`) or only a
  verdict, since a reply without them makes the client derive the baseline from
  its own staged bytes. Everything else in this bullet is closed.

- **Above how many children does `list` break, if ever (what is LEFT of).** 10 000 come back whole (see the 02.08.2026 block), which settles the
  premise for any realistic directory. Where the ceiling actually is - and whether
  a frame that large is refused by the SERVER rather than paged - is unmeasured;
  raising `SYNOSYNC_IT_BIG_LIST` is the whole experiment, at ~25 s of stand time
  per 1 000 children. Only worth running if a field report suggests a cap.

- **How often do real peers produce trailing-dot/space names (what is left of
  this question).** The server side is settled - DSM accepts and stores them
  verbatim, and does not alias the stripped spelling (see the 02.08.2026 block).
  What no stand can answer is frequency: whether the Synology Drive clients on
  Linux/macOS, or the web UI, produce such names in ordinary use, which is what
  decides whether the dead-letter trace is a curiosity or an everyday operator
  chore. That is field data, not a capture.

- **Does `28672` answer `remove`/`query_node` under overload, and for how long -
  NOT REPRODUCED by resource starvation (02.08.2026).**
  The sentinel conflates "no such path" with a server-side timeout, which is why
  an absence verdict needs `absentNodeConfirmations` agreeing probes AND
  `absenceStreakMinAge` (60 s) of wall clock between the counted ones.
  `TestIntegration_PathErrorUnderLoad` (opt-in via `SYNOSYNC_IT_LOAD_PROBE=<s>`)
  hammers a LIVE file and its parent - `query_node`, `list`, and a spaced
  `create`+`remove` pair - and records every verdict. Measured against the stand
  with 6 busy loops on 2 vCPUs plus a continuous fsync'd 1.2 GB write loop
  (load average 0.4 → 14): 9 490 read rounds and 90 remove probes over 3 minutes,
  worst `query_node` latency 419 ms against 27 ms idle - a 15× degradation, so
  the load was real - and **zero** path errors, zero unlisted directories, zero
  refused removes.
  What that establishes: CPU/IO starvation alone does not make this DSM
  misreport a live node as absent, so the class is narrower than "the server is
  busy". What it does NOT establish: the WIDTH of the window, since absence of a
  repro measures nothing about a mechanism that did not fire. The remaining
  hypothesis is a blocked INDEXER rather than a starved host - a package upgrade
  or a share re-index - which is worth capturing when one is scheduled anyway;
  provoking a re-index deliberately churns the whole view's event log and costs
  more stand than the answer is worth.

## The vendor tooling on the stand is OUT OF BOUNDS as a source

A Virtual DSM ships Synology's own binaries under
`/var/packages/SynologyDrive/target/bin/`, including a small CLI over their
protocol library. It is tempting as an oracle, and it must not be used as one:
`PROVENANCE` states that this implementation derives **solely** from observation
of the wire and of a running server's behaviour, and names "verbatim identifiers,
constant names, error-code tables, struct layouts, or comment text taken from …
decompiled binaries" as material that is NOT used. Inspecting those binaries -
disassembly, decompilation, or pulling strings out of them - is the wrong source
regardless of what it would reveal, and it puts the project's clean-room claim at
risk for facts the wire already gives us.

What stays allowed is what this whole document is about: run things and watch the
protocol. If a vendor tool is used at all, it is used the way any user would run
it, and only its TRAFFIC is evidence - record sizes, timings, sequence - never
its contents as read out of the executable. When in doubt, measure the server.

Done once under that rule (02.08.2026), with the result recorded so nobody repeats
it: the CLI was run against the stand's own Drive service and the traffic captured
with tcpdump. **It speaks the protocol in CLEARTEXT** - on loopback and against
the host's own address alike, with no TLS handshake anywhere in either capture
(no `0x16 0x03` byte pair in 4.8 KB and 13.9 KB of traffic). That is concrete
confirmation that the plaintext path DESIGN §1/§9 refuses to implement is real and
is what vendor tooling uses locally. Two consequences: the open question about
DSM's one-byte TLS records **cannot** be answered with this tool - it would need
the actual Synology Drive desktop client - and the exchange itself revealed no new
protocol facts, since the `@proto` header map it carries (`body-continue` and its
siblings) is already modelled in `actions/client.go`. The tool also unlinks its own
session on exit, which independently corroborates `TestIntegration_UnlinkConnection`.

## Manual host checks that need no DSM

These are platform contracts no unit test can settle - the host service manager
is the authority, not a fake. They need a throwaway VM, not a stand, and no
`SYNOSYNC_IT_*` at all.

- **Windows: the SCM records a terminal daemon failure AS a failure.** `Execute` no longer reports `Stopped` itself; the single
  `SetServiceStatus(SERVICE_STOPPED)` is the one the `svc` package publishes from
  Execute's return values, so it carries
  `ERROR_SERVICE_SPECIFIC_ERROR` + the daemon's exit code. That is what
  `sc.exe failureflag 1` keys the restart on. What the tests CANNOT settle is the
  real SCM's response to the pre-fix sequence (a second `SetServiceStatus` after
  STOPPED is undefined by contract, not observably wrong), so the fix is written
  to avoid the undefined case rather than to depend on how any one build of SCM
  handles it. Confirm end-to-end on a VM:

  1. `install-service.ps1 -Install -User …`; `sc.exe qfailure synosync` shows
     `RESTART -- Delay = 5000` twice, then an empty third action, and
     `FAILURE_ACTIONS_ON_NONCRASH_FAILURES: TRUE`.
  2. Force a TERMINAL daemon error - the cheapest is to invalidate the stored
     session so the reconnect answers `8193` (revoke the client under DSM's Drive
     admin console, or corrupt the keychain token), which stops `Run` with a
     non-zero exit.
  3. Expect: exactly one STOPPED transition in the System event log, `sc.exe
     query synosync` reporting `WIN32_EXIT_CODE : 1066 (ERROR_SERVICE_SPECIFIC_ERROR)`
     with `SERVICE_EXIT_CODE` = the daemon's code, and SCM restarting the service
     twice before leaving it stopped. A `WIN32_EXIT_CODE : 0` with no restart is
     the pre-fix behaviour regressing.
  4. Then `sc.exe stop synosync` and confirm the operator-requested stop reports
     `WIN32_EXIT_CODE : 0` and triggers NO restart - failureflag makes any
     non-zero code a failure, so a clean stop leaking a code would fight the
     operator.

- **Windows: `-User` install actually starts.**
  `CreateService` - which `New-Service -Credential` calls - does not grant the
  run-as account `SeServiceLogonRight`; the services.msc snap-in does that as a
  separate step. Without it `Start-Service` fails with 1069 ("logon failure"), so
  the install path the script's own IPC note recommends never comes up on a fresh
  machine. `install-service.ps1` now grants the right itself
  (`LsaAddAccountRights` via an inline P/Invoke) after creating the service.
  `install-service_test.ps1` covers the argument logic and the shim's compile;
  what it CANNOT settle is whether the grant actually satisfies SCM, because
  reading LSA policy needs an elevated token and granting it needs a throwaway
  account. Confirm on a VM:

  1. Create a local test account (`net user synotest <pw> /add`) and verify it
     does NOT already hold the right: `secpol.msc` > Local Policies > User
     Rights Assignment > "Log on as a service" - the account must be absent.
     (A machine where a GPO already grants it cannot observe the regression.)
  2. From an elevated console: `.\install-service.ps1 install -User .\synotest
     -Password (Read-Host -AsSecureString)`.
  3. Expect the script to print `Granted 'Log on as a service'
     (SeServiceLogonRight) to '.\synotest'.` and the service to reach RUNNING.
     A `1069` warning naming the right is the pre-fix behaviour regressing; a
     warning that the grant itself failed names the secpol.msc remedy.
  4. Re-run the install after `uninstall`: the second run must print `already
     holds` (the grant is idempotent, and uninstall deliberately does not revoke -
     the account may run other services).
  5. Confirm the CLI control verbs reach the daemon as that user (the point of
     `-User` per DESIGN §8): from a session logged in as `synotest`,
     `synosync status` must answer rather than report an absent daemon.

## CI policy - manual gate, not a scheduled run (decision)

**Decision: the live-DSM harness stays a manually-triggered
gate; it is intentionally NOT wired into a scheduled/automatic CI job.** The
default CI (`.github/workflows/ci.yml`) only ever exercises the harness in its
compile-and-skip form (`go build -tags integration`, `go vet -tags integration`,
the `-run Skips` smoke test) - never against a live stand.

Why manual rather than scheduled:

- A scheduled run needs a **dedicated, disposable test DSM** reachable from CI
  and the `SYNOSYNC_IT_*` secrets (host/port/user/password, plus an app password
  or a non-2FA account so the second device can pair - see *Pairing & sessions*).
  Standing that up and keeping it healthy is an operator decision with real cost
  and a standing credential in CI; it is not a prerequisite for the code gates.
- The correctness model the harness validates against a real server - self-echo
  via `committer_session_id` (`reconcile.go`), read-only detection via the "w"
  privilege bit (`loop.go`), and `pull_event` pagination - is otherwise covered
  by unit tests over a fake transport. The harness closes the residual
  *protocol-surprise* risk (a live DSM behaving differently than the fake), which
  is best run deliberately around a release or a transport/protocol change, not
  on every push.
- Keeping it off the critical path means a transient DSM outage or a consumed OTP
  never reds the main pipeline.

**When to run it (operator checklist):** before tagging a release, and after any
change under `internal/transport`, `internal/actions`, `internal/auth`, or the
`internal/engine` reconcile/cold-start paths. Point `SYNOSYNC_IT_*` at a scratch
DSM and run `make test-integration` (see *Running* above).

**If a scheduled run is later wanted:** add a separate `workflow_dispatch`
(optionally `schedule`) job - never on `push`/`pull_request` - that injects the
`SYNOSYNC_IT_*` secrets and runs `make test-integration`. Keep it isolated from
the per-PR gates so a stand outage can't block merges. This remains a deliberate
operator decision (dedicated DSM + standing secrets) and is deferred until that
stand exists.
