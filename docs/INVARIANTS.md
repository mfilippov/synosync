# Key invariants and conventions - the full text

Every invariant the code keeps, with the measurement, errno history or bug behind
it. Read the section here before changing the code an invariant guards.

## Contents

- [The two-client findings (H1-H7)](#the-two-client-findings-h1h7)
- [Never-lose doctrine](#never-lose-doctrine)
- [Upload guard](#upload-guard)
- [Recursive remove](#recursive-remove)
- [markSynced](#marksynced)
- [Enqueue first](#enqueue-first)
- [Serialization](#serialization)
- [Reply evidence](#reply-evidence)
- [Schema rule](#schema-rule)
- [Keep-alive frames](#keep-alive-frames)
- [Move endpoints](#move-endpoints)
- [pull_event is a log](#pull_event-is-a-log)
- [long_poll must be able to hold](#long_poll-must-be-able-to-hold)
- [Result codes](#result-codes)
- [Mutating verbs](#mutating-verbs)
- [Path safety](#path-safety)
- [Streaming](#streaming)
- [Conventions - full text](#conventions--full-text)

## The two-client findings (H1-H7)

The code refers to these by number, several hundred times. This is the index: each
was measured on 14.08.2026 with
two live clients against a real DSM, and each is the reason a guard exists.

| # | What was measured | What it forced |
|---|---|---|
| **H1** | `upload` is `compare_mtime`: the server accepts any write whose mtime is not older, and never checks the base version. The 24579 the client waited for essentially never arrives. | The client proves divergence itself before every push (`guardUploadTarget`), and funnels a diverged target into keep-both. |
| **H2** | A dropped inbound rename does NOT repair itself; the promised "a later modify or the cold-start pull will materialise it" never came on either client. | A dropped rename carries its own compensating op, enqueued in the same transaction as the trace and the ack. |
| **H3** | A self-echo naming a node the record does not was read as "another node holds this path" in BOTH directions, leaving the record naming a node the server had already deleted - which armed a destructive remove. | Adoption is decided by POSITION (`sync_id`), not identity alone; an unpositioned echo never supersedes. |
| **H4** | A deterministically-rejected push retried about 80 times a second for the whole confirmation window. | A geometric pause between identical failures, and a synthetic streak code for every non-answer so nothing retries unpaced. |
| **H5** | 32772 is genuinely TRANSIENT: a read of a node another client just overwrote fails and succeeds a second later. | No run of 32772, however long, is evidence of permanence; it can never dead-letter an op. |
| **H6** | A fold-key collision was protected but SILENT, so a second spelling of a synced name looked synchronised and was never backed up. | A durable dead-letter names the synced twin; unsyncable records surface as `Unresolved` rather than sitting invisible. |
| **H7** | `remove` of a directory is RECURSIVE and takes children no local baseline ever saw. | A directory delete enumerates the server subtree first and refuses on any child it cannot account for. |

## Never-lose doctrine

- **Never-lose doctrine (DESIGN §6).** A destructive apply (delete, overwrite,
  rename-onto) must prove the target content is the last-agreed base; anything
  diverged, untracked, or unprovable is preserved whole as a `_Conflict` copy.
  Fail toward protected/unsynced, never toward data loss.

- **An empty directory that still records contents is not a deletion**
  (`emptieddir.go`). The empty-scan debounce covered the view ROOT only, so a
  mount point one level down - `<view>/Photos` on a USB disk, an NFS export, a
  bind mount - read after an unmount as an ordinary empty directory. Every gate
  downstream then behaved correctly and the outcome was still a wipe: the scan
  diffed each baseline beneath it as vanished, each remove's precondition passed
  (the server holding exactly what the baseline recorded is what an UNTOUCHED
  subtree looks like), and `remove` is recursive. The premise was the false part,
  not any gate.

  The discriminator needs no device syscall and no history: **deleting a folder
  removes the folder.** `rm -rf dir` leaves it ABSENT, and the scan surfaces no
  entry; an unmount leaves it PRESENT and EMPTY. A directory that is still there,
  holds nothing on disk, and has synced baselines recording children is therefore
  in a state no deletion produces, and its subtree's removes wait for the same
  corroboration the view root's do - a count of scans AND
  `emptyScanConfirmMinSpacing` of wall clock between counted ones, because a
  watcher wake resets the scan throttle and an unmount's own fsnotify burst
  delivers the whole threshold within seconds.

  The one legitimate shape caught by this is emptying a folder without removing
  it (`rm -rf dir/*`). It converges on its own, just after the debounce. The
  streaks are in memory, so a restart begins the run again: that delays a genuine
  empty-out and can never shorten the evidence for one.

## Upload guard

- **The server is NOT the arbiter of divergence - an outbound push must prove it
  itself.** Measured 14.08.2026 with two live clients: `upload`'s
  `conflict_policy: "compare_mtime"` accepts and OVERWRITES whenever the request's
  mtime is not OLDER than the node's, and the request carries no base version at
  all (`max_id`/`sync_id` are ignored - a deliberately stale value is still
  accepted). So `24579` never arrives in the ordinary case, and the whole
  keep-both funnel it fed was being tested against an input production almost never
  produces. Every `OpUpload` therefore probes the target with `QueryNode` FIRST
  (`guardUploadTarget`, `outbound_uploadguard.go`): a modification compares the
  server hash against `Baseline.ServerHash`, a create asks only whether the path is
  occupied, and either divergence funnels into `funnelUploadConflict` - the same
  resolver `24579` funnels into, which remains the second line, not the first. The
  probe's divergence base is never the RECORD's `LocalHash`: `planUpload` has already
  written the unsent local edit into it, so falling back there would compare the new
  bytes with themselves. It is `Op.BaseLocalHash` instead - `planUpload`'s
  reconcile-time capture of the SYNCED record's `LocalHash`, taken before that stamp -
  and carrying it is what keeps the guard from going inert: `applyUploadOut` assigns
  the upload reply's hash to `ServerHash` verbatim, so a reply shape that omits it
  BLANKS the field on every successful push, and an empty base degrades the guard to
  node IDENTITY, which a peer's overwrite of the same node always satisfies. That is
  not a weaker guard, it is H1 itself. Only a modify off a `StateSynced` record may
  supply the capture (queuerepair's stranded record is `StatePending`, its `LocalHash`
  already the un-pushed edit). An inconclusive probe leaves the op queued - fail
  toward NOT pushing, and that includes the `ErrPathError` answer: for a MODIFY,
  "the node is gone, so there is nothing to overwrite" would make the whole guard
  inert on one timed-out probe (28672 conflates absence with a server-side
  timeout), so the absence is corroborated by repetition through the shared
  `absencestreak.go` memo (`uploadTargetAbsent`) exactly as the sibling
  directory-delete precondition corroborates the root of its walk - and, like it,
  the unproved state carries a synthetic streak code (`uploadAbsenceUnprovedCode`) so
  the re-probe is PACED rather than re-issued every drain (unpaced it is the same
  ~80-requests-a-second spin H4 measured), but a TRANSIENT one, so the dead-letter
  exit never applies: a push must not be dropped for its own precondition still
  gathering evidence. A CREATE keeps
  the single-probe reading - there the sentinel is the ORDINARY answer for a free
  path, and corroborating it would put `absentNodeConfirmations` drain passes in
  front of every new file.

## Recursive remove

- **`remove` of a directory is RECURSIVE on the server**, so an outbound delete
  must enumerate before it applies. Measured on the stand
  (`TestIntegration_RemoveDirectorySemantics`): one `remove` of a non-empty
  directory succeeds and takes the contents with it. The never-lose gates can only
  reason about descendants the local baseline KNOWS, so a node a peer added after
  this client last looked into the directory was invisible to every gate and left
  with it - destroyed on the server and in both replicas at once.
  `verifyServerSubtreeAgrees` (`outbound_removedir.go`) walks the subtree on the
  server breadth-first under `removeDirVerifyCap` (plus `limits.ColdStartCatalogCap`
  where an operator set it - this walk, unlike the once-per-view cold-start pull, runs
  inline on a destructive op under a worker lease and re-runs on every attempt, so it
  is never uncapped) and refuses on any child
  the baseline does not know, a known child whose server hash moved, or a foreign
  `file_id` on a tracked path; the refusal is a `StateConflict` + a dead-letter
  NAMING the offending child, never a silent drop. `list` is an untrusted SERVER
  reply: a child name is refused unless it is one plain component (`safeChildName` -
  `path.Join` CLEANS, so a child called `.` would re-queue the directory being walked
  and spin forever), where the separator is `/` and ONLY `/`: a backslash is an ordinary
  rune in a name a POSIX client can hold, and refusing it locked the directory above any
  such name out of deletion permanently. Each directory is descended into once (visited
  by fold key). The cap bounds BOTH halves of the verification - the server walk and the
  local `subtreeBaselines` snapshot feeding it - since an unbounded O(subtree) read under
  a worker lease on a destructive op is the "bounded work" convention's counter-example.
  A child whose KIND changed refuses in BOTH directions, and neither direction may
  rest on the descend arm's safe exit: base=file/server=dir is decided by `is_dir`,
  but base=dir/server=file is not - a sparse reply simply omits the key, so `!IsDir`
  is no evidence, and descending would stake the whole guarantee on the unmeasured
  premise that `list` of a FILE errors. A server answering it like an empty directory
  enumerates nothing, returns the verification `nil`, and the recursive remove takes
  the peer's replacement. The evidence is the CONTENT HASH, which is measured: a
  file's listing carries one and a directory carries none (docs/INTEGRATION.md,
 ), so a hash on a child the base records as a directory proves a
  file stands there - unless the child's `is_dir` is positively SET, which excludes the
  arm for the same reason the sibling trusts it: a flag the server cannot echo back from
  the request is proof when set, and the base=file arm above has already consumed every
  other shape. Without that exclusion a server decorating a FOLDER entry with any
  hash-shaped value refused every directory delete containing a subdirectory -
  terminally, which abandons the user's deletion for good on evidence of nothing.
  The delete target's own KIND has THREE witnesses, ORed, because the record can be gone
  by apply time (a crash between `Enqueue` and `planRemove`'s flip, an ancestor's
  teardown, a success-path `DeleteBaseline` whose Ack failed): `removeTargetFacts`' record
  (one decode serving both the divergence base and the kind), `op.BaseIsDir`
  (`planRemove`'s durable capture from the vanished baseline - the same queue field the
  inbound remove uses, now carried outbound too) and the pre-turn probe's own positive
  `is_dir`. An unreadable record supplies no kind (`kindUnknown`) rather than "not a
  directory" - reading it as the latter skipped this whole precondition, the one input
  here that fails toward DELETING - but the refusal belongs to the caller once ALL THREE
  witnesses have failed to place the target, never to that read alone. Refusing on the
  record by itself froze plain FILE deletes for good: a file has no recursive-delete
  hazard, yet `errRemoveDirUnverified` is terminal by evidence, so the delete
  dead-lettered and `markOutboundConflict` left a `StateConflict` record at a path with no
  local file that `localReconcile` never drives again. A target the probe places as a FILE
  (a content hash with `is_dir` unset - the same measured witness the subtree walk's
  kind-change arm rests on) therefore proceeds; one nothing places refuses. But a file the
  probe places where the base says DIRECTORY is not a proceed either - it is the target's own
  instance of the subtree walk's kind-change divergence, and it refuses (`StateConflict` + a
  trace) rather than letting the directory arm win and `list` a path that is a file: that is
  the unmeasured premise the child arm exists not to rest on, and a server answering a file
  listing like an empty directory would enumerate nothing and let the recursive remove take
  the peer's replacement.
  `baselineFromOp` reads `BaseIsDir` for the same
  reason: a `StateConflict` record synthesised at a directory path but typed as a FILE
  makes this verification refuse every ancestor's deletion, terminally.
  `listed == false` is not proof
  of emptiness - the op stays queued (`errRemoveDirUnverified`), and neither is an
  `ErrPathError` listing of a DESCENDANT the parent just enumerated as present, since
  28672 conflates absence with a server-side timeout and one such answer would
  authorise the recursive wipe of a whole subtree. The walk's ROOT may read that
  sentinel as "nothing here" only once the absence is CORROBORATED by a path-addressed
  probe of the directory itself (`absenceCorroboratedByNodeProbe` with an empty file id -
  the OCCUPANCY question: any node answering refutes it), repeated across drain
  passes and spaced by `absenceStreakMinAge`. The uncorroborated arm was the one door
  H7 left open: the pre-turn `QueryNode` can have answered POSITIVELY for the very same
  path moments earlier, and the contradiction was silently resolved in favour of
  deleting. **A baseline is not by itself proof of
  agreement** - reconcile pre-writes a `StatePending` record carrying the event's own
  `FileID` and `ServerHash` for every inbound create it has QUEUED but not applied, so
  a peer's brand-new file matches the identity and hash tests exactly like agreed
  content. The queue is the discriminator (`queuedInboundUnderPrefix`): a child a
  queued INBOUND op still owes is divergence. That holds for the interleaving where
  the local delete was scanned FIRST - the directory's own remove then holds the
  lower seq and serialises AHEAD of the child's op - and NOT for the reverse, where
  the peer's download was enqueued first and has already applied, leaving the child a
  synced, hash-matching baseline the enumeration would wave through. The local
  evidence owns that door: a directory standing on disk again at apply time disproves
  the deletion the op was derived from, so the op is reverted to `StateSynced` and
  acked before the lease (`errRemoveDirLocallyPresent`), and the next scan re-diffs
  the tree as it now is. `errRemoveDirUnverified` carries a synthetic negative streak
  code (`removeDirUnverifiedCode`) so a subtree that cannot be enumerated is paced
  and eventually dropped observably, rather than re-walking every drain forever.

## markSynced

- **Only apply advances a baseline to `StateSynced`** - reconcile marks
  pending; `markSynced` is the single agreement-recording point.

**Eight places in the engine assign `StateSynced`, and the invariant above is
about exactly one of them.** The short form was read for a while as "there is one
writer", which is false and made the other seven look like violations. They are
not: NEW agreement - a claim that local and server content match, made on
evidence a transfer produced - is recorded in one place. The rest either overlay
our OWN committed change or put a record back the way it already was.

| Site | Function | Why it is not new agreement |
|---|---|---|
| `enginepath.go` | `markSynced` | **The agreement point.** Every apply that transferred or verified content ends here. |
| `reconcile.go` | `overlaySelfEchoFacts` | Self-echo: the server is describing this client's own committed write. It never touches a record a queued apply owns. |
| `reconcile.go` | `overlaySelfEchoMoveFacts` | The same, for the move axis. |
| `outbound.go` | `revertRelocatedOutbound` | REVERT: a dropped push un-relocates its baseline to the state it held before the push was planned. |
| `outbound_mutate.go` | `revertRelocatedRemove` | REVERT, delete axis. |
| `outbound_vanished.go` | `resolveVanishedUpload` | REVERT: a modify whose local file vanished is put back synced with cleared local facts, which is precisely the shape `detectRemoves` needs to emit the delete. |
| `inbound_remove.go` | `repairSupersededRecord` | REPAIR: a refused delete proved the record current, so its suspension is undone - and only when no queued op still owes the node. |
| `inbound_rename.go` | `applyRename` | The relocation's own record. A BARE one (no hashes, node never tracked) is deliberately excluded and recorded `StateConflict` instead. |

The rule to apply when adding a ninth: if the write would let a path claim
agreement that no transfer established, it belongs in `markSynced` or nowhere.

## Enqueue first

- **Enqueue before baseline mutation** (both reconcile passes), so a crash
  between the two leaves a replayable state, never a pending record with no op.
  **A create/modify commits both writes in ONE transaction instead**
  (`enqueueAndMarkPending`), because ordering is not enough there: the pending
  baseline is the only thing arming that download's never-lose gate, and with it
  missing `untrackedFilePresent` reads the occupant as "not reconcile-produced"
  and promotes over it with a REPLACING rename and no divergence base. The replay
  argument does not save it either - the restart drains the queue BEFORE it
  re-pulls the batch (`coldStartPending` → `localScan` → `drainQueue` →
  `pollOnce`), and `localScan` skips any path that already carries a conflicting
  queued op, so nothing re-arms the gate. A move and a delete stay on the ordered
  path: a delete's divergence base is captured into the op itself before either
  write, and a losing move relocation leaves the node tracked at its source,
  which is what `applyRename` expects to find.
  The one drop that cannot honour any of this is the store's corrupt-queue quarantine -
  the dropped record's view and path are exactly what is undecodable - so the
  engine arms a repair sweep on an advance of `store.QuarantinedOps()` and
  re-derives the owner of every uncovered pending baseline in the same drain pass
  (`engine/queuerepair.go`). The trigger is in-memory by design; residual in
  DESIGN §13.

## Serialization

- **Serialization invariant.** Two queue ops conflict iff their resolved
  on-disk paths are equal or ancestor/descendant (`serialize.go`); a rename
  serialises on both endpoints.

## Reply evidence

- **Reply evidence.** A mutating turn's reply must carry a result VERDICT; read
  verbs whose emptiness is itself consumed (`List`, `QueryUserInfo`, cold-start
  `PullEvent`) need that verdict or the payload key - a bare `{}` is never proof.
  The verdict is NOT a flat `errno`: a live DSM reports failure as a nested
  `error{action,code,reason}` map and success by omitting it while marking the
  frame `type:"response"`. `actions.errno`/`auth.errno` are the single owners of
  that decision and still accept a flat `errno` so the in-memory fakes work.
  `List` surfaces this to its callers as a `listed` bool: every absence prover
  (`resolveServerNodeAbsence`, `nodeGoneByID`, `confirmAbsentByParentList`)
  treats an unlisted reply as inconclusive and leaves the op queued. A *present*
  listing is not completeness evidence either: an absence derived from
  `file_list` is corroborated by a path-addressed `QueryNode` of the node itself
  (`absenceCorroboratedByNodeProbe`), decided by OCCUPANCY rather than identity -
  ANY answering node refutes it, which is what makes it applicable to the id-less
  arcs (`ancestorConfirmsAbsent`, a fresh create's vanished-upload probe) - and,
  because 28672 conflates "no such path" with a server-side timeout, it takes
  `absentNodeConfirmations` agreeing probes counted at most once per drain pass
  AND once per `absenceStreakMinAge` (`absencestreak.go`), the node-level twin of
  `absentRootConfirmations`. **Pass count alone is not a duration** - nothing
  spaces two drain passes or two local scans in wall clock, so every
  corroboration-by-repetition streak carries a wall-clock floor as well as a
  count: `unmappedServerErrMinAge` (the exemplar), `nameRejectionMinAge`,
  `absenceStreakMinAge`, `emptyScanConfirmMinSpacing`. A view
  missing from `view_list` is
  suppressed only after `absentRootConfirmations` consecutive probes of its own
  root confirm it gone - one path-error is no corroboration, since the sentinel
  conflates a real absence with a probe timeout - and the inconclusive arm is
  bounded by `absentProbeAttempts` so a revoked view still converges. Long_poll's per-view quarantine
  applies the same rule to a different axis: a view is quarantined only on the
  SERVER's own errno for its single-member subscription (`pollViewRejected`) -
  a probe that fails on transport, decode or its own budget is no verdict.

## Schema rule

- **Schema rule.** Any change to a persisted `Config`/store record field bumps
  `store.CurrentSchemaVersion`; additive tolerantly-decoded fields are exempt.

## Keep-alive frames

- **The server interleaves unsolicited `type:"keep_alive"` frames.** Every reply
  read must step over them (`actions.isKeepAlive`, `auth.readSkippingKeepAlive`);
  one taken as a reply decodes cleanly, carries no verdict and poisons the
  connection. It is not upload-specific - any turn can be preceded by one.

## Move endpoints

- **A `move` event's endpoints are inverted from the obvious reading.** The
  top-level `path` is the SOURCE (mirroring `source.path`); the new location is in
  `destination.path`. Decoding the top level as the destination silently drops every
  inbound rename.

## pull_event is a log

- **`pull_event(sync_id=0)` is a LOG, not a snapshot.** Cold start no longer
  reads it at all - it walks `list` (`internal/servertree`), whose answers are
  final on arrival. While it did, it replayed the log -
  create/modify assert, delete retracts, move relocates - instead of filtering
  non-creates and clamping, which clamped to the oldest deletion in the view's whole
  history and made a fresh pairing re-consume everything. Measured 02.08.2026: a live
  DSM COMPACTS it per node - a create is absorbed by any later row for the same node,
  so `create+modify` leaves one `modify`, `create+delete` one `delete`, `create+move`
  one `move` at the destination; a move row does NOT absorb into a later delete (both
  survive). A live create is therefore reported as `action:"modify"` and a `create` row
  is a shape this server essentially never emits - a `create`-keyed arm is dead code
  against it (`preBatchDeletePath`). The replay converges on every observed shape,
  which is the property that matters; the tombstones are what still make the log grow
  with a view's deletion history.

- **An empty `pull_event` page proves its own WINDOW is empty, never the log.**
  Measured 23.08.2026 on DSM 7.2.2-72806 / Drive Server 4.0.3-27892:
  `pull_event` pages by a FIXED-WIDTH `sync_id` window (2000), not by a row
  count - the cursor advanced by exactly 2000 per page while the rows on those
  pages ranged from 137 to 2000. Combined with the compaction above the log is
  sparse: a deleted node's tombstone lands at a NEW, higher `sync_id`, vacating
  the slot its create held, so a bulk deletion leaves a HOLE. A window falling
  inside a hole answers with nothing while rows continue past it, and still
  reports `next_sync_id = cursor + 2000`.

  Only the INCREMENTAL poll reads the log, and it already advances the cursor to
  the reported `next` on an empty page, which steps over a hole. Cold start used
  to read the log too, stopped at the first empty page, and a live install
  therefore finished at cursor 16000 - a window boundary - against a head of
  856387, then replayed ~840k historical rows through the ordinary inbound path,
  resurrecting directory trees its owner had reorganised away years earlier. It
  no longer reads the log at all: it walks `list` (`internal/servertree`), whose
  answers are final on arrival.

## long_poll must be able to hold

Measured 17.10.2026 on DSM 7.2-72806 / Drive Server 4.0.3 (pinned by
`TestIntegration_LongPollBlocksWhileIdle`). A `long_poll` is answered AT ONCE,
with the view in `changes`, unless both of these hold:

- **The request carries the `profile_digest` of the previous reply.** Without it
  the reply says `profile_changed: 1` and hands the digest out again.
  `actions.Client` remembers it, and the engine carries it across the rebuilt
  event client (`profileDigester`).
- **Each subscription's `lock_id` is not behind the server's node-lock counter.**
  0 fired every time, 1 and 2^40 held. The client takes and shows no node locks,
  so it sends `math.MaxInt32` and is woken by `sync_id` alone, which behaves as
  documented: it fires below `get_max_sync_id`, holds at or above it.

Before this the client sent neither, every poll returned in ~40 ms with nothing
to pull, and an idle daemon ran its local scan back to back - most of a core on a
184k-file NAS. Unit tests could not see it: every fake answered the poll as the
client assumed the server would.

An idle poll asking for 300 s is answered, empty, LATE - after 320 to over 360 s,
varying poll to poll (measured on two servers) - and the server sends nothing, not
even `keep_alive`, while it waits. So liveness cannot be read from traffic, and the
client deadline (the timeout plus `limits.poll_deadline_slack_s`, 300 by default) is
only a backstop: TCP keepalive, on by default for every dialed connection, detects a
dead peer first. A slack of 15 cut idle polls short; store v12→v13 raised it.

The server raises a timeout under 300 s to 300 s (`MinPollTimeout`), so a
fairness pass (`fairnessPollTimeout`) can only end on its own client-side
deadline; that end is expected, logs nothing, and re-dials the connection.


## Result codes)

- **Server result codes are measured, not guessed.** Absence is reported by
  DIFFERENT codes per verb, and only some of them mean absence at all: `32769` is
  "no such node" for the read/place verbs (`query_node`, `list`, `download`,
  `upload`), `40961` is a mutating verb (`remove`/`rename`) refusing to apply -
  ambiguous between an absent target and a malformed request - and `28672` is
  INPUT validation (an empty path, a below-floor poll timeout). All three map to
  `ErrPathError` because that is the signal the absence provers consume, which is
  safe only because every absence verdict is corroborated by repetition. `40962`
  is the optimistic-concurrency rejection (stale cursor) and is RETRIED, never
  treated as failure. `12289` is a `long_poll` SUBSCRIBE rejection: one
  unacceptable member view rejects the whole subscription, and a single-view
  subscribe of that member is rejected on its own - which is what makes the
  poison-view bisect in `engine/pollquarantine.go` able to attribute it. `24580` (a
  sibling differing only by case), `24581` (the name is held by a node of the
  other type) and `24584` (the PARENT the create names is a file, not a directory)
  are structural verdicts - waiting changes nothing, because the premise is a fact
  about the server's tree rather than about timing - so a push refused by any of
  them dead-letters on the shortened `nameRejectionDrains`/`nameRejectionMinAge`
  confirmation (`engine.serverStructuralRejection`). 24581 and 24584 are the two
  sides of ONE clash: when a peer makes a path a file and this client makes it a
  directory, the mkdir for the path draws 24581 while every push BENEATH it draws
  24584. Left in the unknown-code window those descendants wedge the queue for five
  minutes - `blockedKeys` holds every path-overlapping op behind them, including
  the INBOUND apply that resolves the clash - and each retry of an upload also
  costs a pool session, since a refused upload leaves its two-frame turn desynced
  (measured 2026-08-25; a refused `mkdir` does not). `32772` is the one code measured TRANSIENT (a
  read of a node the server is still settling, served a second later), so no streak
  of it is ever evidence of permanence: it is paced but never escalated.

## Mutating verbs

- **The mutating verbs are asymmetric - do not "unify" them.** `remove` takes a
  leading-slash path and NO cursor (any cursor value is refused). `rename` takes
  `src_path`/`dst_path` (not `path`/`to_path`), also leading-slash, and REQUIRES a
  cursor - but the OBJECT's, not the view's: 40962 is "Object has been modified", so
  the value is the source node's own `node.max_id` (`nodeCursor`, one turn, immune to
  concurrent commits elsewhere). The view-wide `get_max_sync_id` LAGS the log and
  fails under concurrency; a bounded retry still covers a real race on that node. Both shapes are pinned by `TestRemoveWireShape`/`TestRenameWireShape`;
  those tests are load-bearing, since a regression turns 40961 from "already gone"
  into "your request was wrong" while the engine still reads it as the former.

## Path safety

- **Path safety.** Untrusted server paths are containment-checked
  (`filepath.IsLocal`) and applied with no-follow resolution that refuses every
  component redirecting to another named entity - a symlink anywhere, a junction
  or other name-surrogate reparse point on Windows (`pathRedirects`); NFC
  canonical paths are resolved to the real (possibly NFD) on-disk spelling
  before disk mutations. Two Windows-only additions close the "addressed ≠
  landed" class, where no errno ever arrives so no classifier could catch it:
  a path the host will not materialise VERBATIM is refused
  (`hostRepresentablePath`, `namemangle*.go` - a component ending in a dot or a
  space, which Win32 silently STRIPS), and a materialising verb whose path
  component resolves to a differently-named entity is refused
  (`rejectShortNameAlias`, `shortname*.go` - an NTFS 8.3 alias; an inconclusive
  resolve fails OPEN, since a filesystem that cannot answer is not one that
  aliases). Both are enforced at the reconcile layer BEFORE any baseline
  mutation, with `abs()`/`onDiskAbsPrefixNoSymlink` carrying the same gate as
  defence in depth for an op an older build already queued.

## Streaming

- **Streaming.** Never fully buffer large binary values - stream them.

## Conventions - full text

### Pure Go, stdlib-first

- **Pure Go, stdlib-first.** No cgo, minimal deps: `go-keyring`, `x/term`,
  `bbolt`, `x/sys`, `fsnotify`, `x/text` (NFC), `go-winio` (Windows-only
  named pipe).

### TLS only

- **TLS only.** Never add plaintext-HTTP fallbacks.

### Bounded work

- **Bounded work.** Every network or disk unit of work carries a time bound.
  Where the length is server-controlled, that bound is an *idle/progress* budget
  re-armed on progress (`actions/idle.go`), never a fixed deadline that would
  abort a legitimately slow transfer. A long walk+hash bounds its CANCELLATION
  latency instead: batched `ctx.Err()` checks for the stats, an unconditional
  check immediately before each whole-file read (`scanCtxCheckEvery`).

### Per-OS error classification

- **Per-OS error classification.** A terminal-error classifier lives in
  build-tagged per-OS files (`nametoolong_*.go`, `diskfull_*.go`,
  `dirnotempty_*.go`, and the non-errno gates `namemangle_*.go`,
  `shortname_*.go`, `reparse_*.go`), never in a GOOS branch, so each list is
  compiled and vetted on its own platform - and each ships a `*_windows_test.go`
  /`*_other_test.go` fixture pair supplying the HOST-NATIVE code, since a test
  that injects the POSIX errno passes on Windows through the shared check alone
  and leaves the per-OS list dead.

### Direct Win32 calls

- **Direct Win32 calls.** `os.*` gets `fixLongPath` for free; a raw
  `windows.UTF16PtrFromString` does not, and fails on exactly the >MAX_PATH
  trees `os.*` just created. Every direct Win32 path argument goes through
  `fsutil.ExtendedPath` (conditional on length - the extended form disables
  Win32 normalisation and must not be applied everywhere).

### Testing

- **Testing.** Each task ships with tests; prefer table-driven tests and
  in-memory pipes over real network access. A fixture depending on host
  filesystem behaviour (symlinks, chmod denial, read-only dirs) PROBES it and
  `t.Skipf`s (shared probes in `internal/engine/hostcaps_test.go` and
  `internal/watcher/hostcaps_test.go`) rather than
  branching on GOOS/euid - a GOOS branch does not just skip where the behaviour
  is absent, it skips where it is merely INCONVENIENT, and that is how the whole
  Windows arm of the containment guards went unexecuted;
  host-absolute paths via `hostAbs`
  (`internal/config/paths_test.go`). **A fake must never be kinder than the measured
  server** - the root cause the two-client round diagnosed: every keep-both funnel had
  full unit coverage against a fake that returned `ErrConflictVersion`, a verdict the
  live DSM issues only on an OLDER mtime, i.e. essentially never. Where a test rests on
  server semantics, model what the stand MEASURED (a recursive `remove`, an mtime-only
  upload policy). And the never-lose invariants of DESIGN §6 are only truly exercised
  by the TWO-ENGINE integration scenarios (`test/integration/twoclient*_test.go`): a
  single-client harness has no second baseline that can diverge, so it structurally
  cannot reach those shapes.

### Style

- **Style.** Doc comments on exported symbols, big-endian on the wire, errors
  wrapped with context. Follow the surrounding code.

### Go version and module path

- Go 1.27, module path `github.com/mfilippov/synosync`.

### Coverage and cross-GOOS gates

Always run `go test ./...` (or `make test`) and `make lint` before considering
a task done. Coverage is a blocking gate (`make cover-check`) for: `md4`,
`sproto`, `secrets`, `transport`, `auth`, `actions`, `store`, `engine`,
`fsutil`, `watcher`, `ipc`, `daemon`, `normpath`, `netctx`, `backoff`,
`service` - sixteen, matching `DEFAULT_GATED_PACKAGES` in
`scripts/coverage-gate.sh`, which is the authoritative set. The floor is
**per build context**, because a run measures only the files its GOOS compiles:
≥80% on linux (`LINUX_THRESHOLD`, the authoritative DESIGN §12 floor) and
≥`CROSS_THRESHOLD` (75%) on the native windows/macOS CI jobs, which are the only
runs where the `_windows.go`/`_darwin.go` files enter a denominator at all. The
cross floor is a ratchet - raise it as those files gain tests; never lower the
linux one to meet it. Same shape for the other two gates: `lint-cross` and
`vulncheck-cross` run per release GOOS, since a single-GOOS lint or reachability
scan cannot see per-OS files.
