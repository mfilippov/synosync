# `internal/engine` - the two-way sync core

Each section lists the trap that bites an editor; the reasoning behind it is in the
code comments and DESIGN §13.

## Files

- [engine.go](#enginego) - engine lifecycle, views, pause/resume seams
- [transport.go](#transportgo) - syncClient/Transport seams, per-turn deadline
- [reconcile.go](#reconcilego) - inbound reconcile: events -> queue ops
- [scanner.go](#scannergo) - local hashing walk, cheap-scan hints, protected paths, leftover staging temps (`stalestaging.go`)
- [localreconcile.go](#localreconcilego) - outbound diff, move detection, empty-scan guard
- [collisiontrace.go](#collisiontracego) - fold-key collision dead-letter traces
- [unrepresentable.go](#unrepresentablego) - server names this filesystem cannot hold
- [exclude.go](#excludego) - selective sync: excluded paths and subtree rescans
- [walklane.go](#walklanego) - one worker kept free for the tree walk
- [executor.go and serialize.go](#executorgo-and-serializego) - queue scheduler, serialization, dead-lettering
- [opbackoff.go](#opbackoffgo) - geometric pause between identical failures
- [inbound_rename.go](#inbound_renamego) - the rename DROP side and its compensation
- [inbound applies](#inbound-applies) - inbound_download/mkdir/rename/remove.go and their gates
- [deadletter.go](#deadlettergo) - cleanup of pending baselines left by a dropped op
- [absencestreak.go](#absencestreakgo) - node-level corroboration-by-repetition memo
- [emptieddir.go](#emptieddirgo) - the nested-unmount guard
- [casefold.go](#casefoldgo) - the sync-root case-fold probe
- [progress.go](#progressgo) - what this run has actually moved
- [queuerepair.go](#queuerepairgo) - compensating sweep for the store queue quarantine
- [enginepath.go](#enginepathgo) - markSynced and the containment guards
- [outbound applies](#outbound-applies) - outbound*.go: push, mutate, remove-dir preconditions
- [conflict.go](#conflictgo) - the keep-both resolver
- [coldstart.go](#coldstartgo) - one-time reconciliation
- [coldstartwalk.go](#coldstartwalkgo) - the catalog source: a `list` walk
- [coldstartstream.go](#coldstartstreamgo) - planning and transferring while the walk lists
- [loop.go](#loopgo) - Run(ctx): the sync loop
- [pollquarantine.go](#pollquarantinego) - long_poll per-view quarantine
- [pathkey.go and queuescan.go](#pathkeygo-and-queuescango) - the shared containment and queue-scan predicates
- [platform files](#platform-files) - the per-OS halves, one table

## Overview

- `internal/engine` - the two-way sync core (DESIGN §6). Unit-tested against a
  fake transport and in-memory store via narrow seams. Each file below lists the
  trap that bites an editor; the reasoning behind it is in the code comments and
  DESIGN §13.

## engine.go

  - `engine.go` - `Engine`/`New`, `refreshViews` (view upsert, root-id
    resolution, view-rename adoption matrix in `viewrename.go`), `resolveParentID`,
    pause/resume/trigger seam, disk-full latch, stuck-op counter, view-root device
    tracking. A view missing from `view_list` is suppressed only after
    `absentRootConfirmations` probes of its own root agree, with a bounded
    inconclusive-probe escape so a revoked view still converges.

## transport.go

  - `transport.go` - the `syncClient`/`Transport` seams, `poolTransport` over the
    pool, `withWorker` + the optional per-turn deadline (`limits.op_turn_timeout_s`;
    long-poll and server-sized downloads are deliberately unbounded).

## reconcile.go

  - `reconcile.go` - pure store-only inbound reconcile: events → queue ops in
    ascending sync_id order. `enqueueAndMarkPending` owns the durability pair: a
    create/modify commits the op AND its pending baseline in ONE transaction,
    because that baseline is the only thing arming the download's never-lose gate
    and the restart drains before it re-pulls the batch, so the ordered form left a
    queued download whose gate was disarmed (it overwrote an untracked user file
    with no `_Conflict` copy). Move and delete keep the ordered form; their gates
    do not depend on that write. Then: self-echo handling, pre-batch baseline snapshots and
    cross-batch inherited bases feeding the never-lose capture gates, supersession
    drops for doomed downloads, conflict-funnel deferral for paths with queued
    outbound uploads, containment rejection (`containable`, which also refuses a
    name the host cannot spell verbatim - `namemangle*.go`). A skipped event is
    skipped WHOLE, with one exception: a move OFF a non-containable path ONTO a
    containable one becomes a create at the destination
    (`rescueUncontainableSource`) - otherwise the documented operator remedy
    ("rename it on the server") never converges. The reverse asymmetry stays
    skipped: never-lose leaves the local copy alone. Two arms are positional rather
    than identity-based. `selfEchoSupersedesRecord`: an echo of the client's OWN
    create adopts the record when its `sync_id` is strictly ABOVE the record's
    `ServerSyncID` - the old "any id mismatch means another node holds the path" is
    true in one direction only, and reading it both ways left the record naming a
    node the server had already deleted, which is what armed the destructive remove
    H3 measured. An unpositioned echo (`SyncID == 0`) never supersedes.
    `provenDeferralDiverged` + `enqueueProvenDeferralConflicts`: where an inbound
    create/modify is deferred behind a queued upload, the event hash already differs
    from `ServerHash`, so divergence is PROVED - enqueue `OpResolveConflict` now
    instead of betting on a 24579 that the ordinary case never produces. Enqueued
    after the batch walk, or a move-rescue/delete of the same node would leave the
    resolve on an abandoned path.

## scanner.go

  - `scanner.go` - `scanView(ctx, …)`: no-follow hashing walk, ctx-checked every
    `scanCtxCheckEvery` entries so shutdown/pause never wait out O(bytes) of
    hashing (a pause cancels through `pauseAwareCtx`; `errScanPaused` is neither a
    failure nor terminal), cheap-scan hints reusing baseline hashes on
    `(size, mtime)` match, and the protected-path discipline: every
    observed-but-unsurfaced path is recorded so it never diffs as a deletion.
    `adoptViewRootDevice` gates a device change on content evidence, remembers its
    candidate rather than re-sweeping, and remembers an adopted `old→new` move so
    an EMPTY sibling view can corroborate the same renumbering.
  - A staging-named file (`isStagingName`) is unsurfaced, and one an earlier run
    left behind is logged once and **never deleted** (`stalestaging.go`): the name
    shape is no proof of provenance. "Leftover" takes BOTH not held by this run AND
    mtime before the engine started, because the temp carries the server's mtime
    before its promote, and is briefly unregistered right after `CreateTemp`.

## localreconcile.go

  - The periodic full re-hash is SPREAD (`rehashSlice`, engine.go): each pass drops the
    cheap-scan hints of one bucket of paths (FNV of the fold key, `FullRehashInterval`
    buckets), so every file is re-read once per N passes and no pass re-reads the tree. A
    whole-tree pass stopped every transfer for 2.7 minutes on a 115 GB NAS, right after
    every start (the counter begins at 0).
  - `localreconcile.go` - outbound diff as a single sorted merge-join
    (O(scan + baselines) memory), move detection, fold-key collision protection,
    and the empty-scan debounce + mount-swap guard: `limits.EmptyScanThreshold`
    empty scans that are ALSO `emptyScanConfirmMinSpacing` apart in wall clock,
    since a watcher wake resets the scan throttle and an unmount's own fsnotify
    burst otherwise delivered the whole threshold within seconds.

## walklane.go

  - `walklane.go` - while a walk runs (`walkServerSubtree`: cold start, rescans), every
    lease but the walk's own (`withWalkWorker`, used by `leasedLister`) passes a gate of
    Workers-1 slots, so one worker always serves `list`. FIFO leasing had let multi-GB
    downloads hold every worker for minutes: measured 400-470 entries/s with no transfers,
    74-96 with them. The gate never preempts a running transfer, opens when the walk ends
    (waiters released, not drained), and is absent with one worker. New leasing code goes
    through `withWorker`; only the walk's `list` may use `withWalkWorker`.

## exclude.go

  - `exclude.go` - `config.Views.Excluded` paths (`synosync exclude`) are invisible
    both ways: `walkServerSubtree` neither reports nor enters them (`servertree`
    `Options.Skip`), `scopeExcluded` drops their events before any batch pass (the
    cursor still passes them), and the scan skips AND protects them, so a stale
    baseline never reads as a local delete. `exclude add` writes the config first,
    then `store.DropSubtree` (baselines + queued ops, one transaction): the other
    order would leave tracked-nothing, synced-everything, and every file would push.
  - Crossing the boundary: a server move out is applied as a delete of the source
    (remove gates keep unaccounted edits); a move in is queued in `View.Rescan`,
    since a directory move emits nothing for its children. `exclude remove` records
    the rescan BEFORE lifting the exclusion, and `rescanPending` keeps a rescan of a
    still-excluded path waiting. A rescan is the cold start narrowed to a subtree
    (walk from the parent with the subtree as the only child, `scanViewFrom`,
    `newScopedColdPlanner` which must not retire collision memos outside it); it
    runs on the loop before the poll and leaves the cursor alone.
  - Name rules (`Views.ExcludedNames`, `exclude --name`) are globs on every path
    component, case-insensitive; a matching folder takes its subtree. A FilesOnly rule
    needs a KNOWN file (`config.NodeKind`): a sparse move or an unknown kind never
    hides a folder called `tmp`. `config.SystemNames` (`@eaDir`, `#recycle`, ...) apply
    always and cannot be removed. A first `pair` seeds the stock client's defaults
    (measured on Drive Client 4.2.0: its new-task wizard, `conf/filter`,
    `conf/blacklist.filter`; the help article "Sync Task Rules"); `exclude defaults` adds them
    to an older install. Lifting a name rule rescans `""`, the whole view. `diffLocal`
    protects every out-of-scope baseline, present on disk or not, so a stale record of
    a now-filtered name never becomes a pushed delete.
  - Residuals: exclusions are by PATH, so renaming an excluded path's ancestor (on
    the server, or locally, which pushes the rename) carries the subtree out of
    exclusion with no rescan; a sparse move with
    no `from_path` out of an excluded path is not recognised as one; a file the user
    moves locally INTO an excluded folder reads as deleted and its server copy goes
    to the recycle bin. Deleting a folder locally whose server copy holds a filtered
    file is refused by `verifyServerSubtreeAgrees` (an unknown child), so it stays.

## unrepresentable.go

  - `unrepresentable.go` - a server path `containable()` refuses because a name is
    not valid locally (on Windows `<>:"|?*`, a reserved name, a trailing dot or
    space) is skipped with no op and no baseline, as before, but now also leaves one
    bare dead-letter (`OpUnrepresentableName`) naming the FIRST bad component, so a
    folder of 10k files is one record. Memo per `(view, path)` seeded from the ring;
    32 records per view per process, then one pathless summary. A `..` escape is not
    traced (no rename fixes it); the delete of such a name is ignored. Before this the
    `|` names reached the executor and dead-lettered as a failed stat.

## collisiontrace.go

  - `collisiontrace.go` - the fold-key collision is PROTECTED (neither spelling is
    touched) but it used to be silent, so a second spelling of a synced name looked
    synchronised and was never backed up. `noteCollisionTraces`, called from
    `localReconcileWith` and the cold-start planner (`protectCollisions` owns only the
    protection half), writes a durable dead-letter naming the synced twin. A dead-letter, not `Unresolved`:
    `Unresolved` lives on a baseline and the losing spelling has no key of its own -
    a shared key is the whole collision. Idempotent per `(view, fold key)`, since a
    trace per five-second scan would evict every other trace from the FIFO ring - and
    the memo is seeded from the DURABLE ring on the first pass over each view
    (`seedCollisionNoted`), because the collision outlives the process: a purely
    in-memory memo re-recorded every still-colliding key on every restart, so a restart
    loop churned exactly the ring the rule protects. Two corollaries of that memo are
    load-bearing. It is retired only by a pass that ACTUALLY ENUMERATED the view
    (`observed` - which is why the trace runs PAST every availability guard, unlike
    the protection): an unmounted root reconciles with an empty collision set that
    proves nothing, and retiring on it re-armed the recording for every key - the same ring churn, reached
    by a mount flap instead of a restart. And the trace is recognised in the ring by its
    own `store.OpFoldCollision` type (a value no QUEUE op ever carries - the path-keyed
    queue cannot represent two names sharing one key), not by a substring of the
    operator-facing `Reason`: prose is not a discriminator, and rewording it unseeded
    every memo with nothing to say so. An undecodable baseline is treated as ABSENT here
    like everywhere else - reporting it as a store failure left the collision untraced
    on every 5s pass forever, which is the very invisibility this file closes.
    The memo bounds repetition over TIME; `collisionTraceBurstCap` (32) bounds ONE
    pass's burst, and they are different failure modes - a view holding more colliding
    keys than the 1000-entry ring holds records (an NFD tree merged onto an NFC one)
    retired the whole ring in a single scan, the same harm reached at once instead of
    over an hour. Past the cap the keys are counted into ONE overflow record
    (`noteCollisionOverflow`) that names no key and therefore carries an EMPTY `Path` -
    which is exactly how `seedCollisionNoted` tells it from a real trace and declines to
    seed it. The budget is spent by POSITION in the sorted key list, not by records
    actually written: counting only writes made each restart trace the NEXT 32 keys and
    walk the whole collision set through the ring. The overflow's keys are memoised only
    once its summary write SUCCEEDS, the same rule `recordCollisionTrace` states per key -
    noting them regardless let one transient store error hide the whole overflow for
    the life of the process.

## executor.go and serialize.go

  - `executor.go` + `serialize.go` - the queue scheduler: ops whose resolved
    on-disk paths overlap (equal/ancestor/descendant) serialise, independent ops
    parallelise. Permanently-failing ops dead-letter (durable trace atomically
    with the Ack) instead of wedging their chain. Terminal classes: a path
    component the host can never create (`nametoolong.go`), and a placing verb
    blocked by an existing directory - EXCEPT a download whose
    directory occupant is preserved aside as a keep-both copy so the server file
    can land (`funnelFreshCreateIntruder` for the no-replace promote,
    `preserveDirOccupantAndRetry` for both); dropping it instead left two clients
    permanently and silently divergent, which is the never-lose doctrine's own
    answer applied inconsistently to the two occupant kinds. The classification
    reads the OCCUPANT (`Lstat`), never the errno - Linux answers `fs.ErrExist`
    from `os.Rename`'s userspace dir-guard while Windows answers
    `ERROR_ACCESS_DENIED`, so an errno rule would hold on one platform only - and
    only for a failure that came from the rename itself (`promoteRenameErr`),
    since an earlier-stage failure with a directory merely standing there is not
    the clash: reading a transfer blip that way relocates the user's directory. `fs.ErrExist` is
    otherwise verb-gated
    (`existsBlocksForever`), because Go folds ENOTEMPTY into the SAME sentinel as
    a permanent occupant, so the arm keys on the errno shape (`dirnotempty.go`)
    first and the verb second: transient for the verbs whose apply REMOVES a
    directory, permanent for those that place one. Every subtree delete routes
    through the `removeNode` seam so the gate is drivable. A PUSH-CLASS op (an
    outbound apply, or `OpResolveConflict`, which leaves `applyOp` by its own early
    return) failing the same deterministic way for `unmappedServerErrDrains` drains
    AND `unmappedServerErrMinAge` of wall clock dead-letters with the reason in its
    trace (`servererr.go`); the streak carries one identical unmapped errno or a
    synthetic NEGATIVE stand-in for a non-answer - `resolveNoServerIDCode` (-2),
    `removeDirUnverifiedCode` (-3, a server subtree the delete precondition could
    not enumerate) and `uploadAbsenceUnprovedCode` (-4, a push target whose
    path-error answers have not yet corroborated an absence; TRANSIENT, so it is
    paced but never dead-letters - a push must not be dropped for its own
    precondition still gathering the evidence that will arrive) and
    `removeAbsenceUnprovedCode` (-5, its DELETE-axis twin: an outbound remove whose
    post-turn classification has not yet placed the target - an inconclusive parent
    listing, a still-present node, or one renamed away and not yet re-indexed. Also
    TRANSIENT, and the most frequently reached of all of them, since every child of a
    directory the server's RECURSIVE remove already took enters it);
    `resumeNoCompletionCode` (-1) is retained but unreachable now that resume upload
    is a whole-file turn - and `serverErrStreakReason` renders each for the operator.
    Inbound is deliberately excluded - DESIGN §13. Doom propagates to dependent
    **inbound** renames across drain windows (a dead *outbound* op never dooms - a
    failed push says nothing about the on-disk tree) and, when a drain proved a
    doom it could not record, across passes via `carriedDooms`. Disk-full latches
    `Disk: full` and retries (`diskfull.go` + per-OS lists).

## opbackoff.go

  - `opbackoff.go` - the geometric pause between retries of an op failing the SAME
    way (500 ms → ×2 → 5 s, the order of `limits.local_scan_interval`), armed in the
    one place the streak is already counted. A deterministically-rejected push
    measured ~80 requests per second against the NAS for the whole five-minute
    dead-letter window; the first failure still retries immediately and a change of
    failure class clears the pause. The cap is deliberately BELOW both wall-clock
    floors, so the drop stays decided by evidence rather than by the pause.
    `servererr.go` shortens the confirmation for the codes that are terminal by
    NAME (`nameRejectionDrains`/`nameRejectionMinAge` for 24580/24581) and denies
    the terminal exit entirely to `serverTransientRejection` - 32772, a code the
    server clears itself, and `uploadAbsenceUnprovedCode`/`removeAbsenceUnprovedCode`,
    which are not server verdicts at all but this client's own absence corroboration
    still in progress. The name-rejection window is deliberately BELOW `stuckThreshold`,
    so such a push is dropped before it can ever register as `Stuck` - its observability
    is the escalation Warn plus the durable trace, not the stuck counter.
    The name-rejection pair is typed via `ServerError.Unwrap` rather than
    `errorForCode`, since a mapping branch would zero the `ServerErrorCode` that
    feeds the streak AND the pause. The pause - and ONLY the pause - also covers the
    INBOUND applies (`paceInboundServerFailure`): escalation stays closed to them (a
    drop would discard a server change whose event is already consumed), but 32772 was
    measured on an inbound `download`, and an inbound op refused identically every
    drain spins exactly like a push.

## inbound_rename.go

  - `inbound_rename.go` - a BARE destination record (this apply moved content onto
    the path, byid could not attribute it to the moved node, and neither hash is
    known) is recorded `StateConflict`, not `StateSynced`, for a file. Calling it
    synced asserted an agreement no transfer established: the inbound gates
    contained that (the empty-hash rule funnels into keep-both), but
    `localReconcile` drives synced records, so the next scan pushed those
    unattributable bytes and `guardUploadTarget` - with an empty `ServerHash` on
    both sides - degraded to node identity and let them overwrite the SERVER's
    node. A directory is deliberately out of scope (its record legitimately has no
    hashes, and freezing one stops its whole subtree being pushed).
    It also owns the DROP side: a rename whose moved node never
    reached its destination is dead-lettered WITH a compensating inbound op
    (`dropRenameCompensated` → `compensateDroppedRename`), enqueued in the same
    transaction as the trace and the Ack. The old promise - "a later modify or the
    6c pull will materialise the node" - was measured false on two live clients
    (finding H2), so the drop must carry its own repair. The repair's KIND comes only
    from an id-matched baseline (`compensatedKind`), never from the move event alone -
    a sparse move reports `is_dir` false whatever the node is, and an `OpMkdir` guessed
    from it markSyncs a directory carrying a server FILE's id. So the pessimistic
    direction (a download) is taken, and the wedge that direction used to carry - an
    inbound download addressed at a DIRECTORY node, which no classifier made terminal
    and inbound escalation never drops, so it retried every drain forever with every
    path-overlapping op held behind it on `blockedKeys` - is closed at apply time by
    `errDownloadNodeIsDir` (`inbound_download.go`): after a SERVER-stated download
    failure (`serverRejectedDownload`), `downloadNodeIsDir` re-queries the path on a
    fresh lease and the op converts to an inbound `OpMkdir` ONLY on a positively-set
    `is_dir` with a MATCHING `file_id` - a sparse reply states no kind, and a foreign
    directory standing where our FILE should land would otherwise be recorded under our
    id. Trace, compensation and Ack ride one transaction; no `TrackedBaseline` rides
    along, since the `StatePending` record that armed the download's never-lose gate
    stands at the same path and arms the mkdir's. `compensationTracking`
    builds the `StatePending` record that ARMS the compensation's never-lose gate
    (without it the repair overwrites an occupant directly) and it rides in the SAME
    transaction as a `store.TrackedBaseline`: written BEFORE the commit it survives a
    failed drop, and the byid-indexed claim it leaves at the destination is exactly
    what makes the still-queued rename read its never-materialised node as TRACKED on
    replay - disarming the very foreign-source guards that produced the drop, so the
    replay hauls the foreign source content across. Written after, a crash leaves the
    repair queued with its gate unarmed. Two guards suppress the repair entirely:
    `queuedInboundCoversNode` defers to any other queued inbound op of the node (a
    same-node move chain repairs once, at its final destination), and a `PathByID` hit
    at another path means the node IS materialised there - dropStrandedBaseline
    preserves exactly that record - so re-requesting it would download a duplicate AND
    evict the live record through the byid index.

## inbound applies

  - `inbound_{download,mkdir,rename,remove}.go` - the inbound applies, each guarded
    by the keep-both never-lose gates (diverged/untracked local content becomes a
    `_Conflict` copy, never clobbered), with staging-temp → fsync → rename promotes
    (temp name rune-budgeted to ≤255 while keeping the leading dot and the `.dl-`
    marker `watcher/filter.go` matches) and NFD/NFC on-disk path resolution. Two
    facts must come from the SEALED temp, never from the live path afterwards:
    hash/size when the reply omits them (`deriveStagedDownloadFacts` - re-hashing
    later would absorb a concurrent local edit as server-agreed content) and
    `LocalMTime` (`stagedOnDiskMTime` - the wire mtime is not what the disk holds
    when it is out of range or coarser than a second, and a baseline holding it
    builds a cheap-scan hint that never matches again). The remove path's
    pre-delete divergence sweep hashes every descendant, so it is ctx-checked like
    `scanView` and reports cancellation as an ERROR - a cancelled sweep is never
    read as permission to delete. `inbound_remove.go` carries a second gate beyond
    the id check: a remove whose `FileID` matches the record but whose path is held
    by a SUCCESSOR node the client created itself must refuse
    (`removeSupersededByLaterObservation`). It is decided by POSITION - the op
    carries the event's own `SyncID`, and a record stamped strictly above it, at the
    path or on any descendant, proves a later node stands there. The descendant arm
    is load-bearing: in the measured shape the directory's own record still named
    the dead node and only a child carried the late stamp. The refusal REPAIRS the
    record it leaves behind (`repairSupersededRecord`): a DIRECTORY record still
    naming the deleted node is DROPPED, because `resolveParentID` answers from
    `Baseline.FileID` whatever the state, so every later push into it would address
    `id:<dead parent>/name` and draw a bare `ErrPathError` that neither escalates nor
    paces, while `localReconcile` (StateSynced records only) never re-emits the
    directory - an untracked directory is instead re-adopted by the next scan's
    `OpMkdir`. A record naming a DIFFERENT node stamped strictly above the delete is
    restored to `StateSynced` - the refusal proved it current - but ONLY when no queued
    inbound op still owes that node (`queuedInboundCoversNode`): the arm's premise is
    "this remove is the op that suspended the record", true for the self-echo shape and
    false the moment the successor arrived as an ordinary event with a download of its
    own, where restoring would forge agreement for bytes never transferred. The same
    premise is false for an OUTBOUND owner, and `queuedInboundCoversNode` filters on
    `DirInbound` so it cannot see one: `markLocalPending` suspends a record too, when
    `localReconcile` enqueues a push, preserving the server facts while writing the
    UN-PUSHED bytes into `LocalHash` - and `flipPending` deliberately leaves a record
    naming a different node alone, so the record can sit `StateSynced` while this delete
    is already queued and the next scan's edit hands it to an `OpUpload` the delete then
    serialises ahead of. So a record any queued outbound op owes - at its `Path` or, for
    a rename, its `NewPath` (`queuedOutboundOwnsPath`) - is left UNTOUCHED: restoring it
    claims agreement for bytes the server never received, and if that push later drops,
    `localReconcile` re-derives nothing (disk == `LocalHash`) while every never-lose gate
    reads the record as agreed content. A record a
    queued inbound RENAME owes (`queuedInboundRenameCarries`) is left UNTOUCHED, and
    the arm is decided by what that owner's apply does with the state it finds: a
    download or a mkdir ends in `markSynced`, which overwrites the state whatever it
    was, but `applyRename` deliberately PRESERVES `StateConflict` across a relocation
    and `localReconcile` drives only `StateSynced` records - so marking it here would
    itself be what freezes the path forever, the H6 class this repair closes rather
    than opens. Any OTHER non-synced record is marked `StateConflict`: left as
    reconcile's `StatePending` it would drive no work AND be invisible (no drop, no
    `Stuck`, no `Unresolved`), the silent-unsyncable class H6 closes; `StateConflict`
    is never less protective (every gate tests `!= StateSynced`). A record already
    `StateSynced` or `StateConflict` is left alone - not frozen, or already surfaced,
    and marking the synced one would be what stops the path syncing.

## deadletter.go

  - `deadletter.go` - cleanup helpers resolving the pending baselines a dropped op
    leaves behind (no stranded `byid` entries or orphaned files).

## emptieddir.go

  - `emptieddir.go` - the per-DIRECTORY twin of the view-root empty-scan debounce. An
    unmounted nested mount point (`<view>/Photos` on a USB disk, an NFS export, a bind
    mount) reads as an ordinary empty directory, and every gate below then behaves
    correctly while the outcome is a wipe: each baseline beneath it diffs as vanished,
    each remove's precondition passes because the server holding exactly what the
    baseline recorded is what an UNTOUCHED subtree looks like, and `remove` is recursive.
    The discriminator needs no device syscall and no history - deleting a folder REMOVES
    the folder, so an absent directory is a deletion and a present-but-empty one is not -
    and the corroboration rule is the view root's own: a count of scans AND
    `emptyScanConfirmMinSpacing` between counted ones. The streaks are in memory, so a
    restart begins the run again, which only ever delays a genuine empty-out.
  - The debounce taxed the commonest deletion - the last file in a folder - two minutes,
    so it now applies only without IDENTITY evidence. `noteDirIdentities` remembers each
    directory's (dev, inode) / (volume serial, file index) as last seen populated; an
    emptied directory that is still that object was emptied in place and propagates at
    once. An unmount uncovers a different object, a same-device bind mount included. Unix
    takes the identity free from the scan's lstat; Windows reads it by handle
    (`osDirIdentity`), once per directory per run. There is no per-OS switch.

## casefold.go

  - `casefold.go` - the empirical case-fold probe for the sync root, which overrides
    normpath's GOOS default. A case-insensitive mount under Linux (exFAT/VFAT/NTFS, an
    ext4 casefold dir) or a case-sensitive volume under macOS would otherwise make every
    path match the wrong way.

## progress.go

  - `progress.go` - the counters behind `status`'s `Moved` line: operations applied, bytes
    of content transferred, and the window they were moved over. Atomic, so the IPC handler
    reads them without touching the loop. The window stays zero until the first op applies,
    so a caller deriving a rate must treat zero as "nothing to report" rather than dividing
    by it.

## absencestreak.go

  - `absencestreak.go` - node-level corroboration-by-repetition for the absence
    provers: a listing-derived absence needs `absentNodeConfirmations` consecutive
    `ErrPathError` probes of the node's own path, at most one per drain pass
    (`beginDrainPass`) AND one per `absenceStreakMinAge` of wall clock - a pass
    count is not a duration, since loop.go's leading and trailing drains land
    seconds apart inside ONE overload window. Any answering node, or a `markSynced`
    at the path, clears the streak. A run's continuity is wall clock too
    (`absenceStreakMaxSpan`): counting it in drain passes let a stuck op's own
    exponential backoff space its probes past the bound and restart the run every
    time, so a real absence never converged. In-memory, keyed `(view, path)`;
    `absenceStreakCap` triggers the sweep rather than capping the map, whose real
    bound is the distinct paths probed within `absenceStreakMaxSpan` - evicting live
    entries to hold a number would discard the oldest, which under a mass deletion is
    exactly the run that must survive. The `absenceMemo` seam is what the
    deliberately Engine-free provers take.

## queuerepair.go

  - `queuerepair.go` - the compensating sweep for the store's queue quarantine
    (armed by an advance of `store.QuarantinedOps()`). For every `StatePending`
    baseline no queued op covers it re-derives the owner: on-disk bytes differing
    from the last agreed content, or a file the server never held → `OpUpload`; a
    record with a file id but no local hash is inbound-origin → `OpDownload`
    (pushing the stale bytes would overwrite the server's newer version); an
    untracked directory → `OpMkdir`; PROVABLY nothing materialised → drop. Anything
    else - including a tracked directory, where a push would RE-CREATE a deleted
    node - is frozen, and so is every UNDECIDABLE probe (unresolvable path, `Lstat`
    failing with anything but `IsNotExist`): the only drop rests on definitive
    evidence. It only ever ENQUEUES, never a destructive op, and derives inbound
    ops only for a read-only view. `repairDeferral` remembers the deferred KEYS
    behind a capped geometric backoff so a freeze-only candidate does not re-run
    the O(view) stream every arming.

## enginepath.go

  - `enginepath.go` - `markSynced` (the single place agreement is recorded; it also
    clears the path's absence streak) + containment guards
    (`abs`/`noSymlinkEscape`). What counts as a redirecting component is per-OS
    (`reparse*.go`): a Windows junction redirects exactly like a directory symlink,
    needs no privilege, and Go spells it `ModeIrregular` - a mode-bit test is blind
    to it, so the gate keys on the reparse tag's name-surrogate bit, which admits
    cloud placeholders and refuses symlinks and junctions.

## outbound applies

  - `outbound*.go` - the outbound applies (`outbound.go` dispatcher +
    `outbound_{source,upload,uploadguard,vanished,mutate,removedir}.go`): parent-id resolution,
    single-frame vs streamed upload by threshold, idempotent lost-reply probes,
    server-absence classification (`resolveServerNodeAbsence` - "moved" is a
    positive exit, not forever-transient; a listing-derived absence is corroborated
    by a path-addressed `QueryNode`, `absenceCorroboratedByNodeProbe`), stale-base
    24579 funnelling into keep-both, and a divergence precondition on remove.
    THREE of those preconditions are the client proving what the SERVER does not.
    The third is `guardRenameTarget`: a move probes its destination before the
    turn and refuses an occupied one, funnelling to keep-both at `NewPath`
    (`funnelRenameConflict`) instead of moving onto another client's node. Unlike
    the other two the server's behaviour here is UNMEASURED, and both readings are
    destructive - a replacing rename destroys the occupant through a verb that
    reports no conflict, and a refusing one answers a bare `ErrPathError` that no
    probe can distinguish, which carried no code and so re-issued the turn every
    drain forever. `renameUnsettledCode` (-8) names that refusal so it paces and
    escalates; `renameTargetUnprovedCode` (-7) is its transient twin for an
    inconclusive probe. The two remaining preconditions are
    (both measured on two live clients, 14.08.2026): `outbound_uploadguard.go`
    (`guardUploadTarget`) probes the target before every push, because
    `compare_mtime` is an mtime rule and not a base-version check; and
    `outbound_removedir.go` (`verifyServerSubtreeAgrees`) enumerates a directory on
    the server before deleting it, because `remove` is RECURSIVE and takes children
    no local baseline ever saw. Both refuse toward NOT mutating: an inconclusive
    probe or an unlisted reply leaves the op queued.

## conflict.go

  - `conflict.go` - the keep-both resolver (`OpResolveConflict`): a false conflict
    adopts server facts; otherwise the local file is set aside to a `_Conflict`
    copy (no-replace), the server node lands on the canonical path, and the copy is
    uploaded back and recorded. The provenance row is written BEFORE the
    aside-rename and rolled back only on EVIDENCE - `RenameNoReplace` can fail on
    its post-rename fsync with the move already landed, so the row is dropped only
    when the copy is provably absent and otherwise corrected to name the landed
    copy. Path-addressed turns use the `byid`-tracked spelling (`trackedSpelling`,
    adopted in `applyOp`), so a case-only server rename is neither read as a
    relocation nor resolved against a stale case. The false-conflict fast path is
    hash-proved ONLY: identity holds by construction here, so size alone would
    adopt another client's same-sized edit as agreement. Also hosts the shared
    inbound funnels `preserveDivergentLocal`/`preserveAside`.

## coldstart.go

  - `coldstart.go` - a catalog row already OWNED by a queued op (exact path) is
    skipped; it used to be skipped on `conflict` (ancestor/descendant overlap),
    which dropped every descendant of a directory left queued by a partially
    failed attempt while still marking the view done, stranding those server-only
    nodes forever. The one-time reconciliation for `!InitialSyncDone` views:
    read the cursor, run `coldstartstream.go` (walk, full-hash scan,
    per-directory safe-merge plan - **never deletes**; equal → adopt, hash-proved
    and degrading to size+kind with a Warn - `coldStartContentMatch`; divergent →
    keep-both; server-only → inbound; local-only → outbound - while the queue
    drains), then persist the cursor and mark the view done. A server DIRECTORY over a local file plans the inbound `OpMkdir`
    directly - `guardMkdirOccupant` sets the occupant aside as a keep-both copy
    and frees the path, and the subtree plans normally behind it; that arm is
    planned on read-only views too, since inbound work is not a push. The mirror
    image (a server file over a local directory) needs no special case:
    `preserveAside` moves a whole directory as readily as a file.

## coldstartwalk.go

  - `coldstartwalk.go` - the catalog source: `get_max_sync_id` FIRST, then
    `servertree.Walk` over `list`, one turn per directory. The order is the
    correctness: everything the walk sees is at least as new as that cursor, so
    replaying forward re-asserts what was recorded and delivers whatever changed
    mid-walk. Paths stay in the SERVER's spelling - the planner normalises
    after `catalogKeyCollisions`, which exists to notice two distinct server
    paths that fold to one key. `limits.ColdStartCatalogCap` bounds catalog
    NODES (checked per node); the walk's directory and depth budgets are
    separate constants, and neither may stand in for the other.

## coldstartstream.go

  - `coldstartstream.go` - the cold start used to be a batch (walk everything,
    then plan, then drain): on the stand's 50k files nothing moved for three
    minutes, and the first sync took walk PLUS transfer. Now the walk runs on
    its own goroutine and feeds listings (`coldFeed`) to the loop goroutine,
    which plans each directory (`coldPlanner.planDir`) and drains;
    `drainQueue` calls `coldIngest` between windows, so one pass keeps taking on
    new work. Only the walk is concurrent - planner and drain take turns on the
    loop goroutine, so no engine state needs a lock. Per-directory planning is
    complete because a local entry is decided when its PARENT is listed, and a
    local directory the server does not hold as a directory is never listed, so
    its whole subtree is planned at once (`planLocalSubtree`). Scan-wide guards
    stay scan-wide: the scan completes before the first directory is planned.
    A node met twice (moved mid-walk) keeps its FIRST report and a directory's
    second subtree is protected; the move is above the cursor, so the poll
    replays it. The cursor and `InitialSyncDone` are still written only at the
    end; a crash re-runs the cold start, where landed files adopt and queued
    paths are owned. A pause cancels the walk (`errColdStartPaused`, not a
    failure). The drain caps contending applies at two per worker (`slots` in
    `drainBatch`): the pool serves leases FIFO, and a window of parked applies
    put every listing behind the whole window (walk ~300 → ~30 entries/s).

## loop.go

  - `loop.go` - `Run(ctx)`: cold starts → throttled `localScan` → drain →
    long-poll → `syncView` backlog pagination → drain, plus watcher lifecycle and
    the wake seam. A wake cancels the in-flight long-poll only every OTHER time
    (`pollDeferWake`), so continuous local churn cannot starve inbound; the
    deferring poll still consumes the wake and asks for a `fairnessPollTimeout`
    turn. Terminal errors (vanished session, rejected `client_version`, closed
    pool, ctx) stop `Run`; everything else logs and retries.

## pollquarantine.go

  - `pollquarantine.go` - long_poll is the one shared-fate stage, so after a streak
    of SERVER-STATED subscribe rejections each member is probed with a single-view
    subscription on a worker lease, and only a view the SERVER rejects on its own
    errno (`pollViewRejected`, not a transport/decode/budget failure) is quarantined -
    observably and temporarily, rejoining after `pollPoisonRetryCycles`. A sweep
    that blames no member suppresses the next `pollProbeSuppressCycles` armings. A
    probe exhausting `pollProbeBudget` maps to the typed transient
    `errPollProbeBudget`, never the raw `context.DeadlineExceeded` that `isTerminal`
    would read as a run-ctx expiry.

## pathkey.go and queuescan.go

  - `pathkey.go` - the one containment predicate over fold keys: `keyUnder`,
    `keyAtOrUnder`, `rebaseMoved`, `relocateUnderMove`. Use these instead of a local
    `strings.HasPrefix`: seven hand-rolled copies existed, and each could forget that
    "a" must not match "ab" or drop the fold. They take keys ALREADY folded
    (`normpath.Key`); folding again inside would hide a caller that forgot to.
  - `queuescan.go` - `scanQueuedInView`, the one way to read one view's pending
    queue. `errStopScan` is a control signal, not a failure: several callers read an
    error as "the queue is unreadable, so refuse", so an early stop reported as an
    error, or a read failure reported as success, turns a lookup into a wrong
    destructive decision.

## platform files

Every per-OS behaviour lives in a build-tagged pair, never a GOOS branch. What each pair
answers:

| Pair | Answers |
|---|---|
| `dirident_{unix,windows}.go` | the (device, inode/file-id) of a directory, which `emptieddir.go` compares across scans: an unmount changes the object at the path, a user emptying it does not |
| `statdev_{unix,windows}.go` | the device a view root lives on, for the empty-scan debounce; blind to a same-device mount change (accepted) |
| `openflags_{unix,windows}.go` | `openUploadFileNoFollow`: an upload source opened with no symlink ANYWHERE on the path (openat + O_NOFOLLOW per component), stricter than `os.Root`, which follows an in-root link into a sibling view |
| `reparse{,_other,_windows}.go` | whether a component redirects (symlink, junction, other reparse point); Go reports a junction as `ModeIrregular`, not `ModeSymlink` |
| `shortname{,_other,_windows}.go` | `rejectShortNameAlias`: refuses an 8.3 alias below the sync root on the materialising verbs; an unanswerable host is inconclusive and proceeds |
| `namemangle{,_other,_windows}.go` | whether the host materialises a server name VERBATIM (Win32 strips trailing dots and spaces with no error); feeds `containable` |
| `nametoolong{,_unix,_windows}.go` | a name past the local per-name limit: permanent, so the op is dead-lettered instead of retried (the engine never generates such names itself) |
| `dirnotempty{,_other,_windows}.go` | ENOTEMPTY vs ERROR_DIR_NOT_EMPTY, read per verb: a race with a new file is transient, not a permanent occupant |
| `diskfull{,_other,_windows}.go` | the disk-full error classes behind the latch that pauses applies |
