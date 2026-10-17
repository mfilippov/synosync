//go:build integration

package integration

import (
	"bytes"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/md4"
)

// TestIntegration_FileIDPresence measures whether a live DSM ever answers WITHOUT a
// `file_id` on the surfaces the client reads one from.
//
// A large amount of defensive machinery is written for the omitting shape, and how much
// of it is live code rather than dead insurance depends entirely on this answer:
//
//   - `applyUploadOut` (outbound_upload.go) refuses to advance a baseline that would carry
//     an empty id - a fresh create whose reply omits the id, and whose `recoverUploadFileID`
//     probe cannot fill it, is left QUEUED. That error carries no ServerError and no
//     synthetic streak code, so `noteServerErr` clears both the streak and the retry pause
//     on every drain: the op re-streams the WHOLE file body once per drain, forever,
//     unpaced and never escalated. It is the ~80-requests-per-second spin H4 measured,
//     reached through the one arm the synthetic codes (-1…-5) do not cover.
//   - the id-less outbound rename's lost-reply probe (outbound_mutate.go) adopts ANY
//     id-bearing node standing at the destination as its own completed move.
//   - `removeTargetFacts` blanks the divergence base for an id-less remove, so the
//     identity-free hash comparison cannot run even when the kind
//     probe already carried a content hash.
//
// So: if every surface always carries the id, all three are unreachable on this server and
// the defects that rest on them are latent rather than live. If any surface omits
// it, they are live and the unpaced loop above is a real spin against the NAS.
//
// The scenario drives the server directly, with no engine. Two surfaces are ASSERTED -
// the single-frame and the streamed CREATE replies - because their omission is what arms
// the unpaced retry loop; a failure there is not a client bug but the measurement
// changing, and the loop must be paced before the assertion is relaxed. Everything else is
// logged.
//
// It also settles the narrower question docs/INTEGRATION.md leaves open under "the
// completing resume_upload reply": whether that reply carries the finished node's facts
// (file_id / hash) or only a verdict.
func TestIntegration_FileIDPresence(t *testing.T) {
	sc := setupScenarioNoEngine(t)

	// Each surface's answer, recorded in the order the client meets them.
	type surface struct {
		name   string
		fileID string
		hash   string
		// armsLoop marks a surface whose omission arms the unpaced re-upload loop.
		armsLoop bool
	}
	var seen []surface
	record := func(name, fileID, hash string, armsLoop bool) {
		seen = append(seen, surface{name: name, fileID: fileID, hash: hash, armsLoop: armsLoop})
		t.Logf("%-34s file_id=%q hash=%q", name+":", fileID, hash)
	}

	since := sc.logTail(t)

	// 1-2. The single-frame upload, as a CREATE and then as a MODIFY of the same path.
	//      The create is the arm that matters: `applyUploadOut` keeps a modify's existing
	//      op.FileID when the reply omits one, so only a create can reach the empty-id
	//      refusal.
	createBody := []byte("file id capture: single-frame create\n")
	created := sc.remotePut(t, "fileid/single.txt", createBody)
	record("upload reply (create)", created.FileID, created.Hash, true)

	modifyBody := []byte("file id capture: single-frame modify, a longer body\n")
	modified := sc.remotePut(t, "fileid/single.txt", modifyBody)
	record("upload reply (modify)", modified.FileID, modified.Hash, false)

	// 3. mkdir - `applyMkdirOut` carries the same empty-id refusal as the upload path.
	made := sc.remoteMkdirAll(t, "fileid/madedir")
	record("mkdir reply", made.FileID, made.Hash, false)

	// 4-5. The STREAMED whole-file turn, create and modify. resume_upload is a two-frame
	//      turn with no offset and no chunking (DESIGN §2), so its single reply is the
	//      only completion signal there is - and whether it names the finished node is the
	//      open question in docs/INTEGRATION.md. Kept small deliberately: this measures a
	//      reply shape, not throughput, and a big body on a stand behind a userspace
	//      network relay only buys flakiness.
	streamBody := bytes.Repeat([]byte("streamed create payload\n"), 2048)
	streamed := sc.serverPutLarge(t, "fileid/streamed.bin", streamBody)
	record("resume_upload reply (create)", streamed.FileID, streamed.Hash, true)

	streamBody2 := bytes.Repeat([]byte("streamed modify payload\n"), 3072)
	streamedMod := sc.serverPutLarge(t, "fileid/streamed.bin", streamBody2)
	record("resume_upload reply (modify)", streamedMod.FileID, streamedMod.Hash, false)

	// 6. query_node - the recovery probe (`recoverUploadFileID`) and every absence prover
	//    read the id from here.
	qFile, ok := sc.queryRel(t, "fileid/single.txt")
	if !ok {
		t.Fatalf("integration: query_node reports the just-uploaded %q absent", sc.viewRel("fileid/single.txt"))
	}
	record("query_node (file)", qFile.FileID, qFile.Hash, false)

	qDir, ok := sc.queryRel(t, "fileid/madedir")
	if !ok {
		t.Fatalf("integration: query_node reports the just-created %q absent", sc.viewRel("fileid/madedir"))
	}
	record("query_node (directory)", qDir.FileID, qDir.Hash, false)

	// 7. list - the subtree precondition (verifyServerSubtreeAgrees) refuses on a foreign
	//    file_id, so a listing that omits ids weakens that guard to name-and-hash only.
	ctx, cancel := actionCtx()
	children, listed, err := sc.client.List(ctx, sc.view.ViewID, sc.viewRel("fileid"))
	cancel()
	if err != nil {
		t.Fatalf("integration: list %q: %v", sc.viewRel("fileid"), err)
	}
	if !listed {
		t.Fatalf("integration: list %q carried no enumeration evidence", sc.viewRel("fileid"))
	}
	idlessChildren := 0
	for _, ch := range children {
		if ch.FileID == "" {
			idlessChildren++
		}
		t.Logf("list child: name=%q file_id=%q is_dir=%v size=%d hash=%q",
			ch.Name, ch.FileID, ch.IsDir, ch.Size, ch.Hash)
	}
	t.Logf("%-34s %d of %d children carry no file_id", "list reply:", idlessChildren, len(children))

	// 8. The event log. A delete row without an id is the shape `preBatchDeletePath` exists
	//    for, and a modify row without one gives reconcile no identity to match a record by.
	serverRemove(t, sc.client, sc.view.ViewID, sc.viewRel("fileid/single.txt"))
	events := sc.drainEvents(t, since)
	fileRel := sc.viewRel("fileid/single.txt")
	idlessRows := 0
	rows := 0
	for _, ev := range events {
		if !underPath(ev, sc.viewRel("fileid")) {
			continue
		}
		rows++
		if ev.FileID == "" {
			idlessRows++
		}
		t.Logf("event: sync_id=%d action=%q path=%q file_id=%q is_dir=%v kind_reported=%v",
			ev.SyncID, ev.Action, ev.Path, ev.FileID, ev.IsDir, ev.KindReported)
	}
	if del := findDeleteRow(events, fileRel, created.FileID); del != nil {
		t.Logf("%-34s file_id=%q", "delete event:", del.FileID)
	}
	t.Logf("%-34s %d of %d rows carry no file_id", "event rows:", idlessRows, rows)

	// The verdict.
	var omitted []string
	for _, s := range seen {
		if s.fileID != "" {
			continue
		}
		omitted = append(omitted, s.name)
		if s.armsLoop {
			t.Errorf("integration: %s carried NO file_id. A create whose reply omits the id and "+
				"whose recovery probe cannot fill it leaves the op queued with a bare error - no "+
				"ServerError, no synthetic streak code - so noteServerErr clears the streak AND the "+
				"retry pause every drain and the WHOLE file body is re-streamed once per drain, "+
				"forever, unpaced and never escalated (outbound_upload.go, servererr.go). Pace that "+
				"arm with a synthetic code BEFORE relaxing this assertion, and record the new "+
				"measurement in docs/INTEGRATION.md.", s.name)
		}
	}

	switch {
	case len(omitted) == 0 && idlessChildren == 0 && idlessRows == 0:
		t.Logf("MEASURED: every surface carries a file_id - %d reply shapes, all %d listed "+
			"children and all %d event rows. The client's id-less arms are therefore DEAD "+
			"INSURANCE on this server: the unpaced re-upload loop in applyUploadOut is "+
			"unreachable, the id-less rename adoption cannot misfire, and the blanked divergence "+
			"base on an id-less remove never applies. Those defects are latent, not live "+
			"- worth the cheap fixes, not urgent. Record the answer in docs/INTEGRATION.md.",
			len(seen), len(children), rows)
	case len(omitted) > 0:
		t.Logf("MEASURED: %d of %d reply surfaces omit the file_id (%s). Every id-less arm in the "+
			"client is LIVE, and the unpaced re-upload loop is reachable - that is a real spin "+
			"against the NAS, not a latent one. Fix the pacing first, then re-derive which of the "+
			"other id-less arms (rename adoption, blanked remove divergence base) need evidence "+
			"they currently cannot get.", len(omitted), len(seen), strings.Join(omitted, ", "))
	default:
		t.Logf("MEASURED: every reply shape carries a file_id, but %d listed children and %d event "+
			"rows do not. The push arms are safe; the arms that read identity from a LISTING or "+
			"the LOG (verifyServerSubtreeAgrees' foreign-id refusal, reconcile's record matching) "+
			"are the ones to re-derive.", idlessChildren, idlessRows)
	}

	// The resume_upload reply's facts, called out separately: this is the narrower question
	// docs/INTEGRATION.md leaves open, and the answer decides whether a streamed push
	// derives its baseline from the server's word or from its own staged bytes.
	switch {
	case streamed.FileID != "" && streamed.Hash != "":
		t.Logf("MEASURED (settles the open resume_upload question): the completing reply carries "+
			"BOTH the finished node's file_id (%q) and its content hash (%q), so a streamed push "+
			"records the server's own facts rather than deriving them from its staged bytes.",
			streamed.FileID, streamed.Hash)
	case streamed.FileID != "":
		t.Logf("MEASURED (settles the open resume_upload question): the completing reply names the "+
			"node (file_id=%q) but carries NO content hash, so applyUploadOut assigns an EMPTY "+
			"ServerHash on every streamed push - which degrades guardUploadTarget to node identity "+
			"on the next modify, the H1 shape. Record it and make the guard read its base from the "+
			"capture instead.", streamed.FileID)
	default:
		t.Logf("MEASURED (settles the open resume_upload question): the completing reply carries " +
			"only a verdict - no file_id. See the assertion above: this arms the unpaced loop.")
	}

	// A control the whole capture rests on: the bytes really landed. A reply shape means
	// nothing if the transfer it describes did not happen.
	if got, ok := sc.queryRel(t, "fileid/streamed.bin"); !ok {
		t.Errorf("integration: the streamed upload's node is absent from the server")
	} else if want := md4.ContentHash(streamBody2); got.Hash != "" && got.Hash != want {
		t.Errorf("integration: the streamed node's hash %q != MD4 of the bytes sent %q", got.Hash, want)
	}
}
