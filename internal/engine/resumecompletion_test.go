package engine

// Tests for the terminal exit of a chunked upload the server never reports as complete.
//
// Completion of a resume upload is provable to this client in one place only: the reply's
// "done"/"completed" flag. A server that states it in no recognised form - or only by
// committing every byte - leaves the transfer permanently unfinishable: resumeUploadFile
// streams the whole file, sees no completion, and returns an error; the next drain re-tokens
// (the server reports the file fully committed), rewinds the last chunk, re-sends it and
// reaches the same non-answer. The failure carried no server result code, so the
// deterministic-rejection escalation never counted it, and every file over
// limits.UploadResumeThreshold could wedge its serialisation chain for the life of the
// process behind a Stuck counter and nothing else.
//
// Now an UNBROKEN run of that non-answer is determinism evidence of the same shape as an
// unbroken run of one unmapped errno, and the push dead-letters observably once proven.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/md4"
	"github.com/mfilippov/synosync/internal/store"
)

// seedResumeUpload seeds a file past the (lowered) resume threshold and queues its outbound
// upload, returning the content.
func seedResumeUpload(t *testing.T, e *Engine, st *store.Store, viewDir string) []byte {
	t.Helper()
	e.limits.UploadResumeThreshold = 16 // force the chunked path for a small test file
	content := []byte(strings.Repeat("big-file-content", 8))
	if err := os.WriteFile(filepath.Join(viewDir, "big.bin"), content, 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	if _, err := st.Enqueue(store.Op{
		Type: store.OpUpload, Direction: store.DirOutbound, ViewID: 7, Path: "big.bin",
		LocalHash: md4.ContentHash(content), LocalSize: int64(len(content)), LocalMTime: 100, UnixPerm: 644,
	}); err != nil {
		t.Fatalf("Enqueue upload: %v", err)
	}
	return content
}

// TestStreamedUploadWithoutFileIDIsPacedNotDropped drives the STREAMED push against a server that
// names no node, and pins what happens to the op.
//
// The scenario is named for what it exercises, which is not what the file's other tests cover: the
// fake's reply carries no file id, so applyUploadOut refuses to advance a baseline that would
// carry an empty one and the op stays queued (errUploadNoFileID). It was called
// ...NoCompletionNeedsWallClockEvidence, from the era when resume upload was chunked and could
// fail to be reported complete; that shape is unreachable now that the verb is a whole-file turn
// (docs/INTEGRATION.md), and the branch this fixture actually reaches is the id-less one.
//
// What it pins: the op is PACED and never dropped. The server did the work - the bytes landed -
// so dead-lettering would abandon a file the server already holds while the next scan re-derives
// the same push anyway, which is why uploadNoFileIDCode is classified transient. The pacing is the
// point: before it, every drain re-streamed the whole file body.
func TestStreamedUploadWithoutFileIDIsPacedNotDropped(t *testing.T) {
	c := &pushClient{
		resumeToken:      actions.ResumeToken{Token: "tk"},
		resumeNoComplete: true,
	}
	e, st, viewDir, _ := serverErrEngine(t, c)
	stubClock(e) // every drain lands in the same instant
	seedResumeUpload(t, e, st, viewDir)

	// The first drain fails through the real apply path: the fake's reply names no node, so
	// applyUploadOut refuses to advance a baseline that would carry an empty file id.
	err := e.drainQueue(context.Background())
	if err == nil {
		t.Fatal("drain 1: succeeded, want the no-file-id failure")
	}
	if !errors.Is(err, errUploadNoFileID) {
		t.Fatalf("drain 1 error = %v, want errUploadNoFileID", err)
	}
	// The drains that follow may legitimately do NOTHING. That failure now arms a geometric retry
	// pause (opbackoff.go, via the synthetic uploadNoFileIDCode), and the stubbed clock never lets
	// it expire, so the op is held back rather than re-attempted - which is the pacing working.
	// Before it was paced, each of these drains re-streamed the whole file body. What must not
	// happen on either path is the DROP, and that is what the wall-clock floor guarantees.
	for i := 0; i < 2*unmappedServerErrDrains; i++ {
		_ = e.drainQueue(context.Background())
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Fatalf("dead-lettered %d ops with no wall-clock evidence, want 0", dl)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Fatalf("pending = %d, want the upload still queued and retrying", n)
	}
}

// TestNoteServerErrFoldsResumeNoCompletion pins the evidence accounting directly: the
// no-completion failure carries no result code, so it rides a SYNTHETIC one that must behave
// like any other - identical repeats accumulate, and any other failure class (including a real
// server rejection) restarts the run from one. A synthetic code that collided with a real errno
// would let two unrelated failure kinds accumulate into one bogus "deterministic" streak.
func TestNoteServerErrFoldsResumeNoCompletion(t *testing.T) {
	clock := time.Unix(1_750_000_000, 0)
	e := &Engine{opServerErrs: make(map[uint64]serverErrStreak), now: func() time.Time { return clock }}
	steps := []struct {
		err       error
		wantCode  int64
		wantCount int
	}{
		{fmt.Errorf("upload big.bin: %w", errResumeNoCompletion), resumeNoCompletionCode, 1},
		{fmt.Errorf("upload big.bin: %w", errResumeNoCompletion), resumeNoCompletionCode, 2},
		{&actions.ServerError{Code: 33333}, 33333, 1}, // a real rejection is a different fact
		{fmt.Errorf("upload big.bin: %w", errResumeNoCompletion), resumeNoCompletionCode, 1},
		{actions.ErrConnBroken, 0, 0}, // transport noise clears it entirely
		{fmt.Errorf("upload big.bin: %w", errResumeNoCompletion), resumeNoCompletionCode, 1},
	}
	for i, s := range steps {
		code, count, _ := e.noteServerErr(1, s.err)
		if code != s.wantCode || count != s.wantCount {
			t.Errorf("step %d: noteServerErr = (%d, %d), want (%d, %d)", i, code, count, s.wantCode, s.wantCount)
		}
		clock = clock.Add(time.Minute)
	}
	if resumeNoCompletionCode >= 0 {
		t.Errorf("resumeNoCompletionCode = %d, want a negative value no server errno can take",
			resumeNoCompletionCode)
	}
}

// TestResumeUploadStalledOffsetStaysTransient pins the boundary of the new terminal class. A
// transfer that is INCOMPLETE - the server commits nothing past the offset we sent, with no
// completion claimed - is a different failure: bytes are genuinely missing server-side, a
// later drain can still land them, and the op must keep retrying exactly as before. Only
// "every byte streamed, completion never stated" is the deterministic non-answer.
func TestResumeUploadStalledOffsetStaysTransient(t *testing.T) {
	c := &pushClient{resumeToken: actions.ResumeToken{Token: "tk"}}
	e, st, viewDir, _ := serverErrEngine(t, c)
	advance := stubClock(e)
	seedResumeUpload(t, e, st, viewDir)

	for i := 0; i < 2*unmappedServerErrDrains; i++ {
		// The server commits nothing at all on every chunk: offset stays where it was, and
		// the transfer never reaches its end.
		c.mu.Lock()
		c.resumeCommitOffsets = []int64{0}
		c.mu.Unlock()
		if err := e.drainQueue(context.Background()); err == nil {
			t.Fatalf("drain %d: succeeded, want the stalled-offset failure", i+1)
		}
		advance(unmappedServerErrMinAge)
	}
	if dl, _ := st.DeadLetterLen(); dl != 0 {
		t.Errorf("dead-lettered %d ops on an incomplete transfer, want 0 (bytes are still missing server-side)", dl)
	}
	if n, _ := st.PendingLen(); n != 1 {
		t.Errorf("pending = %d, want the upload still queued", n)
	}
}
