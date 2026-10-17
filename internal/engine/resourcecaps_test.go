package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/store"
)

// TestColdStartCapBacksOffRetries confirms a view whose cold start keeps aborting on the
// ColdStartCatalogCap is re-attempted under a doubling cycle-skip backoff
// rather than re-paginating the whole pre-cap catalog from the server every cycle: over four
// coldStartPending passes the pull runs only on the un-skipped cycles (1 and 3), not all four.
func TestColdStartCapBacksOffRetries(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	c := manyCreates(10)
	e, _ := captureEngine(t, st, &catalogTransport{client: c})
	e.SetLimits(config.Limits{ColdStartCatalogCap: 2})

	// Cycle 1: attempt (cap abort) → arm backoff skip=1.
	// Cycle 2: skip counts down (no attempt).
	// Cycle 3: attempt (cap abort) → arm backoff skip=2.
	// Cycle 4: skip counts down (no attempt).
	for i := 0; i < 4; i++ {
		if err := e.coldStartPending(context.Background()); err != nil {
			t.Fatalf("coldStartPending cycle %d: %v", i, err)
		}
	}
	// One list per attempt: manyCreates puts every node in the view root, and the cap aborts
	// the walk on that first listing.
	if c.listCalls != 2 {
		t.Errorf("listCalls = %d, want 2 (attempts only on the un-skipped cycles 1 and 3)", c.listCalls)
	}
	// The view is still pending (cold start never completed): a backoff is armed.
	if e.coldStartCapSkip[7] == 0 {
		t.Errorf("expected a cold-start cap backoff to be armed after the aborts")
	}
}

// TestColdStartCapBackoffClearsOnCompletion confirms the backoff state is dropped once a view
// finishes its cold start, so a future regression retries promptly.
func TestColdStartCapBackoffClearsOnCompletion(t *testing.T) {
	st := newTestStore(t)
	if err := st.PutView(7, store.View{Name: "V", Privilege: "rw", NodeID: "root", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	e, _ := captureEngine(t, st, &catalogTransport{client: &catalogClient{}})
	// Pre-seed a stale backoff as if a prior attempt had aborted before the view completed.
	e.coldStartCapSkip[7] = 5
	e.coldStartCapNext[7] = 8

	if err := e.coldStartPending(context.Background()); err != nil {
		t.Fatalf("coldStartPending: %v", err)
	}
	if _, ok := e.coldStartCapSkip[7]; ok {
		t.Errorf("cold-start cap backoff should be cleared once the view is InitialSyncDone")
	}
	if _, ok := e.coldStartCapNext[7]; ok {
		t.Errorf("cold-start cap next should be cleared once the view is InitialSyncDone")
	}
}

// TestIsDecoderCapError pins the classifier: the exported frame-byte sentinel matches via
// errors.Is, the message-only node/depth caps match on their stable text, and anything else
// (including nil) does not.
func TestIsDecoderCapError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unrelated", errors.New("connection reset"), false},
		{"frame-too-large wrapped", fmt.Errorf("wrap: %w", sproto.ErrFrameTooLarge), true},
		{"drain-too-large wrapped", fmt.Errorf("wrap: %w", sproto.ErrDrainTooLarge), true},
		{"node cap text", errors.New("sproto: value count exceeds 1048576"), true},
		{"depth cap text", errors.New("sproto: nesting depth exceeds 1000"), true},
	}
	for _, tc := range cases {
		if got := isDecoderCapError(tc.err); got != tc.want {
			t.Errorf("%s: isDecoderCapError = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestSyncViewPageCapContinuesFromCursor confirms syncView's per-lease page cap bounds
// wall-clock without losing progress: the first call reads only
// syncViewMaxPages pages, durably advances the cursor page-by-page, and warns; the next call
// resumes from the committed cursor and drains the rest.
func TestSyncViewPageCapContinuesFromCursor(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	pc := &pagePullClient{
		pages: map[int64]catalogPage{
			0: {events: []actions.Event{createEvent("a.txt", "fa", 1, false)}, next: 1},
			1: {events: []actions.Event{createEvent("b.txt", "fb", 2, false)}, next: 2},
			2: {events: []actions.Event{createEvent("c.txt", "fc", 3, false)}, next: 3},
			3: {events: nil, next: 3},
		},
		content: map[string][]byte{"fa": []byte("alpha"), "fb": []byte("beta"), "fc": []byte("gamma")},
		nodes: map[string]actions.Node{
			"fa": {Hash: "ha", MTime: 1700000000, Size: 5},
			"fb": {Hash: "hb", MTime: 1700000000, Size: 4},
			"fc": {Hash: "hc", MTime: 1700000000, Size: 5},
		},
	}
	cap := &logCapture{}
	e := New(st, &concTransport{client: pc}, root, "self", slog.New(cap))
	e.syncViewMaxPages = 2

	// First call: the page cap stops the loop after two advancing pages.
	if err := e.syncView(context.Background(), 7); err != nil {
		t.Fatalf("syncView call 1: %v", err)
	}
	if cap.countWarns("per-lease page cap") == 0 {
		t.Errorf("expected a per-lease page-cap Warn on the capped call")
	}
	v, err := st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.LastSeenSyncID != 2 {
		t.Errorf("cursor after call 1 = %d, want 2 (two pages durably committed)", v.LastSeenSyncID)
	}
	for _, name := range []string{"a.txt", "b.txt"} {
		if _, err := os.Stat(filepath.Join(root, "Photos", name)); err != nil {
			t.Errorf("expected %s applied after call 1: %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "Photos", "c.txt")); !os.IsNotExist(err) {
		t.Errorf("c.txt must NOT be applied by the capped first call (err=%v)", err)
	}

	// Second call: resume from the committed cursor and drain the rest.
	if err := e.syncView(context.Background(), 7); err != nil {
		t.Fatalf("syncView call 2: %v", err)
	}
	v, err = st.GetView(7)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if v.LastSeenSyncID != 3 {
		t.Errorf("cursor after call 2 = %d, want 3 (backlog exhausted)", v.LastSeenSyncID)
	}
	got, err := os.ReadFile(filepath.Join(root, "Photos", "c.txt"))
	if err != nil {
		t.Fatalf("ReadFile c.txt after call 2: %v", err)
	}
	if string(got) != "gamma" {
		t.Errorf("c.txt = %q, want %q", got, "gamma")
	}
}

// TestSyncViewDecoderCapWarn confirms a pull_event decoder-cap breach in the incremental path
// is surfaced with the same recognizable Warn.
func TestSyncViewDecoderCapWarn(t *testing.T) {
	st := newTestStore(t)
	root := t.TempDir()
	if err := st.PutView(7, store.View{Name: "Photos", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	pc := &pagePullClient{pullErrAt: map[int64]error{0: fmt.Errorf("actions: list events: %w", sproto.ErrFrameTooLarge)}}
	cap := &logCapture{}
	e := New(st, &concTransport{client: pc}, root, "self", slog.New(cap))

	if err := e.syncView(context.Background(), 7); err == nil {
		t.Fatalf("expected syncView to propagate the frame-too-large error")
	}
	if cap.countWarns("exceeded a decoder cap") == 0 {
		t.Errorf("expected a decoder-cap Warn, got records: %d", len(cap.records))
	}
}
