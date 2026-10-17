package actions

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/sproto"
)

func TestLongPollWithChanges(t *testing.T) {
	var gotSubscribe, gotTimeout sproto.Value
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		gotSubscribe, _ = req.Get("subscribe")
		gotTimeout, _ = req.Get("timeout")
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("changes", sproto.Array{sproto.Int(3), sproto.Int(7)})
	})

	ids, err := c.LongPoll(testContext(t), subs(3, 7, 9), 30)
	if err != nil {
		t.Fatalf("LongPoll: %v", err)
	}
	// Each subscribe entry is a MAP carrying the view id and its cursor, not a bare
	// int - the shape a live DSM accepts.
	a, ok := gotSubscribe.(sproto.Array)
	if !ok || len(a) != 3 {
		t.Fatalf("subscribe = %v, want 3 entries", gotSubscribe)
	}
	for i, wantID := range []int64{3, 7, 9} {
		m, ok := a[i].(*sproto.Map)
		if !ok {
			t.Fatalf("subscribe[%d] is %T, want map", i, a[i])
		}
		if got, ok := m.Int("view_id"); !ok || got != wantID {
			t.Errorf("subscribe[%d].view_id = %v, want %d", i, got, wantID)
		}
	}
	// A below-floor timeout is raised to MinPollTimeout rather than sent as-is:
	// the server rejects anything under 300 with 28672.
	if gotTimeout != sproto.Int(MinPollTimeout) {
		t.Errorf("timeout = %v, want %d", gotTimeout, MinPollTimeout)
	}
	want := []int64{3, 7}
	if len(ids) != len(want) || ids[0] != 3 || ids[1] != 7 {
		t.Errorf("changes = %v, want %v", ids, want)
	}
}

func TestLongPollTimeoutNoChangesKey(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})
	ids, err := c.LongPoll(testContext(t), subs(1), 10)
	if err != nil {
		t.Fatalf("LongPoll: %v", err)
	}
	if ids == nil || len(ids) != 0 {
		t.Errorf("changes = %v, want empty non-nil slice", ids)
	}
}

func TestLongPollEmptyChangesArray(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("changes", sproto.Array{})
	})
	ids, err := c.LongPoll(testContext(t), subs(1), 10)
	if err != nil {
		t.Fatalf("LongPoll: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("changes = %v, want empty", ids)
	}
}

func TestLongPollBadChangesType(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("changes", sproto.String("nope"))
	})
	_, err := c.LongPoll(testContext(t), subs(1), 10)
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestLongPollServerError(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeSessionVanished))
	})
	_, err := c.LongPoll(testContext(t), subs(1), 10)
	if err == nil {
		t.Fatal("want error for non-zero errno")
	}
}

func TestLongPollContextCanceled(t *testing.T) {
	// A long_poll blocks server-side; a cancelled ctx must interrupt the read.
	conn := serveMock(t, func(t *testing.T, server net.Conn) {
		_, _ = sproto.ReadHeader(server)
		_, _ = sproto.NewDecoder(server).Decode()
		_, _ = io.Copy(io.Discard, server) // never reply
	})
	c := newClient(conn, "sess")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_, err := c.LongPoll(ctx, subs(1), 600)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestPullEvent(t *testing.T) {
	var gotView, gotSync sproto.Value
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		gotView, _ = req.Get("view_id")
		gotSync, _ = req.Get("sync_id")
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("next_sync_id", sproto.Int(105)).
			Set("event_list", sproto.Array{
				sproto.NewMap().
					Set("sync_id", sproto.Int(101)).
					Set("action", sproto.String("create")).
					Set("file_id", sproto.String("f1")).
					Set("path", sproto.String("a.txt")).
					Set("size", sproto.Int(10)).
					Set("hash", sproto.String("abcd")).
					Set("mtime", sproto.Int(1700000000)).
					Set("committer_session_id", sproto.String("other")),
				sproto.NewMap().
					Set("sync_id", sproto.Int(105)).
					Set("action", sproto.String("move")).
					Set("file_id", sproto.String("f1")).
					Set("path", sproto.String("b.txt")).
					Set("from_path", sproto.String("a.txt")).
					Set("from_file_id", sproto.String("f1")),
			})
	})

	events, next, err := c.PullEvent(testContext(t), 7, 1, 100)
	if err != nil {
		t.Fatalf("PullEvent: %v", err)
	}
	if gotView != sproto.Int(7) {
		t.Errorf("view_id = %v, want 7", gotView)
	}
	if gotSync != sproto.Int(100) {
		t.Errorf("sync_id = %v, want 100", gotSync)
	}
	if next != 105 {
		t.Errorf("nextSyncID = %d, want 105", next)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0].Action != "create" || events[0].CommitterSessionID != "other" || events[0].Size != 10 {
		t.Errorf("events[0] = %+v", events[0])
	}
	if events[1].Action != "move" || events[1].FromPath != "a.txt" || events[1].Path != "b.txt" {
		t.Errorf("events[1] = %+v", events[1])
	}
}

func TestPullEventColdStart(t *testing.T) {
	var gotSync sproto.Value
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		gotSync, _ = req.Get("sync_id")
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("event_list", sproto.Array{
				sproto.NewMap().Set("sync_id", sproto.Int(1)).Set("action", sproto.String("create")),
				sproto.NewMap().Set("sync_id", sproto.Int(4)).Set("action", sproto.String("modify")),
			})
	})
	events, next, err := c.PullEvent(testContext(t), 2, 1, 0)
	if err != nil {
		t.Fatalf("PullEvent: %v", err)
	}
	if gotSync != sproto.Int(0) {
		t.Errorf("sync_id = %v, want 0 for cold start", gotSync)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	// No next_sync_id in reply: fall back to the highest event SyncID.
	if next != 4 {
		t.Errorf("nextSyncID = %d, want 4 (max event SyncID)", next)
	}
}

func TestPullEventNoEvents(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})
	events, next, err := c.PullEvent(testContext(t), 7, 1, 50)
	if err != nil {
		t.Fatalf("PullEvent: %v", err)
	}
	if len(events) != 0 {
		t.Errorf("events = %v, want none", events)
	}
	// No events and no next_sync_id: cursor stays where it was.
	if next != 50 {
		t.Errorf("nextSyncID = %d, want 50 (unchanged)", next)
	}
}

func TestPullEventClampsBackwardCursor(t *testing.T) {
	// A server that echoes a next_sync_id below the sinceSyncID we polled from
	// must not rewind the cursor: PullEvent clamps it to at least sinceSyncID.
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("next_sync_id", sproto.Int(40)).
			Set("event_list", sproto.Array{
				sproto.NewMap().Set("sync_id", sproto.Int(42)).Set("action", sproto.String("create")),
			})
	})
	_, next, err := c.PullEvent(testContext(t), 7, 1, 50)
	if err != nil {
		t.Fatalf("PullEvent: %v", err)
	}
	if next != 50 {
		t.Errorf("nextSyncID = %d, want 50 (clamped to sinceSyncID, not rewound to 40)", next)
	}
}

func TestPullEventBadListType(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("event_list", sproto.String("not an array"))
	})
	_, _, err := c.PullEvent(testContext(t), 7, 1, 0)
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestPullEventBadEntryType(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().
			Set("errno", sproto.Int(CodeOK)).
			Set("event_list", sproto.Array{sproto.String("not a map")})
	})
	_, _, err := c.PullEvent(testContext(t), 7, 1, 0)
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

// TestPullEventNodeFloodRejectedBeforePrealloc is the actions-side half of the
// node-flood guard. decodeEvents pre-sizes with make([]Event, 0, len(arr))
// - as do decodeChanges, List and QueryUserInfo - so the decoded array length is
// an allocation amplifier the actions layer itself does not bound: a cheap frame
// of ~1M empty maps (~2 MB on the wire, ~100 MB decoded) would be re-materialised
// into another ~160 MB of Events. Nothing here caps it; the codec's per-node
// frame-budget floor is what must, and it must do so before the array is ever
// handed to decodeEvents. The reply is written raw because building the hostile
// array as Values would cost the TEST the very allocation under discussion.
func TestPullEventNodeFloodRejectedBeforePrealloc(t *testing.T) {
	// {"event_list": [ {} × emptyMaps ]} - enough nodes to exceed the decoder's
	// default frame budget on the per-node floor alone.
	const emptyMaps = 600_000
	var reply []byte
	reply = append(reply, 0x42)                                // map open
	reply = append(reply, 0x10, 0x00, byte(len("event_list"))) // string key length prefix
	reply = append(reply, "event_list"...)                     //
	reply = append(reply, 0x41)                                // array open
	for i := 0; i < emptyMaps; i++ {
		reply = append(reply, 0x42, 0x40) // an empty map: open + end
	}
	reply = append(reply, 0x40, 0x40) // array end, map end

	client, server := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		if _, err := sproto.ReadHeader(server); err != nil {
			return
		}
		if _, err := sproto.NewDecoder(server).Decode(); err != nil {
			return
		}
		// The client bails mid-frame, so this write does not complete; it unblocks
		// when the test closes the client end at cleanup.
		_, _ = server.Write(reply)
	}()
	t.Cleanup(func() {
		_ = client.Close()
		<-done
	})
	// Read the reply through a buffered wrapper: the codec reads tags a byte at a
	// time, and over an unbuffered net.Pipe a million such reads are a million
	// goroutine handoffs - slower than the test deadline under -race, and no part
	// of what is under test (a real TLS conn returns bulk reads).
	c := newClient(sproto.NewConn(bufferedReadConn{Conn: client, r: bufio.NewReader(client)}), "sess")

	_, _, err := c.PullEvent(testContext(t), 7, 1, 0)
	if !errors.Is(err, sproto.ErrFrameTooLarge) {
		t.Fatalf("err = %v, want sproto.ErrFrameTooLarge", err)
	}
}

// bufferedReadConn is a net.Conn whose reads come from a buffered reader over
// the same connection. Writes and lifecycle pass through untouched.
type bufferedReadConn struct {
	net.Conn
	r *bufio.Reader
}

func (c bufferedReadConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func TestPullEventServerError(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodePathError))
	})
	_, _, err := c.PullEvent(testContext(t), 7, 1, 0)
	if !errors.Is(err, ErrPathError) {
		t.Fatalf("err = %v, want ErrPathError", err)
	}
}

// TestPullEventEvidencedBareReply confirms a bare {} reply - no errno, no
// event_list - is reported as UNEVIDENCED: the cold-start
// first-page rule leans on this to reject a half-formed empty page instead of
// concluding the server catalog is empty. The tolerant PullEvent still decodes it
// as an empty page (an unchanged quiet tick for the incremental path).
func TestPullEventEvidencedBareReply(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap() // bare {}: no errno, no event_list
	})
	events, next, evidenced, err := c.PullEventEvidenced(testContext(t), 7, 1, 50)
	if err != nil {
		t.Fatalf("PullEventEvidenced: %v", err)
	}
	if evidenced {
		t.Errorf("evidenced = true, want false for a bare {} reply")
	}
	if len(events) != 0 {
		t.Errorf("events = %v, want none", events)
	}
	if next != 50 {
		t.Errorf("nextSyncID = %d, want 50 (unchanged)", next)
	}
}

// TestPullEventEvidencedErrnoOnly confirms an {errno:0} reply with no event_list is
// EVIDENCED: the errno is positive proof the server answered, so an empty page is a
// legitimately empty catalog rather than a half-formed reply.
func TestPullEventEvidencedErrnoOnly(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})
	events, _, evidenced, err := c.PullEventEvidenced(testContext(t), 7, 1, 0)
	if err != nil {
		t.Fatalf("PullEventEvidenced: %v", err)
	}
	if !evidenced {
		t.Errorf("evidenced = false, want true when errno is present")
	}
	if len(events) != 0 {
		t.Errorf("events = %v, want none", events)
	}
}

// TestPullEventEvidencedEmptyList confirms a present-but-empty event_list is
// EVIDENCED even without an errno: the key's presence is proof of a real, empty
// page.
func TestPullEventEvidencedEmptyList(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("event_list", sproto.Array{})
	})
	events, _, evidenced, err := c.PullEventEvidenced(testContext(t), 7, 1, 0)
	if err != nil {
		t.Fatalf("PullEventEvidenced: %v", err)
	}
	if !evidenced {
		t.Errorf("evidenced = false, want true when event_list is present")
	}
	if len(events) != 0 {
		t.Errorf("events = %v, want none", events)
	}
}

// TestPullEventEvidencedNextSyncIDOnly confirms a {next_sync_id: N}-only reply -
// no errno, no event_list - is EVIDENCED: the same reply is
// consumed as the authoritative durable cursor, so a DSM whose steady-state empty
// page is {next_sync_id: N} no longer wedges cold start forever. The cursor is
// still adopted (advancing past sinceSyncID) and the page decodes as empty.
func TestPullEventEvidencedNextSyncIDOnly(t *testing.T) {
	c := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		return sproto.NewMap().Set("next_sync_id", sproto.Int(7))
	})
	events, next, evidenced, err := c.PullEventEvidenced(testContext(t), 7, 1, 3)
	if err != nil {
		t.Fatalf("PullEventEvidenced: %v", err)
	}
	if !evidenced {
		t.Errorf("evidenced = false, want true when next_sync_id is present")
	}
	if len(events) != 0 {
		t.Errorf("events = %v, want none", events)
	}
	if next != 7 {
		t.Errorf("nextSyncID = %d, want 7 (adopted from next_sync_id)", next)
	}
}

// subs builds a subscribe set from bare view ids, keeping the table-driven tests
// readable now that LongPoll takes structured subscriptions.
func subs(ids ...int64) []Subscription {
	out := make([]Subscription, 0, len(ids))
	for _, id := range ids {
		out = append(out, Subscription{ViewID: id})
	}
	return out
}

// TestLongPollEchoesTheProfileDigest pins what keeps an idle poll blocked on a live DSM: the
// digest a reply carries is remembered and sent back on the next poll, and every subscription
// claims a non-zero lock_id. Without either the server answered every poll at once, and the
// engine spun on it.
func TestLongPollEchoesTheProfileDigest(t *testing.T) {
	var firstHad bool
	first := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		_, firstHad = req.Get("profile_digest")
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK)).
			Set("changes", sproto.Array{sproto.Int(1)}).
			Set("profile_changed", sproto.Int(1)).
			Set("profile_digest", sproto.String("2a83c785"))
	})
	if _, err := first.LongPoll(testContext(t), subs(1), 300); err != nil {
		t.Fatalf("LongPoll: %v", err)
	}
	if firstHad {
		t.Error("the first poll sent a profile_digest it had never been given")
	}
	if got := first.ProfileDigest(); got != "2a83c785" {
		t.Fatalf("ProfileDigest = %q, want the reply's", got)
	}

	var sent string
	var lock int64
	next := mockClient(t, "sess", func(t *testing.T, req *sproto.Map) sproto.Value {
		sent, _ = req.String("profile_digest")
		if a, ok := req.Get("subscribe"); ok {
			if m, ok := a.(sproto.Array)[0].(*sproto.Map); ok {
				lock, _ = m.Int("lock_id")
			}
		}
		return sproto.NewMap().Set("errno", sproto.Int(CodeOK))
	})
	next.SetProfileDigest(first.ProfileDigest())
	if _, err := next.LongPoll(testContext(t), subs(1), 300); err != nil {
		t.Fatalf("LongPoll: %v", err)
	}
	if sent != "2a83c785" {
		t.Errorf("profile_digest sent = %q, want the carried one", sent)
	}
	if lock == 0 {
		t.Error("subscription lock_id = 0; the server reports every view changed for it")
	}
	// A reply without a digest keeps the one the client had.
	if got := next.ProfileDigest(); got != "2a83c785" {
		t.Errorf("ProfileDigest after a digest-less reply = %q, want it kept", got)
	}
}
