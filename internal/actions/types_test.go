package actions

import (
	"errors"
	"testing"

	"github.com/mfilippov/synosync/internal/sproto"
)

func TestDecodeEventActions(t *testing.T) {
	tests := []struct {
		name string
		m    *sproto.Map
		want Event
	}{
		{
			name: "create",
			m: sproto.NewMap().
				Set("sync_id", sproto.Int(101)).
				Set("action", sproto.String("create")).
				Set("file_id", sproto.String("f1")).
				Set("path", sproto.String("docs/a.txt")).
				Set("is_dir", sproto.Int(0)).
				Set("size", sproto.Int(1024)).
				Set("hash", sproto.String("deadbeef")).
				Set("mtime", sproto.Int(1700000000)).
				Set("committer_session_id", sproto.String("sess-A")),
			want: Event{
				SyncID:             101,
				Action:             "create",
				FileID:             "f1",
				Path:               "docs/a.txt",
				IsDir:              false,
				KindReported:       true, // "is_dir" was present, even though it says 0
				Size:               1024,
				Hash:               "deadbeef",
				MTime:              1700000000,
				CommitterSessionID: "sess-A",
			},
		},
		{
			name: "modify",
			m: sproto.NewMap().
				Set("sync_id", sproto.Int(102)).
				Set("action", sproto.String("modify")).
				Set("file_id", sproto.String("f1")).
				Set("path", sproto.String("docs/a.txt")).
				Set("size", sproto.Int(2048)).
				Set("hash", sproto.String("cafebabe")),
			want: Event{
				SyncID: 102,
				Action: "modify",
				FileID: "f1",
				Path:   "docs/a.txt",
				Size:   2048,
				Hash:   "cafebabe",
			},
		},
		{
			name: "delete",
			m: sproto.NewMap().
				Set("sync_id", sproto.Int(103)).
				Set("action", sproto.String("delete")).
				Set("file_id", sproto.String("f1")).
				Set("path", sproto.String("docs/a.txt")),
			want: Event{
				SyncID: 103,
				Action: "delete",
				FileID: "f1",
				Path:   "docs/a.txt",
			},
		},
		{
			name: "move with from fields",
			m: sproto.NewMap().
				Set("sync_id", sproto.Int(104)).
				Set("action", sproto.String("move")).
				Set("file_id", sproto.String("f1")).
				Set("path", sproto.String("docs/b.txt")).
				Set("from_path", sproto.String("docs/a.txt")).
				Set("from_file_id", sproto.String("f1")),
			want: Event{
				SyncID:     104,
				Action:     "move",
				FileID:     "f1",
				Path:       "docs/b.txt",
				FromPath:   "docs/a.txt",
				FromFileID: "f1",
				// The sparse move: neither "is_dir" nor "type" is on the wire, so IsDir's
				// false is the tolerant default and KindReported stays false to say so.
				KindReported: false,
			},
		},
		{
			name: "directory via type string",
			m: sproto.NewMap().
				Set("sync_id", sproto.Int(105)).
				Set("action", sproto.String("create")).
				Set("file_id", sproto.String("d1")).
				Set("path", sproto.String("docs/sub")).
				Set("file_type", sproto.String("dir")),
			want: Event{
				SyncID:       105,
				Action:       "create",
				FileID:       "d1",
				Path:         "docs/sub",
				IsDir:        true,
				KindReported: true,
			},
		},
		{
			name: "wrong-typed fields fall back to zero",
			m: sproto.NewMap().
				Set("sync_id", sproto.String("not-an-int")).
				Set("action", sproto.Int(7)).
				Set("size", sproto.String("big")).
				Set("path", sproto.String("docs/x")),
			want: Event{Path: "docs/x"},
		},
		{
			name: "empty map yields zero value",
			m:    sproto.NewMap(),
			want: Event{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decodeEvent(tt.m)
			if got != tt.want {
				t.Errorf("decodeEvent = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestDecodeViewNodeID confirms decodeView reads the view's root node id from either
// the "file_id" or "node_id" key (preferring file_id) and tolerates its absence.
func TestDecodeViewNodeID(t *testing.T) {
	tests := []struct {
		name string
		m    *sproto.Map
		want View
	}{
		{
			name: "file_id key",
			m: sproto.NewMap().
				Set("view_id", sproto.Int(7)).
				Set("name", sproto.String("Photos")).
				Set("privilege", sproto.String("rw")).
				Set("file_id", sproto.String("root7")),
			want: View{ViewID: 7, Name: "Photos", Privilege: "rw", NodeID: "root7"},
		},
		{
			name: "node_id key",
			m: sproto.NewMap().
				Set("view_id", sproto.Int(9)).
				Set("name", sproto.String("Docs")).
				Set("node_id", sproto.String("root9")),
			want: View{ViewID: 9, Name: "Docs", NodeID: "root9"},
		},
		{
			name: "file_id wins over node_id",
			m: sproto.NewMap().
				Set("view_id", sproto.Int(3)).
				Set("file_id", sproto.String("fid")).
				Set("node_id", sproto.String("nid")),
			want: View{ViewID: 3, NodeID: "fid"},
		},
		{
			name: "absent root id stays empty",
			m: sproto.NewMap().
				Set("view_id", sproto.Int(1)).
				Set("name", sproto.String("Bare")),
			want: View{ViewID: 1, Name: "Bare"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decodeView(tt.m); got != tt.want {
				t.Errorf("decodeView = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestDecodeEvents(t *testing.T) {
	arr := sproto.Array{
		sproto.NewMap().Set("sync_id", sproto.Int(1)).Set("action", sproto.String("create")),
		sproto.NewMap().Set("sync_id", sproto.Int(2)).Set("action", sproto.String("delete")),
	}
	events, err := decodeEvents(arr)
	if err != nil {
		t.Fatalf("decodeEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if events[0].SyncID != 1 || events[0].Action != "create" {
		t.Errorf("events[0] = %+v", events[0])
	}
	if events[1].SyncID != 2 || events[1].Action != "delete" {
		t.Errorf("events[1] = %+v", events[1])
	}
}

func TestDecodeEventsEmpty(t *testing.T) {
	events, err := decodeEvents(sproto.Array{})
	if err != nil {
		t.Fatalf("decodeEvents: %v", err)
	}
	if events == nil || len(events) != 0 {
		t.Errorf("events = %v, want empty non-nil slice", events)
	}
}

func TestDecodeEventsBadEntry(t *testing.T) {
	arr := sproto.Array{sproto.String("not a map")}
	_, err := decodeEvents(arr)
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

func TestDecodeChanges(t *testing.T) {
	arr := sproto.Array{sproto.Int(3), sproto.Int(7), sproto.Int(11)}
	ids, err := decodeChanges(arr)
	if err != nil {
		t.Fatalf("decodeChanges: %v", err)
	}
	want := []int64{3, 7, 11}
	if len(ids) != len(want) {
		t.Fatalf("got %d ids, want %d", len(ids), len(want))
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Errorf("ids[%d] = %d, want %d", i, ids[i], want[i])
		}
	}
}

func TestDecodeChangesEmpty(t *testing.T) {
	ids, err := decodeChanges(sproto.Array{})
	if err != nil {
		t.Fatalf("decodeChanges: %v", err)
	}
	if ids == nil || len(ids) != 0 {
		t.Errorf("ids = %v, want empty non-nil slice", ids)
	}
}

func TestDecodeChangesBadEntry(t *testing.T) {
	arr := sproto.Array{sproto.String("not an int")}
	_, err := decodeChanges(arr)
	if !errors.Is(err, ErrUnexpectedReply) {
		t.Fatalf("err = %v, want ErrUnexpectedReply", err)
	}
}

// TestDecodeHashCanonicalizedLowercase pins the single-point hash
// canonicalization (canonicalHash): the server's hex casing is not a confirmed
// contract, and every downstream consumer compares hashes by byte equality
// against locally computed lowercase hex, so an uppercase or mixed-case server
// hash must land lowercase in both Node.Hash and Event.Hash at decode.
func TestDecodeHashCanonicalizedLowercase(t *testing.T) {
	n := decodeNode(sproto.NewMap().
		Set("file_id", sproto.String("f1")).
		Set("hash", sproto.String("DEADBEEFCAFEBABE0123456789ABCDEF")))
	if n.Hash != "deadbeefcafebabe0123456789abcdef" {
		t.Errorf("Node.Hash = %q, want lowercase canonical form", n.Hash)
	}
	e := decodeEvent(sproto.NewMap().
		Set("sync_id", sproto.Int(1)).
		Set("hash", sproto.String("AbCdEf0123456789aBcDeF0123456789")))
	if e.Hash != "abcdef0123456789abcdef0123456789" {
		t.Errorf("Event.Hash = %q, want lowercase canonical form", e.Hash)
	}
}
