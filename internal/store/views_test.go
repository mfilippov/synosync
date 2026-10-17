package store

import (
	"errors"
	"reflect"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestViewRoundTrip(t *testing.T) {
	s, _ := openTemp(t)

	// Absent before any write.
	if _, err := s.GetView(1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetView before put: got %v, want ErrNotFound", err)
	}

	want := View{NodeID: "node-7", Name: "Team", Privilege: "rw", LastSeenSyncID: 42, InitialSyncDone: true}
	if err := s.PutView(1, want); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	got, err := s.GetView(1)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetView = %+v, want %+v", got, want)
	}

	// The zero value (cold-start not yet done) round-trips too: the omitempty
	// field is simply absent on the wire and decodes back to false.
	zero := View{NodeID: "node-8", Name: "Fresh"}
	if err := s.PutView(2, zero); err != nil {
		t.Fatalf("PutView zero: %v", err)
	}
	gotZero, err := s.GetView(2)
	if err != nil {
		t.Fatalf("GetView zero: %v", err)
	}
	if gotZero.InitialSyncDone {
		t.Errorf("fresh view InitialSyncDone = true, want false")
	}
}

func TestViewOverwriteSyncID(t *testing.T) {
	s, _ := openTemp(t)

	v := View{NodeID: "n1", Name: "A", Privilege: "rw", LastSeenSyncID: 10}
	if err := s.PutView(5, v); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	v.LastSeenSyncID = 99
	if err := s.PutView(5, v); err != nil {
		t.Fatalf("PutView overwrite: %v", err)
	}
	got, err := s.GetView(5)
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if got.LastSeenSyncID != 99 {
		t.Errorf("LastSeenSyncID = %d, want 99", got.LastSeenSyncID)
	}
}

func TestListViewsOrderedByID(t *testing.T) {
	s, _ := openTemp(t)

	if got, err := s.ListViews(); err != nil || len(got) != 0 {
		t.Fatalf("ListViews empty: got %v, err %v", got, err)
	}

	// Insert out of order; ListViews must return them by ascending view_id.
	puts := []struct {
		id uint64
		v  View
	}{
		{30, View{NodeID: "n30", Name: "C"}},
		{10, View{NodeID: "n10", Name: "A"}},
		{20, View{NodeID: "n20", Name: "B"}},
	}
	for _, p := range puts {
		if err := s.PutView(p.id, p.v); err != nil {
			t.Fatalf("PutView %d: %v", p.id, err)
		}
	}

	got, err := s.ListViews()
	if err != nil {
		t.Fatalf("ListViews: %v", err)
	}
	wantIDs := []uint64{10, 20, 30}
	if len(got) != len(wantIDs) {
		t.Fatalf("ListViews len = %d, want %d", len(got), len(wantIDs))
	}
	for i, lv := range got {
		if lv.ViewID != wantIDs[i] {
			t.Errorf("ListViews[%d].ViewID = %d, want %d", i, lv.ViewID, wantIDs[i])
		}
	}
}

func TestDeleteView(t *testing.T) {
	s, _ := openTemp(t)

	if err := s.PutView(7, View{Name: "gone"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := s.DeleteView(7); err != nil {
		t.Fatalf("DeleteView: %v", err)
	}
	if _, err := s.GetView(7); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetView after delete: got %v, want ErrNotFound", err)
	}
	// Deleting an absent view is a no-op.
	if err := s.DeleteView(7); err != nil {
		t.Errorf("DeleteView absent: %v", err)
	}
}

func TestViewsSurviveReopen(t *testing.T) {
	s, path := openTemp(t)

	want := View{NodeID: "n-keep", Name: "Persist", Privilege: "ro", LastSeenSyncID: 7}
	if err := s.PutView(100, want); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	got, err := s2.GetView(100)
	if err != nil {
		t.Fatalf("GetView after reopen: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetView after reopen = %+v, want %+v", got, want)
	}
}

// A malformed (short) view key used to panic decodeUint64 out of ListViews
// before the fix. It is unreachable corruption - no real view id encodes to
// anything but 8 bytes - so ListViews skips it rather than erroring or surfacing
// a phantom view.
func TestListViewsSkipsCorruptShortKey(t *testing.T) {
	s, _ := openTemp(t)

	want := View{NodeID: "n-1", Name: "Team"}
	if err := s.PutView(3, want); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketViews).Put([]byte{0xff}, []byte(`{"name":"junk"}`))
	})
	if err != nil {
		t.Fatalf("insert short key: %v", err)
	}

	got, err := s.ListViews()
	if err != nil {
		t.Fatalf("ListViews over short key: %v", err)
	}
	if len(got) != 1 || got[0].ViewID != 3 || !reflect.DeepEqual(got[0].View, want) {
		t.Fatalf("ListViews = %+v, want just view 3", got)
	}
}

// TestGetViewDecodeError verifies a corrupt view VALUE surfaces a decode error
// distinct from ErrNotFound and tagged ErrCorruptRecord (mirroring
// GetBaseline/GetConfig) so refreshViews can tell an unrecoverable, will-never-decode
// record from a transient backend failure and heal it via overwrite instead of
// aborting the whole refresh forever.
func TestGetViewDecodeError(t *testing.T) {
	s, _ := openTemp(t)

	if err := s.PutView(4, View{NodeID: "n-4", Name: "Team"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	// Overwrite view 4's value with non-JSON bytes under its intact 8-byte key.
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketViews).Put(encodeUint64(4), []byte("not json{"))
	})
	if err != nil {
		t.Fatalf("corrupt view value: %v", err)
	}

	_, err = s.GetView(4)
	if err == nil {
		t.Fatal("GetView on corrupt value = nil error, want decode error")
	}
	if errors.Is(err, ErrNotFound) {
		t.Fatalf("GetView on corrupt value = ErrNotFound, want decode error: %v", err)
	}
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("GetView on corrupt value = %v, want wrapped ErrCorruptRecord", err)
	}
}

// TestListViewsSkipsCorruptValue pins that a bit-rotted view VALUE (undecodable
// JSON under a well-formed key) is skipped rather than aborting the whole scan:
// ListViews feeds localScan/poll/cold-start/status, so surfacing the decode
// error there would wedge the entire daemon on one corrupt record (the global
// poison-pill class already closed for the queue and files
// buckets). The healthy siblings must still list.
func TestListViewsSkipsCorruptValue(t *testing.T) {
	s, _ := openTemp(t)

	want := View{NodeID: "n-7", Name: "Team"}
	if err := s.PutView(5, want); err != nil {
		t.Fatalf("PutView 5: %v", err)
	}
	if err := s.PutView(9, View{NodeID: "n-9", Name: "Ops"}); err != nil {
		t.Fatalf("PutView 9: %v", err)
	}
	// Overwrite view 9's value with non-JSON bytes under its intact 8-byte key.
	err := s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketViews).Put(encodeUint64(9), []byte("not json{"))
	})
	if err != nil {
		t.Fatalf("corrupt view value: %v", err)
	}

	got, err := s.ListViews()
	if err != nil {
		t.Fatalf("ListViews over corrupt value: %v", err)
	}
	if len(got) != 1 || got[0].ViewID != 5 || !reflect.DeepEqual(got[0].View, want) {
		t.Fatalf("ListViews = %+v, want just view 5 (corrupt view 9 skipped)", got)
	}
}
