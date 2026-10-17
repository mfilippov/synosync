package store

import (
	"errors"
	"testing"

	"github.com/mfilippov/synosync/internal/config"
)

func TestResetClearsAllState(t *testing.T) {
	s, _ := openTemp(t)

	// Populate config, a view, a baseline and a queued op.
	cfg := config.Default()
	cfg.DeviceUUID = "dev-uuid"
	cfg.Server.Host = "nas.example"
	cfg.Server.DsID = "ds123"
	if err := s.PutConfig(cfg); err != nil {
		t.Fatalf("PutConfig: %v", err)
	}
	if err := s.PutView(7, View{Name: "home", InitialSyncDone: true}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	if err := s.PutBaseline(7, "a.txt", Baseline{FileID: "f1", State: StateSynced}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if _, err := s.Enqueue(Op{ViewID: 7, Type: OpDownload, Path: "a.txt"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if err := s.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	// Config is gone (back to the unpaired state).
	if _, err := s.GetConfig(); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetConfig after Reset: err = %v, want ErrNotFound", err)
	}
	// Views, baselines and the queue are empty.
	views, err := s.ListViews()
	if err != nil {
		t.Fatalf("ListViews: %v", err)
	}
	if len(views) != 0 {
		t.Errorf("ListViews after Reset: %d views, want 0", len(views))
	}
	if _, err := s.GetBaseline(7, "a.txt"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetBaseline after Reset: err = %v, want ErrNotFound", err)
	}
	n, err := s.PendingLen()
	if err != nil {
		t.Fatalf("PendingLen: %v", err)
	}
	if n != 0 {
		t.Errorf("PendingLen after Reset: %d, want 0", n)
	}

	// The schema version is intact, so the store keeps working after a Reset.
	if err := s.PutView(9, View{Name: "other"}); err != nil {
		t.Fatalf("PutView after Reset: %v", err)
	}
}
