package daemon

import (
	"io"
	"log/slog"
	"testing"

	"github.com/mfilippov/synosync/internal/store"
)

// TestBuildDropsIsNewestFirst pins the ordering. The store walks its ring by ascending
// sequence, but a person reading the list wants the drop they have not seen yet.
func TestBuildDropsIsNewestFirst(t *testing.T) {
	st := openTestStore(t)
	if err := st.PutView(7, store.View{Name: "Photos"}); err != nil {
		t.Fatalf("PutView: %v", err)
	}
	for _, p := range []string{"first.txt", "second.txt", "third.txt"} {
		if _, err := st.RecordDeadLetter(store.DeadLetter{
			Type: store.OpDownload, Direction: store.DirInbound, ViewID: 7,
			Path: p, Reason: "reason for " + p, RecordedAt: 100,
		}); err != nil {
			t.Fatalf("RecordDeadLetter(%s): %v", p, err)
		}
	}

	d := &Daemon{store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	rep, err := d.buildDrops()
	if err != nil {
		t.Fatalf("buildDrops: %v", err)
	}

	if rep.Total != 3 || len(rep.Drops) != 3 || rep.Truncated {
		t.Fatalf("report = %+v, want 3 untruncated records", rep)
	}
	want := []string{"third.txt", "second.txt", "first.txt"}
	for i, w := range want {
		if rep.Drops[i].Path != w {
			t.Errorf("drop %d = %q, want %q (newest first)", i, rep.Drops[i].Path, w)
		}
	}
	if got := rep.Drops[0].Reason; got != "reason for third.txt" {
		t.Errorf("reason = %q, want the engine's own recorded reason", got)
	}
	if got := rep.Drops[0].ViewName; got != "Photos" {
		t.Errorf("view name = %q, want the resolved name", got)
	}
}

// TestBuildDropsTruncatesToTheNewest pins that the cap keeps the newest and says it truncated,
// so a wedged install cannot produce a reply the client refuses to read.
func TestBuildDropsTruncatesToTheNewest(t *testing.T) {
	st := openTestStore(t)
	prev := maxDropEntries
	maxDropEntries = 2
	t.Cleanup(func() { maxDropEntries = prev })

	for _, p := range []string{"a", "b", "c", "d"} {
		if _, err := st.RecordDeadLetter(store.DeadLetter{
			Type: store.OpUpload, ViewID: 7, Path: p, Reason: "r",
		}); err != nil {
			t.Fatalf("RecordDeadLetter: %v", err)
		}
	}

	d := &Daemon{store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	rep, err := d.buildDrops()
	if err != nil {
		t.Fatalf("buildDrops: %v", err)
	}
	if rep.Total != 4 || !rep.Truncated || len(rep.Drops) != 2 {
		t.Fatalf("report = %+v, want 2 of 4 with the truncation flag", rep)
	}
	if rep.Drops[0].Path != "d" || rep.Drops[1].Path != "c" {
		t.Errorf("kept %q,%q, want the newest two", rep.Drops[0].Path, rep.Drops[1].Path)
	}
}

// TestBuildDropsSurvivesAnUnknownView pins that a record naming a view the store no longer
// holds still reports its path and reason. A drop outliving its view is exactly the state an
// operator needs to see, so failing or hiding it would be the wrong direction.
func TestBuildDropsSurvivesAnUnknownView(t *testing.T) {
	st := openTestStore(t)
	if _, err := st.RecordDeadLetter(store.DeadLetter{
		Type: store.OpRemove, ViewID: 99, Path: "orphan.txt", Reason: "view revoked",
	}); err != nil {
		t.Fatalf("RecordDeadLetter: %v", err)
	}

	d := &Daemon{store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	rep, err := d.buildDrops()
	if err != nil {
		t.Fatalf("buildDrops: %v", err)
	}
	if len(rep.Drops) != 1 || rep.Drops[0].Path != "orphan.txt" {
		t.Fatalf("report = %+v, want the orphaned record listed", rep)
	}
	if rep.Drops[0].ViewName != "" {
		t.Errorf("view name = %q, want empty so the renderer can say it is unknown", rep.Drops[0].ViewName)
	}
}
