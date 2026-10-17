package engine

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestScanReportsLeftoverStagingOnce pins the report for a download staging temp an earlier
// run left behind: one Warn naming the path and size, never repeated within the run, and the
// file itself left alone - its name shape is not proof that the engine made it.
func TestScanReportsLeftoverStagingOnce(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	var logbuf bytes.Buffer
	e.log = slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	started := time.Now()
	e.staging = newStagingTracker(started)

	old := filepath.Join(viewDir, ".movie.mkv.dl-4242")
	if err := os.WriteFile(old, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(old, started.Add(-time.Hour), started.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}

	for pass := 1; pass <= 2; pass++ {
		if _, _, err := e.scanView(context.Background(), viewID, nil); err != nil {
			t.Fatalf("scan %d: %v", pass, err)
		}
	}
	out := logbuf.String()
	if n := strings.Count(out, "leftover partial download"); n != 1 {
		t.Fatalf("leftover reported %d times over two scans, want once:\n%s", n, out)
	}
	if !strings.Contains(out, ".movie.mkv.dl-4242") || !strings.Contains(out, "bytes=7") {
		t.Errorf("report does not name the path and size:\n%s", out)
	}
	if _, err := os.Stat(old); err != nil {
		t.Errorf("leftover was touched: %v", err)
	}
}

// TestScanDoesNotReportLiveStaging pins the two facts a leftover needs. A temp this run holds
// is a download in flight even when its mtime is old - promoteStagedDownload stamps the
// server's mtime on it before the promote. A temp newer than the engine may be one whose
// download has not registered it yet.
func TestScanDoesNotReportLiveStaging(t *testing.T) {
	e, viewID, viewDir := newScanEngine(t)
	var logbuf bytes.Buffer
	e.log = slog.New(slog.NewTextHandler(&logbuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	started := time.Now()
	e.staging = newStagingTracker(started)

	held := filepath.Join(viewDir, ".held.bin.dl-1")
	fresh := filepath.Join(viewDir, ".fresh.bin.dl-2")
	for _, p := range []string{held, fresh} {
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chtimes(held, started.Add(-time.Hour), started.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(fresh, started.Add(time.Minute), started.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	release := e.staging.hold(held)

	if _, _, err := e.scanView(context.Background(), viewID, nil); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logbuf.String(), "leftover") {
		t.Fatalf("a live or fresh temp was reported as a leftover:\n%s", logbuf.String())
	}

	// Once released and still present, the held temp IS abandoned from this run's view.
	release()
	if _, _, err := e.scanView(context.Background(), viewID, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logbuf.String(), ".held.bin.dl-1") {
		t.Errorf("released old temp not reported:\n%s", logbuf.String())
	}
	if strings.Contains(logbuf.String(), ".fresh.bin.dl-2") {
		t.Errorf("temp newer than the engine reported:\n%s", logbuf.String())
	}
}
