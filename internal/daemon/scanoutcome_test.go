package daemon

// Tests for the observability and the negative cache of the cache-filling store scans.
// A scan that outran scanBudget was silent at the scan site:
// scanViewCounts/scanConflicts returned a bare error to a handler that had been abandoned ~112
// seconds earlier, so the reply landed in ipc's buffered done channel and was discarded. The log
// carried only the generic "handler did not finish" from two minutes before - the words
// scanBudget, the verb and the view never appeared anywhere - and, because errors were cached
// nowhere, every fresh call restarted the same doomed two-minute walk (and its two-minute bbolt
// read transaction against the data plane).

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/store"
	"github.com/mfilippov/synosync/internal/transport"
)

// newLoggingDaemon builds a Daemon whose log is captured in buf.
func newLoggingDaemon(t *testing.T, st *store.Store, buf *bytes.Buffer) *Daemon {
	t.Helper()
	d, err := New(Deps{
		Store:     st,
		Config:    config.Config{Server: config.Server{Host: "h", Port: 6690}},
		SockPath:  tempSockPath(t),
		LockPath:  filepath.Join(t.TempDir(), "daemon.lock"),
		Connector: func(context.Context) (*transport.Session, error) { return nil, nil },
		Logger:    slog.New(slog.NewTextHandler(buf, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d
}

// TestScanOverBudgetIsLoggedAndNegativelyCached is the red test: a count scan that cannot fit
// scanBudget must say so at the scan site (naming the verb, the view and the budget) and must not
// be restarted by the next call until the negative memory expires.
func TestScanOverBudgetIsLoggedAndNegativelyCached(t *testing.T) {
	st := openTestStore(t)
	seedCountedViews(t, st, 1)

	var buf bytes.Buffer
	d := newLoggingDaemon(t, st, &buf)
	base := time.Unix(1_750_000_000, 0)
	now := base
	d.now = func() time.Time { return now }
	scans := 0
	d.viewScanHook = func(uint64) { scans++ }

	// A budget no walk can fit: the scan context is already past its deadline when the first
	// record is visited, which is the shape of a tree too large for the real budget with the
	// timing race removed.
	restore := scanBudget
	scanBudget = -time.Second
	t.Cleanup(func() { scanBudget = restore })

	// The verb itself survives: the counts are the only O(tree) part of the report, so they
	// degrade to stale while Paused/DiskFull/Stuck/PendingOps stay readable.
	rep, err := d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus with an unfittable scan budget: %v (the counts must degrade, not fail the verb)", err)
	}
	if len(rep.Views) != 1 || !rep.Views[0].CountsStale {
		t.Fatalf("report = %+v, want one view marked CountsStale", rep.Views)
	}
	if scans != 1 {
		t.Fatalf("scans = %d, want 1", scans)
	}
	logged := buf.String()
	if !strings.Contains(logged, "scan exceeded its scan budget") {
		t.Errorf("no scan-site Warn naming the budget; log:\n%s", logged)
	}
	if !strings.Contains(logged, "scan_budget=") || !strings.Contains(logged, "view=1") {
		t.Errorf("the budget Warn does not name the budget and the view; log:\n%s", logged)
	}
	if !strings.Contains(logged, "conflict-count") {
		t.Errorf("the budget Warn does not name the scan; log:\n%s", logged)
	}

	// Within the negative TTL a fresh call is served the remembered failure: no second doomed
	// walk, and no repeated Warn either.
	buf.Reset()
	now = base.Add(scanFailTTL - time.Second)
	rep, err = d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus inside the negative TTL: %v", err)
	}
	if len(rep.Views) != 1 || !rep.Views[0].CountsStale {
		t.Fatalf("report inside the negative TTL = %+v, want one view marked CountsStale", rep.Views)
	}
	if scans != 1 {
		t.Errorf("scans inside the negative TTL = %d, want 1 (the doomed scan must not restart)", scans)
	}
	if strings.Contains(buf.String(), "scan exceeded its scan budget") {
		t.Errorf("the budget Warn repeated inside the negative TTL; log:\n%s", buf.String())
	}

	// Past the negative TTL the scan is retried - and now it fits, so `status` recovers and the
	// remembered failure is dropped.
	scanBudget = restore
	now = base.Add(scanFailTTL)
	rep, err = d.buildStatus(context.Background())
	if err != nil {
		t.Fatalf("buildStatus past the negative TTL: %v", err)
	}
	if scans != 2 {
		t.Errorf("scans past the negative TTL = %d, want 2 (the retry must happen)", scans)
	}
	if rep.Views[0].CountsStale {
		t.Errorf("the recovered report still marks the counts stale")
	}
	if len(rep.Views) != 1 || rep.Views[0].Conflicts != 1 || rep.Views[0].Unresolved != 1 {
		t.Errorf("recovered report = %+v, want one view with 1 conflict and 1 unresolved", rep.Views)
	}
	if _, ok := d.recentCountsFailure(1); ok {
		t.Errorf("a successful scan did not clear the remembered failure")
	}
}

// TestConflictReportScanOverBudgetIsLoggedAndCached covers the whole-report sibling: the
// `conflicts` verb's scan has the same silence and the same restart cadence.
func TestConflictReportScanOverBudgetIsLoggedAndCached(t *testing.T) {
	st := openTestStore(t)
	seedCountedViews(t, st, 1)

	var buf bytes.Buffer
	d := newLoggingDaemon(t, st, &buf)
	base := time.Unix(1_750_000_000, 0)
	now := base
	d.now = func() time.Time { return now }

	restore := scanBudget
	scanBudget = -time.Second
	t.Cleanup(func() { scanBudget = restore })

	if _, err := d.buildConflicts(context.Background()); err == nil {
		t.Fatalf("buildConflicts with an unfittable scan budget = nil error")
	}
	if !strings.Contains(buf.String(), "conflict-report") || !strings.Contains(buf.String(), "scan exceeded its scan budget") {
		t.Errorf("no scan-site Warn for the conflict report; log:\n%s", buf.String())
	}
	if _, ok := d.recentConflictsFailure(); !ok {
		t.Errorf("the failed conflict-report scan was not remembered")
	}

	scanBudget = restore
	now = base.Add(scanFailTTL)
	rep, err := d.buildConflicts(context.Background())
	if err != nil {
		t.Fatalf("buildConflicts past the negative TTL: %v", err)
	}
	if rep.Total != 1 {
		t.Errorf("recovered report Total = %d, want 1", rep.Total)
	}
	if _, ok := d.recentConflictsFailure(); ok {
		t.Errorf("a successful scan did not clear the remembered failure")
	}
}

// TestScanOutcomeDistinguishesItsCauses pins the classification: a store-layer failure, a
// budget expiry and a daemon shutdown are three operationally different things and must read
// differently in the log. A healthy scan logs nothing at all.
func TestScanOutcomeDistinguishesItsCauses(t *testing.T) {
	st := openTestStore(t)
	seedCountedViews(t, st, 1)

	t.Run("store failure", func(t *testing.T) {
		var buf bytes.Buffer
		d := newLoggingDaemon(t, st, &buf)
		live, cancel := context.WithCancel(context.Background())
		defer cancel()
		d.logScanOutcome("daemon: conflict-count", []any{"view", uint64(1)}, live, errors.New("bbolt: read failed"))
		got := buf.String()
		if !strings.Contains(got, "failed in the store layer") {
			t.Errorf("store failure log = %q", got)
		}
		if strings.Contains(got, "scan budget") {
			t.Errorf("a store failure was reported as a budget expiry: %q", got)
		}
	})
	t.Run("budget expiry", func(t *testing.T) {
		var buf bytes.Buffer
		d := newLoggingDaemon(t, st, &buf)
		expired, cancel := context.WithCancel(context.Background())
		cancel()
		d.logScanOutcome("daemon: conflict-count", []any{"view", uint64(1)}, expired, context.DeadlineExceeded)
		if got := buf.String(); !strings.Contains(got, "scan exceeded its scan budget") {
			t.Errorf("budget log = %q", got)
		}
	})
	t.Run("daemon shutdown", func(t *testing.T) {
		var buf bytes.Buffer
		d := newLoggingDaemon(t, st, &buf)
		down, cancelBase := context.WithCancel(context.Background())
		cancelBase()
		d.setBaseCtx(down)
		expired, cancel := context.WithCancel(context.Background())
		cancel()
		d.logScanOutcome("daemon: conflict-count", []any{"view", uint64(1)}, expired, context.Canceled)
		got := buf.String()
		if !strings.Contains(got, "cancelled by daemon shutdown") {
			t.Errorf("shutdown log = %q", got)
		}
		if strings.Contains(got, "level=WARN") {
			t.Errorf("an expected shutdown was reported as a Warn: %q", got)
		}
	})
	t.Run("healthy scan is silent", func(t *testing.T) {
		var buf bytes.Buffer
		d := newLoggingDaemon(t, st, &buf)
		if _, err := d.buildStatus(context.Background()); err != nil {
			t.Fatalf("buildStatus: %v", err)
		}
		if got := buf.String(); got != "" {
			t.Errorf("a healthy scan logged %q, want silence", got)
		}
	})
}

// TestStoreLayerScanFailureIsNotNegativelyCached pins the CLASS restriction on the negative
// cache: scanFailTTL's rationale is about a scan that deterministically outruns its budget, which
// "cannot possibly succeed on a retry started sooner than the budget itself". A one-off store
// read failure says nothing about the next read, so remembering it for three minutes suppressed
// the counts (and, before buildStatus degraded, the whole verb) over a transient blip, with no
// way for an operator to force a retry.
func TestStoreLayerScanFailureIsNotNegativelyCached(t *testing.T) {
	st := openTestStore(t)
	seedCountedViews(t, st, 1)

	var buf bytes.Buffer
	d := newLoggingDaemon(t, st, &buf)
	// Closing the store makes the scan fail in the store layer while its budget is untouched.
	if err := st.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	a := d.viewCountsFor(context.Background(), 1)
	if a.err == nil {
		t.Fatal("viewCountsFor over a closed store: got nil error, want the store failure")
	}
	if !strings.Contains(buf.String(), "failed in the store layer") {
		t.Errorf("the store failure was not logged as one; log:\n%s", buf.String())
	}
	if strings.Contains(buf.String(), "scan exceeded its scan budget") {
		t.Errorf("a store failure was reported as a budget expiry; log:\n%s", buf.String())
	}
	if _, ok := d.recentCountsFailure(1); ok {
		t.Error("a store-layer failure was negatively cached; the next call cannot retry for scanFailTTL")
	}
}

// TestScanFailTTLExceedsScanBudget pins the doctrine behind the negative TTL: retrying a
// budget-killed scan sooner than the budget itself cannot succeed and only costs another
// full-length read transaction.
func TestScanFailTTLExceedsScanBudget(t *testing.T) {
	if scanFailTTL <= scanBudget {
		t.Errorf("scanFailTTL (%v) must exceed scanBudget (%v)", scanFailTTL, scanBudget)
	}
}
