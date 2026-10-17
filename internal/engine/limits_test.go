package engine

import (
	"testing"
	"time"

	"github.com/mfilippov/synosync/internal/config"
)

// TestNewDefaultLimits confirms a freshly constructed engine carries the historical
// hardcoded defaults so behaviour is unchanged when nothing is configured: the
// effective limits equal config.DefaultLimits and the derived scanInterval matches the
// pre-config constant (localScanInterval, 5s).
func TestNewDefaultLimits(t *testing.T) {
	e := New(nil, nil, t.TempDir(), "sess", nil)

	want := config.DefaultLimits()
	if e.limits != want {
		t.Errorf("New limits = %+v, want %+v", e.limits, want)
	}
	if e.limits.DrainBatch != maxDrainBatch {
		t.Errorf("default DrainBatch = %d, want %d", e.limits.DrainBatch, maxDrainBatch)
	}
	if e.limits.UploadResumeThreshold != uploadResumeThreshold {
		t.Errorf("default UploadResumeThreshold = %d, want %d", e.limits.UploadResumeThreshold, uploadResumeThreshold)
	}
	if e.limits.EmptyScanThreshold != emptyScanConfirmThreshold {
		t.Errorf("default EmptyScanThreshold = %d, want %d", e.limits.EmptyScanThreshold, emptyScanConfirmThreshold)
	}
	if want := 5 * time.Second; e.scanInterval != want {
		t.Errorf("default scanInterval = %v, want %v", e.scanInterval, want)
	}
}

// TestSetLimitsHonored confirms SetLimits installs operator-tuned knobs and re-derives
// the scan interval from the new LocalScanInterval.
func TestSetLimitsHonored(t *testing.T) {
	e := New(nil, nil, t.TempDir(), "sess", nil)

	custom := config.Limits{
		Workers:               2,
		LocalScanInterval:     17,
		DrainBatch:            42,
		UploadResumeThreshold: 8 << 20,
		EmptyScanThreshold:    7,
		WatcherDebounce:       250,
		ColdStartCatalogCap:   1000,
		EventPollTimeout:      45,
		PollDeadlineSlack:     20,
	}
	e.SetLimits(custom)

	if e.limits != custom {
		t.Errorf("SetLimits limits = %+v, want %+v", e.limits, custom)
	}
	if want := 17 * time.Second; e.scanInterval != want {
		t.Errorf("scanInterval after SetLimits = %v, want %v", e.scanInterval, want)
	}
}

// TestSetLimitsBackfillsZero confirms a partial Limits (e.g. an older blob with no
// "limits" section, or a test passing only one knob) is back-filled with the historical
// defaults so an unset field behaves exactly as before - only the explicitly-set field
// diverges.
func TestSetLimitsBackfillsZero(t *testing.T) {
	e := New(nil, nil, t.TempDir(), "sess", nil)

	// Only DrainBatch is set; everything else is zero and must back-fill to defaults.
	e.SetLimits(config.Limits{DrainBatch: 99})

	def := config.DefaultLimits()
	if e.limits.DrainBatch != 99 {
		t.Errorf("DrainBatch = %d, want 99 (explicit)", e.limits.DrainBatch)
	}
	if e.limits.LocalScanInterval != def.LocalScanInterval {
		t.Errorf("LocalScanInterval = %d, want %d (back-filled default)", e.limits.LocalScanInterval, def.LocalScanInterval)
	}
	if e.limits.UploadResumeThreshold != def.UploadResumeThreshold {
		t.Errorf("UploadResumeThreshold = %d, want %d (back-filled default)", e.limits.UploadResumeThreshold, def.UploadResumeThreshold)
	}
	if e.limits.EventPollTimeout != def.EventPollTimeout {
		t.Errorf("EventPollTimeout = %d, want %d (back-filled default)", e.limits.EventPollTimeout, def.EventPollTimeout)
	}
	if want := time.Duration(def.LocalScanInterval) * time.Second; e.scanInterval != want {
		t.Errorf("scanInterval = %v, want %v (back-filled default)", e.scanInterval, want)
	}
}
