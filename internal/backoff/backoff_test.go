package backoff

import (
	"context"
	"testing"
	"time"
)

// TestBackoffSequence checks the deterministic (Jitter=0) exponential sequence
// and the ceiling.
func TestBackoffSequence(t *testing.T) {
	b := &Backoff{Base: 100 * time.Millisecond, Max: 1 * time.Second, Factor: 2}
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1 * time.Second, // capped (1600ms > Max)
		1 * time.Second, // stays capped
	}
	for i, w := range want {
		if got := b.Next(); got != w {
			t.Errorf("Next #%d = %v, want %v", i, got, w)
		}
	}
}

// TestBackoffReset returns the sequence to its first delay.
func TestBackoffReset(t *testing.T) {
	b := &Backoff{Base: 100 * time.Millisecond, Max: 1 * time.Second, Factor: 2}
	b.Next()
	b.Next()
	b.Reset()
	if got := b.Next(); got != 100*time.Millisecond {
		t.Errorf("after Reset Next = %v, want %v", got, 100*time.Millisecond)
	}
}

// TestBackoffJitter confirms jitter reduces the raw delay by up to the configured
// fraction, bounded by the injected rng.
func TestBackoffJitter(t *testing.T) {
	// rng fixed at 1.0 removes the full Jitter fraction from each delay.
	b := &Backoff{Base: 100 * time.Millisecond, Max: 1 * time.Second, Factor: 2, Jitter: 0.5, rng: func() float64 { return 1.0 }}
	if got := b.Next(); got != 50*time.Millisecond {
		t.Errorf("jittered Next = %v, want %v (100ms - 50%%)", got, 50*time.Millisecond)
	}
	// rng fixed at 0.0 removes nothing, leaving the raw delay.
	b2 := &Backoff{Base: 100 * time.Millisecond, Max: 1 * time.Second, Factor: 2, Jitter: 0.5, rng: func() float64 { return 0.0 }}
	if got := b2.Next(); got != 100*time.Millisecond {
		t.Errorf("unjittered Next = %v, want %v", got, 100*time.Millisecond)
	}
}

// TestBackoffDefaults verifies the zero value and New fill sensible
// defaults.
func TestBackoffDefaults(t *testing.T) {
	var b Backoff
	if got := b.Next(); got <= 0 {
		t.Errorf("zero-value Next = %v, want a positive default delay", got)
	}
	nb := New()
	if nb.Base != defaultBackoffBase || nb.Max != defaultBackoffMax {
		t.Errorf("New defaults = base %v max %v", nb.Base, nb.Max)
	}
}

// TestBackoffWaitElapses returns nil once the (short) delay elapses.
func TestBackoffWaitElapses(t *testing.T) {
	b := &Backoff{Base: time.Millisecond, Max: time.Millisecond, Factor: 1}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := b.Wait(ctx); err != nil {
		t.Errorf("Wait: %v", err)
	}
}

// TestBackoffWaitCancelled returns the context error when cancelled before the
// delay elapses.
func TestBackoffWaitCancelled(t *testing.T) {
	b := &Backoff{Base: time.Hour, Max: time.Hour, Factor: 1}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.Wait(ctx); err == nil {
		t.Error("Wait with cancelled context: want error, got nil")
	}
}
