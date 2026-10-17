// Package backoff implements a context-aware exponential backoff with jitter
// and a ceiling, for retry loops. The transport pool drives it when re-dialing
// a dropped connection, the daemon between sync-loop bring-up attempts, and the
// IPC server between accept failures.
package backoff

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// Backoff defaults. The sequence starts near defaultBackoffBase and doubles up to
// defaultBackoffMax, with up to defaultBackoffJitter of downward jitter.
const (
	defaultBackoffBase   = 500 * time.Millisecond
	defaultBackoffMax    = 30 * time.Second
	defaultBackoffFactor = 2.0
	defaultBackoffJitter = 0.5
)

// Backoff produces an exponentially increasing, jittered delay sequence capped at
// Max. The zero value is usable (defaults fill in on first use), but New
// gives the recommended jittered configuration. It is not safe for concurrent
// use.
type Backoff struct {
	// Base is the first delay before any growth.
	Base time.Duration
	// Max is the ceiling no single delay exceeds.
	Max time.Duration
	// Factor is the per-attempt growth multiplier (>= 1).
	Factor float64
	// Jitter is the fraction [0,1] by which a delay may be reduced at random; 0
	// yields a deterministic sequence.
	Jitter float64
	// rng returns a float in [0,1); injectable for deterministic tests.
	rng     func() float64
	attempt int
}

// New returns a Backoff with the recommended exponential, jittered
// defaults.
func New() *Backoff {
	return &Backoff{
		Base:   defaultBackoffBase,
		Max:    defaultBackoffMax,
		Factor: defaultBackoffFactor,
		Jitter: defaultBackoffJitter,
	}
}

// ensureDefaults fills in zero-valued numeric fields and the rng source.
func (b *Backoff) ensureDefaults() {
	if b.Base <= 0 {
		b.Base = defaultBackoffBase
	}
	if b.Max <= 0 {
		b.Max = defaultBackoffMax
	}
	if b.Factor < 1 {
		b.Factor = defaultBackoffFactor
	}
	if b.rng == nil {
		b.rng = rand.Float64 // #nosec G404 -- jitter does not require cryptographic randomness
	}
}

// Next returns the next delay and advances the sequence. The raw delay is
// Base*Factor^attempt capped at Max, then reduced by up to Jitter at random.
func (b *Backoff) Next() time.Duration {
	b.ensureDefaults()
	d := float64(b.Base) * math.Pow(b.Factor, float64(b.attempt))
	if ceiling := float64(b.Max); d > ceiling || math.IsInf(d, 1) {
		d = ceiling
	}
	b.attempt++
	if b.Jitter > 0 {
		j := b.Jitter
		if j > 1 {
			j = 1
		}
		d -= d * j * b.rng()
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}

// Reset returns the sequence to its initial delay.
func (b *Backoff) Reset() { b.attempt = 0 }

// Wait sleeps for the next delay, returning early with ctx.Err() if ctx is
// cancelled first. The sequence advances regardless of which case fires.
func (b *Backoff) Wait(ctx context.Context) error {
	t := time.NewTimer(b.Next())
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
