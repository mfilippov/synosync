package main

import (
	"context"
	"testing"
	"time"
)

// TestReleaseSignalsWhenCancelled confirms the signal registration is released
// (stop is invoked) only after ctx is cancelled, so a second Ctrl-C during a slow
// teardown reaches the default disposition instead of being swallowed.
func TestReleaseSignalsWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	releaseSignalsWhenCancelled(ctx, func() { close(stopped) })

	// Before cancellation the registration must stay in place.
	select {
	case <-stopped:
		t.Fatal("stop called before ctx was cancelled")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("stop not called after ctx was cancelled")
	}
}
