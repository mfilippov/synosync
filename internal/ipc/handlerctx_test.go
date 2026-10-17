package ipc

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// shrinkHandlerTimeout shortens the dispatch handler budget for the duration of a
// test so a timeout case does not sleep for the production 8 s.
func shrinkHandlerTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	old := handlerTimeout
	handlerTimeout = d
	t.Cleanup(func() { handlerTimeout = old })
}

// TestHandlerBudgetBoundsWedgedHandler pins the wedged-handler fix: the
// connection deadline bounded only the I/O, so a handler that never returns held
// serveConn (and Serve's WaitGroup) forever while the caller hung until its own
// deadline with no diagnosis. dispatch now bounds the handler with a context, so
// the caller gets a prompt, named error and the handler's context is cancelled.
func TestHandlerBudgetBoundsWedgedHandler(t *testing.T) {
	shrinkHandlerTimeout(t, 50*time.Millisecond)

	release := make(chan struct{})
	// Released only at cleanup: the handler outlives its reply, which is exactly the
	// abandoned-handler case dispatch detaches.
	t.Cleanup(func() { close(release) })
	ctxCh := make(chan context.Context, 1)

	sock := serveTest(t, func(s *Server) {
		s.Handle("wedge", func(ctx context.Context, _ json.RawMessage) (any, error) {
			select {
			case ctxCh <- ctx:
			default:
			}
			<-release // ignores its context entirely: the buggy-handler case
			return map[string]string{"ok": "yes"}, nil
		})
	})

	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	start := time.Now()
	err = c.Call("wedge", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "did not finish") {
		t.Fatalf("Call against a wedged handler = %v, want a \"did not finish\" reply error", err)
	}
	if !strings.Contains(err.Error(), "wedge") {
		t.Errorf("Call error %q does not name the method", err)
	}
	// The reply must arrive on the handler budget, not on the client's own 10 s
	// round-trip deadline.
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("reply took %v, want ~the handler budget (the client hit its own deadline instead)", elapsed)
	}

	// The abandoned handler is told to stop: its context is cancelled, which is what
	// makes detaching a cooperative handler safe.
	select {
	case hctx := <-ctxCh:
		select {
		case <-hctx.Done():
			if !errors.Is(hctx.Err(), context.DeadlineExceeded) {
				t.Errorf("handler ctx err = %v, want DeadlineExceeded", hctx.Err())
			}
		case <-time.After(2 * time.Second):
			t.Error("handler context was never cancelled after the budget expired")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}
}

// TestServeDrainNotBlockedByWedgedHandler is the shutdown half of the same
// problem, at the PRODUCTION handler budget: cancelling Serve's context - a daemon
// shutdown or a Windows SCM stop - must collapse the in-flight handler's wait
// immediately rather than wait out the budget, let alone the handler. Before the
// bound, Serve's wg.Wait blocked for as long as the handler ran, so the SCM stop
// hint ("bounded by the IPC deadline") was a fiction.
func TestServeDrainNotBlockedByWedgedHandler(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	entered := make(chan struct{}, 1)

	sock := filepath.Join(sockDir(t), "drain.sock")
	l, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv := NewServer()
	srv.Handle("wedge", func(context.Context, json.RawMessage) (any, error) {
		entered <- struct{}{}
		<-release
		return nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, l) }()

	callDone := make(chan struct{})
	go func() {
		defer close(callDone)
		c, derr := Dial(sock)
		if derr != nil {
			return
		}
		defer c.Close()
		_ = c.Call("wedge", nil, nil)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("handler never ran")
	}

	cancel()
	select {
	case serr := <-done:
		if serr != nil {
			t.Fatalf("Serve after cancel = %v, want nil", serr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not drain within 2s while a handler ignored its context")
	}
	select {
	case <-callDone:
	case <-time.After(2 * time.Second):
		t.Error("client call never returned after shutdown")
	}
}

// TestServeShutdownWithSaturatedInFlightCap covers the accept loop's own wait:
// with every in-flight slot taken it used to block on a plain semaphore send, so a
// shutdown was noticed only once a slot freed and the connection queued behind it
// was left dangling. The send is now a select on ctx, so cancellation unwinds the
// loop and releases the queued connection.
func TestServeShutdownWithSaturatedInFlightCap(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var inFlight atomic.Int32

	sock := filepath.Join(sockDir(t), "sat.sock")
	l, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv := NewServer()
	srv.Handle("wedge", func(context.Context, json.RawMessage) (any, error) {
		inFlight.Add(1)
		<-release
		return nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, l) }()

	// More clients than slots: the surplus connections sit accepted-but-unserved (or
	// queued on the listener) with the accept loop parked on the saturated semaphore.
	const clients = maxInFlightConns + 4
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, derr := Dial(sock)
			if derr != nil {
				return
			}
			defer c.Close()
			_ = c.Call("wedge", nil, nil)
		}()
	}

	deadline := time.Now().Add(5 * time.Second)
	for inFlight.Load() < int32(maxInFlightConns) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := inFlight.Load(); got < int32(maxInFlightConns) {
		cancel()
		wg.Wait()
		t.Fatalf("in-flight handlers = %d, want the full cap of %d before cancelling", got, maxInFlightConns)
	}

	cancel()
	select {
	case serr := <-done:
		if serr != nil {
			t.Fatalf("Serve after cancel with the cap saturated = %v, want nil", serr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return within 2s with the in-flight cap saturated")
	}

	// Every client - served or queued behind the cap - is released rather than left
	// waiting out its own 10 s deadline.
	clientsDone := make(chan struct{})
	go func() { wg.Wait(); close(clientsDone) }()
	select {
	case <-clientsDone:
	case <-time.After(3 * time.Second):
		t.Error("clients still waiting 3s after shutdown; queued connections were not released")
	}
}

// TestHandlerContextCarriesServeCancel confirms a cooperative handler - the shape
// the daemon's store scans now use - sees the shutdown through its own context and
// can stop working, instead of learning about it only from a closed connection.
func TestHandlerContextCarriesServeCancel(t *testing.T) {
	sock := filepath.Join(sockDir(t), "coop.sock")
	l, err := Listen(sock)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv := NewServer()
	entered := make(chan struct{}, 1)
	srv.Handle("scan", func(ctx context.Context, _ json.RawMessage) (any, error) {
		entered <- struct{}{}
		<-ctx.Done() // the cooperative handler unwinds on cancellation
		return nil, ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, l) }()

	callErr := make(chan error, 1)
	go func() {
		c, derr := Dial(sock)
		if derr != nil {
			callErr <- derr
			return
		}
		defer c.Close()
		callErr <- c.Call("scan", nil, nil)
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("handler never ran")
	}
	cancel()

	select {
	case cerr := <-callErr:
		if cerr == nil {
			t.Fatal("Call during shutdown = nil error, want the cancellation surfaced")
		}
	case <-time.After(2 * time.Second):
		t.Error("client call never returned after shutdown")
	}
	select {
	case serr := <-done:
		if serr != nil {
			t.Fatalf("Serve = %v, want nil", serr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
}

// TestDegradingHandlerAnswerIsDelivered pins the seam a degrading handler depends on.
//
// A handler may be written to answer PARTIALLY when its context expires rather than fail - the
// daemon's status handler is exactly that: it bounds its single-flight wait on the handler context
// so an expired waiter falls through to the last known counts marked CountsStale, instead of
// holding the whole call hostage to a scan that can run for two minutes.
//
// That only works if dispatch is still listening when the answer arrives. It used to race the
// handler on the SAME deadline and always win - its select had a ready case while the handler
// still had to unwind, assemble and marshal - so the degraded report was written into a channel
// nobody read and the operator got the generic "did not finish". On a store whose counts scan
// outlives the budget that was every status call during the scan, cyclically. The unit test for
// the degrade drives buildStatus directly and never crosses this layer, which is how it survived.
func TestDegradingHandlerAnswerIsDelivered(t *testing.T) {
	shrinkHandlerTimeout(t, 400*time.Millisecond)

	sock := serveTest(t, func(s *Server) {
		s.Handle("counts", func(ctx context.Context, _ json.RawMessage) (any, error) {
			// The cooperative shape: wait until the budget expires, then answer with what is
			// already known rather than failing.
			<-ctx.Done()
			return map[string]any{"stale": true}, nil
		})
	})

	c, err := Dial(sock)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	var got struct {
		Stale bool `json:"stale"`
	}
	if err := c.Call("counts", nil, &got); err != nil {
		t.Fatalf("Call = %v, want the handler's degraded answer to be delivered: a handler that "+
			"answers when its context expires must not be abandoned mid-unwind, or every "+
			"degrade-instead-of-fail path silently becomes fail", err)
	}
	if !got.Stale {
		t.Errorf("result = %+v, want the handler's own degraded payload", got)
	}
}
