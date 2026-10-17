package store

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

// TestBatchReentrantWritePanicsInsteadOfHanging pins the whole point of the guard.
//
// bolt serialises writers with a plain mutex, so a Batch closure that reaches back for a
// *Store method blocks on it forever: no timeout, no error, no panic. The daemon stops while
// still holding the database lock, and the operator sees a live process doing nothing. Turning
// that into an immediate panic that names the remedy is the difference between a five-minute
// fix and an undiagnosable hang.
func TestBatchReentrantWritePanicsInsteadOfHanging(t *testing.T) {
	s, _ := openTemp(t)

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("a reentrant write from inside a Batch did not panic; it deadlocks in production")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value = %#v, want the diagnostic string", r)
		}
		for _, want := range []string{"Batch closure", "deadlock", "store.Writer"} {
			if !strings.Contains(msg, want) {
				t.Errorf("panic message missing %q:\n%s", want, msg)
			}
		}
	}()

	_ = s.Batch(func(*Batch) error {
		// The mistake: the closure was handed a Writer and reaches for the store instead.
		return s.PutBaseline(7, "a.txt", Baseline{State: StateSynced})
	})
}

// TestBatchReentrantReadPanicsToo pins the subtler half. A nested READ deadlocks only when the
// batch's commit has to remap a grown file, so it survives testing and appears under load; the
// guard must not wait for that to be reachable before reporting it.
func TestBatchReentrantReadPanicsToo(t *testing.T) {
	s, _ := openTemp(t)

	defer func() {
		if recover() == nil {
			t.Fatal("a reentrant read from inside a Batch did not panic")
		}
	}()

	_ = s.Batch(func(*Batch) error {
		_, err := s.GetBaseline(7, "a.txt")
		return err
	})
}

// TestBatchDoesNotBlameAnotherGoroutine pins the discrimination the guard has to make: other
// goroutines legitimately touch the store while a batch runs - the daemon's status scan, an IPC
// handler - and blocking on the writer lock is the CORRECT behaviour for them. Only the
// batching goroutine is deadlocked, so only it may be blamed.
func TestBatchDoesNotBlameAnotherGoroutine(t *testing.T) {
	s, _ := openTemp(t)

	inBatch := make(chan struct{})
	otherDone := make(chan error, 1)
	var once sync.Once

	err := s.Batch(func(b *Batch) error {
		go func() {
			<-inBatch
			// A read from a DIFFERENT goroutine: it may block until the batch commits, but it
			// must never panic.
			defer func() {
				if r := recover(); r != nil {
					otherDone <- errPanicked
					return
				}
			}()
			_, gerr := s.GetBaseline(7, "other.txt")
			if errors.Is(gerr, ErrNotFound) {
				gerr = nil
			}
			otherDone <- gerr
		}()
		once.Do(func() { close(inBatch) })
		return b.PutBaseline(7, "a.txt", Baseline{State: StateSynced})
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}

	if gerr := <-otherDone; gerr == errPanicked {
		t.Fatal("a read from another goroutine was blamed for the batch's reentrancy")
	} else if gerr != nil {
		t.Fatalf("concurrent read: %v", gerr)
	}
}

// TestGoroutineIDIsStableAndDistinct pins the identity primitive the guard rests on: stable
// within a goroutine, different across them. A parse failure returns 0, which makes the guard
// decline to fire rather than risk a spurious panic - so the only thing that must hold is that
// real ids are non-zero and distinct.
func TestGoroutineIDIsStableAndDistinct(t *testing.T) {
	mine := goroutineID()
	if mine == 0 {
		t.Fatal("goroutineID returned 0 on a real goroutine; the guard would never fire")
	}
	if again := goroutineID(); again != mine {
		t.Errorf("goroutineID = %d then %d on one goroutine, want it stable", mine, again)
	}

	other := make(chan uint64, 1)
	go func() { other <- goroutineID() }()
	if got := <-other; got == mine {
		t.Errorf("a second goroutine reported the same id %d", got)
	}
}

// errPanicked is the sentinel the concurrent-reader goroutine reports when the guard blamed it.
var errPanicked = errors.New("the guard panicked on another goroutine")
