package store

// Reentrancy detection for Batch.
//
// A Batch runs its closure inside one bolt write transaction. bolt serialises writers with a
// plain sync.Mutex, so a closure that reaches back for a *Store method - s.PutBaseline instead
// of the b.PutBaseline it was handed - blocks on that mutex forever. There is no timeout, no
// panic and no error: the daemon simply stops, holding the database lock, and the operator sees
// a process that is running and doing nothing. A nested READ is worse still, because it only
// hangs when the batch's commit needs to remap a grown file, so it survives testing and appears
// under load.
//
// PACKAGES.md used to say the Writer parameter made this "a compile-time question". It does not:
// every apply closure captures the engine, so `e.store.PathByID(...)` inside a batch compiles
// perfectly well. The seam makes the right call convenient, not the wrong one impossible.
//
// So it is made observable instead. The check costs one atomic load on the common path - no
// batch open, nothing to compare - and reads the goroutine identity only while a batch actually
// is in flight, which happens on a cold start and on the create/modify reconcile arm.
//
// Identity, not mere presence, is what has to be compared: other goroutines legitimately touch
// the store while a batch runs (the daemon's status scan, an IPC handler), and blocking on the
// writer lock is the correct thing for THEM to do. Only the batching goroutine itself is in a
// deadlock.

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"sync/atomic"
)

// batchOwner is the goroutine currently inside a Batch closure, or 0 when none is. Only one
// batch can be open per Store at a time - bolt admits one writer - so a single slot suffices.
type batchOwner struct {
	id atomic.Uint64
}

// enter records the calling goroutine as the batch owner and returns the release func.
func (o *batchOwner) enter() func() {
	o.id.Store(goroutineID())
	return func() { o.id.Store(0) }
}

// check panics when the caller is the goroutine currently running a Batch closure.
//
// A panic, not an error: this is a programming mistake with exactly one correct fix (use the
// Writer the closure was given), it is not recoverable at run time, and the alternative is not
// an error but a silent permanent hang. The message names the way out.
func (o *batchOwner) check(op string) {
	owner := o.id.Load()
	if owner == 0 {
		return
	}
	if owner != goroutineID() {
		// A different goroutine: blocking on the writer lock is correct for it.
		return
	}
	panic(fmt.Sprintf("store: %s called on the *Store from inside a Batch closure; "+
		"this deadlocks bolt's writer lock permanently. Use the *Batch (store.Writer) the "+
		"closure was given instead of reaching back for the store.", op))
}

// goroutineIDPrefix is the fixed header runtime.Stack writes before the numeric id.
var goroutineIDPrefix = []byte("goroutine ")

// goroutineID returns the calling goroutine's id.
//
// The runtime exposes no supported accessor, so this parses the first line of its own stack
// header - a technique whose only real cost is that it is ugly, and whose format has been stable
// for the entire life of the runtime. It is confined to this file and reached only while a batch
// is open. A parse failure returns 0, which makes check() fall through to "some other
// goroutine": the guard declines to fire rather than risk a spurious panic.
func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	line := buf[:n]
	if !bytes.HasPrefix(line, goroutineIDPrefix) {
		return 0
	}
	line = line[len(goroutineIDPrefix):]
	end := bytes.IndexByte(line, ' ')
	if end < 0 {
		return 0
	}
	id, err := strconv.ParseUint(string(line[:end]), 10, 64)
	if err != nil {
		return 0
	}
	return id
}
