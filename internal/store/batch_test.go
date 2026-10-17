package store

import (
	"errors"
	"testing"
)

// Both granularities must satisfy the same contract, or the apply step could tell them
// apart - which is the whole point of the interface.
var (
	_ Writer = (*Store)(nil)
	_ Writer = (*Batch)(nil)
)

// TestBatchCommitsEveryWriteTogether: the ordinary case, and the reason the type exists.
func TestBatchCommitsEveryWriteTogether(t *testing.T) {
	s, _ := openTemp(t)
	var seqs []uint64
	err := s.Batch(func(b *Batch) error {
		for _, p := range []string{"a.txt", "b.txt", "c.txt"} {
			n, err := b.Enqueue(Op{Type: OpDownload, Direction: DirInbound, ViewID: 7, Path: p})
			if err != nil {
				return err
			}
			seqs = append(seqs, n)
			if err := b.PutBaseline(7, p, Baseline{State: StatePending, FileID: "f-" + p}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if len(seqs) != 3 || seqs[0] >= seqs[1] || seqs[1] >= seqs[2] {
		t.Errorf("sequences = %v, want three strictly increasing", seqs)
	}
	n, err := s.PendingLen()
	if err != nil || n != 3 {
		t.Fatalf("PendingLen = %d (err %v), want 3", n, err)
	}
	for _, p := range []string{"a.txt", "b.txt", "c.txt"} {
		if _, err := s.GetBaseline(7, p); err != nil {
			t.Errorf("GetBaseline %q: %v", p, err)
		}
	}
}

// TestBatchDiscardsEverythingOnError is the property that makes batching safe for the
// enqueue-first rule. The rule is about ORDER - a queue record ahead of the baseline it
// owns - and a transaction that never commits leaves neither, which is a batch that never
// happened. A partial batch would be the dangerous shape, and there is no such state.
func TestBatchDiscardsEverythingOnError(t *testing.T) {
	s, _ := openTemp(t)
	boom := errors.New("planner failed midway")
	err := s.Batch(func(b *Batch) error {
		if _, err := b.Enqueue(Op{Type: OpDownload, Direction: DirInbound, ViewID: 7, Path: "a.txt"}); err != nil {
			return err
		}
		if err := b.PutBaseline(7, "a.txt", Baseline{State: StatePending}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Batch error = %v, want the caller's error", err)
	}
	if n, err := s.PendingLen(); err != nil || n != 0 {
		t.Errorf("PendingLen = %d (err %v), want 0 (a failed batch must leave no op)", n, err)
	}
	if _, err := s.GetBaseline(7, "a.txt"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetBaseline err = %v, want ErrNotFound (a failed batch must leave no baseline)", err)
	}
}

// TestBatchReadsItsOwnWrites: an apply step that reads a baseline before mutating it -
// markLocalPending does exactly that - must see what the same batch already wrote, or
// batching would change the outcome rather than only the number of fsyncs.
func TestBatchReadsItsOwnWrites(t *testing.T) {
	s, _ := openTemp(t)
	err := s.Batch(func(b *Batch) error {
		if err := b.PutBaseline(7, "a.txt", Baseline{State: StatePending, LocalHash: "h1"}); err != nil {
			return err
		}
		got, err := b.GetBaseline(7, "a.txt")
		if err != nil {
			return err
		}
		if got.LocalHash != "h1" {
			t.Errorf("GetBaseline inside the batch = %q, want the write this batch just made", got.LocalHash)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Batch: %v", err)
	}
}

// TestBatchAndStandaloneShareOneSequence: the executor reads the queue in sequence order and
// nothing else, so a batched op must be numbered from the same counter as a standalone one.
func TestBatchAndStandaloneShareOneSequence(t *testing.T) {
	s, _ := openTemp(t)
	first, err := s.Enqueue(Op{Type: OpDownload, Direction: DirInbound, ViewID: 7, Path: "one"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	var batched uint64
	if err := s.Batch(func(b *Batch) error {
		n, berr := b.Enqueue(Op{Type: OpDownload, Direction: DirInbound, ViewID: 7, Path: "two"})
		batched = n
		return berr
	}); err != nil {
		t.Fatalf("Batch: %v", err)
	}
	last, err := s.Enqueue(Op{Type: OpDownload, Direction: DirInbound, ViewID: 7, Path: "three"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if first >= batched || batched >= last {
		t.Errorf("sequences = %d, %d, %d; want one strictly increasing order across both paths",
			first, batched, last)
	}
}

// TestBatchMoveBaselineKeepsTheIndexGuards: a batched relocation must run the same index
// maintenance as a standalone one, or byid would keep pointing at the vacated path.
func TestBatchMoveBaselineKeepsTheIndexGuards(t *testing.T) {
	s, _ := openTemp(t)
	if err := s.PutBaseline(7, "from.txt", Baseline{State: StateSynced, FileID: "f1"}); err != nil {
		t.Fatalf("PutBaseline: %v", err)
	}
	if err := s.Batch(func(b *Batch) error {
		return b.MoveBaseline(7, "from.txt", "to.txt", Baseline{State: StateSynced, FileID: "f1"})
	}); err != nil {
		t.Fatalf("Batch: %v", err)
	}
	if _, err := s.GetBaseline(7, "from.txt"); !errors.Is(err, ErrNotFound) {
		t.Errorf("source baseline err = %v, want ErrNotFound after the move", err)
	}
	if _, err := s.GetBaseline(7, "to.txt"); err != nil {
		t.Errorf("GetBaseline to.txt: %v", err)
	}
	p, err := s.PathByID(7, "f1")
	if err != nil {
		t.Fatalf("PathByID: %v", err)
	}
	if p != "to.txt" {
		t.Errorf("byid points at %q, want the destination", p)
	}
}
