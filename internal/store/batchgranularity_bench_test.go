package store

// Measurement for the write-granularity question: what does one transaction per PLANNED OP
// actually cost against one transaction per CHUNK of them?
//
// It is a measurement, not a test, and it asserts nothing - the point is to produce both halves
// so the decision rests on numbers rather than on the inherited claim. Run it deliberately:
//
//	go test ./internal/store/ -run XXX -bench 'BenchmarkPlanGranularity' -benchtime 1x -v
//
// WHY IT EXISTS. The engine's planning loops write a queue record and then the baseline that
// record owns - two committed transactions per op, because the enqueue-first doctrine is about
// ORDER and the loops honoured it by committing separately. Cold start measured that shape on a
// real 190k-node view at 40 ops/s and 47.6 GB of physical I/O into a 241 MB database, and batches
// because of it; the incremental loops kept the per-op shape on the reading that they plan "a
// handful of ops". Nothing bounds an incremental diff to a handful - unpacking an archive into
// the sync root plans as many ops as a cold start does - so the same amortisation was applied
// there, and this is the measurement that says what it buys rather than inheriting the claim.
//
// MEASURED 29.08.2026 (Ryzen 9 9950X, NVMe, state dir on disk - fsync 6.0-6.1 ms):
//
//	           per-op        batched      speed-up
//	 2 000     40.09 ops/s   15 110 ops/s     377x   (50 s   -> 0.29 s)
//	10 000     40.04 ops/s   17 763 ops/s     444x   (4m10s  -> 0.72 s)
//
// The per-op arm reproduces the historical 40 ops/s to three significant figures, which is the
// strongest thing this benchmark says: the figure was never about that one machine. Two commits
// per op at ~6 ms of fsync each IS 40 ops/s, on any disk that honours a flush. A 100k-file
// unpack therefore spends ~42 minutes in the planning loop alone with the network idle, exactly
// as the original measurement predicted.
//
// WHY THIS WRITE PATTERN. It reproduces exactly what a planning loop emits: for each op a queue
// record followed by the baseline it owns, in that order, differing only in how many commits the
// pair costs. Anything simpler (one bulk load of baselines) would measure a shape the client
// never produces.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// planBenchSizes bracket what an incremental diff can actually reach: a large directory dropped
// into the sync root, and an archive unpacked into it.
var planBenchSizes = []int{2_000, 10_000}

// planBenchOp builds the queue record a planned upload carries.
func planBenchOp(i int) Op {
	return Op{
		Type:      OpUpload,
		Direction: DirOutbound,
		ViewID:    7,
		Path:      fmt.Sprintf("dir%03d/file%06d.bin", i/512, i),
		LocalHash: "0123456789abcdef0123456789abcdef",
		LocalSize: 4096,
	}
}

// planBenchBaseline is the pending record the op's apply step writes.
func planBenchBaseline(i int) Baseline {
	return Baseline{
		FileID:    fmt.Sprintf("srv-%09d", i),
		FileType:  "file",
		LocalHash: "0123456789abcdef0123456789abcdef",
		LocalSize: 4096,
		State:     StatePending,
	}
}

func BenchmarkPlanGranularity(b *testing.B) {
	for _, n := range planBenchSizes {
		b.Run(fmt.Sprintf("per-op/%d", n), func(b *testing.B) {
			for range b.N {
				s := planBenchStore(b)
				start := time.Now()
				for i := range n {
					op := planBenchOp(i)
					// The shape the loops used to emit: the record and its baseline each
					// committed on their own, in the order the doctrine requires.
					if _, err := s.Enqueue(op); err != nil {
						b.Fatalf("Enqueue: %v", err)
					}
					if err := s.PutBaseline(7, op.Path, planBenchBaseline(i)); err != nil {
						b.Fatalf("PutBaseline: %v", err)
					}
				}
				planBenchReport(b, s, n, time.Since(start))
			}
		})

		b.Run(fmt.Sprintf("batched/%d", n), func(b *testing.B) {
			for range b.N {
				s := planBenchStore(b)
				start := time.Now()
				const chunk = 256 // engine.localReconcileEnqueueBatch
				for from := 0; from < n; from += chunk {
					to := min(from+chunk, n)
					if err := s.Batch(func(bt *Batch) error {
						for i := from; i < to; i++ {
							op := planBenchOp(i)
							// Same order, same records; one commit for the whole chunk.
							if _, err := bt.Enqueue(op); err != nil {
								return err
							}
							if err := bt.PutBaseline(7, op.Path, planBenchBaseline(i)); err != nil {
								return err
							}
						}
						return nil
					}); err != nil {
						b.Fatalf("Batch: %v", err)
					}
				}
				planBenchReport(b, s, n, time.Since(start))
			}
		})
	}
}

// planBenchStore opens a store on a fresh temp file, closed when the benchmark ends.
//
// WHERE IT RUNS IS THE MEASUREMENT. b.TempDir() honours TMPDIR, and on a modern Linux desktop
// that is often /tmp on TMPFS - where fsync costs nothing and both arms collapse to encoding
// speed, flattering the per-op shape by three orders of magnitude and hiding the very cost this
// benchmark exists to weigh. bolt fsyncs per commit, so the number that decides everything is the
// fsync latency of the filesystem the database sits on. Point TMPDIR at the disk the daemon's
// state dir really uses:
//
//	TMPDIR=$HOME/.local/share go test ./internal/store/ -run XXX -bench PlanGranularity -benchtime 1x
//
// planBenchReport prints the measured latency with every result so a run can never be read
// without knowing which disk produced it.
func planBenchStore(b *testing.B) *Store {
	b.Helper()
	s, err := Open(filepath.Join(b.TempDir(), "state.db"))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}

// benchFsyncMillis measures the fsync latency of the filesystem holding dir, in milliseconds. It
// is reported alongside every result: a sub-microsecond value means the run measured tmpfs and
// says nothing about a real install.
func benchFsyncMillis(b *testing.B, dir string) float64 {
	b.Helper()
	f, err := os.CreateTemp(dir, "fsync-probe-*")
	if err != nil {
		b.Fatalf("probe: %v", err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()
	const probes = 20
	start := time.Now()
	for range probes {
		if _, err := f.Write(make([]byte, 4096)); err != nil {
			b.Fatalf("probe write: %v", err)
		}
		if err := f.Sync(); err != nil {
			b.Fatalf("probe sync: %v", err)
		}
	}
	return float64(time.Since(start).Microseconds()) / float64(probes) / 1000
}

// planBenchReport prints the two numbers the decision rests on: how fast the ops were planned,
// and how large the database the writes landed in ended up.
func planBenchReport(b *testing.B, s *Store, n int, elapsed time.Duration) {
	b.Helper()
	var dbBytes int64
	if err := s.db.View(func(tx *bolt.Tx) error {
		dbBytes = tx.Size()
		return nil
	}); err != nil {
		b.Fatalf("size: %v", err)
	}
	b.ReportMetric(float64(n)/elapsed.Seconds(), "ops/s")
	b.ReportMetric(float64(dbBytes)/(1<<20), "db_MiB")
	b.ReportMetric(benchFsyncMillis(b, filepath.Dir(s.db.Path())), "fsync_ms")
}

// BenchmarkPlanGranularityLoaded repeats the measurement against a database that already holds a
// large tree, which is the variable the two figures disagree about.
//
// The historical number came from a 190k-node view in a 241 MB database, so tree size was the
// obvious suspect for any gap: bolt rewrites the b-tree path plus the freelist on every commit,
// and both grow with the tree. This isolates that variable.
//
//	go test ./internal/store/ -run XXX -bench 'BenchmarkPlanGranularityLoaded' -benchtime 1x -v
//
// MEASURED 29.08.2026 (same box, state dir on disk), preload 200k records into a 143.7 MiB
// database, then plan 5k ops into it:
//
//	per-op     40.45 ops/s   (2m07s)
//	batched   18 839 ops/s   (3.4 s, 466x, identical final database size)
//
// SIZE IS NOT THE VARIABLE. The per-op arm holds at ~40 ops/s whether the database is 2 MB or
// 143 MB, because what it pays is fsync LATENCY, not b-tree work: two commits per op at ~6 ms
// each is 40 ops/s and nothing about the tree changes that. The batched arm scales the other way
// - it commits once per 256 ops, so the same fsync is amortised and what is left is encoding.
//
// A CAUTION THIS BENCHMARK LEARNED THE HARD WAY. Run with TMPDIR on tmpfs (the default on many
// desktops) the per-op arm reports ~51 000 ops/s and the batched one ~9x that, which reads as
// "the historical figure does not reproduce, the fix is a minor win". Both halves are artifacts
// of fsync costing nothing there. The fsync_ms metric is reported with every result precisely so
// that mistake announces itself: anything under a millisecond means the run measured RAM.
func BenchmarkPlanGranularityLoaded(b *testing.B) {
	const preload = 200_000 // the tree the store already holds
	const plan = 5_000      // what one incremental pass then plans into it

	load := func(b *testing.B) *Store {
		b.Helper()
		s := planBenchStore(b)
		for from := 0; from < preload; from += 1024 {
			to := min(from+1024, preload)
			if err := s.Batch(func(bt *Batch) error {
				for i := from; i < to; i++ {
					p := fmt.Sprintf("pre%04d/file%07d.bin", i/512, i)
					if err := bt.PutBaseline(7, p, planBenchBaseline(i)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatalf("preload: %v", err)
			}
		}
		return s
	}

	b.Run("per-op", func(b *testing.B) {
		for range b.N {
			s := load(b)
			start := time.Now()
			for i := range plan {
				op := planBenchOp(preload + i)
				if _, err := s.Enqueue(op); err != nil {
					b.Fatalf("Enqueue: %v", err)
				}
				if err := s.PutBaseline(7, op.Path, planBenchBaseline(preload+i)); err != nil {
					b.Fatalf("PutBaseline: %v", err)
				}
			}
			planBenchReport(b, s, plan, time.Since(start))
		}
	})

	b.Run("batched", func(b *testing.B) {
		for range b.N {
			s := load(b)
			start := time.Now()
			const chunk = 256
			for from := 0; from < plan; from += chunk {
				to := min(from+chunk, plan)
				if err := s.Batch(func(bt *Batch) error {
					for i := from; i < to; i++ {
						op := planBenchOp(preload + i)
						if _, err := bt.Enqueue(op); err != nil {
							return err
						}
						if err := bt.PutBaseline(7, op.Path, planBenchBaseline(preload+i)); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatalf("Batch: %v", err)
				}
			}
			planBenchReport(b, s, plan, time.Since(start))
		}
	})
}
