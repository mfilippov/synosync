package store

// Measurement for the NoFreelistSync question: does skipping the freelist sync buy
// enough commit throughput to pay for what it costs at Open?
//
// It is a measurement, not a test, and it asserts nothing about which configuration wins --
// the point is to produce both halves of the trade so the decision is made on numbers. Run it
// deliberately:
//
//	go test ./internal/store/ -run XXX -bench 'BenchmarkFreelist' -benchtime 1x -v
//
// WHY BOTH HALVES. NoFreelistSync trades a cheaper commit for a more expensive open: with the
// freelist unsynced, bbolt's meta carries PgidNoFreelist, so hasSyncedFreelist() is false and
// loadFreelist() reconstructs the freelist by SCANNING -- freepages() walks every bucket
// recursively, building a map of every reachable page. In write mode that scan is not optional:
// bbolt sets PreLoadFreelist unconditionally (db.go:229, "always load free pages in write
// mode"), and the daemon opens read-write. So the daemon pays the scan on EVERY start, and
// state.db grows with the synced tree -- the same first-sync latency that #8 and #14 were about.
//
// WHY THIS WRITE PATTERN. The engine does not write in one big transaction. A cold start batches
// its adoptions (store.Batch), but the drain path commits per applied op, and the enqueue-first
// doctrine deliberately splits a queue write from the baseline mutation it owns. Benchmarking a
// single bulk load would measure a shape the client never produces and would flatter whichever
// option is better at it.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// freelistBenchSizes are the tree sizes the measurement reports. They bracket what a real
// install holds: a modest home folder and a large one.
var freelistBenchSizes = []int{10_000, 100_000}

// benchBaseline is a realistically-sized record: a server id, a content hash and the path
// metadata every synced node carries, so page counts resemble production rather than a
// micro-benchmark's empty structs.
func benchBaseline(i int) Baseline {
	return Baseline{
		FileID:     fmt.Sprintf("96911240156995%07d", i),
		FileType:   "file",
		ServerHash: fmt.Sprintf("%032x", i),
		LocalHash:  fmt.Sprintf("%032x", i),
		LocalSize:  int64(1024 + i%4096),
		LocalMTime: int64(1756000000 + i),
		State:      StateSynced,
	}
}

func benchPath(i int) string {
	return fmt.Sprintf("Documents/folder%03d/subfolder%02d/file-%06d.txt", i%512, i%64, i)
}

// buildStore replays the engine's write shape into a fresh database and returns how long the
// writes took plus the resulting file size.
//
// The mix mirrors a first sync: most nodes arrive through the cold start's batched adoption,
// and a minority go through the per-op drain path (enqueue, apply, ack) that dominates steady
// state. Both are exercised because they stress the freelist differently -- one big transaction
// frees few pages, thousands of small ones churn.
func buildStore(tb testing.TB, path string, opts *bolt.Options, n int) (time.Duration, int64) {
	tb.Helper()
	st, err := OpenWithBoltOptions(path, opts)
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	if err := st.PutView(7, View{Name: "home", NodeID: "root1"}); err != nil {
		tb.Fatalf("PutView: %v", err)
	}

	start := time.Now()

	// Cold-start shape: batched adoptions, 256 per transaction (coldStartEnqueueBatch).
	const batchSize = 256
	batched := n - n/10
	for i := 0; i < batched; i += batchSize {
		end := i + batchSize
		if end > batched {
			end = batched
		}
		if err := st.Batch(func(w *Batch) error {
			for j := i; j < end; j++ {
				if err := w.PutBaseline(7, benchPath(j), benchBaseline(j)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			tb.Fatalf("batch %d: %v", i, err)
		}
	}

	// Drain shape: one op's worth of commits per node -- enqueue, then the baseline the apply
	// writes, then the ack. Three transactions per node, which is what the queue really costs.
	for i := batched; i < n; i++ {
		seq, err := st.Enqueue(Op{
			Type: OpDownload, Direction: DirInbound, ViewID: 7,
			Path: benchPath(i), FileID: benchBaseline(i).FileID,
		})
		if err != nil {
			tb.Fatalf("enqueue %d: %v", i, err)
		}
		if err := st.PutBaseline(7, benchPath(i), benchBaseline(i)); err != nil {
			tb.Fatalf("put %d: %v", i, err)
		}
		if err := st.Ack(seq); err != nil {
			tb.Fatalf("ack %d: %v", i, err)
		}
	}

	elapsed := time.Since(start)
	if err := st.Close(); err != nil {
		tb.Fatalf("close: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		tb.Fatalf("stat: %v", err)
	}
	return elapsed, fi.Size()
}

// timeOpen reports how long opening an existing database takes -- the cost the daemon pays on
// every start, and the half a commit-throughput benchmark alone would hide.
func timeOpen(tb testing.TB, path string, opts *bolt.Options) time.Duration {
	tb.Helper()
	start := time.Now()
	st, err := OpenWithBoltOptions(path, opts)
	if err != nil {
		tb.Fatalf("reopen: %v", err)
	}
	elapsed := time.Since(start)
	if err := st.Close(); err != nil {
		tb.Fatalf("close: %v", err)
	}
	return elapsed
}

// BenchmarkFreelistSyncTradeoff reports both halves side by side for each tree size.
func BenchmarkFreelistSyncTradeoff(b *testing.B) {
	configs := []struct {
		name string
		opts *bolt.Options
	}{
		{"shipped", &bolt.Options{Timeout: openTimeout}},
		{"NoFreelistSync", &bolt.Options{Timeout: openTimeout, NoFreelistSync: true}},
	}
	for _, n := range freelistBenchSizes {
		for _, c := range configs {
			b.Run(fmt.Sprintf("n=%d/%s", n, c.name), func(b *testing.B) {
				dir := b.TempDir()
				path := filepath.Join(dir, "state.db")
				writeFor, size := buildStore(b, path, c.opts, n)

				// Open the built database several times and report the median-ish mean; a
				// single reading is noise on a laptop.
				const opens = 5
				var total time.Duration
				var worst time.Duration
				for i := 0; i < opens; i++ {
					d := timeOpen(b, path, c.opts)
					total += d
					if d > worst {
						worst = d
					}
				}
				b.ReportMetric(float64(writeFor.Milliseconds()), "write_ms")
				b.ReportMetric(float64(total.Microseconds())/opens/1000, "open_ms")
				b.ReportMetric(float64(worst.Microseconds())/1000, "open_worst_ms")
				b.ReportMetric(float64(size)/(1<<20), "db_MiB")
			})
		}
	}
}
