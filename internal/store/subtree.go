package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/mfilippov/synosync/internal/normpath"

	bolt "go.etcd.io/bbolt"
)

// DropSubtree forgets a subtree of view viewID in ONE transaction: every baseline whose path
// match accepts, and every queued op of the view with either endpoint (Path, or a rename's
// NewPath) accepted. It is what excluding a path does (`synosync exclude add`), and it touches
// nothing on disk or on the server - dropping a pending download leaves the server node where it
// is, dropping a queued upload leaves the local file where it is.
//
// match is told whether the record is a directory, for name rules that hide files only (a queued
// op is a directory when it is a mkdir or a directory rename).
// match decides membership rather than a byte prefix because an exclusion matches by fold key
// (config.ExcludedPath.Excludes), which a cursor prefix over the raw spelling cannot express;
// the scan is therefore O(view), which is fine for a command run by hand with the daemon
// stopped. A queued record that no longer decodes is left for the queue's own quarantine.
func (s *Store) DropSubtree(viewID uint64, match func(path string, isDir bool) bool) (baselines, ops int, err error) {
	err = s.update(func(tx *bolt.Tx) error {
		vp := viewPrefix(viewID)
		var doomed []string
		c := tx.Bucket(bucketFiles).Cursor()
		for k, raw := c.Seek(vp); k != nil && bytes.HasPrefix(k, vp); k, raw = c.Next() {
			var b Baseline
			_ = json.Unmarshal(raw, &b) // an undecodable record reads as a file; the drop still applies by path
			if p := string(k[len(vp):]); match(p, b.FileType == "dir") {
				doomed = append(doomed, p)
			}
		}
		for _, p := range doomed {
			if err := deleteBaselineTx(tx, viewID, p); err != nil {
				return err
			}
		}
		q := tx.Bucket(bucketQueue)
		var seqs [][]byte
		qc := q.Cursor()
		for k, raw := qc.First(); k != nil; k, raw = qc.Next() {
			var op Op
			if json.Unmarshal(raw, &op) != nil || op.ViewID != viewID {
				continue
			}
			isDir := op.Type == OpMkdir || op.IsDir
			if match(op.Path, isDir) || (op.NewPath != "" && match(op.NewPath, isDir)) {
				seqs = append(seqs, append([]byte(nil), k...))
			}
		}
		for _, k := range seqs {
			if err := q.Delete(k); err != nil {
				return fmt.Errorf("store: drop queued op %d: %w", decodeUint64(k), err)
			}
		}
		baselines, ops = len(doomed), len(seqs)
		return nil
	})
	return baselines, ops, err
}

// MergeRescans adds paths to a view's pending rescans (View.Rescan), keeping one entry per
// subtree: a path an entry already covers is dropped, and a path that covers entries replaces
// them. Coverage is by fold key, as an exclusion matches; "" is the whole view and covers all.
func MergeRescans(have, paths []string) []string {
	under := func(rel, root string) bool {
		if root == "" {
			return true // the whole view
		}
		k, r := normpath.Key(root), normpath.Key(rel)
		return r == k || strings.HasPrefix(r, k+"/")
	}
	out := slices.Clone(have)
	for _, p := range paths {
		if slices.ContainsFunc(out, func(h string) bool { return under(p, h) }) {
			continue
		}
		out = slices.DeleteFunc(out, func(h string) bool { return under(h, p) })
		out = append(out, p)
	}
	return out
}
