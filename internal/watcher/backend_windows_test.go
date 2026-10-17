//go:build windows

package watcher

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/fsnotify/fsnotify"
	"golang.org/x/sys/windows"
)

// notifyRecord encodes one FILE_NOTIFY_INFORMATION; next is the offset to the following
// record, 0 for the last.
func notifyRecord(next, action uint32, name string) []byte {
	u := utf16.Encode([]rune(name))
	b := make([]byte, 12+2*len(u))
	binary.LittleEndian.PutUint32(b[0:], next)
	binary.LittleEndian.PutUint32(b[4:], action)
	binary.LittleEndian.PutUint32(b[8:], uint32(2*len(u)))
	for i, c := range u {
		binary.LittleEndian.PutUint16(b[12+2*i:], c)
	}
	return b
}

// TestSubtreeDispatchDecodesTheRecordChain pins the decoding of what ReadDirectoryChangesW
// writes: a chain of records, nested names relative to the root, non-ASCII names, the two
// halves of a rename, and a record that runs past the buffer, which is reported as an
// overflow rather than half-trusted.
func TestSubtreeDispatchDecodesTheRecordChain(t *testing.T) {
	root := `C:\sync`
	recs := [][]byte{
		notifyRecord(0, windows.FILE_ACTION_ADDED, `a\b\new.txt`),
		notifyRecord(0, windows.FILE_ACTION_RENAMED_OLD_NAME, `old`),
		notifyRecord(0, windows.FILE_ACTION_RENAMED_NEW_NAME, `Привет`),
		notifyRecord(0, windows.FILE_ACTION_MODIFIED, `f.txt`),
		notifyRecord(0, windows.FILE_ACTION_REMOVED, `gone`),
	}
	var chain []byte
	for i, r := range recs {
		if i < len(recs)-1 {
			// Records are DWORD-aligned in the kernel's buffer.
			pad := (4 - len(r)%4) % 4
			r = append(r, make([]byte, pad)...)
			binary.LittleEndian.PutUint32(r[0:], uint32(len(r)))
		}
		chain = append(chain, r...)
	}
	s, err := newSubtreeWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	go s.dispatch(root, chain)
	want := []fsnotify.Event{
		{Name: filepath.Join(root, `a\b\new.txt`), Op: fsnotify.Create},
		{Name: filepath.Join(root, `old`), Op: fsnotify.Rename},
		{Name: filepath.Join(root, `Привет`), Op: fsnotify.Create},
		{Name: filepath.Join(root, `f.txt`), Op: fsnotify.Write},
		{Name: filepath.Join(root, `gone`), Op: fsnotify.Remove},
	}
	for _, w := range want {
		if got := <-s.Events(); got != w {
			t.Fatalf("event = %v, want %v", got, w)
		}
	}

	truncated := notifyRecord(0, windows.FILE_ACTION_ADDED, "long-name.txt")[:20]
	go s.dispatch(root, truncated)
	if err := <-s.Errors(); !errors.Is(err, fsnotify.ErrEventOverflow) {
		t.Fatalf("truncated record: err = %v, want ErrEventOverflow", err)
	}
}

// TestSubtreeKernelOverflowIsReportedAndTheWatchSurvives drives a REAL overflow: a small
// buffer, a consumer that is not reading, and a burst far larger than the kernel can hold
// for the handle. The overflow must arrive as fsnotify.ErrEventOverflow - which the event
// loop turns into a prompt scan - and the watch must keep working afterwards rather than
// being mistaken for a vanished root.
func TestSubtreeKernelOverflowIsReportedAndTheWatchSurvives(t *testing.T) {
	root := t.TempDir()
	s, err := newSubtreeWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	s.bufSize = 4096
	if err := s.Add(root); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// Nobody reads Events yet, so the reader stalls once the channel fills and the kernel
	// accumulates the rest of the burst on its own.
	for i := 0; i < 3000; i++ {
		name := filepath.Join(root, fmt.Sprintf("a-reasonably-long-file-name-to-fill-the-buffer-%05d.txt", i))
		if err := os.WriteFile(name, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(20 * time.Second)
	overflowed := false
	for !overflowed {
		select {
		case <-s.Events():
		case err := <-s.Errors():
			if !errors.Is(err, fsnotify.ErrEventOverflow) {
				t.Fatalf("error = %v, want ErrEventOverflow", err)
			}
			overflowed = true
		case <-deadline:
			t.Fatal("no overflow reported for a burst far beyond the buffer")
		}
	}
	if got := s.WatchList(); len(got) != 1 || got[0] != root {
		t.Fatalf("WatchList after the overflow = %q, want the root still watched", got)
	}
	// Drain what is left of the burst, then prove the watch still delivers.
	marker := filepath.Join(root, "after-overflow.txt")
	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case ev := <-s.Events():
			if ev.Name == marker {
				return
			}
		case err := <-s.Errors():
			if !errors.Is(err, fsnotify.ErrEventOverflow) {
				t.Fatalf("error = %v, want only overflows", err)
			}
			// A second overflow may swallow the marker's own event; write another.
			if err := os.WriteFile(marker, []byte("again"), 0o644); err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			t.Fatal("the watch delivered nothing after the overflow")
		}
	}
}
