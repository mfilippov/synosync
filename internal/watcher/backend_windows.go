//go:build windows

package watcher

// The Windows backend: ONE ReadDirectoryChangesW over the whole sync root.
//
// fsnotify registers a separate watch per directory, and on Windows each one owns a 64 KiB
// ReadDirectoryChangesW buffer plus an open directory handle. Measured on a 5.5k-folder view
// that was 338 MB of a 352 MB heap; a 500k-file tree has tens of thousands of folders, which
// is gigabytes spent on an optimisation this package documents as latency-only. The kernel
// can watch a subtree with a single handle (bWatchSubtree), but fsnotify does not expose that
// (v1.10 gates it to its own tests), so this backend calls it directly.
//
// It speaks fsWatcher, so the event loop is unchanged: Add of the root opens the watch, Add of
// anything beneath it is already covered and does nothing, and WatchList reports the root for
// as long as its watch is alive - which is how recheckRoot learns that a deleted root took the
// watch with it.

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"unsafe"

	"github.com/fsnotify/fsnotify"
	"golang.org/x/sys/windows"

	"github.com/mfilippov/synosync/internal/fsutil"
)

// newFSWatcher returns the recursive Windows backend; recursive is true, so the Watcher
// registers the root alone.
func newFSWatcher() (fsWatcher, bool, error) {
	w, err := newSubtreeWatcher()
	return w, true, err
}

// subtreeBufferSize is the ReadDirectoryChangesW buffer. 64 KiB is the largest size that
// works on an SMB share; a burst that overflows it is reported as fsnotify.ErrEventOverflow,
// which the event loop answers with a prompt scan.
const subtreeBufferSize = 64 * 1024

// subtreeNotifyMask is what the watch reports: names (create, delete, rename) of files and
// directories, and content and metadata writes.
const subtreeNotifyMask = windows.FILE_NOTIFY_CHANGE_FILE_NAME | windows.FILE_NOTIFY_CHANGE_DIR_NAME |
	windows.FILE_NOTIFY_CHANGE_SIZE | windows.FILE_NOTIFY_CHANGE_LAST_WRITE | windows.FILE_NOTIFY_CHANGE_CREATION

type subtreeWatcher struct {
	events chan fsnotify.Event
	errors chan error
	quit   chan struct{}
	// bufSize is the ReadDirectoryChangesW buffer for each watch (subtreeBufferSize; tests
	// shrink it to force an overflow).
	bufSize int

	mu     sync.Mutex
	root   string
	h      windows.Handle // the root's directory handle; 0 while no watch is alive
	stop   windows.Handle // signalled by Close to interrupt the reader; lives as long as the watcher
	wg     sync.WaitGroup
	closed bool
}

func newSubtreeWatcher() (*subtreeWatcher, error) {
	stop, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		return nil, fmt.Errorf("watcher: create stop event: %w", err)
	}
	return &subtreeWatcher{
		events:  make(chan fsnotify.Event, 64),
		errors:  make(chan error, 8),
		quit:    make(chan struct{}),
		stop:    stop,
		bufSize: subtreeBufferSize,
	}, nil
}

func (s *subtreeWatcher) Events() <-chan fsnotify.Event { return s.events }
func (s *subtreeWatcher) Errors() <-chan error          { return s.errors }

// Add watches path and everything beneath it. The first call names the root; a later call for
// a path under that root is already covered, and re-adding the root after its watch died opens
// a fresh one.
func (s *subtreeWatcher) Add(path string) error {
	path = filepath.Clean(path)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("watcher: closed")
	}
	if s.root == "" {
		s.root = path
	}
	if path != s.root {
		if under(s.root, path) {
			return nil
		}
		return fmt.Errorf("watcher: %q is outside the watched root %q", path, s.root)
	}
	if s.h != 0 {
		return nil
	}
	p, err := windows.UTF16PtrFromString(fsutil.ExtendedPath(path))
	if err != nil {
		return err
	}
	h, err := windows.CreateFile(p, windows.FILE_LIST_DIRECTORY,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OVERLAPPED, 0)
	if err != nil {
		return &fsErr{op: "open", path: path, err: err}
	}
	done, err := windows.CreateEvent(nil, 1, 0, nil)
	if err != nil {
		_ = windows.CloseHandle(h)
		return fmt.Errorf("watcher: create read event: %w", err)
	}
	r := &watchRead{h: h, done: done, ov: new(windows.Overlapped), buf: make([]byte, s.bufSize)}
	if err := r.issue(); err != nil {
		_ = windows.CloseHandle(h)
		_ = windows.CloseHandle(done)
		return &fsErr{op: "read changes", path: path, err: err}
	}
	s.h = h
	s.wg.Add(1)
	go s.read(r, s.stop, path)
	return nil
}

// under reports whether p lies strictly beneath root.
func under(root, p string) bool {
	rel, err := filepath.Rel(root, p)
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}

func (s *subtreeWatcher) WatchList() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.h == 0 {
		return nil
	}
	return []string{s.root}
}

// Close stops the reader and closes both channels.
func (s *subtreeWatcher) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	close(s.quit)
	_ = windows.SetEvent(s.stop)
	s.wg.Wait()
	_ = windows.CloseHandle(s.stop)
	close(s.events)
	close(s.errors)
	return nil
}

// watchRead is a watch's outstanding ReadDirectoryChangesW. The OVERLAPPED and the buffer are
// the kernel's to write until the read completes or its cancellation lands, so they live here,
// referenced, for the whole of the watch.
type watchRead struct {
	h, done windows.Handle
	ov      *windows.Overlapped
	buf     []byte
}

// issue starts the next read.
//
// The FIRST one is issued by Add, before it returns. The kernel records changes for a handle
// only once a read has been requested on it, so a read left to the goroutine would miss
// whatever the caller does in between - measured: a file written right after Watch returned
// went unseen one run in six.
func (r *watchRead) issue() error {
	*r.ov = windows.Overlapped{HEvent: r.done}
	if err := windows.ResetEvent(r.done); err != nil {
		return err
	}
	return windows.ReadDirectoryChanges(r.h, &r.buf[0], uint32(len(r.buf)), true, subtreeNotifyMask, nil, r.ov, 0) // #nosec G115 -- the buffer is bufSize, at most 64 KiB
}

// read runs one watch until it is stopped or fails. A failure ends the watch: the root has
// gone (deleted, or pending delete behind this very handle) or become unreadable, and the
// handle is released so a delete can complete. The root drops out of WatchList, and
// recheckRoot re-adds it once a directory is there again.
func (s *subtreeWatcher) read(r *watchRead, stop windows.Handle, root string) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		if s.h == r.h {
			s.h = 0
		}
		s.mu.Unlock()
		_ = windows.CloseHandle(r.h)
		_ = windows.CloseHandle(r.done)
	}()
	for {
		ev, werr := windows.WaitForMultipleObjects([]windows.Handle{r.done, stop}, false, windows.INFINITE)
		if werr != nil || ev != windows.WAIT_OBJECT_0 {
			// Stopped (or the wait itself failed): abandon the read and wait for the
			// cancellation to land, so the kernel no longer writes into the buffer.
			_ = windows.CancelIoEx(r.h, r.ov)
			var n uint32
			_ = windows.GetOverlappedResult(r.h, r.ov, &n, true)
			return
		}
		var n uint32
		switch err := windows.GetOverlappedResult(r.h, r.ov, &n, false); {
		case errors.Is(err, windows.ERROR_NOTIFY_ENUM_DIR):
			s.sendErr(fsnotify.ErrEventOverflow)
		case err != nil:
			s.rootGone(root, err)
			return
		case n == 0:
			// The kernel had more changes than the buffer holds and kept none of them.
			s.sendErr(fsnotify.ErrEventOverflow)
		default:
			s.dispatch(root, r.buf[:n])
		}
		if err := r.issue(); err != nil {
			if errors.Is(err, windows.ERROR_NOTIFY_ENUM_DIR) {
				// The changes since the last read overflowed again before this one was even
				// queued. Report it and try once more; a second refusal ends the watch.
				s.sendErr(fsnotify.ErrEventOverflow)
				if err = r.issue(); err == nil {
					continue
				}
			}
			s.rootGone(root, err)
			return
		}
	}
}

// rootGone reports the end of the root's watch as a Remove of the root, which is how the
// fsnotify backend reports a deleted watched directory too.
func (s *subtreeWatcher) rootGone(root string, err error) {
	if !errors.Is(err, windows.ERROR_ACCESS_DENIED) && !errors.Is(err, windows.ERROR_OPERATION_ABORTED) {
		s.sendErr(&fsErr{op: "read changes", path: root, err: err})
	}
	s.send(fsnotify.Event{Name: root, Op: fsnotify.Remove})
}

// dispatch decodes a FILE_NOTIFY_INFORMATION chain into events.
func (s *subtreeWatcher) dispatch(root string, b []byte) {
	for off := 0; off+12 <= len(b); {
		next := int(leU32(b[off:]))
		action := leU32(b[off+4:])
		nameLen := int(leU32(b[off+8:]))
		start, end := off+12, off+12+nameLen
		if nameLen%2 != 0 || end > len(b) {
			s.sendErr(fsnotify.ErrEventOverflow) // a record that does not fit: trust none of it
			return
		}
		name := windows.UTF16ToString(unsafe.Slice((*uint16)(unsafe.Pointer(&b[start])), nameLen/2)) // #nosec G103 -- decoding the kernel's record in place
		if op, ok := subtreeOp(action); ok {
			s.send(fsnotify.Event{Name: filepath.Join(root, name), Op: op})
		}
		if next == 0 {
			return
		}
		off += next
	}
}

func leU32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// subtreeOp maps a FILE_ACTION_* to the fsnotify op the event loop reads. The new name of a
// rename is a Create, as fsnotify reports it: that is what makes the loop treat a directory
// moved into the tree like one made there.
func subtreeOp(action uint32) (fsnotify.Op, bool) {
	switch action {
	case windows.FILE_ACTION_ADDED, windows.FILE_ACTION_RENAMED_NEW_NAME:
		return fsnotify.Create, true
	case windows.FILE_ACTION_REMOVED:
		return fsnotify.Remove, true
	case windows.FILE_ACTION_MODIFIED:
		return fsnotify.Write, true
	case windows.FILE_ACTION_RENAMED_OLD_NAME:
		return fsnotify.Rename, true
	}
	return 0, false
}

func (s *subtreeWatcher) send(ev fsnotify.Event) {
	select {
	case s.events <- ev:
	case <-s.quit:
	}
}

func (s *subtreeWatcher) sendErr(err error) {
	select {
	case s.errors <- err:
	case <-s.quit:
	}
}

// fsErr names the operation and path a Win32 failure belongs to.
type fsErr struct {
	op, path string
	err      error
}

func (e *fsErr) Error() string { return fmt.Sprintf("watcher: %s %q: %v", e.op, e.path, e.err) }
func (e *fsErr) Unwrap() error { return e.err }
