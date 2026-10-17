//go:build !windows

package watcher

import "github.com/fsnotify/fsnotify"

// newFSWatcher returns fsnotify's watcher, which watches one directory per Add; recursive is
// false, so the Watcher registers every directory itself. inotify, kqueue and FEN have no
// subtree watch to offer instead (backend_windows.go has the one platform that does).
func newFSWatcher() (fsWatcher, bool, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, false, err
	}
	return realFSWatcher{fsw}, false, nil
}

// realFSWatcher adapts *fsnotify.Watcher (whose Events/Errors are struct fields)
// to the fsWatcher interface.
type realFSWatcher struct{ *fsnotify.Watcher }

func (r realFSWatcher) Events() <-chan fsnotify.Event { return r.Watcher.Events }
func (r realFSWatcher) Errors() <-chan error          { return r.Watcher.Errors }
