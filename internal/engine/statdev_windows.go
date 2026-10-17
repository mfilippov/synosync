//go:build windows

package engine

import (
	"fmt"

	"golang.org/x/sys/windows"

	"github.com/mfilippov/synosync/internal/fsutil"
)

// osDeviceID returns the volume serial number of the volume the path resides on - the Windows
// analogue of the Unix st_dev the empty-scan debounce uses to detect a mount swap underneath a
// view root. A directory handle requires FILE_FLAG_BACKUP_SEMANTICS; the handle is
// opened read-only with full sharing so it never blocks other access, and closed immediately.
//
// The path goes through fsutil.ExtendedPath because a direct Win32 call gets none of the
// stdlib's MAX_PATH fixup: a view root nested deeper than 260 characters
// - which the os package itself creates without complaint - would otherwise fail to open, and
// the empty-scan debounce would lose its mount-swap guard on that view for good.
func osDeviceID(path string) (uint64, error) {
	p, err := windows.UTF16PtrFromString(fsutil.ExtendedPath(path))
	if err != nil {
		return 0, fmt.Errorf("engine: device id path %q: %w", path, err)
	}
	h, err := windows.CreateFile(
		p,
		0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return 0, fmt.Errorf("engine: open for device id %q: %w", path, err)
	}
	// The handle is opened read-only for metadata, so a close failure has nothing to
	// report and nothing to retry; discard it explicitly rather than leaving an
	// unchecked return.
	defer func() { _ = windows.CloseHandle(h) }()
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil {
		return 0, fmt.Errorf("engine: file information %q: %w", path, err)
	}
	return uint64(info.VolumeSerialNumber), nil
}
