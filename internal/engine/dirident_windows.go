//go:build windows

package engine

import (
	"fmt"
	"io/fs"

	"golang.org/x/sys/windows"

	"github.com/mfilippov/synosync/internal/fsutil"
)

// dirIdentity reports no identity on Windows: the FileInfo a directory walk yields comes from
// the directory listing and carries no file index, and reading one means opening the path. The
// guard therefore asks osDirIdentity lazily, only for the few directories it needs (emptieddir.go).
func dirIdentity(fs.FileInfo) (fileIdent, bool) { return fileIdent{}, false }

// osDirIdentity returns a directory's (volume serial, file index) - the Windows analogue of
// (st_dev, st_ino), and the pair os.SameFile compares. The handle is opened for metadata only,
// with full sharing and FILE_FLAG_BACKUP_SEMANTICS (required for a directory), and
// FILE_FLAG_OPEN_REPARSE_POINT so a reparse point is identified as itself rather than followed;
// the path goes through fsutil.ExtendedPath like every direct Win32 call.
func osDirIdentity(path string) (fileIdent, error) {
	p, err := windows.UTF16PtrFromString(fsutil.ExtendedPath(path))
	if err != nil {
		return fileIdent{}, fmt.Errorf("engine: identity path %q: %w", path, err)
	}
	h, err := windows.CreateFile(p, 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return fileIdent{}, fmt.Errorf("engine: open for identity %q: %w", path, err)
	}
	// Metadata-only handle: a close failure has nothing to report or retry.
	defer func() { _ = windows.CloseHandle(h) }()
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &info); err != nil {
		return fileIdent{}, fmt.Errorf("engine: file information %q: %w", path, err)
	}
	return fileIdent{
		dev: uint64(info.VolumeSerialNumber),
		ino: uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow),
	}, nil
}
