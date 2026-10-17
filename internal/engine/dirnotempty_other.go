//go:build !windows

package engine

// Unix-like hosts report a non-empty directory as the POSIX ENOTEMPTY that dirNotEmpty already
// checks (Linux/macOS/BSD all use it for both rmdir(2) and rename(2) onto a populated
// directory), so there is no extra code to fold in. The variable exists to keep the shared
// classifier free of a GOOS branch, matching extraDiskFullErrors/extraNameTooLongErrors.
var extraDirNotEmptyErrors []error
