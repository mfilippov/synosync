//go:build !windows

package engine

// Unix-like hosts report an over-long path component as the POSIX ENAMETOOLONG that
// nameTooLong already checks, so there is nothing to add here.
var extraNameTooLongErrors []error
