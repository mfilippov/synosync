package engine

// nameTooLong classifies the host's "this name can never exist here" failure so applyOp
// can dead-letter such an op instead of retrying it forever.
//
// A path component past the filesystem's per-name limit is a PERMANENT condition: no
// amount of retrying shortens the name, and the local side never fixes it on its own
// under the never-delete policy. Left transient it behaves exactly like the EEXIST and
// ENOTDIR shapes closed earlier - the op retries every drain, escalates to Stuck, and via
// blockedKeys wedges every op whose path overlaps it. Dead-lettering records a durable
// trace, frees the chain, and surfaces in the Dropped count; nothing local is touched, so
// the never-lose doctrine is unaffected (the server copy simply is not materialised).
//
// The primary defence is not to generate such names at all (stagingTempPattern,
// conflictCopyName); this classifier is the backstop for a name the SERVER supplies that
// the local filesystem cannot hold - a component within DSM's limit but past the local
// one, or (on Windows) one whose spelling is rejected outright.

import (
	"errors"
	"syscall"
)

// nameTooLong reports whether err is the host's rejection of an unusable path component.
func nameTooLong(err error) bool {
	if errors.Is(err, syscall.ENAMETOOLONG) {
		return true
	}
	for _, target := range extraNameTooLongErrors {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
