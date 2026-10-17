package ipc

import "fmt"

// The IPC protocol version, and what a mismatch means.
//
// The CLI and the daemon ship in one binary, which made a version look unnecessary - and
// it is, right up to the moment the two are not the same binary. That happens routinely:
// a package upgrade replaces the executable while the old daemon is still running, so
// every CLI command until the next restart is a new client talking to an old server. It
// also happens the other way, when someone runs an older synosync out of a build
// directory against a running service.
//
// Until now nothing said so. An added reply field is harmless (an older reader ignores
// it, which is why the field docs in types.go call such additions safe), but a method the
// two ends disagree about surfaced as `unknown method "drops"`, and a field whose MEANING
// changed would not surface at all: the CLI would print confident nonsense. Both ends now
// state their version, so a mismatch is diagnosable rather than mysterious.
//
// Honesty about what this can and cannot do. A client that predates the field sends no
// protocol, decodes as 0, and the server treats it as pre-versioning and answers normally
// - nothing has changed meaning, so refusing it would break a working setup for nothing.
// And a pre-versioning CLI ignores the version in the reply, so it cannot warn about
// anything. The value is forward: from here on, a change that is NOT backward compatible
// bumps Protocol, and both ends can say exactly what is wrong and what to do about it.
const Protocol = 1

// The field is `protocol,omitempty` in both envelopes, so a pre-versioning peer's absent
// field and this build's own value are distinguishable at the receiver (0 versus ≥1) and
// the wire form stays compatible in both directions.

// ProtocolMismatchError reports that the peer speaks a protocol version this build does
// not understand. It carries both numbers so the message can name them, and it is a typed
// error so a caller can tell it from a transport failure.
type ProtocolMismatchError struct {
	// Peer is the version the other end announced.
	Peer int
	// Local is the version this build speaks (Protocol).
	Local int
	// Role names which end the peer is, so the message can say which one to upgrade.
	Role string
}

func (e *ProtocolMismatchError) Error() string {
	return fmt.Sprintf("ipc: the %s speaks a newer control protocol (v%d) than this build (v%d); "+
		"the two are different versions of synosync - restart the daemon so it runs the installed binary",
		e.Role, e.Peer, e.Local)
}

// clientProtocolCheck rejects a reply from a daemon speaking a protocol this build does
// not know. It runs before the reply is interpreted - including before its error field is
// read - because a newer daemon may mean anything by the fields it sent, including ones
// this build recognises by name, and its error text is no more informative than the
// mismatch itself.
func clientProtocolCheck(resp response) error {
	if resp.Protocol > Protocol {
		return &ProtocolMismatchError{Peer: resp.Protocol, Local: Protocol, Role: "daemon"}
	}
	return nil
}
