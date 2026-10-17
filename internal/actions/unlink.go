package actions

// This file implements the device-detach action used by the `unlink` subcommand.
// UnlinkConnection asks the server to forget this paired device, mirroring the
// "unlink_connection" call the official client makes; like the other structural
// actions it is a thin single-turn wrapper over Client.do, reporting only success
// or a typed error (errno mapping is shared with the rest of the action layer).

import "context"

// UnlinkConnection asks the server to detach this device's pairing
// (the "unlink_connection" action). It carries no parameters beyond the
// injected action/session; the turn is bound by ctx so cancellation or a deadline
// interrupts the blocking I/O. A non-zero errno is mapped to a typed error
// (e.g. auth.ErrSessionVanished for 8193) like every other action, and - being a
// mutation whose success gates clearing the local keychain/state - the reply must
// carry an errno (doMutate): a half-formed reply is ErrUnexpectedReply, not a
// confirmed detach.
func (c *Client) UnlinkConnection(ctx context.Context) error {
	return c.doMutate(ctx, "unlink_connection", nil)
}
