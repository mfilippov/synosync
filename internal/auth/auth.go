// Package auth implements the two authentication flows synosync uses over an
// already-encrypted sproto channel: mode C (interactive pairing with password and
// an optional one-time password) and mode A (reuse of a previously issued session
// token). It also builds the client identity announced to the server (the agent
// PObject and the client_version string), generates the stable per-install
// device_uuid, and maps the server's numeric auth result codes to typed errors.
//
// The flows run on a *sproto.Conn that the transport package has already brought
// up through encrypt_channel and the pinned TLS upgrade; this package only frames
// the auth/action requests and interprets the replies. No secret is stored here -
// the returned session token is handed back to the caller to persist in the
// keychain.
package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"

	"github.com/mfilippov/synosync/internal/netctx"
	"github.com/mfilippov/synosync/internal/sproto"
)

// maxAuthReplyInlineBinary bounds how many bytes of an inline Binary an auth
// reply may carry before it is treated as hostile. Auth replies (the pairing
// session token, the reuse errno) legitimately carry no Binary at all, so a
// server returning a huge inline Binary here is hostile or corrupt: rather than
// let an attacker-controlled 64-bit length drive an unbounded allocation, its
// bytes are drained and the reply rejected - the shared
// sproto.Conn.ReadCappedResponse core. Mirrors actions.maxReplyInlineBinary.
const maxAuthReplyInlineBinary = 1 << 20 // 1 MiB

// Server auth result codes carried in the reply's "errno" field. Zero means
// success; the handful below are the ones the flows react to specifically.
const (
	// CodeOK is the success result.
	CodeOK = 0
	// CodeUserNotFound (4096) means the account name is unknown to the server.
	// Measured on a live DSM 7.x / Drive Server 4.0.3, reason "User not found(auth)".
	CodeUserNotFound = 4096
	// CodeInvalidPassword (4100) means the account exists but the password was
	// rejected. Measured on the same stand, reason "Invalid password".
	//
	// The two are DISTINCT codes, measured side by side: a nonexistent account
	// draws 4096, a real account with a wrong password draws 4100. Neither is
	// documented anywhere, so the nested block's reason string is the whole
	// evidence - see errorForCode. Do not collapse them into one "bad credentials"
	// verdict on the assumption that a server would not disclose which was wrong;
	// this one does, and the distinction is what tells an operator whether to fix
	// the account name or the password.
	CodeInvalidPassword = 4100
	// CodeOTPRequired (4104) means the account has 2FA enabled and the request
	// must be retried with a one-time password.
	CodeOTPRequired = 4104
	// CodeOTPInvalid (4105) means the supplied one-time password was wrong or has
	// already been consumed.
	CodeOTPInvalid = 4105
	// CodeSessionVanished (8193) means the presented session token is no longer
	// valid server-side, so a fresh mode-C pairing is required.
	CodeSessionVanished = 8193
	// CodeClientVersion (20481) means the server rejected the client_version.
	CodeClientVersion = 20481
)

// Typed sentinel errors for the result codes above. Errors returned by Pair and
// Reuse wrap the matching sentinel, so callers can dispatch with errors.Is -
// e.g. errors.Is(err, ErrOTPRequired) to prompt for a code, or
// errors.Is(err, ErrSessionVanished) to fall back to pairing.
var (
	// ErrUserNotFound reports that the server does not know the account name (4096).
	ErrUserNotFound = errors.New("auth: no such account on the server")
	// ErrInvalidPassword reports that the account exists but the password was
	// rejected (4100).
	ErrInvalidPassword = errors.New("auth: invalid password")
	// ErrOTPRequired reports that the account needs a one-time password (4104).
	ErrOTPRequired = errors.New("auth: one-time password required")
	// ErrOTPInvalid reports a wrong or already-used one-time password (4105).
	ErrOTPInvalid = errors.New("auth: invalid or already-used one-time password")
	// ErrSessionVanished reports that the session token is no longer valid and
	// mode-C pairing must be repeated (8193).
	ErrSessionVanished = errors.New("auth: session vanished; re-pairing required")
	// ErrClientVersion reports that the server rejected the client_version (20481).
	ErrClientVersion = errors.New("auth: client version rejected by server")
	// ErrNoSession reports a success reply that carried no session token.
	ErrNoSession = errors.New("auth: reply contained no session")
)

// ClientInfo is the static identity of this client build, announced to the
// server in the agent PObject and the client_version string.
type ClientInfo struct {
	// Platform is the OS family, e.g. "linux", "windows" or "mac".
	Platform string
	// Type is the client category, e.g. "desktop".
	Type string
	// Major, Mini, Minor and Build form the version reported as
	// "MAJOR.MINOR.MINI-BUILD" - see ClientVersion, which is the authority on the
	// order. The field order in this declaration is NOT the wire order, and the two
	// middle components are easy to transpose: a value the server cannot parse is
	// refused with 20481, so a "fix" that makes the method match a mis-stated order
	// here would break every action turn.
	Major, Mini, Minor, Build int
}

// ClientVersion formats the version strictly as "MAJOR.MINOR.MINI-BUILD" - the
// order the real client uses: agent.version {major:4, minor:0, mini:2, build:17889}
// is announced as the string "4.0.2-17889". The server rejects a malformed value
// with 20481 ("client version too low", which actually means "cannot parse this"),
// so it is always produced from the numeric fields rather than hand-written.
func (ci ClientInfo) ClientVersion() string {
	return fmt.Sprintf("%d.%d.%d-%d", ci.Major, ci.Minor, ci.Mini, ci.Build)
}

// AgentVersion builds the agent's version PObject. The server expects a MAP of
// numeric fields here - {build, major, mini, minor} - not the dotted
// client_version string. A capture of the real Linux Drive Client 4.2.0-17889
// encodes it as a nested map, and sending a String instead made the server drop
// the connection without a reply during encrypt_channel. The client_version
// string is a separate, string-valued field on the auth request.
func (ci ClientInfo) AgentVersion() *sproto.Map {
	return sproto.NewMap().
		Set("build", sproto.Int(int64(ci.Build))).
		Set("major", sproto.Int(int64(ci.Major))).
		Set("mini", sproto.Int(int64(ci.Mini))).
		Set("minor", sproto.Int(int64(ci.Minor)))
}

// Agent builds the agent PObject announced to the server: platform, type,
// version (a nested numeric map) and the per-install device_uuid.
func (ci ClientInfo) Agent(deviceUUID string) *sproto.Map {
	return sproto.NewMap().
		Set("platform", sproto.String(ci.Platform)).
		Set("type", sproto.String(ci.Type)).
		Set("version", ci.AgentVersion()).
		Set("device_uuid", sproto.String(deviceUUID))
}

// Credentials carries the interactive secrets for a mode-C pairing. OTP is empty
// unless the account has 2FA enabled (or the server has asked for it via 4104).
type Credentials struct {
	// User is the account login name.
	User string
	// Password is the account password.
	Password string
	// OTP is the optional one-time (2FA) code.
	OTP string
}

// NewDeviceUUID generates a fresh, stable per-install identifier: 16 random bytes
// from crypto/rand rendered as 32 lowercase-hex characters. It is generated once
// at first pairing and then persisted (config.DeviceUUID).
func NewDeviceUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("auth: generate device_uuid: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// Pair runs mode C over conn: it sends an auth request carrying the credentials
// and client identity, and returns the issued session token (a 32-hex string) on
// success. A 2FA-enabled account without an OTP yields ErrOTPRequired; a bad OTP
// yields ErrOTPInvalid; a rejected client_version yields ErrClientVersion.
func Pair(ctx context.Context, conn *sproto.Conn, client ClientInfo, deviceUUID, dsID string, creds Credentials) (string, error) {
	stop := netctx.Watch(ctx, conn.NetConn())
	defer stop()
	req := authRequest(client, deviceUUID, dsID, creds)
	if err := conn.WriteRequest(sproto.CmdAuth, req); err != nil {
		return "", fmt.Errorf("auth: write auth request: %w", netctx.Err(ctx, err))
	}
	resp, oversize, err := readSkippingKeepAlive(conn)
	if err != nil {
		return "", fmt.Errorf("auth: read auth reply: %w", netctx.Err(ctx, err))
	}
	if oversize {
		return "", fmt.Errorf("auth: auth reply has oversized inline binary (>%d bytes)", maxAuthReplyInlineBinary)
	}
	return parseAuthReply(resp)
}

// Reuse runs mode A over conn: it presents an existing session token in a first
// action request (query_user_info) to confirm the token is still valid server-
// side. It returns nil when the session is accepted, ErrSessionVanished (8193)
// when the token is stale and pairing must be repeated, or another typed error
// for any other non-zero result.
//
// Success evidence is errno OR a present "view_list" array (mirroring
// actions.QueryUserInfo's errno-or-view_list rule): reuse presents exactly the query_user_info action, so a DSM whose
// successful reply carries view_list without an errno already passes
// actions.QueryUserInfo, and requiring errno here too would deterministically
// (but non-terminally) fail such a reuse, wedging the daemon in an endless
// backoff-reconnect (status stuck "connecting..."). A bare {} - neither field -
// and a present-but-wrong-typed view_list stay rejected.
// The request carries the same fields the real client presents on a mode-A
// reconnect - agent, get_view and server_id alongside the session - rather than
// the bare action+session pair an earlier revision sent.
func Reuse(ctx context.Context, conn *sproto.Conn, session string, agent sproto.Value, dsID string) error {
	stop := netctx.Watch(ctx, conn.NetConn())
	defer stop()
	req := sproto.NewMap().
		Set("action", sproto.String("query_user_info")).
		Set("session", sproto.String(session))
	if agent != nil {
		req.Set("agent", agent)
	}
	req.Set("get_view", sproto.Int(1)).
		Set("server_id", sproto.NewMap().Set("restore_id", sproto.String(dsID)))
	if err := conn.WriteRequest(sproto.CmdAction, req); err != nil {
		return fmt.Errorf("auth: write reuse request: %w", netctx.Err(ctx, err))
	}
	resp, oversize, err := readSkippingKeepAlive(conn)
	if err != nil {
		return fmt.Errorf("auth: read reuse reply: %w", netctx.Err(ctx, err))
	}
	if oversize {
		return fmt.Errorf("auth: reuse reply has oversized inline binary (>%d bytes)", maxAuthReplyInlineBinary)
	}
	m, ok := resp.(*sproto.Map)
	if !ok {
		return fmt.Errorf("auth: reuse reply is %T, want map", resp)
	}
	code, reason, present, err := errno(m)
	if err != nil {
		return err
	}
	if present {
		if code != CodeOK {
			return errorForCode(code, reason)
		}
		return nil
	}
	// No errno. Accept a present view_list array as positive success evidence
	// (see the doc comment): the reuse verb IS query_user_info, whose successful
	// reply legitimately carries view_list without an errno. A present-but-wrong-
	// typed view_list, and a bare {} carrying neither field, stay rejected - an
	// empty or half-formed reply must not confirm the token as valid, mirroring
	// parseAuthReply's strictness about its session field.
	if v, ok := m.Get("view_list"); ok {
		if _, ok := v.(sproto.Array); ok {
			return nil
		}
		return fmt.Errorf("auth: reuse reply view_list is %T, want array", v)
	}
	return errors.New("auth: reuse reply carries neither errno nor view_list")
}

// authRequest builds the mode-C auth PObject. The OTP field is included only when
// a code is supplied, so a 2FA-less account's request stays minimal.
//
// The shape is the one a live DSM accepts. Three fields were previously absent or
// misnamed and each was fatal on its own:
//   - "action": "auth" - the server dispatches on this string, not on the header's
//     cmd byte. Without it the request is discarded and the connection closed
//     with no reply at all.
//   - "otp" - the one-time code's key. It was sent as "otp_code", so a 2FA
//     account's code never reached the server and the turn failed as if no OTP
//     had been supplied.
//   - "server_id": {"restore_id": ds_id} - identifies which DiskStation the
//     credentials are for.
//
// client_version stays a STRING ("MAJOR.MINI.MINOR-BUILD") even though
// agent.version is a map: sending it as a map draws 20481 "client version too
// low" regardless of the values inside.
func authRequest(client ClientInfo, deviceUUID, dsID string, creds Credentials) *sproto.Map {
	m := sproto.NewMap().
		Set("action", sproto.String("auth")).
		Set("username", sproto.String(creds.User)).
		Set("password", sproto.String(creds.Password)).
		Set("device_uuid", sproto.String(deviceUUID)).
		Set("agent", client.Agent(deviceUUID)).
		Set("client", sproto.String(hostname())).
		Set("client_type", sproto.String(client.Type)).
		Set("client_version", sproto.String(client.ClientVersion())).
		Set("dry_run", sproto.Int(0)).
		Set("renew_session", sproto.String("")).
		Set("server_id", sproto.NewMap().Set("restore_id", sproto.String(dsID)))
	if creds.OTP != "" {
		m.Set("otp", sproto.String(creds.OTP))
	}
	return m
}

// parseAuthReply extracts the session token from a successful auth reply, or maps
// a non-zero result code to a typed error.
func parseAuthReply(v sproto.Value) (string, error) {
	m, ok := v.(*sproto.Map)
	if !ok {
		return "", fmt.Errorf("auth: reply is %T, want map", v)
	}
	code, reason, present, err := errno(m)
	if err != nil {
		return "", err
	}
	if present && code != CodeOK {
		return "", errorForCode(code, reason)
	}
	session, ok := m.String("session")
	if !ok || session == "" {
		return "", ErrNoSession
	}
	return session, nil
}

// errorForCode maps a non-zero server result code to a typed error, wrapping the
// matching sentinel so callers can match it with errors.Is. Unknown codes get a
// generic error that still reports the numeric value.
//
// reason is the server's own explanation from the nested error block, passed
// through verbatim when it is non-empty. It matters most for an UNMAPPED code,
// which is otherwise a bare number an operator cannot act on and this client
// cannot classify: the code space is measured, not documented, so the server's
// own words are the only evidence available about a verdict we have never seen.
// Guessing a meaning instead is how the invented resume-token code 40976 came
// about. It is appended to the mapped sentinels too - a live DSM has been seen
// returning a known code for an unexpected reason.
func errorForCode(code int64, reason string) error {
	var err error
	switch code {
	case CodeUserNotFound:
		err = fmt.Errorf("%w (code %d)", ErrUserNotFound, code)
	case CodeInvalidPassword:
		err = fmt.Errorf("%w (code %d)", ErrInvalidPassword, code)
	case CodeOTPRequired:
		err = fmt.Errorf("%w (code %d)", ErrOTPRequired, code)
	case CodeOTPInvalid:
		err = fmt.Errorf("%w (code %d)", ErrOTPInvalid, code)
	case CodeSessionVanished:
		err = fmt.Errorf("%w (code %d)", ErrSessionVanished, code)
	case CodeClientVersion:
		err = fmt.Errorf("%w (code %d)", ErrClientVersion, code)
	default:
		err = fmt.Errorf("auth: server returned error code %d", code)
	}
	if reason == "" {
		return err
	}
	return fmt.Errorf("%w (server reason: %q)", err, reason)
}

// hostname returns the machine's hostname for the "client" field, falling back to
// a fixed label when it cannot be determined so the request always carries a value.
func hostname() string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return "synosync"
}

// errno extracts the server result code from a reply. It returns (code, reason,
// true, nil) when the reply carries a result verdict, (0, "", false, nil) when it
// carries none, and a protocol error when a verdict is present but malformed - so
// a wrong-typed code is never mistaken for success.
//
// It mirrors actions.errno: a live DSM reports failure as a nested
// {"error": {"code": N, "reason": "..."}} map and success by omitting it while
// marking the frame "type": "response". There is no flat "errno" key in the real
// protocol; it is still accepted first so the in-memory fakes keep working.
//
// reason is the server's own explanation of the verdict, empty when the reply
// carries none (the flat-errno and type:"response" forms never do). It is
// reported rather than dropped because an unmapped code is otherwise unactionable
// - see errorForCode.
func errno(m *sproto.Map) (code int64, reason string, present bool, err error) {
	if v, ok := m.Get("errno"); ok {
		iv, ok := v.(sproto.Int)
		if !ok {
			return 0, "", false, fmt.Errorf("auth: errno is %T, want int", v)
		}
		return int64(iv), "", true, nil
	}
	if v, ok := m.Get("error"); ok {
		em, ok := v.(*sproto.Map)
		if !ok {
			return 0, "", false, fmt.Errorf("auth: error is %T, want map", v)
		}
		code, ok := em.Int("code")
		if !ok {
			return 0, "", false, errors.New("auth: error map carries no integer code")
		}
		// A missing or wrong-typed reason is not an error: it is optional
		// diagnostic text, and refusing the reply over it would turn a decodable
		// verdict into an undecodable one.
		reason, _ := em.String("reason")
		return code, reason, true, nil
	}
	if t, ok := m.String("type"); ok && t == "response" {
		return CodeOK, "", true, nil
	}
	return 0, "", false, nil
}

// maxSkippedKeepAlives bounds how many unsolicited frames one reply read steps over.
const maxSkippedKeepAlives = 8

// readSkippingKeepAlive reads one reply, stepping over any unsolicited keep-alive
// frame the server interleaves ahead of it. The server emits
// {"@proto": {...}, "type": "keep_alive"} on its own cadence; it answers nothing and
// expects no response, but a read that does not skip it takes it as the reply - it
// decodes cleanly and carries no session and no result verdict, failing an auth turn
// that in fact succeeded. Mirrors actions.isKeepAlive.
func readSkippingKeepAlive(conn *sproto.Conn) (sproto.Value, bool, error) {
	resp, oversize, err := conn.ReadCappedResponse(maxAuthReplyInlineBinary)
	for i := 0; err == nil && !oversize && isKeepAlive(resp) && i < maxSkippedKeepAlives; i++ {
		resp, oversize, err = conn.ReadCappedResponse(maxAuthReplyInlineBinary)
	}
	return resp, oversize, err
}

// isKeepAlive reports whether a decoded frame is the server's unsolicited keep-alive.
func isKeepAlive(v sproto.Value) bool {
	m, ok := v.(*sproto.Map)
	if !ok {
		return false
	}
	t, ok := m.String("type")
	return ok && t == "keep_alive"
}
