package main

// Shared plumbing for the interactive `pair` and `login` subcommands: the
// prompter abstraction (so tests inject canned password/OTP input instead of a
// real TTY), the small keychain interface the commands depend on, the client
// identity announced to the server, and the injection points (package vars) the
// tests override to point at a temp state database, a mocked keyring and a fake
// prompter while still driving everything through run().

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"time"

	"golang.org/x/term"

	"github.com/mfilippov/synosync/internal/actions"
	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/secrets"
	"github.com/mfilippov/synosync/internal/sproto"
	"github.com/mfilippov/synosync/internal/store"
)

// authTimeout bounds a single pair/login network exchange.
const authTimeout = 30 * time.Second

// stdinFile is the process standard input, passed to the production prompter so
// it can detect a terminal and disable echo. Tests override newPrompter and so
// never read it.
var stdinFile = os.Stdin

// prompter obtains interactive secrets. It is abstracted behind an interface so
// tests can feed canned input without a terminal; production uses
// terminalPrompter, which reads the password without echo.
type prompter interface {
	// ReadPassword reads a secret line without echoing it.
	ReadPassword(prompt string) (string, error)
	// ReadLine reads a plain, echoed line (e.g. a one-time password).
	ReadLine(prompt string) (string, error)
	// Confirm asks a yes/no question; anything but an explicit yes is no.
	Confirm(prompt string) (bool, error)
}

// secretStore is the subset of *secrets.Keyring the auth commands use. Narrowing
// it to an interface lets tests substitute the mocked keyring backend.
type secretStore interface {
	SetSession(server, user, token string) error
	Session(server, user string) (string, error)
	SetPin(server, fingerprint string) error
	Pin(server string) (string, error)
	// The `unlink` subcommand erases the stored credentials. Deleting an absent
	// item returns secrets.ErrNotFound, which unlink treats as already-clear.
	DeleteSession(server, user string) error
	DeletePin(server string) error
	DeletePassword(server, user string) error
}

// Injection points overridden by tests. In production they open the real state
// database (where the settings blob lives), the system keychain and a
// terminal-backed prompter.
var (
	// openKeyring returns the secret store used by pair/login.
	openKeyring = func() secretStore { return secrets.New() }
	// openConfigStore opens the bbolt state database holding the settings blob.
	// bbolt takes an exclusive file lock, so a running daemon makes this fail with
	// store.ErrLocked - pair/login then print a friendly hint to stop the daemon.
	openConfigStore = func() (*store.Store, error) {
		p, err := config.DefaultPaths()
		if err != nil {
			return nil, err
		}
		return store.Open(p.StateDB())
	}
	// newPrompter builds a prompter over the given input/output. Tests swap this
	// for one returning canned answers.
	newPrompter = func(in io.Reader, out io.Writer) prompter {
		return &terminalPrompter{in: in, out: out}
	}
)

// clientInfo is the static identity this build announces to the server.
//
// Type is "drive" and the version numbers match a real Synology Drive Client
// release: the server validates both. Announcing type "desktop" with version
// 3.4.1-25556 drew 20481 ("client version too low") on every action turn of a
// live DSM 7.2 / Drive Server 4.0.3 - auth itself passed, so the rejection only
// surfaced later, on the first action.
func clientInfo() auth.ClientInfo {
	return auth.ClientInfo{
		Platform: platformName(),
		Type:     "drive",
		Major:    4, Minor: 0, Mini: 2, Build: 17889,
	}
}

// clientIdentity is the identity block a live DSM validates on EVERY action turn,
// not only during auth: a turn that does not carry agent / client_version /
// client_type / server_id is answered with 20481 ("client version too low").
//
// The daemon has always sent it (engine.NewPoolTransportWithIdentity). pair and
// unlink built a bare actions.NewClient instead, so their turns went out without
// it. That is invisible against a lenient Drive Server and fatal against a
// current one: measured on DSM 7.2.2-72806 with Drive Server 4.0.3-27892, `pair`
// authenticated and then died with 20481 on QueryUserInfo - the one action it
// must complete - which reads as "client version rejected" and is not fixable by
// announcing a newer version, because the version was never the missing part.
func clientIdentity(ci auth.ClientInfo, deviceUUID, dsID string) actions.Identity {
	return actions.Identity{
		Agent:         ci.Agent(deviceUUID),
		ClientVersion: ci.ClientVersion(),
		ClientType:    ci.Type,
		DsID:          dsID,
	}
}

// platformName maps the Go OS name onto the platform label the server expects.
func platformName() string {
	switch runtime.GOOS {
	case "darwin":
		return "mac"
	case "windows":
		return "windows"
	default:
		return "linux"
	}
}

// serverAddr is the "host:port" dial target and keychain server key for cfg.
func serverAddr(cfg config.Config) string {
	return net.JoinHostPort(cfg.Server.Host, strconv.Itoa(cfg.Server.Port))
}

// loadConfigFromStore reads the settings blob from st, returning a fresh default
// when none has been written yet (the first-pairing case).
func loadConfigFromStore(st *store.Store) (config.Config, error) {
	cfg, err := st.GetConfig()
	if errors.Is(err, store.ErrNotFound) {
		return config.Default(), nil
	}
	if err != nil {
		return config.Config{}, err
	}
	// Intentionally not validated here: a stored blob can be legitimately
	// incomplete mid-pairing (e.g. server coordinates set but sync_root not yet),
	// and `pair`/`config` are the very commands that complete it - they surface a
	// friendly, field-specific message for a missing field rather than a blanket
	// "invalid config". The write paths (runPair, configSet) validate the whole
	// config before persisting, so a *completed* blob is always valid by
	// construction.
	return cfg, nil
}

// reportConfigLoadError prints a config-load failure to stderr, appending a
// pointer to `synosync unlink` as the recovery path when the stored settings
// blob is corrupt beyond repair (store.ErrCorruptRecord). The strict subcommands
// (pair/login/config/daemon) all dead-end on the same undecodable blob, so the
// hint routes the user to unlink - which tolerates a corrupt blob and clears
// local state so a fresh `pair` can follow.
func reportConfigLoadError(stderr io.Writer, err error) {
	fmt.Fprintf(stderr, "synosync: load config: %s\n", userErr(err))
	if errors.Is(err, store.ErrCorruptRecord) {
		fmt.Fprintln(stderr, "synosync: the stored settings are corrupt; run `synosync unlink` to clear local state, then `synosync pair` to re-pair")
	}
}

// lockedStoreMessage is the friendly hint printed when the state database is
// already held by another synosync process (typically the running daemon).
// It names the command that does the stopping. Telling a user to "stop it" while giving no way
// to is the gap `service stop` closes: once the daemon runs under a service manager, stopping it
// meant knowing that platform's own invocation, or uninstalling and reinstalling around a
// settings change.
const lockedStoreMessage = "synosync: the state database is in use by another synosync process (the daemon?); " +
	"stop it and retry - `synosync service stop` if it runs as a service, or Ctrl-C if you started it yourself"

// authContext returns a context bounded by authTimeout, plus its cancel func.
func authContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), authTimeout)
}

// promptCredentials prompts for the password (no echo) and an optional OTP. The
// returned Credentials carry user; OTP is empty when the user just pressed Enter.
func promptCredentials(p prompter, user string) (auth.Credentials, error) {
	password, err := p.ReadPassword(fmt.Sprintf("Password for %s: ", user))
	if err != nil {
		return auth.Credentials{}, fmt.Errorf("read password: %w", err)
	}
	otp, err := p.ReadLine("One-time password (press Enter if none): ")
	if err != nil {
		return auth.Credentials{}, fmt.Errorf("read otp: %w", err)
	}
	return auth.Credentials{User: user, Password: password, OTP: otp}, nil
}

// pairWithOTPRetry runs mode-C Pair over conn and, if the server demands an OTP
// that was not supplied, prompts once for it and retries on the same connection.
// Each Pair turn gets a fresh authTimeout-bounded context so the time the user
// spends typing the OTP does not count against the network deadline.
func pairWithOTPRetry(conn *sproto.Conn, client auth.ClientInfo, deviceUUID, dsID string, creds auth.Credentials, p prompter) (string, error) {
	session, err := pairOnce(conn, client, deviceUUID, dsID, creds)
	if errors.Is(err, auth.ErrOTPRequired) && creds.OTP == "" {
		otp, rerr := p.ReadLine("One-time password: ")
		if rerr != nil {
			return "", fmt.Errorf("read otp: %w", rerr)
		}
		creds.OTP = otp
		session, err = pairOnce(conn, client, deviceUUID, dsID, creds)
	}
	return session, err
}

// pairOnce performs a single mode-C Pair turn under a fresh bounded context.
func pairOnce(conn *sproto.Conn, client auth.ClientInfo, deviceUUID, dsID string, creds auth.Credentials) (string, error) {
	ctx, cancel := authContext()
	defer cancel()
	return auth.Pair(ctx, conn, client, deviceUUID, dsID, creds)
}

// terminalPrompter reads interactive input from a terminal, hiding the password.
// When the input is not a terminal (e.g. piped in), it falls back to reading a
// line so non-interactive invocation still works.
type terminalPrompter struct {
	in  io.Reader
	out io.Writer
}

// ReadPassword reads a line without echoing it when in is a terminal.
func (p *terminalPrompter) ReadPassword(prompt string) (string, error) {
	fmt.Fprint(p.out, prompt)
	if f, ok := p.in.(*os.File); ok && term.IsTerminal(int(f.Fd())) {
		b, err := term.ReadPassword(int(f.Fd()))
		fmt.Fprintln(p.out)
		if err != nil {
			return "", err
		}
		// The raw read may have stopped one byte SHORT of the line ending (see
		// rawPasswordLeavesNewline). Push that orphan back onto the stream so the next
		// prompted read steps over it instead of mistaking it for a complete empty
		// line - which is exactly how the OTP prompt came to return "" without ever
		// waiting for input. Wrapping rather than consuming it here keeps the drop
		// lazy: nothing blocks waiting for a byte that a console never left behind.
		if rawPasswordLeavesNewline {
			p.in = &dropLeadingNewline{r: p.in}
		}
		return string(b), nil
	}
	return readLine(p.in)
}

// ReadLine prints the prompt and reads a single echoed line.
func (p *terminalPrompter) ReadLine(prompt string) (string, error) {
	fmt.Fprint(p.out, prompt)
	return readLine(p.in)
}

// Confirm prints the prompt and reports whether the answer was yes.
func (p *terminalPrompter) Confirm(prompt string) (bool, error) {
	s, err := p.ReadLine(prompt)
	if err != nil {
		return false, err
	}
	return isAffirmative(s), nil
}

// maxReadLineBytes bounds one prompted line. Every value read this way is a
// hostname, a username, a password or a 6-digit OTP - a few KiB is orders of
// magnitude more than any of them needs. Without the bound a redirected stdin that
// never emits a newline (a large file, /dev/zero, a pipe from a runaway process)
// grows the accumulator until the process is OOM-killed, so the cap turns an
// unbounded read into a plain error.
const maxReadLineBytes = 4096

// errLineTooLong reports input exceeding maxReadLineBytes before any newline.
var errLineTooLong = errors.New("input line too long")

// readLine reads one line from r, byte by byte so it never over-reads past the
// newline - important when the same terminal also serves a raw password read. It
// reads at most maxReadLineBytes of line content before giving up.
func readLine(r io.Reader) (string, error) {
	var sb []byte
	var buf [1]byte
	for {
		n, err := r.Read(buf[:])
		if n > 0 {
			switch buf[0] {
			case '\n':
				return string(sb), nil
			case '\r':
				// ignore
			default:
				if len(sb) >= maxReadLineBytes {
					return "", errLineTooLong
				}
				sb = append(sb, buf[0])
			}
		}
		if err != nil {
			if err == io.EOF {
				return string(sb), nil
			}
			return "", err
		}
	}
}

// dropLeadingNewline wraps a reader so a single leading '\n' - the orphan byte a raw
// terminal password read can leave behind (see rawPasswordLeavesNewline) - is skipped
// before any other byte reaches the caller.
//
// It is deliberately NOT a bufio.Reader. Buffering would read ahead past the newline
// and steal bytes belonging to a later raw console read, which is the very thing
// readLine is written byte-at-a-time to avoid. This reads exactly ONE byte, and
// swallows it only when it really is the orphan: any other first byte is handed
// through intact, so a console that leaves nothing behind loses no input.
type dropLeadingNewline struct {
	r    io.Reader
	done bool
}

// Read delivers the wrapped stream, minus one leading newline.
func (d *dropLeadingNewline) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for !d.done {
		var b [1]byte
		n, err := d.r.Read(b[:])
		if n > 0 {
			d.done = true
			if b[0] != '\n' {
				p[0] = b[0]
				return 1, nil
			}
			break
		}
		if err != nil {
			return 0, err
		}
	}
	return d.r.Read(p)
}
