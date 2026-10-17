package main

// Shared plumbing for the CLI control verbs that talk to the running daemon over
// the local IPC socket (status, sync, pause, resume, conflicts): a single dial
// helper that resolves the socket path, connects, and renders the friendly
// "daemon is not running" hint so every subcommand reports a missing daemon the
// same way.

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/mfilippov/synosync/internal/config"
	"github.com/mfilippov/synosync/internal/ipc"
	"github.com/mfilippov/synosync/internal/store"
)

// ipcDial dials the daemon's IPC socket. An injection var so tests can fail the
// dial with per-OS error shapes (e.g. the Windows named-pipe busy timeout) that
// cannot be produced on the host running the test.
var ipcDial = ipc.Dial

// daemonBusy reports whether a dial error means the daemon is alive but too
// busy to accept the connection right now. It delegates to ipc.IsDaemonBusy -
// only the Windows named-pipe connect can signal busy (winio retries
// ERROR_PIPE_BUSY until its timeout, so the timeout is positive evidence a live
// daemon holds the pipe); on Unix it is always false. An injection var so the
// busy branch is exercisable in tests on any host.
var daemonBusy = ipc.IsDaemonBusy

// dialDaemon resolves the daemon socket path and dials it, returning a connected
// IPC client. On failure it writes a user-facing message to stderr - a friendly
// "daemon is not running" hint when the socket is missing or refuses the
// connection, a "busy" hint when the daemon is alive but slow to accept (the
// Windows named-pipe connect timeout), otherwise the raw dial error - and
// returns a non-nil error so the caller can exit non-zero. The caller owns
// Close on success.
func dialDaemon(stderr io.Writer) (*ipc.Client, error) {
	paths, err := resolveDaemonPaths()
	if err != nil {
		fmt.Fprintf(stderr, "synosync: %s\n", userErr(err))
		return nil, err
	}

	client, err := ipcDial(paths.DaemonSock())
	if err != nil {
		switch {
		case daemonNotRunning(err):
			fmt.Fprintln(stderr, notRunningHint(paths))
		case daemonBusy(err):
			fmt.Fprintln(stderr, "synosync: daemon is busy or slow to respond - try again")
		default:
			fmt.Fprintf(stderr, "synosync: connect to daemon: %s\n", userErr(err))
		}
		return nil, err
	}
	return client, nil
}

// daemonNotRunning reports whether a dial error means there is no live daemon to
// talk to. It delegates to ipc.IsDaemonAbsent, which encapsulates the per-OS
// differences (a missing socket / refused connection on Unix, a missing named
// pipe on Windows - a Windows connect timeout is positive evidence of a
// live-but-busy daemon and is classified by daemonBusy instead)
// so the CLI does not import OS-specific error shapes or the Windows
// named-pipe library.
func daemonNotRunning(err error) bool {
	return ipc.IsDaemonAbsent(err)
}

// notRunningHint picks the advice for a dial that found no daemon.
//
// "start it with `synosync daemon`" is the right hint for an install that has
// been paired. On a FRESH one it sends the user to a command that answers "no
// config found; run `synosync pair` first" - correct, but one step too late, and
// it is the first thing a newcomer runs into: `status`, `sync`, `pause`,
// `resume` and `conflicts` all reached it.
//
// An absent state database is definitive evidence: nothing has ever been
// stored, so the install cannot be paired. When the database DOES exist the
// generic hint stands - the store may be held by a daemon that is merely slow to
// accept, and guessing "not paired" there would be worse than saying nothing.
// Stat only: a read-only verb must not create a state database as a side effect
// of explaining itself.
func notRunningHint(paths config.Paths) string {
	if _, err := os.Stat(paths.StateDB()); errors.Is(err, fs.ErrNotExist) {
		return "synosync: not paired yet; run `synosync pair` first"
	}
	// The FILE existing is not evidence of a pairing. `config get`/`config set` open the store
	// to read or write the settings blob, and opening creates it - and the pair help page
	// itself suggests `config set server.host ...` before pairing. So one entirely reasonable
	// pre-pair command turned this advice from "run pair" into "start the daemon", which then
	// answers "no config found; run pair first" - the right instruction, two commands later.
	// Ask the question that actually matters: has an identity ever been recorded? That is what
	// `daemon` and `login` themselves test.
	if !storeHasIdentity(paths) {
		return "synosync: not paired yet; run `synosync pair` first"
	}
	hint := "synosync: daemon is not running (start it with `synosync daemon`)"
	// A daemon that stopped for a reason the operator has to act on recorded it before
	// exiting. Saying only "not running" here is what made a pin mismatch, a vanished session
	// or a rejected client version invisible under a service manager, which restarts the
	// daemon into the same failure indefinitely.
	if why := lastStopHint(paths); why != "" {
		hint += "\n" + why
	}
	return hint
}

// daemonStore is the state database the running daemon already holds open.
//
// The two helpers below cannot open their own: the daemon holds bbolt's exclusive file lock
// for its whole life, so a second Open would wait out the lock timeout and then fail - on every
// start, and again on the exit path where the reason is classified. cmdDaemon publishes the
// handle it already has before handing off to the run loop, which is reached from the
// foreground, launchd and SCM paths alike.
var daemonStore *store.Store

// recordLastStop persists why the daemon stopped, so the next CLI run can say so.
var recordLastStop = func(ls store.LastStop) error {
	if daemonStore == nil {
		return errors.New("no open state database to record the stop reason in")
	}
	return daemonStore.PutLastStop(ls)
}

// lastStopHint reads the persisted stop reason, if one is recorded, and renders it as a line
// to append to the "daemon is not running" message.
//
// It opens the state database, which is safe exactly here: the caller has already established
// that nothing is listening on the control socket. It stays best-effort - any failure yields no
// line at all, because this runs while explaining a problem and must not become one.
func lastStopHint(paths config.Paths) string {
	if _, err := os.Stat(paths.StateDB()); err != nil {
		return ""
	}
	st, err := openStateDB(paths.StateDB())
	if err != nil {
		return ""
	}
	defer func() { _ = st.Close() }()
	ls, ok, err := st.GetLastStop()
	if err != nil || !ok {
		return ""
	}
	line := "synosync: it last stopped because " + ls.Reason
	if ls.At > 0 {
		line += " (" + time.Unix(ls.At, 0).Format(time.RFC3339) + ")"
	}
	if ls.Hint != "" {
		line += "\nsynosync: fix it with `" + ls.Hint + "`"
	}
	return line
}

// clearLastStop drops a recorded stop reason once the daemon is up.
var clearLastStop = func() error {
	if daemonStore == nil {
		return errors.New("no open state database to clear the stop reason in")
	}
	return daemonStore.ClearLastStop()
}

// storeHasIdentity reports whether a pairing has ever been recorded.
//
// The test is the device uuid: pair generates one and writes it into the settings blob, so its
// presence is what separates "paired" from "a state database that exists because some read-only
// command opened it". Best-effort in the CONSERVATIVE direction - a store that cannot be opened
// or read is treated as paired, so a database merely held by a starting daemon is never
// mislabelled a fresh install.
func storeHasIdentity(paths config.Paths) bool {
	// The store is opened at the path this hint is ABOUT, not through openConfigStore, which
	// resolves the real platform location. The two are the same in production and deliberately
	// different in tests, and going through the seam meant a test that pointed the hint at a
	// temp directory still read - and CREATED - a database in the developer's own profile,
	// which is also how one of these tests came to pass for the wrong reason. The caller has
	// already stat'ed this path, so opening it creates nothing.
	st, err := openStateDB(paths.StateDB())
	if err != nil {
		return true
	}
	defer func() { _ = st.Close() }()
	cfg, err := st.GetConfig()
	if err != nil {
		return !errors.Is(err, store.ErrNotFound)
	}
	return cfg.DeviceUUID != ""
}

// openStateDB opens the state database at an explicit path.
//
// Separate from openConfigStore, which resolves the platform location on its own: the
// not-running hint reasons about a path it was HANDED, and the two are deliberately different
// under test. A seam of its own so a test can fail the open without touching a real profile.
var openStateDB = func(path string) (*store.Store, error) { return store.Open(path) }
