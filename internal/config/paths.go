package config

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
)

// appDir is the per-application subdirectory name used under the platform
// configuration/state root (e.g. ~/.config/synosync).
const appDir = "synosync"

const (
	// stateDBName is the bbolt state database file name within the state dir.
	// It is the single file holding all non-secret state, including settings.
	stateDBName = "state.db"
	// daemonSockName is the IPC endpoint file name within the state dir. On Unix
	// it is the Unix-domain socket path; on Windows it is a stable path seed which,
	// combined with a per-user secret in the same dir, derives the named-pipe name
	// (see internal/ipc.pipeName).
	daemonSockName = "daemon.sock"
	// lockFileName is the single-instance daemon lock file name within the state dir.
	lockFileName = "daemon.lock"
	// logFileName is the daemon log file name within the log dir.
	logFileName = "synosync.log"
	// pipeSecretName is the per-user Windows named-pipe secret file name within the
	// state dir. It must match the name internal/ipc derives next to the socket
	// path (internal/ipc.pipeSecretFile); it is surfaced here so `unlink` can
	// best-effort remove it as part of the local cleanup.
	pipeSecretName = "daemon.pipe-secret" //nolint:gosec // G101 false positive: a filename, not a credential
)

// Paths holds the resolved on-disk locations used by synosync.
type Paths struct {
	// StateDir is the directory holding state.db (and other mutable state).
	StateDir string
	// LogDir is the platform-appropriate directory holding the daemon log file,
	// kept separate from StateDir so logs land in the OS log location.
	LogDir string
}

// StateDB returns the full path to the bbolt state database.
func (p Paths) StateDB() string {
	return filepath.Join(p.StateDir, stateDBName)
}

// DaemonSock returns the full path to the daemon's IPC endpoint, alongside the
// state database under StateDir. On Unix this is the Unix-domain socket path; on
// Windows it is a stable seed which, combined with a per-user secret in the same
// dir, derives the named-pipe name (see internal/ipc.pipeName), not a socket path.
func (p Paths) DaemonSock() string {
	return filepath.Join(p.StateDir, daemonSockName)
}

// LockFile returns the full path to the single-instance daemon lock file,
// alongside the state database under StateDir.
func (p Paths) LockFile() string {
	return filepath.Join(p.StateDir, lockFileName)
}

// LogFile returns the full path to the daemon log file under LogDir.
func (p Paths) LogFile() string {
	return filepath.Join(p.LogDir, logFileName)
}

// PipeSecret returns the full path to the per-user Windows named-pipe secret
// file, alongside the state database and socket under StateDir. It is only ever
// created on Windows (see internal/ipc); on Unix the path names a file that never
// exists, so `unlink`'s best-effort removal is a harmless no-op there.
func (p Paths) PipeSecret() string {
	return filepath.Join(p.StateDir, pipeSecretName)
}

// DefaultPaths resolves the platform-appropriate state and log directories.
//
// State directory:
//   - Windows: %LocalAppData%\synosync (LOCAL: state.db is a large mmap'd database
//     carrying a per-DEVICE identity, and a roaming profile would copy it between
//     machines).
//   - Linux:   $XDG_DATA_HOME/synosync, falling back to ~/.local/share.
//   - macOS:   ~/Library/Application Support/synosync, unless XDG_DATA_HOME is
//     explicitly set (honoured for parity with the Linux layout).
//
// Log directory:
//   - Windows: %LocalAppData%\synosync\logs.
//   - Linux:   $XDG_STATE_HOME/synosync, falling back to ~/.local/state.
//   - macOS:   ~/Library/Logs/synosync.
//
// A relative environment override - an XDG variable per the spec rule in
// xdgEnv, or %LocalAppData% per winAbsEnv - is ignored (treated as
// unset → home-dir fallback) so the resolved dirs are never CWD-relative.
//
// It returns an error only when no home directory can be determined and no
// suitable environment override is present.
func DefaultPaths() (Paths, error) {
	return defaultPaths(runtime.GOOS, os.Getenv, os.UserHomeDir)
}

// xdgEnv reads an XDG base-directory environment variable, honouring the spec
// requirement that a relative path is invalid and must be ignored: only an
// absolute value is returned, anything else (unset, empty or relative) yields ""
// so the caller falls back to the platform default.
func xdgEnv(getenv func(string) string, name string) string {
	v := getenv(name)
	if v == "" || !filepath.IsAbs(v) {
		return ""
	}
	return v
}

// winAbsEnv reads a Windows environment variable that must contain an absolute
// path, mirroring xdgEnv's relative-value rejection: a relative
// %LocalAppData% would resolve the state dir relative to the process CWD, so a
// daemon and a CLI launched from different directories would silently use
// different state dirs ("daemon is not running" while it is alive). Only an
// absolute value is returned; anything else (unset, empty or relative) yields
// "" so the caller falls back to the home-relative default.
func winAbsEnv(getenv func(string) string, name string) string {
	v := getenv(name)
	if !winIsAbs(v) {
		return ""
	}
	return v
}

// winIsAbs reports whether p is an absolute path under Windows rules,
// applied directly rather than via filepath.IsAbs because the GOOS here is
// injected for tests while the host filepath follows host rules. Absolute
// means a drive-letter root ("C:\" or "C:/") or a UNC / extended-length
// prefix ("\\server\share", "\\?\C:\..." - never CWD-relative); a
// drive-relative ("C:foo") or rooted ("\foo") path depends on per-process
// state and is treated as relative.
func winIsAbs(p string) bool {
	if strings.HasPrefix(p, `\\`) || strings.HasPrefix(p, `//`) {
		return true
	}
	if len(p) >= 3 && p[1] == ':' && (p[2] == '\\' || p[2] == '/') {
		c := p[0]
		return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
	}
	return false
}

// defaultPaths is the testable core of DefaultPaths: the operating system,
// environment lookup and home-directory resolution are injected so platform
// behaviour can be exercised on any host.
func defaultPaths(goos string, getenv func(string) string, homeDir func() (string, error)) (Paths, error) {
	switch goos {
	case "windows":
		// LOCAL, not Roaming, for the state as well as the log. state.db is an mmap'd
		// bbolt database that reaches hundreds of megabytes on a large view, and a roaming
		// profile copies %AppData% between machines at logon and logoff. That would carry
		// the file across the network on every login, and - worse than the cost - deliver
		// one machine's device_uuid and its baselines, which describe a DIFFERENT disk, to
		// another machine. Two hosts would then present the same device identity while
		// each one's baselines named files the other does not have.
		//
		// Nothing here is meant to follow the user between machines: a pairing is per
		// device by construction, and the keychain entries it depends on do not roam
		// either. The log directory already sat under %LocalAppData% for the same reason,
		// so the two now share one tree.
		base := winAbsEnv(getenv, "LocalAppData")
		if base == "" {
			home, err := homeDir()
			if err != nil {
				return Paths{}, err
			}
			base = filepath.Join(home, "AppData", "Local")
		}
		return Paths{
			StateDir: filepath.Join(base, appDir),
			LogDir:   filepath.Join(base, appDir, "logs"),
		}, nil

	case "darwin":
		// Honour an explicit XDG override; otherwise use the native location.
		stateBase := xdgEnv(getenv, "XDG_DATA_HOME")
		if stateBase == "" {
			home, err := homeDir()
			if err != nil {
				return Paths{}, err
			}
			stateBase = filepath.Join(home, "Library", "Application Support")
		}
		home, err := homeDir()
		if err != nil {
			return Paths{}, err
		}
		return Paths{
			StateDir: filepath.Join(stateBase, appDir),
			LogDir:   filepath.Join(home, "Library", "Logs", appDir),
		}, nil

	default: // linux and other unix-likes
		stateBase := xdgEnv(getenv, "XDG_DATA_HOME")
		if stateBase == "" {
			home, err := homeDir()
			if err != nil {
				return Paths{}, err
			}
			stateBase = filepath.Join(home, ".local", "share")
		}
		logBase := xdgEnv(getenv, "XDG_STATE_HOME")
		if logBase == "" {
			home, err := homeDir()
			if err != nil {
				return Paths{}, err
			}
			logBase = filepath.Join(home, ".local", "state")
		}
		return Paths{
			StateDir: filepath.Join(stateBase, appDir),
			LogDir:   filepath.Join(logBase, appDir),
		}, nil
	}
}

// ErrSyncRootHoldsState reports a sync root that contains synosync's own state or log
// directory.
var ErrSyncRootHoldsState = errors.New("config: the sync root would contain synosync's own state")

// CheckSyncRootLocation refuses a sync root that holds the state or log directory.
//
// Nothing stops a user from pointing the sync root at their home directory, or on Windows at
// %LocalAppData% itself, and with a single selected view the sync root IS the view - so the
// scanner walks state.db, the daemon lock and the pipe secret as ordinary files. bbolt holds
// state.db open and mmap'd while it is hashed and uploaded, every write to it and to the
// rotating log wakes the watcher, which triggers the scan that reads them again, and the
// server ends up holding a copy of this device's pairing.
//
// Checked where the sync root is SET (pair, config set), not where it is used: refusing at
// daemon start would strand an install that already has this shape with no way to run the
// command that fixes it.
func CheckSyncRootLocation(syncRoot string, p Paths) error {
	return checkSyncRootLocation(runtime.GOOS, syncRoot, p)
}

// checkSyncRootLocation is the testable core, with the OS injected so both path
// case-sensitivity rules are exercisable on any host.
func checkSyncRootLocation(goos, syncRoot string, p Paths) error {
	if syncRoot == "" {
		return nil
	}
	for _, d := range []struct{ name, path string }{
		{"state directory", p.StateDir},
		{"log directory", p.LogDir},
	} {
		if d.path == "" {
			continue
		}
		if pathWithin(goos, d.path, syncRoot) {
			return fmt.Errorf("%w: %s %q is inside the sync root %q, so syncing would upload "+
				"this device's own database and logs to the server",
				ErrSyncRootHoldsState, d.name, d.path, syncRoot)
		}
	}
	return nil
}

// pathWithin reports whether inner is outer itself or lies beneath it, comparing the way the
// platform's filesystem does.
func pathWithin(goos, inner, outer string) bool {
	i, o := comparablePath(goos, inner), comparablePath(goos, outer)
	if i == o {
		return true
	}
	// A lone root contains everything under it; anywhere else the separator is what makes
	// this containment rather than a string prefix, so a sibling named "Drive-state" is not
	// inside "Drive".
	if o == "/" {
		return strings.HasPrefix(i, "/")
	}
	return strings.HasPrefix(i, o+"/")
}

// comparablePath renders p in one host-independent form: forward slashes throughout, cleaned
// (so "." and ".." resolve and duplicate or trailing separators collapse), and lowercased for
// Windows, whose filesystem compares case-insensitively - a check that did not would be
// trivially defeated by whatever casing the user happened to type.
//
// It must not consult the host, and that is the whole point. This used filepath.Separator and
// filepath.Clean, which follow the HOST rather than the goos argument, so on a Linux or macOS
// runner the Windows branch compared backslash-separated paths against a forward-slash
// separator and answered "not contained" for every Windows input - the rule was live only on
// the platform it was least needed to be tested on. CI caught it; a Windows-only developer
// machine never would. The rule is pure string syntax over a NAMED platform, so path.Clean
// (slash-based, host-independent) does the cleaning, not path/filepath.
func comparablePath(goos, p string) string {
	if goos == "windows" {
		p = strings.ReplaceAll(p, `\`, "/")
		p = strings.ToLower(p)
	}
	return path.Clean(p)
}
