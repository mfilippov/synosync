package config

import (
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// hostAbs builds a host-absolute path out of POSIX-style segments. A POSIX
// literal ("/xdg/data") is absolute only under POSIX rules: on Windows a rooted
// path carrying no volume ("\xdg\data") is drive-relative, so filepath.IsAbs -
// which xdgEnv and Config.Validate apply with HOST rules while the GOOS under
// test is injected - rejects it and the fixture silently stops exercising the
// absolute-value branch. Fixtures that mean "an absolute path" must therefore
// build one for the host the test runs on. The path is a string fixture only;
// nothing is created on disk, so a fixed volume is fine.
func hostAbs(elem ...string) string {
	root := string(filepath.Separator)
	if runtime.GOOS == "windows" {
		root = `C:\`
	}
	return filepath.Join(append([]string{root}, elem...)...)
}

// envMap builds an env lookup func from a map.
func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func fakeHome(home string) func() (string, error) {
	return func() (string, error) { return home, nil }
}

func TestDefaultPathsLinuxXDG(t *testing.T) {
	xdgData := hostAbs("xdg", "data")
	env := envMap(map[string]string{
		"XDG_DATA_HOME": xdgData,
	})
	p, err := defaultPaths("linux", env, fakeHome("/home/u"))
	if err != nil {
		t.Fatalf("defaultPaths: %v", err)
	}
	if want := filepath.Join(xdgData, appDir); p.StateDir != want {
		t.Errorf("StateDir = %q, want %q", p.StateDir, want)
	}
}

func TestDefaultPathsLinuxFallback(t *testing.T) {
	p, err := defaultPaths("linux", envMap(nil), fakeHome("/home/u"))
	if err != nil {
		t.Fatalf("defaultPaths: %v", err)
	}
	if want := filepath.Join("/home/u", ".local", "share", appDir); p.StateDir != want {
		t.Errorf("StateDir = %q, want %q", p.StateDir, want)
	}
}

// TestDefaultPathsRelativeXDGIgnored verifies that a relative XDG_DATA_HOME /
// XDG_STATE_HOME value is invalid per the XDG spec and is ignored (falling back
// to the home-relative default) rather than taken as-is.
func TestDefaultPathsRelativeXDGIgnored(t *testing.T) {
	env := envMap(map[string]string{
		"XDG_DATA_HOME":  "relative/data",
		"XDG_STATE_HOME": "relative/state",
	})
	p, err := defaultPaths("linux", env, fakeHome("/home/u"))
	if err != nil {
		t.Fatalf("defaultPaths: %v", err)
	}
	if want := filepath.Join("/home/u", ".local", "share", appDir); p.StateDir != want {
		t.Errorf("StateDir = %q, want fallback %q", p.StateDir, want)
	}
	if want := filepath.Join("/home/u", ".local", "state", appDir); p.LogDir != want {
		t.Errorf("LogDir = %q, want fallback %q", p.LogDir, want)
	}
}

// TestDefaultPathsRelativeXDGIgnoredDarwin verifies the same relative-value
// rejection on darwin, where XDG_DATA_HOME overrides the native state location.
func TestDefaultPathsRelativeXDGIgnoredDarwin(t *testing.T) {
	env := envMap(map[string]string{"XDG_DATA_HOME": "relative/data"})
	p, err := defaultPaths("darwin", env, fakeHome("/Users/u"))
	if err != nil {
		t.Fatalf("defaultPaths: %v", err)
	}
	if want := filepath.Join("/Users/u", "Library", "Application Support", appDir); p.StateDir != want {
		t.Errorf("StateDir = %q, want native fallback %q", p.StateDir, want)
	}
}

// TestDefaultPathsWindows pins that the state lives under LocalAppData, not Roaming.
//
// state.db is an mmap'd bbolt database reaching hundreds of megabytes, and a roaming profile
// copies %AppData% between machines at logon and logoff - carrying it over the network every
// login and, worse, delivering one machine's device_uuid and its baselines, which describe a
// DIFFERENT disk, to another. A pairing is per device by construction.
func TestDefaultPathsWindows(t *testing.T) {
	env := envMap(map[string]string{
		"AppData":      `C:\Users\u\AppData\Roaming`,
		"LocalAppData": `C:\Users\u\AppData\Local`,
	})
	p, err := defaultPaths("windows", env, fakeHome(`C:\Users\u`))
	if err != nil {
		t.Fatalf("defaultPaths: %v", err)
	}
	want := filepath.Join(`C:\Users\u\AppData\Local`, appDir)
	if p.StateDir != want {
		t.Errorf("StateDir = %q, want %q", p.StateDir, want)
	}
	if strings.Contains(p.StateDir, "Roaming") {
		t.Errorf("StateDir = %q, want nothing under a roaming profile", p.StateDir)
	}
}

func TestDefaultPathsWindowsFallback(t *testing.T) {
	p, err := defaultPaths("windows", envMap(nil), fakeHome(`C:\Users\u`))
	if err != nil {
		t.Fatalf("defaultPaths: %v", err)
	}
	want := filepath.Join(`C:\Users\u`, "AppData", "Local", appDir)
	if p.StateDir != want {
		t.Errorf("StateDir = %q, want %q", p.StateDir, want)
	}
}

// TestDefaultPathsRelativeAppDataIgnored verifies that a relative %AppData% /
// %LocalAppData% value is ignored (falling back to the home-relative default)
// rather than resolved CWD-relative, mirroring the XDG relative-value rule -
// otherwise a daemon and a CLI launched from different directories would
// silently use different state dirs.
func TestDefaultPathsRelativeAppDataIgnored(t *testing.T) {
	wantState := filepath.Join(`C:\Users\u`, "AppData", "Local", appDir)
	wantLog := filepath.Join(`C:\Users\u`, "AppData", "Local", appDir, "logs")
	cases := []struct {
		name         string
		appData      string
		localAppData string
	}{
		{name: "plain relative", appData: `foo`, localAppData: `bar\baz`},
		{name: "drive-relative", appData: `C:foo`, localAppData: `C:bar`},
		{name: "rooted", appData: `\foo`, localAppData: `\bar`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := envMap(map[string]string{
				"AppData":      tc.appData,
				"LocalAppData": tc.localAppData,
			})
			p, err := defaultPaths("windows", env, fakeHome(`C:\Users\u`))
			if err != nil {
				t.Fatalf("defaultPaths: %v", err)
			}
			if p.StateDir != wantState {
				t.Errorf("StateDir = %q, want fallback %q", p.StateDir, wantState)
			}
			if p.LogDir != wantLog {
				t.Errorf("LogDir = %q, want fallback %q", p.LogDir, wantLog)
			}
		})
	}
}

// TestDefaultPathsWindowsUNC verifies that a UNC %LocalAppData% value is accepted as absolute.
//
// A redirected local profile is unusual but legal, and a folder-redirection policy can put
// %LocalAppData% on a share. The rule being tested is winIsAbs's acceptance of the UNC form, not
// where a profile ought to live.
func TestDefaultPathsWindowsUNC(t *testing.T) {
	env := envMap(map[string]string{"LocalAppData": `\\server\share\local`})
	p, err := defaultPaths("windows", env, fakeHome(`C:\Users\u`))
	if err != nil {
		t.Fatalf("defaultPaths: %v", err)
	}
	if want := filepath.Join(`\\server\share\local`, appDir); p.StateDir != want {
		t.Errorf("StateDir = %q, want %q", p.StateDir, want)
	}
}

func TestWinIsAbs(t *testing.T) {
	cases := []struct {
		p    string
		want bool
	}{
		{`C:\Users\u`, true},
		{`c:/users/u`, true},
		{`\\server\share`, true},
		{`\\?\C:\Users\u`, true},
		{`//server/share`, true},
		{`C:foo`, false},
		{`\foo`, false},
		{`/foo`, false},
		{`foo\bar`, false},
		{``, false},
		{`1:\foo`, false},
	}
	for _, tc := range cases {
		if got := winIsAbs(tc.p); got != tc.want {
			t.Errorf("winIsAbs(%q) = %v, want %v", tc.p, got, tc.want)
		}
	}
}

func TestDefaultPathsDarwin(t *testing.T) {
	p, err := defaultPaths("darwin", envMap(nil), fakeHome("/Users/u"))
	if err != nil {
		t.Fatalf("defaultPaths: %v", err)
	}
	want := filepath.Join("/Users/u", "Library", "Application Support", appDir)
	if p.StateDir != want {
		t.Errorf("StateDir = %q, want %q", p.StateDir, want)
	}
}

func TestDefaultPathsDarwinXDGOverride(t *testing.T) {
	xdgData := hostAbs("xdg", "data")
	env := envMap(map[string]string{
		"XDG_DATA_HOME": xdgData,
	})
	p, err := defaultPaths("darwin", env, fakeHome("/Users/u"))
	if err != nil {
		t.Fatalf("defaultPaths: %v", err)
	}
	if want := filepath.Join(xdgData, appDir); p.StateDir != want {
		t.Errorf("StateDir = %q, want %q", p.StateDir, want)
	}
}

func TestPathsFileAccessors(t *testing.T) {
	p := Paths{StateDir: "/state", LogDir: "/logs"}
	if want := filepath.Join("/state", stateDBName); p.StateDB() != want {
		t.Errorf("StateDB() = %q, want %q", p.StateDB(), want)
	}
	if want := filepath.Join("/state", daemonSockName); p.DaemonSock() != want {
		t.Errorf("DaemonSock() = %q, want %q", p.DaemonSock(), want)
	}
	if want := filepath.Join("/state", lockFileName); p.LockFile() != want {
		t.Errorf("LockFile() = %q, want %q", p.LockFile(), want)
	}
	if want := filepath.Join("/logs", logFileName); p.LogFile() != want {
		t.Errorf("LogFile() = %q, want %q", p.LogFile(), want)
	}
}

func TestDefaultPathsLogDir(t *testing.T) {
	cases := []struct {
		name string
		goos string
		env  map[string]string
		home string
		want string
	}{
		{
			name: "linux XDG_STATE_HOME",
			goos: "linux",
			env:  map[string]string{"XDG_STATE_HOME": hostAbs("xdg", "state")},
			home: "/home/u",
			want: filepath.Join(hostAbs("xdg", "state"), appDir),
		},
		{
			name: "linux fallback",
			goos: "linux",
			home: "/home/u",
			want: filepath.Join("/home/u", ".local", "state", appDir),
		},
		{
			name: "darwin",
			goos: "darwin",
			home: "/Users/u",
			want: filepath.Join("/Users/u", "Library", "Logs", appDir),
		},
		{
			name: "windows LocalAppData",
			goos: "windows",
			env:  map[string]string{"LocalAppData": `C:\Users\u\AppData\Local`},
			home: `C:\Users\u`,
			want: filepath.Join(`C:\Users\u\AppData\Local`, appDir, "logs"),
		},
		{
			name: "windows fallback",
			goos: "windows",
			home: `C:\Users\u`,
			want: filepath.Join(`C:\Users\u`, "AppData", "Local", appDir, "logs"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := defaultPaths(tc.goos, envMap(tc.env), fakeHome(tc.home))
			if err != nil {
				t.Fatalf("defaultPaths: %v", err)
			}
			if p.LogDir != tc.want {
				t.Errorf("LogDir = %q, want %q", p.LogDir, tc.want)
			}
			if want := filepath.Join(tc.want, logFileName); p.LogFile() != want {
				t.Errorf("LogFile() = %q, want %q", p.LogFile(), want)
			}
		})
	}
}

func TestPathsDaemonResolution(t *testing.T) {
	cases := []struct {
		name string
		goos string
		env  map[string]string
		home string
	}{
		{
			name: "linux",
			goos: "linux",
			env:  map[string]string{"XDG_DATA_HOME": "/xdg/data"},
			home: "/home/u",
		},
		{
			name: "darwin",
			goos: "darwin",
			home: "/Users/u",
		},
		{
			name: "windows",
			goos: "windows",
			env:  map[string]string{"AppData": `C:\Users\u\AppData\Roaming`},
			home: `C:\Users\u`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := defaultPaths(tc.goos, envMap(tc.env), fakeHome(tc.home))
			if err != nil {
				t.Fatalf("defaultPaths: %v", err)
			}
			if want := filepath.Join(p.StateDir, daemonSockName); p.DaemonSock() != want {
				t.Errorf("DaemonSock() = %q, want %q", p.DaemonSock(), want)
			}
			if want := filepath.Join(p.StateDir, lockFileName); p.LockFile() != want {
				t.Errorf("LockFile() = %q, want %q", p.LockFile(), want)
			}
		})
	}
}

func TestDefaultPathsResolvesOnHost(t *testing.T) {
	// Smoke test the exported entrypoint on the real platform.
	p, err := DefaultPaths()
	if err != nil {
		t.Fatalf("DefaultPaths: %v", err)
	}
	if p.StateDir == "" {
		t.Errorf("empty paths: %+v", p)
	}
}

// TestCheckSyncRootLocationRefusesSelfContainment pins the foot-gun: nothing stops a user from
// pointing the sync root at their home directory, or on Windows at %LocalAppData% itself. With
// a single selected view the sync root IS the view, so the scanner walks state.db while bbolt
// holds it open and mmap'd, uploads this device's pairing to the server, and every write to the
// database and the rotating log wakes the watcher that triggers the scan reading them again.
func TestCheckSyncRootLocationRefusesSelfContainment(t *testing.T) {
	cases := []struct {
		name    string
		goos    string
		root    string
		paths   Paths
		wantErr bool
	}{
		{
			name:    "state dir beneath the sync root",
			goos:    "linux",
			root:    "/home/u",
			paths:   Paths{StateDir: "/home/u/.local/share/synosync", LogDir: "/home/u/.local/state/synosync"},
			wantErr: true,
		},
		{
			name:    "sync root IS the state dir",
			goos:    "linux",
			root:    "/home/u/.local/share/synosync",
			paths:   Paths{StateDir: "/home/u/.local/share/synosync"},
			wantErr: true,
		},
		{
			name:    "only the log dir is inside",
			goos:    "linux",
			root:    "/home/u/.local/state",
			paths:   Paths{StateDir: "/var/lib/synosync", LogDir: "/home/u/.local/state/synosync"},
			wantErr: true,
		},
		{
			name:    "ordinary layout",
			goos:    "linux",
			root:    "/home/u/Drive",
			paths:   Paths{StateDir: "/home/u/.local/share/synosync", LogDir: "/home/u/.local/state/synosync"},
			wantErr: false,
		},
		{
			// A sibling whose name merely starts with the same characters is not inside.
			name:    "prefix that is not a path component",
			goos:    "linux",
			root:    "/home/u/Drive",
			paths:   Paths{StateDir: "/home/u/Drive-state/synosync"},
			wantErr: false,
		},
		{
			// Windows compares paths case-insensitively, so a check that did not would be
			// defeated by whatever casing the user happened to type.
			name:    "windows differing case",
			goos:    "windows",
			root:    `C:\Users\U\AppData\Local`,
			paths:   Paths{StateDir: `C:\Users\u\appdata\local\synosync`},
			wantErr: true,
		},
		{
			name:    "unset sync root",
			goos:    "linux",
			root:    "",
			paths:   Paths{StateDir: "/home/u/.local/share/synosync"},
			wantErr: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkSyncRootLocation(tc.goos, tc.root, tc.paths)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("checkSyncRootLocation(%q, %+v) = nil, want a refusal", tc.root, tc.paths)
				}
				if !errors.Is(err, ErrSyncRootHoldsState) {
					t.Errorf("err = %v, want it to wrap ErrSyncRootHoldsState", err)
				}
				return
			}
			if err != nil {
				t.Errorf("checkSyncRootLocation(%q, %+v) = %v, want nil", tc.root, tc.paths, err)
			}
		})
	}
}

// TestPathWithinIsHostIndependent is the regression for what CI caught and this developer
// machine could not: pathWithin took a goos argument but did its path arithmetic with
// path/filepath, whose separator and Clean follow the HOST. On a Linux or macOS runner the
// Windows branch therefore compared backslash-separated paths against a forward slash and
// answered "not contained" for every Windows input - so the containment refusal was live only
// on Windows, the one platform where a Windows-only developer would never notice.
//
// Every case below names a platform other than the one the test may be running on, which is
// the point: the answers must not move with the host.
func TestPathWithinIsHostIndependent(t *testing.T) {
	cases := []struct {
		name         string
		goos         string
		inner, outer string
		want         bool
	}{
		{"windows backslashes", "windows", `C:\Users\U\AppData\Local\synosync`, `C:\Users\U\AppData\Local`, true},
		{"windows case folds", "windows", `c:\users\u\appdata\local\synosync`, `C:\Users\U\AppData\Local`, true},
		{"windows forward slashes accepted", "windows", "C:/Users/U/Drive/state", "C:/Users/U/Drive", true},
		{"windows mixed separators", "windows", `C:/Users/U\Drive\state`, `C:\Users\U/Drive`, true},
		{"windows identical", "windows", `C:\Users\U\Drive`, `C:\Users\U\Drive`, true},
		{"windows trailing separator", "windows", `C:\Users\U\Drive\state`, `C:\Users\U\Drive\`, true},
		// A sibling sharing a character prefix is not inside: the separator is what makes it
		// containment, and this is the case a bare strings.HasPrefix gets wrong.
		{"windows sibling prefix", "windows", `C:\Users\U\Drive-state`, `C:\Users\U\Drive`, false},
		{"windows unrelated", "windows", `D:\Other`, `C:\Users\U\Drive`, false},
		// Case is NOT folded off Windows: two names differing only in case are two directories.
		{"linux case is significant", "linux", "/home/u/drive/state", "/home/U/Drive", false},
		{"linux contained", "linux", "/home/u/Drive/state", "/home/u/Drive", true},
		{"linux sibling prefix", "linux", "/home/u/Drive-state", "/home/u/Drive", false},
		{"linux root contains all", "linux", "/home/u", "/", true},
		{"linux dot segments resolve", "linux", "/home/u/Drive/../Drive/state", "/home/u/Drive", true},
		{"linux parent is not inside its child", "linux", "/home/u", "/home/u/Drive", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := pathWithin(tc.goos, tc.inner, tc.outer); got != tc.want {
				t.Fatalf("pathWithin(%q, %q, %q) = %v, want %v", tc.goos, tc.inner, tc.outer, got, tc.want)
			}
		})
	}
}
