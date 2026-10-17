package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mfilippov/synosync/internal/service"
)

// stubService points the subcommand at a canned outcome so the CLI surface is
// driven end-to-end without a service manager, a real home, or an install.
//
// Each seam records its OWN name. Pointing all three at one shared counter - as
// this did - makes the stubs indistinguishable, so a copy-paste slip in the
// action switch (`case "uninstall": run = serviceInstall`) would send
// `synosync service uninstall` off to write a unit and enable the service, and
// every assertion here would still pass. Per-seam identity is also the local
// convention: control_test.go's stubControlSocket keys its handlers by method
// name for the same reason.
//
// The returned pointer holds the action actually invoked, or "" if none was.
func stubService(t *testing.T, rep service.Report, err error) *string {
	t.Helper()
	var got string
	make := func(name string) func(context.Context, service.Deps) (service.Report, error) {
		return func(context.Context, service.Deps) (service.Report, error) {
			got += name + ";"
			return rep, err
		}
	}
	origDeps, origIn, origUn, origSt := serviceDeps, serviceInstall, serviceUninstall, serviceStatus
	serviceDeps = func() (service.Deps, error) {
		return service.Deps{Exe: "/opt/synosync", Home: "/home/alice", User: "alice"}, nil
	}
	serviceInstall, serviceUninstall, serviceStatus = make("install"), make("uninstall"), make("status")
	t.Cleanup(func() {
		serviceDeps, serviceInstall, serviceUninstall, serviceStatus = origDeps, origIn, origUn, origSt
	})
	return &got
}

// TestServiceActionsDispatch confirms each action reaches ITS OWN operation, not
// merely that some operation ran.
func TestServiceActionsDispatch(t *testing.T) {
	for _, action := range []string{"install", "uninstall", "status"} {
		t.Run(action, func(t *testing.T) {
			got := stubService(t, service.Report{State: service.StateRunning}, nil)
			var stdout, stderr bytes.Buffer
			if code := run([]string{"service", action}, &stdout, &stderr); code != 0 {
				t.Fatalf("exit = %d, want 0 (stderr: %q)", code, stderr.String())
			}
			if *got != action+";" {
				t.Errorf("invoked %q, want %q", *got, action+";")
			}
		})
	}
}

// TestServiceNeedsAnAction pins that a bare `synosync service` cannot reach
// install - which writes a unit and enables a background service.
func TestServiceNeedsAnAction(t *testing.T) {
	got := stubService(t, service.Report{}, nil)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"service"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if *got != "" {
		t.Errorf("a bare `service` reached %q", *got)
	}
	if !strings.Contains(stderr.String(), "service needs an action") {
		t.Errorf("stderr = %q, want the missing action named", stderr.String())
	}
	if !strings.Contains(stderr.String(), "Usage:\n  synosync service install") {
		t.Errorf("stderr = %q, want the invocation forms", stderr.String())
	}
}

func TestServiceUnknownAction(t *testing.T) {
	got := stubService(t, service.Report{}, nil)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"service", "frobnicate"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if *got != "" {
		t.Errorf("an unknown action reached %q", *got)
	}
	for _, want := range []string{`"frobnicate"`, `"install"`, `"uninstall"`, `"status"`} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
		}
	}
}

func TestServiceRejectsExtraArgs(t *testing.T) {
	got := stubService(t, service.Report{}, nil)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"service", "install", "extra"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	if *got != "" {
		t.Errorf("an extra argument still reached %q", *got)
	}
	// The message names the ACTION. rejectExtraArgs would say "service takes no
	// positional arguments", contradicting the `install` the user just passed.
	if !strings.Contains(stderr.String(), `service install takes no further arguments, got "extra"`) {
		t.Errorf("stderr = %q, want an action-aware message", stderr.String())
	}
	if !strings.Contains(stderr.String(), "synosync service --help") {
		t.Errorf("stderr = %q, want a pointer at --help", stderr.String())
	}
}

// TestServiceReportRendering pins the shape a user reads: what was done, where
// things stand, and what is left for them.
func TestServiceReportRendering(t *testing.T) {
	rep := service.Report{
		Steps:    []string{"wrote   ~/.config/systemd/user/synosync.service", "ran     systemctl --user daemon-reload"},
		State:    service.StateRunning,
		Notes:    []string{"enable linger to keep syncing after logout"},
		UnitPath: "/home/alice/.config/systemd/user/synosync.service",
	}
	stubService(t, rep, nil)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"service", "install"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	out := stdout.String()
	for _, want := range []string{
		"wrote   ~/.config/systemd/user/synosync.service",
		"ran     systemctl --user daemon-reload",
		// The unit path is shortened to the name a user recognises.
		"synosync.service: running",
		"note: enable linger to keep syncing after logout",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("stdout = %q, want it to contain %q", out, want)
		}
	}
}

// TestServiceReportsStepsOnFailure is the half-finished-install case: a user
// whose enable failed still needs to know the unit file was written, or they
// cannot tell what state the machine is in.
func TestServiceReportsStepsOnFailure(t *testing.T) {
	rep := service.Report{Steps: []string{"wrote   ~/.config/systemd/user/synosync.service"}}
	stubService(t, rep, errors.New("Failed to connect to bus"))

	var stdout, stderr bytes.Buffer
	if code := run([]string{"service", "install"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stdout.String(), "wrote   ~/.config/systemd/user/synosync.service") {
		t.Errorf("stdout = %q, want the completed step reported", stdout.String())
	}
	if !strings.Contains(stderr.String(), "Failed to connect to bus") {
		t.Errorf("stderr = %q, want the underlying failure", stderr.String())
	}
}

// TestServiceUnsupportedPlatform confirms the sentinel becomes advice rather
// than a raw error, on the builds that have no integration.
func TestServiceUnsupportedPlatform(t *testing.T) {
	stubService(t, service.Report{}, service.ErrUnsupported)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"service", "install"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "no service-manager integration") {
		t.Errorf("stderr = %q, want the platform limit explained", stderr.String())
	}
	if !strings.Contains(stderr.String(), "synosync daemon") {
		t.Errorf("stderr = %q, want the fallback named", stderr.String())
	}
}

// TestServiceDepsFailure covers the resolver failing before any operation runs.
func TestServiceDepsFailure(t *testing.T) {
	got := stubService(t, service.Report{}, nil)
	orig := serviceDeps
	serviceDeps = func() (service.Deps, error) { return service.Deps{}, errors.New("no home directory") }
	t.Cleanup(func() { serviceDeps = orig })

	var stdout, stderr bytes.Buffer
	if code := run([]string{"service", "install"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	if *got != "" {
		t.Errorf("an operation (%q) ran despite unresolved deps", *got)
	}
	if !strings.Contains(stderr.String(), "no home directory") {
		t.Errorf("stderr = %q, want the resolver error", stderr.String())
	}
}

func TestServiceDisplayName(t *testing.T) {
	cases := map[string]string{
		"/home/alice/.config/systemd/user/synosync.service": "synosync.service",
		`C:\ProgramData\x\me.filippov.synosync.plist`:       "me.filippov.synosync.plist",
		"synosync": "synosync",
	}
	for in, want := range cases {
		if got := serviceDisplayName(in); got != want {
			t.Errorf("serviceDisplayName(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestServiceControlActionsDispatch pins that the three control verbs route to their own
// package functions. They exist because every "stop the daemon and retry" hint had no answer
// once the daemon ran as a service: the user was left to know the systemctl, launchctl or
// sc.exe invocation, or to uninstall and reinstall around a settings change.
func TestServiceControlActionsDispatch(t *testing.T) {
	cases := []struct {
		action string
		seam   *func(context.Context, service.Deps) (service.Report, error)
	}{
		{"start", &serviceStart},
		{"stop", &serviceStop},
		{"restart", &serviceRestart},
	}
	for _, tc := range cases {
		t.Run(tc.action, func(t *testing.T) {
			prevDeps := serviceDeps
			serviceDeps = func() (service.Deps, error) { return service.Deps{}, nil }
			prev := *tc.seam
			called := false
			*tc.seam = func(context.Context, service.Deps) (service.Report, error) {
				called = true
				return service.Report{State: service.StateRunning}, nil
			}
			t.Cleanup(func() { *tc.seam = prev; serviceDeps = prevDeps })

			var stdout, stderr bytes.Buffer
			if code := cmdService([]string{tc.action}, &stdout, &stderr); code != 0 {
				t.Fatalf("exit = %d (stderr: %q)", code, stderr.String())
			}
			if !called {
				t.Errorf("%s did not reach its service action", tc.action)
			}
		})
	}
}

// TestServiceUnknownActionListsAllOfThem pins that the error names every action, so a user who
// mistypes learns the control verbs exist at all.
func TestServiceUnknownActionListsAllOfThem(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := cmdService([]string{"bogus"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit = %d, want 2", code)
	}
	for _, want := range []string{"install", "uninstall", "status", "start", "stop", "restart"} {
		if !strings.Contains(stderr.String(), `"`+want+`"`) {
			t.Errorf("error does not name %q:\n%s", want, stderr.String())
		}
	}
}
