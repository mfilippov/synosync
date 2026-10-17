package secrets

import (
	"errors"
	"testing"

	"github.com/zalando/go-keyring"
)

func TestSessionRoundTrip(t *testing.T) {
	keyring.MockInit()
	k := New()

	const (
		server = "ds.example.com:6690"
		user   = "alice"
		token  = "0123456789abcdef0123456789abcdef" // gitleaks:allow -- a fixture, not a credential
	)

	if err := k.SetSession(server, user, token); err != nil {
		t.Fatalf("SetSession: %v", err)
	}
	got, err := k.Session(server, user)
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	if got != token {
		t.Fatalf("Session = %q, want %q", got, token)
	}

	if err := k.DeleteSession(server, user); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if _, err := k.Session(server, user); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Session after delete: got %v, want ErrNotFound", err)
	}
}

func TestPinRoundTrip(t *testing.T) {
	keyring.MockInit()
	k := New()

	const (
		server = "ds.example.com:6690"
		fp     = "sha256:deadbeef"
	)

	if err := k.SetPin(server, fp); err != nil {
		t.Fatalf("SetPin: %v", err)
	}
	got, err := k.Pin(server)
	if err != nil {
		t.Fatalf("Pin: %v", err)
	}
	if got != fp {
		t.Fatalf("Pin = %q, want %q", got, fp)
	}
	if err := k.DeletePin(server); err != nil {
		t.Fatalf("DeletePin: %v", err)
	}
	if _, err := k.Pin(server); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Pin after delete: got %v, want ErrNotFound", err)
	}
}

func TestPasswordRoundTrip(t *testing.T) {
	keyring.MockInit()
	k := New()

	const (
		server = "ds.example.com:6690"
		user   = "bob"
		pw     = "s3cr3t"
	)

	if err := k.SetLegacyPassword(server, user, pw); err != nil {
		t.Fatalf("SetLegacyPassword: %v", err)
	}
	got, err := k.Password(server, user)
	if err != nil {
		t.Fatalf("Password: %v", err)
	}
	if got != pw {
		t.Fatalf("Password = %q, want %q", got, pw)
	}
	if err := k.DeletePassword(server, user); err != nil {
		t.Fatalf("DeletePassword: %v", err)
	}
}

func TestOverwrite(t *testing.T) {
	keyring.MockInit()
	k := New()

	const (
		server = "ds.example.com:6690"
		user   = "alice"
	)

	if err := k.SetSession(server, user, "first"); err != nil {
		t.Fatalf("SetSession first: %v", err)
	}
	if err := k.SetSession(server, user, "second"); err != nil {
		t.Fatalf("SetSession second: %v", err)
	}
	got, err := k.Session(server, user)
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	if got != "second" {
		t.Fatalf("Session = %q, want overwritten value %q", got, "second")
	}
}

func TestMissingKey(t *testing.T) {
	keyring.MockInit()
	k := New()

	if _, err := k.Session("nope", "nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Session missing: got %v, want ErrNotFound", err)
	}
	if _, err := k.Pin("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Pin missing: got %v, want ErrNotFound", err)
	}
	if _, err := k.Password("nope", "nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Password missing: got %v, want ErrNotFound", err)
	}
}

func TestDeleteMissing(t *testing.T) {
	keyring.MockInit()
	k := New()

	if err := k.DeleteSession("nope", "nobody"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteSession missing: got %v, want ErrNotFound", err)
	}
}

// Categories must not collide: a session and a password for the same server/user
// are independent entries.
func TestCategoriesIndependent(t *testing.T) {
	keyring.MockInit()
	k := New()

	const (
		server = "ds.example.com:6690"
		user   = "alice"
	)

	if err := k.SetSession(server, user, "tok"); err != nil {
		t.Fatalf("SetSession: %v", err)
	}
	if err := k.SetLegacyPassword(server, user, "pw"); err != nil {
		t.Fatalf("SetLegacyPassword: %v", err)
	}

	sess, err := k.Session(server, user)
	if err != nil {
		t.Fatalf("Session: %v", err)
	}
	pw, err := k.Password(server, user)
	if err != nil {
		t.Fatalf("Password: %v", err)
	}
	if sess != "tok" || pw != "pw" {
		t.Fatalf("session/password collided: sess=%q pw=%q", sess, pw)
	}
}

// TestBackendFailureIsUnavailable pins the split a caller relies on to give the right
// advice: a backend that cannot serve at all (no Secret Service, a locked keychain) is
// ErrUnavailable on every operation, with the backend's own words kept in the message;
// a size refusal is not, since no change on the host cures it.
func TestBackendFailureIsUnavailable(t *testing.T) {
	t.Cleanup(keyring.MockInit)
	cause := errors.New("The name org.freedesktop.secrets was not provided by any .service files")
	keyring.MockInitWithError(cause)
	k := New()

	ops := map[string]error{}
	_, ops["read"] = k.Session("nas:6690", "alice")
	ops["write"] = k.SetPin("nas:6690", "spki-sha256:00")
	ops["delete"] = k.DeleteSession("nas:6690", "alice")
	for op, err := range ops {
		if !errors.Is(err, ErrUnavailable) || !errors.Is(err, cause) {
			t.Errorf("%s: err = %v, want ErrUnavailable wrapping the backend error", op, err)
		}
		if errors.Is(err, ErrNotFound) {
			t.Errorf("%s: a backend failure read as not-found: %v", op, err)
		}
	}

	keyring.MockInitWithError(keyring.ErrSetDataTooBig)
	if err := k.SetSession("nas:6690", "alice", "token"); errors.Is(err, ErrUnavailable) || !errors.Is(err, keyring.ErrSetDataTooBig) {
		t.Errorf("size refusal: err = %v, want ErrSetDataTooBig and not ErrUnavailable", err)
	}
}
