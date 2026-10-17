// Package secrets wraps the system keychain (via go-keyring) used to persist the
// few credentials synosync needs to keep across runs: the reusable session token,
// the pinned certificate fingerprint (SPKI public-key form, or a legacy
// leaf-DER pin until migrated). The account password is NEVER among them: pair and
// login read it once, in memory, for a single auth turn (DESIGN §9), so the keychain
// holds exactly two durable secrets per (host, user). What remains here of the
// password is the CLEANUP half - an older version did persist one, and an install
// carrying that entry must still be able to shed it. Non-secret values (device_uuid, ds_id) live in the config blob
// inside state.db instead; only the items in this package are kept in the OS
// keychain.
//
// The store is keyed by a single service name plus a per-item account string that
// encodes the item category and its scope (server, and for per-user items the
// user). This keeps a session token, a pinned cert and a password for the same
// server/user from colliding under one keychain entry.
package secrets

import (
	"errors"
	"fmt"

	"github.com/zalando/go-keyring"
)

// ServiceName is the keychain service under which every synosync secret is
// stored. Tests swap the backend with keyring.MockInit; the service name is
// unchanged.
const ServiceName = "synosync"

// ErrNotFound reports that a requested secret is absent from the keychain. It is
// distinct from a backend failure (locked keychain, missing dbus, ...), so a
// caller can tell "never paired" from "keychain unavailable" via errors.Is.
var ErrNotFound = errors.New("secrets: not found")

// ErrUnavailable marks a failure of the keychain backend itself - no Secret Service
// on the D-Bus session bus, a locked keychain, an unsupported platform - as opposed
// to an absent item (ErrNotFound) or a value the backend refused for its size. It is
// wrapped alongside the backend's own error, so the message keeps the detail and a
// caller can still tell, via errors.Is, that the fix is on the host, not in synosync.
var ErrUnavailable = errors.New("system keychain unavailable")

// backendErr wraps a keychain failure from op on account, marking it ErrUnavailable
// unless it is a size refusal, which no host change cures.
func backendErr(op, account string, err error) error {
	if errors.Is(err, keyring.ErrSetDataTooBig) {
		return fmt.Errorf("secrets: %s %q: %w", op, account, err)
	}
	return fmt.Errorf("secrets: %s %q: %w: %w", op, account, ErrUnavailable, err)
}

// Keyring is a thin, testable wrapper over the process-global go-keyring backend.
// The zero value is not ready for use; construct one with New.
type Keyring struct {
	// service is the keychain service name; defaults to ServiceName.
	service string
}

// New returns a Keyring backed by the system keychain under ServiceName.
func New() *Keyring {
	return &Keyring{service: ServiceName}
}

// account keys. Each item category gets its own prefix so the same server/user
// pair can hold a session, a password and (per-server) a pinned cert without
// overwriting one another.
func sessionKey(server, user string) string  { return "session/" + server + "/" + user }
func passwordKey(server, user string) string { return "password/" + server + "/" + user }
func pinKey(server string) string            { return "pin/" + server }

// get reads a single keychain item, mapping the backend's not-found sentinel to
// ErrNotFound and wrapping any other failure with context.
func (k *Keyring) get(account string) (string, error) {
	v, err := keyring.Get(k.service, account)
	if errors.Is(err, keyring.ErrNotFound) {
		return "", fmt.Errorf("%w: %s", ErrNotFound, account)
	}
	if err != nil {
		return "", backendErr("read", account, err)
	}
	return v, nil
}

// set writes a single keychain item, overwriting any existing value.
func (k *Keyring) set(account, value string) error {
	if err := keyring.Set(k.service, account, value); err != nil {
		return backendErr("write", account, err)
	}
	return nil
}

// del removes a single keychain item. Deleting an absent item maps to
// ErrNotFound; any other failure is wrapped.
func (k *Keyring) del(account string) error {
	err := keyring.Delete(k.service, account)
	if errors.Is(err, keyring.ErrNotFound) {
		return fmt.Errorf("%w: %s", ErrNotFound, account)
	}
	if err != nil {
		return backendErr("delete", account, err)
	}
	return nil
}

// SetSession stores the reusable session token for a server/user pair.
func (k *Keyring) SetSession(server, user, token string) error {
	return k.set(sessionKey(server, user), token)
}

// Session returns the stored session token for a server/user pair, or ErrNotFound
// if none has been paired.
func (k *Keyring) Session(server, user string) (string, error) {
	return k.get(sessionKey(server, user))
}

// DeleteSession removes the stored session token for a server/user pair.
func (k *Keyring) DeleteSession(server, user string) error {
	return k.del(sessionKey(server, user))
}

// SetPin stores the pinned certificate fingerprint for a server.
func (k *Keyring) SetPin(server, fingerprint string) error {
	return k.set(pinKey(server), fingerprint)
}

// Pin returns the pinned certificate fingerprint for a server, or
// ErrNotFound if the server has not been paired.
func (k *Keyring) Pin(server string) (string, error) {
	return k.get(pinKey(server))
}

// DeletePin removes the pinned certificate fingerprint for a server.
func (k *Keyring) DeletePin(server string) error {
	return k.del(pinKey(server))
}

// SetLegacyPassword writes a stored account password.
//
// Nothing in synosync calls it: the password is never persisted (DESIGN §9), and a public
// SetPassword sitting here invited exactly the change that doctrine forbids. It survives under
// a name that says so, because the CLEANUP path still has to be testable - an install carrying
// an entry from the version that did persist one must be able to shed it, and a test proving
// that has to be able to put the entry there.
func (k *Keyring) SetLegacyPassword(server, user, password string) error {
	return k.set(passwordKey(server, user), password)
}

// Password returns the stored account password for a server/user pair, or
// ErrNotFound if none was saved.
func (k *Keyring) Password(server, user string) (string, error) {
	return k.get(passwordKey(server, user))
}

// DeletePassword removes the stored account password for a server/user pair.
func (k *Keyring) DeletePassword(server, user string) error {
	return k.del(passwordKey(server, user))
}
