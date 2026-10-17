package main

// keychainRequirement says where synosync keeps its secrets on this platform and what a
// host needs for that to work; keyringHint shows it when the keychain backend fails.
const keychainRequirement = "synosync keeps its session token and certificate pin in the Secret Service " +
	"on the D-Bus session bus (GNOME Keyring, KWallet or KeePassXC). A headless machine, an SSH " +
	"session without a session bus, or a keyring that is still locked has none: log in to a desktop " +
	"session, or start and unlock a Secret Service provider, and retry"
