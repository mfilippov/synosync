//go:build !linux && !darwin && !windows

package main

// keychainRequirement says where synosync keeps its secrets on this platform and what a
// host needs for that to work; keyringHint shows it when the keychain backend fails.
const keychainRequirement = "synosync keeps its session token and certificate pin in the system keychain, " +
	"which this platform does not provide"
