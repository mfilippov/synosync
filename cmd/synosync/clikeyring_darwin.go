package main

// keychainRequirement says where synosync keeps its secrets on this platform and what a
// host needs for that to work; keyringHint shows it when the keychain backend fails.
const keychainRequirement = "synosync keeps its session token and certificate pin in your login keychain, " +
	"which must be unlocked. It is in a logged-in desktop session; over SSH it may not be " +
	"(`security unlock-keychain`)"
