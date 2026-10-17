package main

// keychainRequirement says where synosync keeps its secrets on this platform and what a
// host needs for that to work; keyringHint shows it when the keychain backend fails.
const keychainRequirement = "synosync keeps its session token and certificate pin in Windows Credential " +
	"Manager, in the profile of the user running it; a service must run as the user who paired"
