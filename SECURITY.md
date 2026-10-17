# Security policy

synosync handles an account password at pairing, keeps a session token in the
system keychain, and reads and writes your files. A flaw in any of that matters,
so please report it privately.

## Reporting a vulnerability

Use GitHub's private reporting:
[Report a vulnerability](https://github.com/mfilippov/synosync/security/advisories/new).
Please do not open a public issue for it.

Include what you can:

- the version (`synosync version`) and the OS;
- what an attacker needs (network position, local access, a crafted server reply,
  a crafted file name) and what they get;
- steps or a proof of concept, and whether it needs a real DSM.

You will get an acknowledgement, and the fix is coordinated with you before anything
is published. Credit is given in the release notes unless you prefer otherwise.

## Scope

In scope: the client in this repository, including the wire protocol code, pairing
and certificate pinning, the keychain use, the local control channel, path handling
for names a server supplies, and the release artifacts and their signatures.

Out of scope: Synology DSM, Synology Drive Server and the official Drive Client.
Report those to [Synology](https://www.synology.com/security).

## Supported versions

Fixes go into the latest release. There are no long-term support branches yet.

## What the client is designed to resist

The design assumptions behind these defences are in
[docs/DESIGN.md](docs/DESIGN.md) §9 and [docs/INVARIANTS.md](docs/INVARIANTS.md):

- TLS only, with the server's public key pinned at pairing; a changed key is refused.
- The password is never stored; the session token and the pin live in the keychain.
- The control channel is local only: a 0600 Unix socket or a named pipe restricted
  to the current user.
- Paths from the server are untrusted: containment-checked, resolved without
  following links, and refused when a component would redirect.
- A delete or overwrite must prove the target is the last agreed version; anything
  else is kept as a `_Conflict` copy.
