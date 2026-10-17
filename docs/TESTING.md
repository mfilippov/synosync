# Product testing guide

This guide is for people trying synosync before its first release: how to get a
build, set it up, what to exercise, what "correct" looks like, and how to report
what you find. It assumes no knowledge of the code. The developer-side test
harness is a separate document, [INTEGRATION.md](INTEGRATION.md).

## Before you start: protect your data

synosync is pre-release software that **deletes and overwrites files** as part of
normal two-way sync. Its design keeps every diverged copy (see "Conflicts" below),
but a pre-release is exactly where an unknown bug lives. So:

- **Use a test account or a test shared folder**, not the only copy of anything.
- **Make sure the Synology Drive recycle bin is enabled** for the shared folders you
  sync (DSM → Control Panel → Shared Folder → Edit → *Enable Recycle Bin*). synosync
  keeps no local trash; the server recycle bin is the recovery path for a deletion.
- Point the sync root at a **new, empty local directory**.
- Do not run the official Synology Drive Client and synosync **on the same local
  folder**. Running both against the same *account* from different folders or
  machines is fine, and useful (see scenario 9).

## Getting a build

Every push to the repository builds archives for six targets: Linux, macOS and
Windows, each for amd64 and arm64. A `v*` tag publishes them, with a
`SHA256SUMS` file and build-provenance attestation, as a GitHub release. Between
releases, the same archives are attached to each CI run as the `release-archives`
artifact.

Each archive holds the `synosync` binary, `LICENSE`/`NOTICE`, and `packaging/` with
the platform's service unit.

- **Linux:** unpack and put `synosync` on your `PATH`, e.g. `~/.local/bin`.
- **macOS:** the binary is not notarized. After unpacking, clear the quarantine flag
  once: `xattr -d com.apple.quarantine ./synosync`.
- **Windows:** unpack anywhere stable, e.g. `C:\Tools\synosync\`. Windows may show a
  SmartScreen prompt on first run.

Check what you are running with `synosync version`. It prints the version, the Go
toolchain and the platform, e.g. `synosync 0.1.0-rc.1 (go1.27.2, linux/amd64)`.
Quote that line in every report.

### Client requirements

synosync keeps its session token and certificate pin in the system keychain:
Windows Credential Manager, the macOS login keychain, or on Linux a Secret
Service provider (GNOME Keyring, KWallet, KeePassXC) on the D-Bus session bus.
A **headless Linux machine** has no Secret Service, so pairing fails there with a
hint saying so. Test Linux on a desktop session.

### Server requirements

- DSM 7.2 with **Synology Drive Server** installed (measured on Drive Server 4.0.3).
- The Drive client port, **6690** by default, reachable from the test machine.
  This is not the DSM web port (5000/5001).
- An account with access to at least one Drive view (its `home`, or a team folder).
  For a 2FA account, `pair` asks for the one-time code.

## Setting up

```sh
synosync pair --host nas.example.com --user alice --sync-root ~/DriveTest
```

`pair` asks for the password, which is never stored, and, when the account sees
more than one view, which views to sync. Several views each get a subdirectory;
a single view syncs **directly** into the sync root.

If the NAS uses its own self-signed certificate, or you pair by IP address, `pair`
says the certificate is not trusted and shows its key before asking for anything
else. Answer `y` if it is your NAS; `--accept-untrusted` does the same in a script.

Run the daemon in the foreground first, so you can watch it:

```sh
synosync daemon            # Ctrl-C to stop
synosync status            # from another terminal
```

Once the first sync has finished, you can install it as a background service
instead (`synosync service install`; on Windows it prints the elevated command to
run). See the README section "Running it in the background".

For a test session, debug logging is worth turning on. Settings change only while
the daemon is stopped:

```sh
synosync config set log_level debug
```

## What to watch

| Command | Tells you |
|---|---|
| `synosync status` | first line: up to date, syncing (items to go and a time estimate), first sync, paused or not connected; then the current rate, what this run moved, the folder, what is left out, and **Conflicts**, **Unresolved**, **Retrying** and **Skipped** when there are any. `status -v` adds the internals for a bug report |
| `synosync conflicts` | the keep-both `_Conflict` copies created so far |
| `synosync drops` | work the engine gave up on, each with the path and the reason |
| `synosync log -n 200 -f` | the daemon log; the Linux systemd service logs to `journalctl --user -u synosync` instead |

`status`, `conflicts` and `drops` accept `--json`.

A healthy idle client shows `connected`, zero queued operations, and no Stuck or
Dropped lines. Stuck and Dropped are not failures in themselves, since some are
deliberate safety refusals. But each one deserves a look at `drops` and the log.

## Scenarios

Work through these in order; each builds on the previous state. "Server side"
means the DSM web UI (Synology Drive web portal), another machine running the
official client, or a second synosync install.

1. **First sync, server → local.** Put a few hundred files, a nested directory tree
   and one large file (over 1 GB) on the server before pairing. Pair and start the
   daemon. *Expect:* everything arrives with matching content and modification
   times; `status` shows progress during the pull.
2. **Local → server.** Create, edit and delete files locally. *Expect:* each change
   appears on the server within seconds of the change, or at worst at the next
   periodic scan, a few seconds by default.
3. **Server → local edits.** Create, edit, rename, move and delete on the server.
   *Expect:* mirrored locally, renames as renames rather than a delete plus a new
   download.
4. **Directories.** Rename a directory with contents on each side; delete a
   directory with contents on each side. *Expect:* mirrored. Deleting a large
   directory locally is slow, about two server round trips per file, and can
   take minutes for thousands of files.
5. **Conflicts.** Stop the daemon (or pause with `synosync pause`). Edit the
   same file locally and on the server, then resume. *Expect:* both versions
   survive: one under the original name, the other as a `_Conflict` copy listed
   in `synosync conflicts`. Repeat with a local edit plus a server delete, and a
   server edit plus a local delete. *Expect:* the edit is never lost.
6. **Offline and reconnect.** Disconnect the network (or stop Drive Server) for a
   few minutes while changing files on both sides. *Expect:* `status` shows
   `reconnecting...`. After reconnect, all changes converge, with conflicts as in 5.
7. **Interrupted transfer.** Kill the daemon (not Ctrl-C: end the process) in the
   middle of a large upload, and again in the middle of a large download. Restart.
   *Expect:* the transfer completes, with no corrupt or zero-length files on either
   side and no stray temporary files left in the sync root.
8. **Names.** Unicode names (Cyrillic, CJK, emoji, accented letters typed on macOS),
   names differing only by case (`Report.txt` vs `report.txt`), and on a
   Linux or macOS peer, names Windows cannot hold (`notes.`, `a:b`, `CON`).
   *Expect:* nothing is silently renamed or overwritten. A name this platform cannot
   represent is skipped and reported (log, Dropped count), never mis-applied.
9. **A second client.** Run the official Synology Drive Client, or another
   synosync, on another machine against the same account, and edit on both.
   *Expect:* convergence; no ping-pong of the same change; conflicts as in 5.
10. **Pause, resume, restart.** `synosync pause`, change things, restart the daemon
    or the machine, then `synosync resume`. *Expect:* the pause survives the restart;
    nothing syncs until resume; everything converges after it.
11. **Service.** `synosync service install`, log out and back in (on Linux, also
    `loginctl enable-linger`), reboot. *Expect:* `synosync service status` reports
    it running and `synosync status` answers.
12. **Unlink and re-pair.** `synosync unlink`, then pair again into the same
    directory. *Expect:* with a single view, `pair` refuses because the directory
    is not empty. That refusal is correct: a fresh setup would upload everything in
    it. With several views, the re-paired client reconciles against the existing
    tree. Files identical on both sides are adopted with no transfer, local-only
    files are uploaded, and files that differ get a `_Conflict` copy. No file is
    deleted on either side.
13. **Scale.** A tree of 50 000+ small files, and a single file of several GB.
    *Expect:* steady progress in `status`, bounded memory, the daemon remains
    responsive to `status`.

## Known limitations (not bugs)

- **No local trash.** A deletion that syncs is recovered from the server recycle bin.
- **Rename vs delete.** If one side renames a file while the other deletes it, the
  delete wins and the file is gone on both, recoverable from the recycle bin.
- **Very large directory deletes are refused.** A directory with more than about
  50 000 entries cannot be deleted from the client; it stays and is listed under
  `drops`. Delete it from the DSM web UI.
- **Two names that differ only in Unicode normalization or case** in one directory
  (possible on Linux) are both left unsynced, and reported, until one is renamed.
- **Windows cannot hold some names** a Linux/macOS peer can create; those are skipped
  and reported. Renaming a *directory* on the server to a representable name does
  not bring its skipped contents along; move or re-create them.
- **What is not synced by default.** A new pairing leaves out the home view's
  `Photos` and `Backup` and files like desktop.ini, thumbs.db, .DS_Store, *.tmp and
  ~*, as the official client does (`synosync exclude list`). Files whose names
  start with a dot ARE synced. An install paired earlier gets the same set with
  `synosync exclude defaults`.
- **Exclusions are by path.** Renaming an excluded folder's parent (on either side)
  brings the folder back into the sync without fetching its contents. To keep it
  out, `exclude add` the new path; to fetch it, `exclude add` and then
  `exclude remove` the new path. A file you move locally INTO an
  excluded folder counts as deleted, and its server copy goes to the recycle bin.
- **Changing the view layout** (one view ↔ several, or which view is flat) after
  files are synced is refused; `unlink` and pair again.
- **Settings are read at daemon start.** `config set` needs the daemon stopped;
  restart it (or `synosync service restart`) afterwards.
- **Linux needs a desktop keyring.** No headless use. A lingering systemd service
  started at boot cannot read its session until the keyring is unlocked, which
  usually happens at login.
- **Linux needs `/proc`.** In a chroot or a stripped container without `/proc`,
  every download fails to finish.
- **A killed daemon can leave a partial download behind** as a hidden
  `.<name>.dl-<digits>` file next to the target. It is never synced, and the next
  run logs it once ("leftover partial download"). Delete it by hand: synosync does
  not, because a user file could carry the same name.
- **Swapped or unmounted sync roots** pause the upload side for that view, with a
  warning in the log only; `status` has no line for it yet.

The full list of accepted limits, with the reasoning, is DESIGN §13
([docs/DESIGN.md](DESIGN.md)).

## Reporting a problem

One report per problem, filed as a GitHub issue with the **Bug report** template,
which asks for the same things. Include:

1. **`synosync version`**: the whole line.
2. **OS and file system** of the sync root (e.g. Windows 11 / NTFS, macOS 15 / APFS,
   Ubuntu 24.04 / ext4), and DSM / Drive Server versions if you know them.
3. **What you did, what you expected, what happened**, with approximate times, so
   the log can be matched.
4. **Reports**, captured while the problem is visible:
   ```sh
   synosync status --json    > status.json
   synosync drops --json     > drops.json
   synosync conflicts --json > conflicts.json
   ```
5. **The log** around the time of the problem (`synosync log -n 2000 > synosync.log`),
   ideally with `log_level debug`.

The log and the reports contain **file and directory names** from your sync
root, and your server host and user name. They never contain your password,
one-time codes or session token. Look over them, and redact names you do not
want to share, before attaching.

**Data loss is the top-priority class.** That means a file gone or overwritten
without a `_Conflict` copy, or a deletion you did not make. If you see one, stop
the daemon at once (`Ctrl-C` or `synosync service stop`) so the state is
preserved. Note the path, and check the server recycle bin. Then report it,
together with `state.db` from the state directory (see "Where things live" in the
README) if you are willing to share it.
