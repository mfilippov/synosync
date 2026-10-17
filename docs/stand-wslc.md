# A Windows stand: virtual-dsm under wslc

Measured 11.10.2026 on Windows 11 (WSL 2.9.12, `wslc` 5.0.1.1), virtual-dsm image
v7.66 (DSM 7.2.2-72806, Synology Drive Server installed from the setup wizard).
This complements the podman recipe in [INTEGRATION.md](INTEGRATION.md) for a
workstation that has WSL but no Linux host.

## What wslc does not offer, and what replaces it

A `wslc` container gets no `--device`, no `--cap-add` and no `--privileged`, and its
VM exposes neither `/dev/kvm` nor the `vmx`/`svm` CPU flags. So:

- **No KVM:** `KVM=N` runs DSM under QEMU TCG. It is about ten times slower and
  fine for the harness; the first boot (download, install, setup) takes a few minutes.
- **No TUN / NET_ADMIN:** NAT mode is impossible, and `NETWORK=passt` with
  `USER_PORTS=6690` is the only network mode. The boot log says `Mode: User (passt)`.

```powershell
wslc volume create dsm-storage
wslc run -d --name dsm -e DISK_SIZE=64G -e RAM_SIZE=4G -e CPU_CORES=4 `
  -e KVM=N -e NETWORK=passt -e USER_PORTS=6690 `
  -p 5000:5000 -p 6690:6690 -v dsm-storage:/storage --stop-timeout 120 `
  docker.io/vdsm/virtual-dsm
```

The setup wizard on `http://127.0.0.1:5000/` was driven with `agent-browser
--executable-path <msedge.exe>`. The steps, in order: device name plus an
administrator account, manual updates, skip the Synology Account, no analytics,
**Install Now** for Synology Drive Server and Office (this also enables the home
service), and **No, thanks** to 2FA. 2FA must stay off: the harness pairs two or
three devices with one password. The administrator account doubles as
`SYNOSYNC_IT_USER`.

## The passt `-race` stall does not reproduce here

INTEGRATION.md requires NAT mode, because passt dropped in-window TCP segments
under a slow reader (measured 02.08.2026 on a rootless-podman stand). On this
stand, with passt `2026_07_28.f8df3f1` and the same image family:

| run | result |
|---|---|
| full harness, plain | 49/49 pass once the emptied-folder fix landed (before it: 42/49, all 7 failures from that guard) |
| full harness, `-race` | **49/49 pass**, 526 s, no data race |
| LargeFileResume + KeepBothColdStart + ServerToLocal, `-race -count=8` | **24/24 pass**, slowest 4.2 s |
| same two-transfer pair with the 2 ms-per-read throttle that stalled the old stand at 186 s | **4/4 pass**, ~25 s each, no watchdog fire |

So the drop is a property of that stand's relay path, not of passt as shipped here
and not of the client. NAT mode stays the requirement for a podman stand, where the
failure was measured. A wslc stand is valid as it is, provided its boot log names
the passt build above or a later one.
