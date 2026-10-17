<#
.SYNOPSIS
    Install, uninstall, or query the synosync daemon as a Windows service.

.DESCRIPTION
    Registers the synosync daemon as a Windows service using the built-in
    sc.exe service controller. The service is configured to start the binary
    with `daemon`, logging to the rotating per-user log file (under SCM there is
    no console, so stderr would be discarded - the daemon forces the file sink in
    that mode regardless of any --log-stderr flag).

    The synosync.exe binary natively speaks the Service Control Manager
    dispatcher protocol when launched by SCM (it detects this via
    svc.IsWindowsService and runs svc.Run), reporting Running/Stopped and
    stopping cleanly on a Stop/Shutdown control; the same binary still runs in
    the foreground when started from a console. Without that dispatcher SCM
    would kill a plain foreground process after the start timeout with error
    1053 - no NSSM/WinSW wrapper is required.

    Required privileges:
      Creating, deleting, starting, and stopping a Windows service requires an
      elevated (Administrator) PowerShell session. Run this script from a
      console launched with "Run as administrator"; otherwise sc.exe returns
      "Access is denied" (error 5).

    "Log on as a service" (SeServiceLogonRight):
      A service that runs under a named account needs that account to hold the
      SeServiceLogonRight privilege, or SCM refuses to start it with error 1069
      ("The service did not start due to a logon failure"). Creating the service
      does NOT grant it: the services.msc snap-in grants the right as a separate
      step behind the scenes, but the CreateService API that New-Service calls
      does not. This script therefore grants it itself (LsaAddAccountRights, via
      an inline P/Invoke - no external module) whenever -User is passed, which is
      idempotent: the call succeeds whether or not the account already holds it.
      Uninstall deliberately does NOT revoke the right - the account may well be
      running other services, and revoking a privilege this script did not
      necessarily introduce would break them.

    IPC note (DESIGN §8):
      The daemon's control plane (used by `synosync status`/`sync`/`pause`/…)
      listens on a named pipe whose name is derived from the per-user state
      directory, with a protected DACL granting access to exactly the current
      user's SID (kernel-enforced, unlike POSIX file permissions). Because the
      pipe name and the daemon's paired state both live under the per-user
      profile, run the service under the same user account whose synosync install
      you paired; the default LocalSystem account would neither see that per-user
      state nor match the pipe's DACL, so the CLI control verbs could not reach
      it. Pass -User / -Password (or reconfigure the service afterwards with
      `sc.exe config`) to run as the paired user.

.PARAMETER Action
    One of: install, uninstall, status. Defaults to status.

.PARAMETER BinPath
    Full path to synosync.exe. Defaults to synosync.exe next to this script's
    parent (..\synosync.exe), then falls back to "synosync.exe" on PATH.

.PARAMETER ServiceName
    The Windows service name. Defaults to "synosync".

.PARAMETER User
    Optional account to run the service as (e.g. ".\\alice"). When omitted the
    service runs as LocalSystem - see the IPC note above. The account is granted
    "Log on as a service" as part of the install (see Required privileges).
    Requires -Password.

.PARAMETER Password
    Password for -User (required with it, and meaningless without it), as a
    SecureString. Supply it without echoing to the
    console - e.g. (Read-Host -AsSecureString) - so the plaintext never appears
    on a command line, in the shell history, or in another process's view of the
    argument list. The service is created with New-Service -Credential, which
    hands the secret to the SCM via a PSCredential rather than sc.exe's plaintext
    "password=" argument.

.EXAMPLE
    # From an elevated PowerShell (prompts for the password without echoing):
    .\install-service.ps1 install -BinPath C:\Tools\synosync\synosync.exe -User .\alice -Password (Read-Host -AsSecureString)

.EXAMPLE
    .\install-service.ps1 status

.EXAMPLE
    .\install-service.ps1 uninstall
#>

[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('install', 'uninstall', 'status', 'start', 'stop', 'restart')]
    [string]$Action = 'status',

    [string]$BinPath,

    [string]$ServiceName = 'synosync',

    [string]$User,

    [System.Security.SecureString]$Password
)

$ErrorActionPreference = 'Stop'

function Resolve-BinPath {
    param([string]$Candidate)

    if ($Candidate) {
        if (-not (Test-Path -LiteralPath $Candidate)) {
            throw "synosync.exe not found at '$Candidate'."
        }
        return (Resolve-Path -LiteralPath $Candidate).Path
    }

    # Prefer a binary that ships next to the packaging dir (..\synosync.exe).
    $sibling = Join-Path (Split-Path -Parent $PSScriptRoot) 'synosync.exe'
    if (Test-Path -LiteralPath $sibling) {
        return (Resolve-Path -LiteralPath $sibling).Path
    }

    $onPath = Get-Command 'synosync.exe' -ErrorAction SilentlyContinue
    if ($onPath) {
        return $onPath.Source
    }

    throw "Could not locate synosync.exe. Pass -BinPath explicitly."
}

# Assert-CredentialArgs rejects the two half-specified run-as forms. It runs
# BEFORE the elevation check on purpose: a mistyped argument list is the
# operator's error, not a privilege problem, so it should be reported without
# first demanding an elevated console (and it stays testable unelevated).
#
# -Password without -User is rejected rather than ignored: silently installing as
# LocalSystem is the one outcome the IPC note says does not work - the control
# verbs could not reach the daemon - so a password typed with no account is far
# more likely a typo than an intent to run as LocalSystem.
function Assert-CredentialArgs {
    param([string]$User, [System.Security.SecureString]$Password)

    if ($User -and -not $Password) {
        throw "Running the service as '$User' requires -Password (a SecureString, e.g. (Read-Host -AsSecureString))."
    }
    if ($Password -and -not $User) {
        throw "-Password was given without -User; pass the run-as account too (e.g. -User .\alice), or omit both to run as LocalSystem."
    }
}

# Initialize-LsaRightsType compiles the LSA account-rights P/Invoke shim once per
# session. It is self-contained C# via Add-Type - deliberately not an external
# module (Carbon/PSDscResources), so a fresh machine needs no gallery access.
function Initialize-LsaRightsType {
    if ('SynoSync.LsaRights' -as [type]) {
        return
    }

    Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;

namespace SynoSync {
    public static class LsaRights {
        [StructLayout(LayoutKind.Sequential)]
        private struct LSA_UNICODE_STRING {
            public ushort Length;
            public ushort MaximumLength;
            public IntPtr Buffer;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct LSA_OBJECT_ATTRIBUTES {
            public int Length;
            public IntPtr RootDirectory;
            public IntPtr ObjectName;
            public uint Attributes;
            public IntPtr SecurityDescriptor;
            public IntPtr SecurityQualityOfService;
        }

        private const uint STATUS_SUCCESS = 0;
        private const uint STATUS_OBJECT_NAME_NOT_FOUND = 0xC0000034;
        private const uint POLICY_VIEW_LOCAL_INFORMATION = 0x00000001;
        private const uint POLICY_CREATE_ACCOUNT = 0x00000010;
        private const uint POLICY_LOOKUP_NAMES = 0x00000800;

        [DllImport("advapi32.dll")]
        private static extern uint LsaOpenPolicy(IntPtr systemName, ref LSA_OBJECT_ATTRIBUTES objectAttributes, uint desiredAccess, out IntPtr policyHandle);

        [DllImport("advapi32.dll")]
        private static extern uint LsaAddAccountRights(IntPtr policyHandle, byte[] accountSid, LSA_UNICODE_STRING[] userRights, uint countOfRights);

        [DllImport("advapi32.dll")]
        private static extern uint LsaEnumerateAccountRights(IntPtr policyHandle, byte[] accountSid, out IntPtr userRights, out uint countOfRights);

        [DllImport("advapi32.dll")]
        private static extern uint LsaClose(IntPtr policyHandle);

        [DllImport("advapi32.dll")]
        private static extern uint LsaFreeMemory(IntPtr buffer);

        [DllImport("advapi32.dll")]
        private static extern int LsaNtStatusToWinError(uint status);

        private static IntPtr OpenPolicy(uint access) {
            LSA_OBJECT_ATTRIBUTES attrs = new LSA_OBJECT_ATTRIBUTES();
            attrs.Length = Marshal.SizeOf(typeof(LSA_OBJECT_ATTRIBUTES));
            IntPtr handle;
            uint status = LsaOpenPolicy(IntPtr.Zero, ref attrs, access, out handle);
            if (status != STATUS_SUCCESS) {
                throw new Win32Exception(LsaNtStatusToWinError(status));
            }
            return handle;
        }

        // Has reports whether the account already holds the right. An account
        // with no rights at all answers STATUS_OBJECT_NAME_NOT_FOUND rather than
        // an empty list - that is a "no", not a failure.
        public static bool Has(byte[] accountSid, string right) {
            IntPtr policy = OpenPolicy(POLICY_VIEW_LOCAL_INFORMATION | POLICY_LOOKUP_NAMES);
            try {
                IntPtr rights;
                uint count;
                uint status = LsaEnumerateAccountRights(policy, accountSid, out rights, out count);
                if (status == STATUS_OBJECT_NAME_NOT_FOUND) {
                    return false;
                }
                if (status != STATUS_SUCCESS) {
                    throw new Win32Exception(LsaNtStatusToWinError(status));
                }
                try {
                    int stride = Marshal.SizeOf(typeof(LSA_UNICODE_STRING));
                    for (uint i = 0; i < count; i++) {
                        IntPtr at = new IntPtr(rights.ToInt64() + (long)i * stride);
                        LSA_UNICODE_STRING item = (LSA_UNICODE_STRING)Marshal.PtrToStructure(at, typeof(LSA_UNICODE_STRING));
                        string name = Marshal.PtrToStringUni(item.Buffer, item.Length / 2);
                        if (string.Equals(name, right, StringComparison.OrdinalIgnoreCase)) {
                            return true;
                        }
                    }
                } finally {
                    LsaFreeMemory(rights);
                }
                return false;
            } finally {
                LsaClose(policy);
            }
        }

        // Grant is idempotent: LsaAddAccountRights succeeds whether or not the
        // account already holds the right.
        public static void Grant(byte[] accountSid, string right) {
            IntPtr policy = OpenPolicy(POLICY_CREATE_ACCOUNT | POLICY_LOOKUP_NAMES);
            LSA_UNICODE_STRING[] rights = new LSA_UNICODE_STRING[1];
            rights[0].Buffer = Marshal.StringToHGlobalUni(right);
            rights[0].Length = (ushort)(right.Length * 2);
            rights[0].MaximumLength = (ushort)(rights[0].Length + 2);
            try {
                uint status = LsaAddAccountRights(policy, accountSid, rights, 1);
                if (status != STATUS_SUCCESS) {
                    throw new Win32Exception(LsaNtStatusToWinError(status));
                }
            } finally {
                Marshal.FreeHGlobal(rights[0].Buffer);
                LsaClose(policy);
            }
        }
    }
}
'@
}

# Resolve-AccountSidBytes turns a run-as account name into its binary SID, which
# is what the LSA account-rights APIs take. ".\name" is the form the examples and
# sc.exe use; NTAccount does not understand it, so it is expanded to the local
# machine name first.
function Resolve-AccountSidBytes {
    param([Parameter(Mandatory)][string]$Account)

    $name = $Account
    if ($name.StartsWith('.\')) {
        $name = '{0}\{1}' -f $env:COMPUTERNAME, $name.Substring(2)
    }
    $sid = (New-Object System.Security.Principal.NTAccount($name)).Translate([System.Security.Principal.SecurityIdentifier])
    $bytes = New-Object byte[] $sid.BinaryLength
    $sid.GetBinaryForm($bytes, 0)
    # Comma-wrap: PowerShell would otherwise unroll the byte array onto the
    # pipeline and the caller would receive N bytes instead of one array.
    return , $bytes
}

# Grant-ServiceLogonRight ensures the run-as account can be used by SCM. It is
# best-effort: a failure here is reported with the manual remedy rather than
# aborting an otherwise-created service, because the operator may have granted
# the right already by policy (GPO) in a way this account cannot read back.
function Grant-ServiceLogonRight {
    param([Parameter(Mandatory)][string]$Account)

    $right = 'SeServiceLogonRight'
    try {
        Initialize-LsaRightsType
        $sid = Resolve-AccountSidBytes -Account $Account
        if ([SynoSync.LsaRights]::Has($sid, $right)) {
            Write-Host "Account '$Account' already holds 'Log on as a service' ($right)."
            return
        }
        [SynoSync.LsaRights]::Grant($sid, $right)
        Write-Host "Granted 'Log on as a service' ($right) to '$Account'."
    } catch {
        Write-Warning ("Could not grant 'Log on as a service' ($right) to '{0}': {1}. " -f $Account, $_.Exception.Message +
            "If the account does not already hold it, Start-Service will fail with error 1069 - grant it via " +
            "secpol.msc (Local Policies > User Rights Assignment > Log on as a service) and start the service again.")
    }
}

function Assert-Admin {
    $id = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object System.Security.Principal.WindowsPrincipal($id)
    if (-not $principal.IsInRole([System.Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "This action requires an elevated (Administrator) PowerShell session."
    }
}

function Invoke-Install {
    Assert-CredentialArgs -User $User -Password $Password
    Assert-Admin
    $bin = Resolve-BinPath -Candidate $BinPath

    # The binary path must include the daemon args. No --log-stderr: under SCM there
    # is no console, so stderr is discarded; the daemon logs to the rotating
    # per-user log file (it forces that sink under SCM even if --log-stderr is set).
    $binPathArg = '"{0}" daemon' -f $bin

    # New-Service is used in place of `sc.exe create` so a run-as account password
    # is handed to the SCM via a PSCredential (a SecureString under the hood) rather
    # than appearing as plaintext in `sc.exe ... password=<secret>` - that command
    # line is otherwise visible to every process able to list arguments and may land
    # in shell history.
    $newServiceArgs = @{
        Name           = $ServiceName
        BinaryPathName = $binPathArg
        StartupType    = 'Automatic'
        DisplayName    = 'synosync - Synology Drive sync daemon'
        Description    = 'synosync two-way Synology Drive sync daemon.'
        ErrorAction    = 'Stop'
    }
    if ($User) {
        $newServiceArgs['Credential'] = New-Object System.Management.Automation.PSCredential($User, $Password)
    }

    Write-Host "Creating service '$ServiceName' -> $binPathArg"
    New-Service @newServiceArgs | Out-Null

    if ($User) {
        # CreateService (which New-Service calls) does NOT grant the run-as
        # account "Log on as a service" - services.msc does that as a separate
        # step. Without it Start-Service fails with 1069 on a fresh machine, so
        # the documented -User install below would never come up.
        Grant-ServiceLogonRight -Account $User
    }

    # Restart on failure: two restarts 5s apart, then nothing. (sc.exe failure
    # carries no secret, so it stays the simplest way to set this.)
    #
    # The trailing empty slot is load-bearing. SERVICE_FAILURE_ACTIONS does NOT
    # stop after the last array entry: SCM repeats the LAST action for every
    # subsequent failure (services.msc spells this out - its third row is labelled
    # "Subsequent failures"). So the obvious `restart/5000/restart/5000/restart/5000`
    # is an INFINITE 5s restart loop, not a budget of three. An empty action
    # (SC_ACTION_NONE) as the last entry is what actually bounds it: failures 1 and
    # 2 restart, failure 3 and every one after it do nothing until the failure
    # count resets.
    #
    # `reset= 86400` is the counter's reset window: the count clears only after a
    # full day with no failures. Because the actions above stop the loop, that day
    # is measured against a *stopped* service, so a terminal error stays stopped
    # and visible - the closest sc.exe gets to systemd's `failed` state, which
    # likewise needs an operator (`sc.exe start synosync`, vs `systemctl
    # reset-failed`) once the cause is fixed.
    & sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/5000// | Out-Null

    # Without failureflag those actions fire only on a true crash (the process
    # dying without reporting STOPPED). This daemon's dispatcher always reports
    # STOPPED with a service-specific exit code - even for a terminal error such
    # as a vanished session - so a non-zero exit would otherwise never restart.
    # That code reaches SCM because Execute RETURNS it and never reports Stopped
    # itself (cmd/synosync/service_windows.go): a handler-sent Stopped would be
    # published with NO_ERROR, and this restart arc would be dead.
    # failureflag 1 makes SCM treat any stop with a non-zero exit code as a
    # failure too, matching systemd's Restart=on-failure (which likewise
    # restarts on terminal re-pair errors). Together with the empty last action
    # above this bounds the loop the way the unit's StartLimitBurst does: after
    # two restarts SCM does nothing further until the reset window elapses, so a
    # terminal error stays visible while the operator re-pairs instead of
    # hammering the NAS with a full handshake every 5 s.
    & sc.exe failureflag $ServiceName 1 | Out-Null

    Write-Host "Starting service '$ServiceName'..."
    try {
        Start-Service -Name $ServiceName -ErrorAction Stop
    } catch {
        # Name the run-as failure mode explicitly. "Check the event log" alone
        # sent operators looking at pairing state for what is a privilege
        # problem: 1069 is SCM refusing the LOGON, before the daemon ever runs.
        $hint = 'Check the event log / pairing state.'
        if ($User) {
            $hint = ("If this is error 1069 (""The service did not start due to a logon failure""), '{0}' lacks " -f $User) +
                "'Log on as a service' or the password is wrong - the grant above reports which. " +
                "Otherwise check the event log / pairing state."
        }
        Write-Warning "Service created but did not start ($($_.Exception.Message)). $hint"
    }
}

function Invoke-Uninstall {
    Assert-Admin

    # Best-effort stop before delete; ignore "not started" errors.
    & sc.exe stop $ServiceName | Out-Null
    Start-Sleep -Seconds 1

    # Note: the run-as account keeps "Log on as a service" (SeServiceLogonRight).
    # Revoking it here would be wrong - the account may have held it before this
    # install (GPO, another service), and this script cannot tell, so a revoke
    # could stop unrelated services. Remove it by hand via secpol.msc if the
    # account was created for synosync alone.
    Write-Host "Deleting service '$ServiceName'..."
    & sc.exe delete $ServiceName
    if ($LASTEXITCODE -ne 0) {
        throw "sc.exe delete failed (exit $LASTEXITCODE)."
    }
}

function Invoke-Status {
    & sc.exe query $ServiceName
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Service '$ServiceName' is not installed."
    }
}

# Start, Stop and Restart drive an already-registered service, so a settings change no longer
# means uninstalling and reinstalling around it. They need the same elevation the install does:
# stopping a service is an administrative right unless its DACL grants it, so an access-denied
# failure here is expected for a normal shell and says so.
function Assert-Installed {
    & sc.exe query $ServiceName > $null 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Service '$ServiceName' is not installed; run this script with -Action install first."
    }
}

function Invoke-Start {
    Assert-Installed
    Start-Service -Name $ServiceName
    Write-Host "Service '$ServiceName' started."
}

function Invoke-Stop {
    Assert-Installed
    # Already stopped is the state the caller asked for, not a failure.
    $svc = Get-Service -Name $ServiceName
    if ($svc.Status -eq 'Stopped') {
        Write-Host "Service '$ServiceName' is already stopped."
        return
    }
    Stop-Service -Name $ServiceName
    Write-Host "Service '$ServiceName' stopped."
}

function Invoke-Restart {
    Assert-Installed
    Restart-Service -Name $ServiceName
    Write-Host "Service '$ServiceName' restarted."
}

# Dispatch only when the script is EXECUTED. Dot-sourcing (InvocationName '.')
# loads the functions without side effects, which is how install-service_test.ps1
# drives the argument and SID logic in-process - with no service touched and no
# elevation needed.
if ($MyInvocation.InvocationName -ne '.') {
    switch ($Action) {
        'install'   { Invoke-Install }
        'uninstall' { Invoke-Uninstall }
        'status'    { Invoke-Status }
        'start'     { Invoke-Start }
        'stop'      { Invoke-Stop }
        'restart'   { Invoke-Restart }
    }
}
