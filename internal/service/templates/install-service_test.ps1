<#
.SYNOPSIS
    Self-test for install-service.ps1 - parser health and run-as argument logic.

.DESCRIPTION
    The sibling of scripts/coverage-gate_test.sh for the Windows packaging
    script. It never creates, starts, deletes or configures a service: every
    case exercises either the parser or the pure argument/SID logic, so it runs
    unelevated and leaves the machine untouched.

    Cases:
      1. install-service.ps1 (and this file) parse with 0 errors.
      2. Assert-CredentialArgs: the four -User/-Password combinations.
      3. The same two negative combinations end-to-end through a child process
         (non-zero exit, message names the missing argument) - this also pins
         that the check runs BEFORE the elevation check, so an unelevated
         operator is told about the typo rather than about privileges.
      4. Resolve-AccountSidBytes agrees with the current identity's SID for the
         bare, ".\" and "DOMAIN\" spellings.
      5. The LSA P/Invoke shim compiles and exposes Has/Grant. When the session
         may read LSA policy it also reads rights back: some stock-default
         (well-known SID, right) pair must report held, and a non-existent right
         must not. Which pairs a host actually carries is host policy, so the
         positive case skips rather than fails when none of them is granted, and
         the whole read-back skips when the token may not read policy at all
         (probed, never branched on the host).

    Exits 1 on the first failing expectation set, printing every failure.

.EXAMPLE
    pwsh -NoProfile -File .\install-service_test.ps1
#>

[CmdletBinding()]
param()

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:Failures = 0
$script:ScriptPath = Join-Path $PSScriptRoot 'install-service.ps1'

function Report-Fail {
    param([string]$Message)
    Write-Host "  FAIL: $Message"
    $script:Failures++
}

function Report-Ok {
    param([string]$Message)
    Write-Host "  ok: $Message"
}

function Report-Skip {
    param([string]$Message)
    Write-Host "  skip: $Message"
}

# Get-WellKnownSidBytes returns the binary form of a well-known SID string.
# The LSA case addresses accounts by SID rather than by name because
# NTAccount.Translate is localized: 'BUILTIN\Administrators' and
# 'NT AUTHORITY\NetworkService' do not resolve on a non-English Windows, which
# would fail the case for a reason that has nothing to do with the shim.
function Get-WellKnownSidBytes {
    param([Parameter(Mandatory)][string]$Sid)

    $identifier = New-Object System.Security.Principal.SecurityIdentifier($Sid)
    $bytes = New-Object byte[] $identifier.BinaryLength
    $identifier.GetBinaryForm($bytes, 0)
    # Comma-wrap so PowerShell hands the caller one array, not N bytes.
    return , $bytes
}

# --- Case 1: both scripts parse cleanly. ------------------------------------

function Test-Parses {
    foreach ($file in @($script:ScriptPath, $PSCommandPath)) {
        $errors = $null
        $tokens = $null
        [System.Management.Automation.Language.Parser]::ParseFile($file, [ref]$tokens, [ref]$errors) | Out-Null
        if ($errors -and $errors.Count -gt 0) {
            Report-Fail ("parser: {0} reported {1} error(s): {2}" -f (Split-Path -Leaf $file), $errors.Count, ($errors[0].Message))
            continue
        }
        Report-Ok ("parser: {0} - 0 errors" -f (Split-Path -Leaf $file))
    }
}

# --- Case 2: the run-as argument matrix, in-process. ------------------------

# Invoke-CredentialArgs runs Assert-CredentialArgs and returns the thrown
# message, or $null when it accepted the combination.
function Invoke-CredentialArgs {
    param([string]$User, [System.Security.SecureString]$Password)
    try {
        Assert-CredentialArgs -User $User -Password $Password
        return $null
    } catch {
        return $_.Exception.Message
    }
}

function New-DummySecret {
    return (ConvertTo-SecureString 'not-a-real-password' -AsPlainText -Force)
}

function Test-CredentialMatrix {
    $secret = New-DummySecret

    # Neither: LocalSystem install - accepted.
    $msg = Invoke-CredentialArgs -User $null -Password $null
    if ($null -ne $msg) {
        Report-Fail "credential matrix: neither argument should be accepted (LocalSystem), got: $msg"
    } else {
        Report-Ok 'credential matrix: no -User/-Password accepted (LocalSystem)'
    }

    # Both: accepted.
    $msg = Invoke-CredentialArgs -User '.\alice' -Password $secret
    if ($null -ne $msg) {
        Report-Fail "credential matrix: -User with -Password should be accepted, got: $msg"
    } else {
        Report-Ok 'credential matrix: -User with -Password accepted'
    }

    # -User alone: rejected, naming -Password.
    $msg = Invoke-CredentialArgs -User '.\alice' -Password $null
    if ($null -eq $msg) {
        Report-Fail 'credential matrix: -User without -Password must be rejected'
    } elseif ($msg -notmatch '-Password') {
        Report-Fail "credential matrix: -User without -Password must name -Password, got: $msg"
    } else {
        Report-Ok 'credential matrix: -User without -Password rejected'
    }

    # -Password alone: rejected, naming -User. Silently installing as
    # LocalSystem is the outcome the IPC note rules out.
    $msg = Invoke-CredentialArgs -User $null -Password $secret
    if ($null -eq $msg) {
        Report-Fail 'credential matrix: -Password without -User must be rejected'
    } elseif ($msg -notmatch '-User') {
        Report-Fail "credential matrix: -Password without -User must name -User, got: $msg"
    } else {
        Report-Ok 'credential matrix: -Password without -User rejected'
    }
}

# --- Case 3: the same negatives end-to-end, as a child process. -------------

function Get-HostExecutable {
    $current = (Get-Process -Id $PID).Path
    if ($current) {
        return $current
    }
    $pwsh = Get-Command 'pwsh.exe' -ErrorAction SilentlyContinue
    if ($pwsh) {
        return $pwsh.Source
    }
    return (Get-Command 'powershell.exe').Source
}

# Invoke-ScriptCase runs install-service.ps1 in a child process and returns
# @{ Code; Output }. -Command (not -File) is required: -Password takes a
# SecureString, which cannot be spelled on a -File argument list.
function Invoke-ScriptCase {
    param([Parameter(Mandatory)][string]$Arguments)

    $exe = Get-HostExecutable
    $command = "& '{0}' {1}" -f $script:ScriptPath, $Arguments
    # $ErrorActionPreference is pinned to Continue for the call. Windows PowerShell
    # 5.1 surfaces a native command's stderr as an ErrorRecord, so under the
    # script's own 'Stop' preference the 2>&1 merge below terminates the whole run
    # on the very output these negative cases exist to inspect. pwsh does not, which
    # is why this only showed up once the self-test started running on both hosts.
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & $exe -NoProfile -NonInteractive -Command $command 2>&1 | Out-String
    } finally {
        $ErrorActionPreference = $previous
    }
    return @{ Code = $LASTEXITCODE; Output = $output }
}

function Test-ScriptNegatives {
    $cases = @(
        @{
            Name      = "install -User without -Password"
            Arguments = "install -User '.\alice'"
            Needle    = '-Password'
        },
        @{
            Name      = "install -Password without -User"
            Arguments = "install -Password (ConvertTo-SecureString 'not-a-real-password' -AsPlainText -Force)"
            Needle    = '-User'
        }
    )

    foreach ($case in $cases) {
        $result = Invoke-ScriptCase -Arguments $case.Arguments
        if ($result.Code -eq 0) {
            Report-Fail ("{0}: exit 0, want non-zero (output: {1})" -f $case.Name, $result.Output.Trim())
            continue
        }
        if ($result.Output -notmatch [regex]::Escape($case.Needle)) {
            Report-Fail ("{0}: output does not name '{1}' (output: {2})" -f $case.Name, $case.Needle, $result.Output.Trim())
            continue
        }
        # The argument check must win over the elevation check: an operator who
        # mistyped should not be told to open an elevated console instead.
        if ($result.Output -match 'elevated') {
            Report-Fail ("{0}: elevation error reported before the argument error (output: {1})" -f $case.Name, $result.Output.Trim())
            continue
        }
        Report-Ok ("{0}: rejected with exit {1}" -f $case.Name, $result.Code)
    }
}

# --- Case 4: account name -> SID. -------------------------------------------

function Test-SidResolution {
    $me = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    $want = $me.User.Value
    $bare = $me.Name  # DOMAIN\user or MACHINE\user
    $leaf = $bare.Split('\')[-1]

    $spellings = @($bare, ".\$leaf")
    if ($env:COMPUTERNAME -and $bare -like "$env:COMPUTERNAME\*") {
        $spellings += $leaf   # a local account resolves bare, too
    }

    foreach ($spelling in $spellings) {
        try {
            $bytes = Resolve-AccountSidBytes -Account $spelling
        } catch {
            Report-Fail ("sid: '{0}' did not resolve: {1}" -f $spelling, $_.Exception.Message)
            continue
        }
        if ($bytes -isnot [byte[]]) {
            Report-Fail ("sid: '{0}' returned {1}, want a single byte[]" -f $spelling, $bytes.GetType().Name)
            continue
        }
        $got = (New-Object System.Security.Principal.SecurityIdentifier($bytes, 0)).Value
        if ($got -ne $want) {
            Report-Fail ("sid: '{0}' resolved to {1}, want {2}" -f $spelling, $got, $want)
            continue
        }
        Report-Ok ("sid: '{0}' -> {1}" -f $spelling, $got)
    }

    # A name that cannot be a principal must fail loudly, not return an empty SID.
    try {
        Resolve-AccountSidBytes -Account 'no-such-account-8f3a1c' | Out-Null
        Report-Fail 'sid: an unknown account must throw'
    } catch {
        Report-Ok 'sid: unknown account throws'
    }
}

# --- Case 5: the LSA shim. ---------------------------------------------------

function Test-LsaShim {
    Initialize-LsaRightsType
    $type = 'SynoSync.LsaRights' -as [type]
    if (-not $type) {
        Report-Fail 'lsa: SynoSync.LsaRights did not compile'
        return
    }
    foreach ($method in @('Has', 'Grant')) {
        if (-not $type.GetMethod($method)) {
            Report-Fail "lsa: SynoSync.LsaRights lacks $method"
            return
        }
    }
    Report-Ok 'lsa: shim compiles and exposes Has/Grant'

    # Compiling twice in one session must be a no-op, not a duplicate-type error.
    Initialize-LsaRightsType
    Report-Ok 'lsa: Initialize-LsaRightsType is idempotent'

    # A read-only probe that the enumerate path marshals a real right list.
    # Reading LSA policy needs a token that may do so (empirically: "Access is
    # denied" otherwise), so probe the call itself rather than branching on
    # IsInRole - if a host does allow the read, the case runs there too.
    #
    # LsaEnumerateAccountRights reports only EXPLICITLY assigned rights, and
    # which of them a host carries is host policy, not this script's behaviour:
    # the GitHub windows-latest image carries no explicit SeServiceLogonRight on
    # NETWORK SERVICE, so pinning that one account/right pair failed the case on
    # every CI run while the shim was working perfectly. Probe several
    # stock-default pairs instead and require only that SOME pair reads back as
    # held; if none does, skip - there is nothing left to prove about the shim.
    $probes = @(
        @{ Sid = 'S-1-5-32-544'; Right = 'SeSecurityPrivilege';      Who = 'BUILTIN\Administrators' },
        @{ Sid = 'S-1-5-32-544'; Right = 'SeTakeOwnershipPrivilege'; Who = 'BUILTIN\Administrators' },
        @{ Sid = 'S-1-5-20';     Right = 'SeServiceLogonRight';      Who = 'NT AUTHORITY\NetworkService' },
        @{ Sid = 'S-1-5-20';     Right = 'SeImpersonatePrivilege';   Who = 'NT AUTHORITY\NetworkService' },
        @{ Sid = 'S-1-5-19';     Right = 'SeServiceLogonRight';      Who = 'NT AUTHORITY\LocalService' }
    )
    $anySid = $null
    $heldBy = $null
    foreach ($probe in $probes) {
        try {
            $sid = Get-WellKnownSidBytes -Sid $probe.Sid
        } catch {
            continue
        }
        if ($null -eq $anySid) { $anySid = $sid }
        try {
            if ([SynoSync.LsaRights]::Has($sid, $probe.Right)) {
                $heldBy = $probe
                break
            }
        } catch {
            if ($_.Exception.Message -match 'Access is denied') {
                Report-Skip 'lsa: read-back needs a token that may read LSA policy (elevate to run it)'
                return
            }
            Report-Fail ("lsa: read-back failed: {0}" -f $_.Exception.Message)
            return
        }
    }
    if ($null -eq $anySid) {
        Report-Skip 'lsa: no well-known SID resolved on this host'
        return
    }
    # The negative holds whatever the host policy is, so it is asserted, not
    # probed: a right that does not exist must never read back as held - which
    # is what a marshalling bug returning "true" for everything would trip on.
    if ([SynoSync.LsaRights]::Has($anySid, 'SeSynosyncNotARight')) {
        Report-Fail 'lsa: a non-existent right must not read back as held'
        return
    }
    if ($null -eq $heldBy) {
        Report-Skip 'lsa: no probed account holds an explicit right on this host; the positive read-back cannot run'
        return
    }
    Report-Ok ("lsa: {0} reads back as holding {1}" -f $heldBy.Who, $heldBy.Right)
}

# --- Runner -----------------------------------------------------------------

if (-not (Test-Path -LiteralPath $script:ScriptPath)) {
    Write-Host "install-service_test: install-service.ps1 not found next to this script"
    exit 1
}

Test-Parses

# Load the functions without dispatching an action (see the guard at the bottom
# of install-service.ps1).
. $script:ScriptPath

Test-CredentialMatrix
Test-ScriptNegatives
Test-SidResolution
Test-LsaShim

Write-Host ''
if ($script:Failures -ne 0) {
    Write-Host "install-service_test: $($script:Failures) case(s) FAILED"
    exit 1
}
Write-Host 'install-service_test: OK - all cases passed'
