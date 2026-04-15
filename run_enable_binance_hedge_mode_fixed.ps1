param(
    [string]$PythonExe = "python",
    [string]$ScriptPath = ".\enable_binance_hedge_mode.py",
    [string]$EnvPath = ".\.env",
    [switch]$UseTestnet
)

$ErrorActionPreference = "Stop"

try {
    chcp 65001 > $null
} catch {
}

try {
    [Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
    [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
    $OutputEncoding = [System.Text.UTF8Encoding]::new($false)
} catch {
}

function Set-EnvFromDotEnv {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        Write-Host "[HEDGE] .env not found, continuing with current process environment."
        return
    }

    Get-Content -Encoding UTF8 $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line) { return }
        if ($line.StartsWith("#")) { return }
        if ($line -notmatch "=") { return }

        $parts = $line -split "=", 2
        $name = $parts[0].Trim()
        $value = $parts[1].Trim()

        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        if ($name) {
            [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
}

function Get-FirstEnvValue {
    param([string[]]$Names)

    foreach ($n in $Names) {
        $v = [System.Environment]::GetEnvironmentVariable($n, "Process")
        if (-not [string]::IsNullOrWhiteSpace($v)) {
            return $v
        }
    }
    return $null
}

Write-Host "[HEDGE] Loading environment from $EnvPath"
Set-EnvFromDotEnv -Path $EnvPath

$apiKey = Get-FirstEnvValue @(
    "BINANCE_API_KEY",
    "BINANCE_KEY",
    "BINANCE_FUTURES_API_KEY",
    "BINANCE_MAIN_API_KEY",
    "BINANCE_BASE_MAIN_API_KEY"
)

$apiSecret = Get-FirstEnvValue @(
    "BINANCE_API_SECRET",
    "BINANCE_SECRET",
    "BINANCE_FUTURES_API_SECRET",
    "BINANCE_MAIN_API_SECRET",
    "BINANCE_BASE_MAIN_API_SECRET"
)

if (-not $apiKey) {
    throw "Binance API key not found. Add BINANCE_API_KEY or BINANCE_BASE_MAIN_API_KEY to .env or set it in the current PowerShell session."
}

if (-not $apiSecret) {
    throw "Binance API secret not found. Add BINANCE_API_SECRET or BINANCE_BASE_MAIN_API_SECRET to .env or set it in the current PowerShell session."
}

[System.Environment]::SetEnvironmentVariable("BINANCE_API_KEY", $apiKey, "Process")
[System.Environment]::SetEnvironmentVariable("BINANCE_API_SECRET", $apiSecret, "Process")

if ($UseTestnet) {
    $env:BINANCE_FAPI_BASE_URL = "https://testnet.binancefuture.com"
    Write-Host "[HEDGE] Testnet enabled: $($env:BINANCE_FAPI_BASE_URL)"
}
elseif (-not $env:BINANCE_FAPI_BASE_URL) {
    $env:BINANCE_FAPI_BASE_URL = "https://fapi.binance.com"
}

if (-not (Test-Path $ScriptPath)) {
    throw "Python script not found: $ScriptPath"
}

Write-Host "[HEDGE] Running: $PythonExe $ScriptPath"
& $PythonExe $ScriptPath
$exitCode = $LASTEXITCODE

if ($exitCode -ne 0) {
    throw "Hedge mode was not confirmed. Exit code: $exitCode"
}

Write-Host "[HEDGE] Hedge mode enabled and confirmed."