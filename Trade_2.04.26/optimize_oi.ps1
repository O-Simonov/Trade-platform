param(
    [Parameter(Position=0)]
    [ValidateSet("quick-5m","balanced-5m","balanced-15m","balanced-1h","narrow-15m","all-balanced")]
    [string]$Profile = "balanced-15m"
)

$ErrorActionPreference = "Stop"

# ------------------------------------------------------------
# Настройка окружения
# ------------------------------------------------------------
# Вариант 1: задай переменные окружения перед запуском:
#   $env:BACKTEST_DSN="postgresql://user:pass@host:5432/dbname"
#   $env:BACKTEST_POOL_DSN="postgresql://user:pass@host:5432/dbname"
#
# Вариант 2: впиши DSN прямо сюда:
#   $env:BACKTEST_DSN = "postgresql://user:pass@host:5432/dbname"
#   $env:BACKTEST_POOL_DSN = "postgresql://user:pass@host:5432/dbname"
# ------------------------------------------------------------

if (-not $env:BACKTEST_DSN) {
    $env:BACKTEST_DSN = "postgresql://user:pass@host:5432/dbname"
}
if (-not $env:BACKTEST_POOL_DSN) {
    $env:BACKTEST_POOL_DSN = $env:BACKTEST_DSN
}

$python = "python"
$script = "scripts\optimize_oi_screener_history.py"

function Run-Optimization {
    param(
        [string]$Interval,
        [string]$From,
        [string]$To,
        [int]$MaxSymbols,
        [string]$Tag,
        [string[]]$ExtraArgs = @()
    )

    $args = @(
        $script,
        "--interval", $Interval,
        "--from", $From,
        "--to", $To,
        "--max-symbols", "$MaxSymbols",
        "--tag", $Tag
    ) + $ExtraArgs

    Write-Host ""
    Write-Host ">>> RUN: $Profile | $Interval | $Tag" -ForegroundColor Cyan
    Write-Host "$python $($args -join ' ')" -ForegroundColor DarkGray
    & $python @args
}

switch ($Profile) {
    "quick-5m" {
        Run-Optimization `
            -Interval "5m" `
            -From "2026-02-15" `
            -To "2026-03-15" `
            -MaxSymbols 80 `
            -Tag "oi_quick_5m" `
            -ExtraArgs @(
                "--buy-oi-rise-pct-grid", "2.0,2.5,3.0",
                "--oi-value-confirm-grid", "1.5,2.0",
                "--buy-volume-mult-grid", "2.0,2.2",
                "--buy-volume-spike-grid", "3.5,4.0",
                "--buy-price-rise-grid", "2.0,2.5",
                "--buy-min-green-grid", "2,3"
            )
    }

    "balanced-5m" {
        Run-Optimization `
            -Interval "5m" `
            -From "2026-01-15" `
            -To "2026-03-20" `
            -MaxSymbols 120 `
            -Tag "oi_balanced_5m" `
            -ExtraArgs @(
                "--buy-oi-rise-pct-grid", "2.0,2.5,3.0,3.5",
                "--oi-value-confirm-grid", "1.5,2.0,2.5",
                "--buy-volume-mult-grid", "2.0,2.2,2.5",
                "--buy-volume-spike-grid", "3.5,4.0,4.5",
                "--buy-price-rise-grid", "2.0,2.5,3.0",
                "--buy-min-green-grid", "2,3,4"
            )
    }

    "balanced-15m" {
        Run-Optimization `
            -Interval "15m" `
            -From "2026-01-01" `
            -To "2026-03-20" `
            -MaxSymbols 150 `
            -Tag "oi_balanced_15m" `
            -ExtraArgs @(
                "--buy-oi-rise-pct-grid", "2.0,2.5,3.0,3.5",
                "--oi-value-confirm-grid", "1.5,2.0,2.5",
                "--buy-volume-mult-grid", "1.8,2.0,2.2,2.5",
                "--buy-volume-spike-grid", "3.0,3.5,4.0",
                "--buy-price-rise-grid", "1.8,2.2,2.6,3.0",
                "--buy-min-green-grid", "2,3,4"
            )
    }

    "balanced-1h" {
        Run-Optimization `
            -Interval "1h" `
            -From "2025-11-01" `
            -To "2026-03-20" `
            -MaxSymbols 180 `
            -Tag "oi_balanced_1h" `
            -ExtraArgs @(
                "--buy-oi-rise-pct-grid", "1.8,2.2,2.6,3.0",
                "--oi-value-confirm-grid", "1.5,2.0,2.5",
                "--buy-volume-mult-grid", "1.6,1.8,2.0,2.2",
                "--buy-volume-spike-grid", "2.5,3.0,3.5",
                "--buy-price-rise-grid", "1.5,2.0,2.5,3.0",
                "--buy-min-green-grid", "2,3"
            )
    }

    "narrow-15m" {
        Run-Optimization `
            -Interval "15m" `
            -From "2026-01-01" `
            -To "2026-03-20" `
            -MaxSymbols 180 `
            -Tag "oi_narrow_15m" `
            -ExtraArgs @(
                "--buy-oi-rise-pct-grid", "2.3,2.5,2.7",
                "--oi-value-confirm-grid", "1.8,2.0,2.2",
                "--buy-volume-mult-grid", "1.9,2.1,2.3",
                "--buy-volume-spike-grid", "3.2,3.5,3.8",
                "--buy-price-rise-grid", "2.0,2.2,2.4",
                "--buy-min-green-grid", "2,3"
            )
    }

    "all-balanced" {
        & $PSCommandPath "balanced-5m"
        & $PSCommandPath "balanced-15m"
        & $PSCommandPath "balanced-1h"
    }
}
