# SPDX-License-Identifier: MIT
# Copyright (c) 2026 houzc

# AgentHeart Quick Start (one command, public repo)
#
# Builds the kernel + sidecar + cdylib, starts the sidecar, then runs the
# SDK smoke tests that ship with this repo (Python / Node).
#
# Go / Java SDK smoke and the embedded FFI tests are driven by the private
# test repository (agentheart-test), which is not open-sourced.
#
# ASCII-only messages on purpose: Windows PowerShell 5.1 mis-decodes
# BOM-less UTF-8 scripts, which would garble non-ASCII output.

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$env:PATH = "$env:USERPROFILE\.cargo\bin;" + $env:PATH

Push-Location $root

# ---------------------------------------------------------------------------
Write-Host "[1/3] Building kernel + sidecar ..."
cargo build --release
if ($LASTEXITCODE -ne 0) { Pop-Location; Write-Error "build failed"; exit 1 }

$exe = Join-Path $root "target\release\agentheartd.exe"
if (-not (Test-Path -LiteralPath $exe)) { $exe = Join-Path $root "target\release\agentheartd" }
if (-not (Test-Path -LiteralPath $exe)) { Pop-Location; Write-Error "sidecar binary not found"; exit 1 }

# ---------------------------------------------------------------------------
Write-Host "[2/3] Starting sidecar and waiting for addr/token ..."
$tmp = Join-Path $root "target\quickstart"
New-Item -ItemType Directory -Force -Path $tmp | Out-Null
$outFile = Join-Path $tmp "sidecar.out"
$errFile = Join-Path $tmp "sidecar.err"

$proc = Start-Process -FilePath $exe -PassThru -NoNewWindow `
    -RedirectStandardOutput $outFile -RedirectStandardError $errFile

$addr = $null
$token = $null
$deadline = (Get-Date).AddSeconds(20)
while ((Get-Date) -lt $deadline) {
    Start-Sleep -Milliseconds 200
    if (Test-Path -LiteralPath $outFile) {
        $text = Get-Content -Raw -LiteralPath $outFile -ErrorAction SilentlyContinue
        if ($text) {
            $m = [regex]::Match($text, "ready addr=(\S+)")
            $t = [regex]::Match($text, "token=(\S+)")
            if ($m.Success -and $t.Success) {
                $addr = $m.Groups[1].Value
                $token = $t.Groups[1].Value
                break
            }
        }
    }
    if ($proc.HasExited) { break }
}

if (-not $addr) {
    Write-Host "--- sidecar stdout ---"
    Get-Content -LiteralPath $outFile -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  $_" }
    Write-Host "--- sidecar stderr ---"
    Get-Content -LiteralPath $errFile -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  $_" }
    if (-not $proc.HasExited) { $proc.Kill() }
    Pop-Location
    Write-Error "sidecar did not report ready addr/token"
    exit 1
}
Write-Host "      sidecar ready addr=$addr"

# ---------------------------------------------------------------------------
Write-Host "[3/3] SDK smoke tests (python / node) ..."
$env:AH_ADDR = $addr
$env:AH_TOKEN = $token

$ran = 0
$failed = 0

if (Get-Command python -ErrorAction SilentlyContinue) {
    $ran += 1
    & python (Join-Path $root "agentheart-sdk\python\smoke_test.py") 2>&1 |
        Select-Object -Last 2 | ForEach-Object { Write-Host "      $_" }
    if ($LASTEXITCODE -ne 0) { $failed += 1; Write-Host "      python SMOKE_FAILED" }
} else {
    Write-Host "      python not found - skipped"
}

if (Get-Command node -ErrorAction SilentlyContinue) {
    $ran += 1
    & node (Join-Path $root "agentheart-sdk\node\smoke_test.js") 2>&1 |
        Select-Object -Last 2 | ForEach-Object { Write-Host "      $_" }
    if ($LASTEXITCODE -ne 0) { $failed += 1; Write-Host "      node SMOKE_FAILED" }
} else {
    Write-Host "      node not found - skipped"
}

if (-not $proc.HasExited) { $proc.Kill(); $proc.WaitForExit(5000) | Out-Null }
Pop-Location

# ---------------------------------------------------------------------------
Write-Host ""
if ($ran -gt 0 -and $failed -eq 0) {
    Write-Host "QUICKSTART_OK (ran=$ran failed=0)"
    exit 0
}
Write-Host "QUICKSTART_FAILED (ran=$ran failed=$failed)"
exit 1
