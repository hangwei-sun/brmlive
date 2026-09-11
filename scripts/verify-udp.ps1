[CmdletBinding()]
param(
    [int]$SplitterPort = 9101,
    [int]$ApiPort = 19997,
    [int]$HlsPort = 8888,
    [switch]$RequireOnline
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

docker compose --profile udp config | Out-Null
Write-Host '[OK] docker compose --profile udp config' -ForegroundColor Green
$services = docker compose --profile udp ps --status running --services
if ($services -notcontains 'udp-splitter') {
    throw '[FAIL] udp-splitter is not running. Run: docker compose --profile udp up -d --build udp-splitter'
}

$status = Invoke-RestMethod -Uri "http://127.0.0.1:$SplitterPort/api/v1/status" -TimeoutSec 10
$group = @($status.groups | Where-Object { $_.name -eq 'audio-mpts-10002' }) | Select-Object -First 1
if (-not $group) { throw '[FAIL] audio-mpts-10002 is missing from splitter status' }
if ([int]$group.programs -ne 4) { throw "[FAIL] expected 4 configured programs, got $($group.programs)" }
Write-Host '[OK] 10002 has four configured programs' -ForegroundColor Green
if ($RequireOnline -and -not [bool]$group.running) { throw "[FAIL] splitter group is offline: $($group.lastError)" }
if ([bool]$group.running) { Write-Host '[OK] 10002 FFmpeg group is running' -ForegroundColor Green }
else { Write-Warning "10002 group is not online yet: $($group.lastError)" }

$paths = Invoke-RestMethod -Uri "http://127.0.0.1:$ApiPort/v3/paths/list" -TimeoutSec 10
foreach ($slug in 'audio_encoder_1','audio_encoder_2','audio_encoder_3','audio_encoder_4') {
    $path = @($paths.items | Where-Object { $_.name -eq $slug }) | Select-Object -First 1
    if (-not $path) { Write-Warning "$slug is not yet present in MediaMTX"; continue }
    if ($RequireOnline -and -not [bool]$path.ready) { throw "[FAIL] $slug is not ready" }
    Write-Host "[OK] MediaMTX path $slug exists" -ForegroundColor Green
    if ([bool]$path.ready) {
        $playlist = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$HlsPort/$slug/index.m3u8" -TimeoutSec 15
        if ($playlist.Content -notmatch '#EXTM3U') { throw "[FAIL] $slug HLS playlist is invalid" }
        Write-Host "[OK] $slug HLS playlist" -ForegroundColor Green
    }
}
Write-Host 'UDP verification completed.' -ForegroundColor Cyan
