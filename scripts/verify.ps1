[CmdletBinding()]
param(
    [switch]$EnableRecording,
    [switch]$SkipComposeCheck,
    [int]$ApiPort = 19997,
    [int]$MetricsPort = 19998,
    [int]$SinkApiPort = 29997,
    [int]$HlsPort = 8888,
    [int]$ControlApiPort = 8080
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

function Assert-HttpOk([string]$Url, [string]$Name, [int]$Retries = 1) {
    $lastError = $null
    for ($attempt = 1; $attempt -le $Retries; $attempt++) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 15
            if ($response.StatusCode -lt 200 -or $response.StatusCode -ge 300) {
                throw "HTTP $($response.StatusCode)"
            }
            Write-Host "[OK] $Name ($($response.StatusCode))" -ForegroundColor Green
            return $response
        } catch {
            $lastError = $_.Exception.Message
            if ($attempt -lt $Retries) {
                Start-Sleep -Seconds 5
            }
        }
    }
    throw "[FAIL] $Name`: $lastError"
}

if (-not $SkipComposeCheck) {
    docker compose config | Out-Null
    Write-Host '[OK] docker compose config' -ForegroundColor Green
}

$containers = docker compose ps --status running --services
if ($containers -notcontains 'mediamtx') {
    throw '[FAIL] mediamtx is not running. Run: docker compose up -d'
}
if ($containers -notcontains 'mediamtx-sink') {
    throw '[FAIL] mediamtx-sink is not running. Run: docker compose up -d'
}
if ($containers -notcontains 'control-api') {
    throw '[FAIL] control-api is not running. Run: docker compose up -d --build control-api'
}
Write-Host '[OK] MediaMTX containers are running' -ForegroundColor Green

$ffmpeg = docker compose exec -T control-api sh -lc 'command -v ffmpeg'
if (-not $ffmpeg) {
    throw '[FAIL] control-api image does not contain ffmpeg; recording previews cannot be generated'
}
Write-Host "[OK] control-api ffmpeg ($($ffmpeg.Trim()))" -ForegroundColor Green

$controlHealth = Assert-HttpOk "http://localhost:$ControlApiPort/healthz" 'control-api health' 6

$hls = Assert-HttpOk "http://localhost:$HlsPort/test_video/index.m3u8" 'HLS playlist' 12
$hlsText = if ($hls.Content -is [byte[]]) { [Text.Encoding]::UTF8.GetString($hls.Content) } else { [string]$hls.Content }
if ($hlsText -notmatch '#EXTM3U') {
    throw '[FAIL] HLS response is not an M3U8 playlist'
}

$paths = Assert-HttpOk "http://localhost:$ApiPort/v3/paths/list" 'MediaMTX Control API'
if ($paths.Content -notmatch 'test_video') {
    throw '[FAIL] Control API does not list test_video'
}

$metrics = Assert-HttpOk "http://localhost:$MetricsPort/metrics" 'MediaMTX Metrics'
if ($metrics.Content -notmatch 'paths') {
    throw '[FAIL] Metrics response does not contain path metrics'
}

if ($EnableRecording) {
    $body = '{"record":true}'
    $patch = Invoke-WebRequest -UseBasicParsing -Method Patch -Uri "http://localhost:$ApiPort/v3/config/paths/patch/test_video" -ContentType 'application/json' -Body $body -TimeoutSec 15
    if ($patch.StatusCode -lt 200 -or $patch.StatusCode -ge 300) {
        throw "[FAIL] Enabling recording failed: HTTP $($patch.StatusCode)"
    }
    Write-Host '[OK] recording enabled through Control API' -ForegroundColor Green
    Write-Host 'Wait at least 35 seconds, then inspect data/recordings/test_video.' -ForegroundColor Yellow
}

$sink = Assert-HttpOk "http://localhost:$SinkApiPort/v3/paths/list" 'RTMP sink Control API'
if ($sink.Content -notmatch 'test_video/smoke') {
    Write-Warning 'RTMP sink has not exposed test_video/smoke yet; wait for the source/forwarder to become available and rerun the script.'
} else {
    Write-Host '[OK] RTMP sink exposes test_video/smoke' -ForegroundColor Green
}

Write-Host 'Verification completed.' -ForegroundColor Cyan
