[CmdletBinding(SupportsShouldProcess)]
param(
    [string]$KeepTag = "brmlive-control-api:local",
    [string[]]$KeepTags = @()
)

$ErrorActionPreference = "Stop"
$keep = @($KeepTag, "brmlive-control-api:latest") + $KeepTags | Where-Object { $_ } | Select-Object -Unique
$runningIds = @(docker ps -a --format '{{.Image}}' | ForEach-Object {
    try { (docker image inspect $_ --format '{{.Id}}' 2>$null) } catch { }
}) | Select-Object -Unique

$images = @(docker image ls 'brmlive-control-api' --format '{{.Repository}}:{{.Tag}}\t{{.ID}}')
foreach ($line in $images) {
    if (-not $line) { continue }
    $parts = $line -split "`t", 2
    $tag, $id = $parts[0], $parts[1]
    if ($keep -contains $tag) {
        Write-Host "保留 $tag"
        continue
    }
    if ($runningIds -contains $id) {
        Write-Host "跳过正在被容器引用的 $tag ($id)"
        continue
    }
    if ($PSCmdlet.ShouldProcess($tag, "删除未保留的本地 control-api 镜像标签")) {
        docker image rm $tag
    }
}

Write-Host "完成。当前 control-api 镜像："
docker image ls 'brmlive-control-api' --format 'table {{.Repository}}\t{{.Tag}}\t{{.ID}}\t{{.CreatedSince}}\t{{.Size}}'
