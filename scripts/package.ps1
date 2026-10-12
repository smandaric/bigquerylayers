param(
    [string]$QgisPython = "C:\Program Files\QGIS 3.40.11\apps\Python312\python.exe"
)

$pluginRoot = Split-Path -Parent $PSScriptRoot
$repositoryRoot = $pluginRoot
$stagingRoot = Join-Path $repositoryRoot '.package-staging'
$distRoot = Join-Path $repositoryRoot 'dist'
$archive = Join-Path $distRoot 'bigquerylayers-0.3.0-qgis3.40.zip'

& (Join-Path $PSScriptRoot 'install_dependencies.ps1') -QgisPython $QgisPython

if (Test-Path $stagingRoot) {
    Remove-Item -Recurse -Force $stagingRoot
}
if (Test-Path $archive) {
    Remove-Item -Force $archive
}

New-Item -ItemType Directory -Force $stagingRoot | Out-Null
$pluginStage = Join-Path $stagingRoot 'bigquerylayers'
New-Item -ItemType Directory -Force $pluginStage | Out-Null
Get-ChildItem -Force $pluginRoot | Where-Object {
    $_.Name -notin '.git', '.package-staging', 'dist', '__pycache__'
} | Copy-Item -Destination $pluginStage -Recurse -Force
New-Item -ItemType Directory -Force $distRoot | Out-Null
Compress-Archive -Path $pluginStage -DestinationPath $archive -CompressionLevel Optimal
Remove-Item -Recurse -Force $stagingRoot
Write-Output "Created $archive"