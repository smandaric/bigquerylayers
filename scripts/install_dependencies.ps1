param(
    [string]$QgisPython = "C:\Program Files\QGIS 3.40.11\apps\Python312\python.exe"
)

$pluginRoot = Split-Path -Parent $PSScriptRoot
$requirements = Join-Path $pluginRoot 'requirements.txt'
$target = Join-Path $pluginRoot 'libs'

if (-not (Test-Path $QgisPython)) {
    throw "QGIS Python was not found at '$QgisPython'. Pass its path with -QgisPython."
}

if (Test-Path $target) {
    Remove-Item -Recurse -Force $target
}

& $QgisPython -m pip install --upgrade --target $target -r $requirements
if ($LASTEXITCODE -ne 0) {
    throw 'Google Cloud dependency installation failed.'
}