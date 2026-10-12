# bigquerylayers 
QGIS plugin for importing data from BigQuery

![screenshot](screenshot.png)

## Install
This plugin requires an active (free tier or paid) subscription to Google Cloud Platform
1. Create a project using the [Google Cloud Console](https://console.cloud.google.com)
2. Install the Google Cloud SDK and authenticate using the command `gcloud auth application-default login`.
3. Install the plugin ZIP using QGIS Plugin Manager > Install from ZIP.
4. In PowerShell, run `scripts\install_dependencies.ps1` from the installed plugin directory. Pass `-QgisPython` when QGIS is installed somewhere other than `C:\Program Files\QGIS 3.40.11`.

## Build a local ZIP
Run `scripts\package.ps1` from the repository root. It installs current Google Cloud dependencies with QGIS's Python and writes `dist\bigquerylayers-0.3.0-qgis3.40.zip`, ready for QGIS Plugin Manager > Install from ZIP.

## Development
1. Make sure you have an active GCP project and authenticated Google Cloud SDK
2. Clone this repo and symlink it to the QGIS plugins folder

*  Mac: `ln -s ${PWD} ~/Library/"Application Support"/QGIS/QGIS3/profiles/default/python/plugins`


## Updating bundled BigQuery libs
Run `scripts\install_dependencies.ps1` from the plugin directory. It uses QGIS's Python 3.12 interpreter to install the versions in `requirements.txt` into `libs`.
