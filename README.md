# bigquerylayers 
QGIS plugin for importing data from BigQuery

## Install
1. Install the Google Cloud SDK and authenticate using the command `gcloud auth application-default login`
2. Download the zipped plugin using  `curl -L https://github.com/smandaric/bigquerylayers/archive/master.zip -o /tmp/bigquerylayers.zip`
3. Install the plugin from zip using the plugin menus in QGIS

## Development

1. Make sure the QGIS python interpreter has Google Cloud SDK installed
2. Symlink the directory to the QGIS plugins folder

*  Mac: `ln -s ${PWD} ~/Library/"Application Support"/QGIS/QGIS3/profiles/default/python/plugins`


## Updating bundled BigQuery libs

`pip install --target bqloader/libs google-cloud-bigquery --upgrade` 
