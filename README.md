# bigquerylayers 
QGIS plugin for importing data from BigQuery

## Install
1. Authenticate on Google Cloud using `gcloud auth`
2. Download the zipped plugin using  `curl -L https://github.com/unacast/bigquerylayers/archive/master.zip -o /tmp/bigquerylayers.zip`
3. Install the plugin from zip using the plugin menus in QGIS

## Development

1. Make sure the QGIS python interpreter has Google Cloud SDK installed
<<<<<<< HEAD
2. Symlink the directory to the QGIS plugins folder

*  Mac: `ln -s ${PWD} ~/Library/"Application Support"/QGIS/QGIS3/profiles/default/python/plugins`

=======
2. Symlink the directory to the QGIS plugins folder 
*  Mac: `ln -s ${PWD} ~/Library/"Application Support"/QGIS/QGIS3/profiles/default/python/plugins`
>>>>>>> master

## Updating bundled BigQuery libs

<<<<<<< HEAD
`pip install --target bqloader/libs google-cloud-bigquery --upgrade` 
=======
## Development status
It works on my computer 😅
>>>>>>> master
