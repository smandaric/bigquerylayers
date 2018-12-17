extent = iface.mapCanvas().extent()

crcSource = QgsCoordinateReferenceSystem(3857)
crcTarget = QgsCoordinateReferenceSystem(4326)
qq = QgsCoordinateTransform(crcSource, crcTarget, QgsProject.instance())

extent_wkt = qq.transform(extent).asWktPolygon()

/var/folders/tt/vwhgjvbj1mdg2w3qs39csvmc0000gn/T/tmp2l0k42ie


uri = 'file:///var/folders/tt/vwhgjvbj1mdg2w3qs39csvmc0000gn/T/tmp2l0k42ie?delimiter=,&crs=epsg:4326&wktField=venue_geog'
vlayer = iface.addVectorLayer(uri, "layer name you like", "delimitedtext")
/usr/local/opt/python/Frameworks/Python.framework/Versions/3.7/bin
