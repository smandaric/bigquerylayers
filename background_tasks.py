from qgis.core import QgsTask, QgsMessageLog, Qgis
import time, csv, tempfile

class TestTask(QgsTask):
    """Here we subclass QgsTask"""
    def __init__(self, desc, iface):
        QgsTask.__init__(self, desc)
        self.iface = iface

    def run(self):
        """This function is where you do the 'heavy lifting' or implement
        the task which you want to run in a background thread. This function 
        must return True or False and should only interact with the main thread
        via signals"""
        for i in range (21):
            time.sleep(0.5)
            val = i * 5
            #report progress which can be received by the main thread
            self.setProgress(val)
            #check to see if the task has been cancelled
            if self.isCanceled():
                return False
        return True

    def finished(self, result):
        """This function is called automatically when the task is completed and is
        called from the main thread so it is safe to interact with the GUI etc here"""
        self.iface.messageBar().clearWidgets()
        
        if result is False:
            self.iface.messageBar().pushMessage('Task was cancelled')
        else:
            self.iface.messageBar().pushMessage('Task Complete')


class BackgroundQueryTask(QgsTask):
    """Here we subclass QgsTask"""
    def __init__(self, desc, iface, client, query, query_job, query_progress_field, geometry_column_combo_box, base_query_elements, layer_import_elements, run_query_button):
        QgsTask.__init__(self, desc)
        self.iface = iface
        self.client = client
        self.query = query
        self.query_job_param = query_job
        self.exception = None
        self.query_progress_field = query_progress_field
        self.geometry_column_combo_box = geometry_column_combo_box
        self.base_query_elements = base_query_elements
        self.layer_import_elements = layer_import_elements
        self.run_query_button = run_query_button
        self.query_result = None

    def run(self):
        """This function is where you do the 'heavy lifting' or implement
        the task which you want to run in a background thread. This function 
        must return True or False and should only interact with the main thread
        via signals"""
        try:
            QgsMessageLog.logMessage('In backgrond task', 'BigQuery Layers', Qgis.Info)
            self.query_job = self.client.query(self.query)
            self.query_result = self.query_job.result()
            self.query_job_param.put(self.query_job, block=True)
            QgsMessageLog.logMessage('Query complete', 'BigQuery Layers', Qgis.Info)
            self.setProgress(100)
            return True
        except Exception as e:
            self.exception = e
            return True

    def finished(self, result):
        """This function is called automatically when the task is completed and is
        called from the main thread so it is safe to interact with the GUI etc here"""
        QgsMessageLog.logMessage('Finished is called', 'BigQuery Layers', Qgis.Info)
        
        if result is False:
            self.iface.messageBar().pushMessage('Task was cancelled')
        elif result is True and self.exception:
            QgsMessageLog.logMessage(self.exception.__repr__(), 'BigQuery Layers', Qgis.Critical)
        else:
            QgsMessageLog.logMessage('Query successfull', 'BigQuery Layers', Qgis.Info)

            num_rows = self.query_result.total_rows
            self.query_progress_field.setText('Rows returned: {}'.format(num_rows))

            fields = [field.name for field in self.query_result.schema]
            self.geometry_column_combo_box.addItems(fields)

            for elm in self.base_query_elements:
                elm.setEnabled(True)

        for elm in self.layer_import_elements:
            elm.setEnabled(True)
        self.run_query_button.setText('Run query')

class RetrieveQueryResultTask(QgsTask):
    """Here we subclass QgsTask"""
    def __init__(self, desc, iface, query_job, file_queue):
        QgsTask.__init__(self, desc)
        self.iface = iface
        self.query_job = query_job
        self.file_queue = file_queue
        self.exception = False


    def run(self):
        """This function is where you do the 'heavy lifting' or implement
        the task which you want to run in a background thread. This function 
        must return True or False and should only interact with the main thread
        via signals"""
        try:
            QgsMessageLog.logMessage('In write tempfile task', 'BigQuery Layers', Qgis.Info)
            query_result = self.query_job.result()
            schema_fields =  [field.name for field in query_result.schema]
            total_rows = query_result.total_rows
            QgsMessageLog.logMessage('Total rows: '+str(total_rows), 'BigQuery Layers', Qgis.Info)

            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as fp:
                filepath = fp.name
                writer = csv.DictWriter(fp, fieldnames=schema_fields)
                writer.writeheader()
                progress_percent = 0
                self.setProgress(progress_percent)
                QgsMessageLog.logMessage('File path: '+filepath, 'BigQuery Layers', Qgis.Info)

                for i, row in enumerate(query_result, 1):
                    new_progress_percent = int(100 * (i / total_rows))
                    if new_progress_percent > progress_percent:
                        progress_percent = new_progress_percent
                        self.setProgress(progress_percent)

                    writer.writerow(dict(row.items()))
            self.file_queue.put(filepath)
            QgsMessageLog.logMessage('Query complete', 'BigQuery Layers', Qgis.Info)
            self.setProgress(100)
            return True
        except Exception as e:
            self.exception = e
            return True

    def finished(self, result):
        """This function is called automatically when the task is completed and is
        called from the main thread so it is safe to interact with the GUI etc here"""
        QgsMessageLog.logMessage('Finished is called', 'BigQuery Layers', Qgis.Info)
        
        if result is False:
            self.iface.messageBar().pushMessage('Task was cancelled')
        elif result is True and self.exception:
            QgsMessageLog.logMessage(self.exception.__repr__(), 'BigQuery Layers', Qgis.Critical)
        else:
            QgsMessageLog.logMessage('Finished import', 'BigQuery Layers', Qgis.Info)
            












    def csv_to_spatiallite(self):
        ogr2ogr_executable = shutil.which('ogr2ogr')
        geom_field = self.geometry_column_combo_box.currentText()

        if not ogr2ogr_executable:
            QgsMessageLog.logMessage('ogr2ogr executable not found', 'BigQuery Layers', Qgis.Info)
            return

        cp_params = [
            'cp',
            self.layer_file_path,
            self.layer_file_path + '.csv'
        ]

        ogr2ogr_params = [
            ogr2ogr_executable,
            '-f', 'GPKG', '{}.gpkg'.format(self.layer_file_path),
            self.layer_file_path + '.csv',
            '-oo', 'HEADERS=YES',
            '-oo', 'GEOM_POSSIBLE_NAMES={}'.format(geom_field),
            '-a_srs', 'EPSG:4326'
        ]

        try:
            subprocess.check_output(cp_params)
            subprocess.check_output(ogr2ogr_params)
        except Exception as e:
            print(e)

        return self.layer_file_path + ".gpkg"