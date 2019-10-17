from qgis.core import QgsTask, QgsMessageLog, Qgis
import time

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
    def __init__(self, desc, iface, client, query, result, query_progress_field, geometry_column_combo_box, base_query_elements, layer_import_elements, run_query_button):
        QgsTask.__init__(self, desc)
        self.iface = iface
        self.client = client
        self.query = query
        self.result = result
        self.exception = None
        self.query_progress_field = query_progress_field
        self.geometry_column_combo_box = geometry_column_combo_box
        self.base_query_elements = base_query_elements
        self.layer_import_elements = layer_import_elements
        self.run_query_button = run_query_button

    def run(self):
        """This function is where you do the 'heavy lifting' or implement
        the task which you want to run in a background thread. This function 
        must return True or False and should only interact with the main thread
        via signals"""
        try:
            QgsMessageLog.logMessage('In backgrond task', 'BigQuery Layers', Qgis.Info)
            self.query_result = self.client.query(self.query).result()
            self.result.put(self.query_result, block=True)
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
    def __init__(self, desc, iface, client, query, result, query_progress_field, geometry_column_combo_box, base_query_elements, layer_import_elements, run_query_button):
        QgsTask.__init__(self, desc)
        self.iface = iface
        self.client = client
        self.query = query
        self.result = result
        self.exception = None
        self.query_progress_field = query_progress_field
        self.geometry_column_combo_box = geometry_column_combo_box
        self.base_query_elements = base_query_elements
        self.layer_import_elements = layer_import_elements
        self.run_query_button = run_query_button

    def run(self):
        """This function is where you do the 'heavy lifting' or implement
        the task which you want to run in a background thread. This function 
        must return True or False and should only interact with the main thread
        via signals"""
        try:
            QgsMessageLog.logMessage('In backgrond task', 'BigQuery Layers', Qgis.Info)
            self.query_result = self.client.query(self.query).result()
            self.result.put(self.query_result, block=True)
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

