import time
import tempfile
import os
import csv
import sys

#Bigquery module as bundled dependencies, prepend path in case already installed in environment
sys.path = [os.path.join(os.path.dirname(__file__), 'libs')] + sys.path
from google.cloud import bigquery

class BigQueryConnector:
    def __init__(self):
        self.timeout = 30
        self.last_query_run = None

    def set_query(self, q):
        self.query_string = q

    def run_base_query(self, project):
        assert self.query_string
        self.client = bigquery.Client(project)

        self.base_query_job = self.client.query(self.query_string)
        self.query_result = self.base_query_job.result()
        self.last_query_run = self.base_query_job

    def base_query_status(self):
        return self.base_query_job.done()

    def get_columns(self):
        pass

    def get_destination(self):
        pass

    def num_rows_base(self):
        return BigQueryConnector.num_rows(self.client, self.base_query_job)

    def fields(self):
        return BigQueryConnector.query_fields(self.base_query_job)

    def write_extent_result(self, extent, geo_field):
        source_table_path = '.'.join([self.client.project,
                                    self.base_query_job.destination.dataset_id,
                                    self.base_query_job.destination.table_id])

        source_table = self.client.get_table(source_table_path)
        source_table_geo_field = [field for field in source_table.schema if field.name == geo_field][0]

        if source_table_geo_field.field_type == 'GEOGRAPHY':
            q = """SELECT
              *
            FROM
              `{}`
            WHERE
              ST_INTERSECTS({}, ST_GEOGFROMTEXT('{}'))""".format(source_table_path,
                                                                 geo_field,
                                                                 extent)
        else:
            q = """SELECT
                *
                FROM
                `{}`
                WHERE
                    ST_INTERSECTS(ST_GEOGFROMTEXT({}), ST_GEOGFROMTEXT('{}'))""".format(source_table_path,
                                                                        geo_field,
                                                                        extent)

        extent_query_job = self.client.query(q)
        self.last_query_run = extent_query_job
        filepath = BigQueryConnector.write_to_tempfile(extent_query_job)

        return filepath


    @staticmethod
    def num_rows(client, query_job):
        return client.get_table(query_job.destination).num_rows

    @staticmethod
    def query_fields(query_job):
        return [field.name for field in query_job.result().schema]


    @staticmethod
    def write_to_tempfile(query_job):
        schema_fields = BigQueryConnector.query_fields(query_job)
        

        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False) as fp:
            filepath = fp.name
            writer = csv.DictWriter(fp, fieldnames=schema_fields)
            writer.writeheader()
            for row in query_job.result():
                writer.writerow(dict(row.items()))

        return filepath

    def write_base_result(self):
        return BigQueryConnector.write_to_tempfile(self.base_query_job)



