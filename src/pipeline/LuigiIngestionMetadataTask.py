from luigi.contrib.postgres import CopyToTable
from src.utils.general import ingestion_metadata, read_yaml_file
from src.utils.utils import load_df
from src.pipeline.LuigiIngestionTasks import IngestionTask
import src.utils.constants as cte
import pandas as pd
import luigi
import psycopg2
import yaml


class IngestionMetadata(CopyToTable):

	path_cred = luigi.Parameter(default = 'credentials.yaml')
	initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
	limit = luigi.IntParameter(default = 300000)
	date = luigi.DateParameter(default = None)
	initial_date = luigi.DateParameter(default = None)
	
	with open(cte.CREDENTIALS, 'r') as f:
	        config = yaml.safe_load(f)

	credentials = config['db']

	user = credentials['user']
	password = credentials['pass']
	database = credentials['database']
	host = credentials['host']
	port = credentials['port']

	table = 'metadata.ingestion'

	columns = [("file_name", "VARCHAR"),
                                                 ("data_date", "DATE"),
                                                 ("processing_date", "TIMESTAMPTZ"),
                                                 ("nrows", "INTEGER"),
                                                 ("ncols", "INTEGER"),
                                                 ("extension_file","VARCHAR"),
                                                 ("col_names", "VARCHAR"),
                                                 ("source","VARCHAR"),
                                                 ("dataset", "VARCHAR")
                                                 ]


	def requires(self):
                return IngestionTask(
                        self.path_cred,
                        self.initial,
                        self.limit,
                        self.date,
                        self.initial_date
                      )


	def input(self):

		if self.initial:
			file_name = "results/" + cte.BUCKET_PATH_HIST + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))
		else:
			file_name = "results/" + cte.BUCKET_PATH_CONS + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))
		
		binary_data = load_df(file_name)
		data = pd.DataFrame(binary_data)

		return data

	def rows(self):

		if self.initial:
			file_name = 'historic-inspections-{}.pkl'.format(self.date.strftime('%Y-%m-%d'))
		else:
			file_name = 'consecutive-inspections-{}.pkl'.format(self.date.strftime('%Y-%m-%d'))
		
		data = ingestion_metadata(self.input(), file_name, self.date)
		records = data.to_records(index=False)
		r = list(records)
		for element in r:
			yield element
