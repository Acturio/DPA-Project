from luigi.contrib.postgres import CopyToTable
from src.utils.general import transform_metadata, read_yaml_file
from src.utils.utils import load_df
from src.pipeline.LuigiModelSelectionTestTask import ModelSelectionTestTask
from src.pipeline.ingesta_almacenamiento import get_s3_client
from src.pipeline import modeling as mod
import src.utils.constants as cte
import pandas as pd
import luigi
import psycopg2
import yaml
import pickle


class ModelSelectionMetadataTask(CopyToTable):

	path_cred = luigi.Parameter(default = 'credentials.yaml')
	initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
	limit = luigi.IntParameter(default = 300000)
	date = luigi.DateParameter(default = None)
	initial_date = luigi.DateParameter(default = None)
	bucket_path = luigi.Parameter(default = cte.BUCKET)
	exercise = luigi.BoolParameter(default=False, parsing = luigi.BoolParameter.EXPLICIT_PARSING)

	with open(cte.CREDENTIALS, 'r') as f:
		config = yaml.safe_load(f)

	credentials = config['db']

	user = credentials['user']
	password = credentials['pass']
	database = credentials['database']
	host = credentials['host']
	port = credentials['port']

	table = 'metadata.seleccion'

	columns = [("processing_date", "TIMESTAMPTZ"),
             ("data_date", "DATE"),
             ("value_params", "VARCHAR")
             ]


	def requires(self):
		return ModelSelectionTestTask(
      					self.path_cred,
      					self.initial,
     					  self.limit,
      					self.date,
      					self.initial_date,
      					self.bucket_path,
								self.exercise
      					)

	def input(self):

		file_name = "models/best-models/best-food-inspections-model-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))

		s3 = get_s3_client(self.path_cred)
		s3_object = s3.get_object(Bucket = self.bucket_path, Key = file_name)
		body = s3_object['Body']
		best_model = pickle.loads(body.read())
		
		return best_model

	def rows(self):

		best_model_data = self.input()["best_model"]
		
		data = mod.metadata_best_model(
			best_model = best_model_data,
			data_date = self.date.strftime('%Y-%m-%d'))
		print(data)
		records = data.to_records(index=False)
		r = list(records)
    
		for element in r:
			yield element
