from luigi.contrib.postgres import CopyToTable
from src.utils.general import transform_metadata, read_yaml_file
from src.utils.utils import load_df
from src.pipeline.LuigiFeatureEngineeringTask import FeatureEngineeringTask
from src.pipeline.ingesta_almacenamiento import get_s3_client
import src.utils.constants as cte
import pandas as pd
import luigi
import psycopg2
import yaml
import pickle


class TransformationMetadataTask(CopyToTable):

	path_cred = luigi.Parameter(default = 'credentials.yaml')
	initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
	limit = luigi.IntParameter(default = 300000)
	date = luigi.DateParameter(default = None)
	initial_date = luigi.DateParameter(default = None)
	bucket_path = luigi.Parameter(default = cte.BUCKET)

	with open(cte.CREDENTIALS, 'r') as f:
		config = yaml.safe_load(f)

	credentials = config['db']

	user = credentials['user']
	password = credentials['pass']
	database = credentials['database']
	host = credentials['host']
	port = credentials['port']

	table = 'metadata.feature'

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
		return FeatureEngineeringTask(
      					self.path_cred,
      					self.initial,
     					  self.limit,
      					self.date,
      					self.initial_date,
      					self.bucket_path
      					)

	def input(self):

		if self.initial:
			file_name = "feature-engineering/feature-historic-inspections-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))
		else:
			file_name = "feature-engineering/feature-consecutive-inspections-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d')

		s3 = get_s3_client(self.path_cred)
		s3_object = s3.get_object(Bucket = self.bucket_path, Key = file_name)
		body = s3_object['Body']
		my_pickle = pickle.loads(body.read())
		
    data = pd.DataFrame(my_pickle)
		return data

	def rows(self):
		
		data = feature_metadata(self.input(), self.date, self.initial)
		records = data.to_records(index=False)
		r = list(records)
    
		for element in r:
			yield element
