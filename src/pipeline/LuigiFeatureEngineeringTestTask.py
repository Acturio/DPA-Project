from luigi.contrib.postgres import CopyToTable
from src.utils.general import read_yaml_file
from src.utils.utils import load_df
from src.pipeline.LuigiFeatureEngineeringTask import FeatureEngineeringTask
from src.pipeline.ingesta_almacenamiento import get_s3_client
from datetime import date
from time import gmtime, strftime
import src.utils.constants as cte
import pandas as pd
import luigi
import psycopg2
import yaml
import pickle
import marbles.core
import marbles.mixins

class FeatureEngineeringTest(marbles.core.TestCase, marbles.mixins.DateTimeMixins):

	def __init__(self, my_date, path_cred, data):
		super(FeatureEngineeringTest, self).__init__()
		self.date = my_date
		self.path_cred = path_cred
		self.data = data

	def test_get_date_validation(self):
		self.assertDateTimesPast(
      		sequence = [self.date], 
      		strict = True, 
      		msg = "La fecha solicitada debe ser menor a la fecha de hoy"
		)
		return True

	def test_get_nrow_file_validation(self):
		data = self.data
		nrow = data.shape[0]
		self.assertGreater(nrow, 0, note = "El archivo no tiene registros")
		return True

	def test_get_ncol_file_validation(self):
		data = self.data
		ncol = data.shape[1]
		self.assertGreater(ncol, 0, note = "El archivo no tiene registros")
		return True



class FeatureEngineeringTestTask(CopyToTable):

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

  table = 'metadata.test_feature'

  columns = [("file_name", "VARCHAR"),
             ("data_date", "DATE"),
             ("processing_date", "TIMESTAMPTZ"),
             ("test_name", "VARCHAR"),
             ("result", "BOOLEAN")
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
      file_name = "feature-engineering/feature-consecutive-inspections-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))

    s3 = get_s3_client(self.path_cred)
    s3_object = s3.get_object(Bucket = self.bucket_path, Key = file_name)
    body = s3_object['Body']
    my_pickle = pickle.loads(body.read())
		
    data = pd.DataFrame(my_pickle)
    return data

  def rows(self):

    if self.initial:
      file_name = "feature-engineering/feature-historic-inspections-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))
    else:
      file_name = "feature-engineering/feature-consecutive-inspections-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))

    test = FeatureEngineeringTest(path_cred = self.path_cred, data = self.input(), my_date = self.date)

    print("Realizando prueba unitaria: Validación de Fecha")
    test_val = test.test_get_date_validation()
    print("Prueba uitaria aprobada")

    print("Realizando prueba unitaria: Validación de número de renglones")    
    test_nrow = test.test_get_nrow_file_validation()
    print("Prueba uitaria aprobada")

    print("Realizando prueba unitaria: Validación de número de columnas")
    test_ncol = test.test_get_ncol_file_validation()
    print("Prueba uitaria aprobada")

    date_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    data_test = {
    	"file_name": [file_name, file_name, file_name],
    	"data_date": [self.date, self.date, self.date],
    	"processing_date": [date_time, date_time, date_time],
    	"test_name": ["test_get_date_validation", 
    		"test_get_nrow_file_validation", 
    		"test_get_ncol_file_validation"],
    	"result": [test_val, test_nrow, test_ncol]
    }

    data_test = pd.DataFrame(data_test)
    records = data_test.to_records(index=False)
    r = list(records)
    for element in r:
    	yield element
