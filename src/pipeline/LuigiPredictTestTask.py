from src.pipeline import transformation as transf
from src.pipeline import bias_fairness as bf
from src.pipeline.ingesta_almacenamiento import get_s3_client, guardar_ingesta
from src.utils import general as gral
from src.pipeline.LuigiPredictTask import PredictTask

import luigi
#import boto3
import pandas as pd
import pickle
import luigi.contrib.s3
import src.utils.constants as cte
import yaml
import marbles.core
import marbles.mixins
from time import gmtime, strftime
import psycopg2

class PredictTest(marbles.core.TestCase, marbles.mixins.DateTimeMixins):

	def __init__(self, my_date, data):
		super(PredictTest, self).__init__()
		self.date = my_date
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

class PredictTestTask(luigi.Task):

  path_cred = luigi.Parameter(default = 'credentials.yaml')
  initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
  limit = luigi.IntParameter(default = 300000)
  date = luigi.DateParameter(default = None)
  initial_date = luigi.DateParameter(default = None)
  bucket_path = luigi.Parameter(default = cte.BUCKET)
  exercise = luigi.BoolParameter(default=False, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
  rama = luigi.Parameter(default = 'almacenamiento')# o monitoreo
  date_bestmodel = luigi.DateParameter(default = None)

  with open(cte.CREDENTIALS, 'r') as f:
    config = yaml.safe_load(f)

  credentials = config['db']

  user = credentials['user']
  password = credentials['pass']
  database = credentials['database']
  host = credentials['host']
  port = credentials['port']

  table = 'metadata.test_predict'

  columns = [("file_name", "VARCHAR"),
             ("data_date", "DATE"),
             ("processing_date", "TIMESTAMPTZ"),
             ("test_name", "VARCHAR"),
             ("result", "BOOLEAN")
             ]

  def requires(self):
    return PredictTask(
			self.path_cred,
			self.initial,
			self.limit,
			self.date,
			self.initial_date,
      self.bucket_path,
      self.exercise,
      self.rama,
      self.date_bestmodel
		)


  def input(self):

    with open(cte.CREDENTIALS, 'r') as f:
          config = yaml.safe_load(f)

    credentials = config['db']
    user = credentials['user']
    password = credentials['pass']
    database = credentials['database']
    host = credentials['host']

    conn = psycopg2.connect(
        dbname=database,
        user=user,
        host=host,
        password=password
    )

    cur = conn.cursor()
    cur.execute(
        """ SELECT *
            FROM predict.predictions
        """
    )
    rows = cur.fetchall()
      
    data = pd.DataFrame(rows)
    data.columns = [desc[0] for desc in cur.description]

    return data


  def rows(self):

    file_name = "predict-" + self.date.strftime('%Y-%m-%d')
    test = PredictTest(path_cred = self.path_cred, data = self.input(), my_date = self.date)

    print("Realizando prueba unitaria: Validación de Fecha")
    test_val = test.test_get_date_validation()
    print("Prueba uitaria aprobada")

    print("Realizando prueba unitaria: Validación de número de renglones")    
    test_nrow = test.test_get_nrow_file_validation()
    print("Prueba uitaria aprobada")

    date_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    data_test = {
    	"file_name": [file_name, file_name],
    	"data_date": [self.date, self.date],
    	"processing_date": [date_time, date_time],
    	"test_name": ["test_get_date_validation", 
    		"test_get_nrow_file_validation"],
    	"result": [test_val, test_nrow]
    }

    data_test = pd.DataFrame(data_test)
    records = data_test.to_records(index=False)
    r = list(records)
    for element in r:
    	yield element
