from luigi.contrib.postgres import CopyToTable
from src.utils.general import read_yaml_file, get_db_credentials
from src.utils.utils import load_df
from src.pipeline.LuigiModelSelectionTask import ModelSelectionTask
from src.pipeline.ingesta_almacenamiento import get_s3_client
from datetime import date
from src.pipeline import modeling as mod
from time import gmtime, strftime
import src.utils.constants as cte
import pandas as pd
import luigi
import psycopg2
import yaml
import pickle
import marbles.core
import marbles.mixins

class ModelSelectionTest(marbles.core.TestCase, marbles.mixins.DateTimeMixins):

  def __init__(self, my_date, path_cred, data):
    super(ModelSelectionTest, self).__init__()
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

  def test_n_models_validation(self):
    if self.data:
      val = True
    else:
      val = False
    self.assertTrue(val, note = "El archivo debe contener al menos 1 modelo")
    return True


class ModelSelectionTestTask(CopyToTable):

  path_cred = luigi.Parameter(default = 'credentials.yaml')
  initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
  limit = luigi.IntParameter(default = 300000)
  date = luigi.DateParameter(default = None)
  initial_date = luigi.DateParameter(default = None)
  bucket_path = luigi.Parameter(default = cte.BUCKET)
  exercise = luigi.BoolParameter(default=False, parsing = luigi.BoolParameter.EXPLICIT_PARSING)

  # Load postgres credentials
  with open(cte.CREDENTIALS, 'r') as f:
    config = yaml.safe_load(f)

  credentials = config['db']

  user = credentials['user']
  password = credentials['pass']
  database = credentials['database']
  host = credentials['host']
  port = credentials['port']

  table = 'metadata.test_seleccion'

  columns = [("file_name", "VARCHAR"),
             ("data_date", "DATE"),
             ("processing_date", "TIMESTAMPTZ"),
             ("test_name", "VARCHAR"),
             ("result", "BOOLEAN")
             ]

  def requires(self):
    return ModelSelectionTask(
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
    training_models = pickle.loads(body.read())
		
    return training_models

  def rows(self):

    file_name = "models/best-models/best-food-inspections-model-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))
    models_data = self.input()["best_model"]
    test = ModelSelectionTest(path_cred = self.path_cred, data = models_data, my_date = self.date)

    print("Realizando prueba unitaria: Validación de Fecha")
    test_val = test.test_get_date_validation()
    print("Prueba uitaria aprobada")

    print("Realizando prueba unitaria: Validación de número de modelos")    
    test_nmodels = test.test_n_models_validation()
    print("Prueba uitaria aprobada")

    date_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    data_test = {
    	"file_name": [file_name, file_name],
    	"data_date": [self.date, self.date],
    	"processing_date": [date_time, date_time],
    	"test_name": ["test_get_date_validation", 
                    "test_n_models_validation"],
    	"result": [test_val, test_nmodels]
    }

    data_test = pd.DataFrame(data_test)
    records = data_test.to_records(index=False)
    r = list(records)
    for element in r:
    	yield element
