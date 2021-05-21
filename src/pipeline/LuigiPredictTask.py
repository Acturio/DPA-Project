from luigi.contrib.postgres import CopyToTable
from src.pipeline import transformation as transf
from src.pipeline import bias_fairness as bf
from src.pipeline.ingesta_almacenamiento import get_s3_client, guardar_ingesta
from src.utils import general as gral
from src.pipeline.LuigiModelSelectionMetadataTask import ModelSelectionMetadataTask
from src.pipeline.LuigiFeatureEngineeringTask import FeatureEngineeringTask

import luigi
#import boto3
import pandas as pd
import pickle
import luigi.contrib.s3
import src.utils.constants as cte
from time import gmtime, strftime

class PredictTask(CopyToTable):

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

  table = 'predict.predictions'
  	          
  columns = [("fecha_load", "DATE"),
             ("fecha", "DATE"),
             ("inspection_id", "INTEGER"),#Cambiar por ID establecimiento
             ("label", "INTEGER"),
             ("score","DOUBLE"),
             ("facility_type","VARCHAR"),
             ("inspection_type","VARCHAR")
             ]

  def requires(self):
  	#requiere dataframe_fe, mejor modelo y autovariables
    return {'best_model': ModelSelectionMetadataTask(
      self.path_cred,
      self.initial,
      self.limit,
      self.date,
      self.initial_date,
      self.bucket_path
    ), 'feature': FeatureEngineeringTask(
			self.path_cred,
			self.initial,
			self.limit,
			self.date,
			self.initial_date,
      self.bucket_path
		)}


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
    # ------------------------------------------------------------------------
    file_best_model = "models/best-models/best-food-inspections-model-" + '{}.pkl'.format(self.date_bestmodel.strftime('%Y-%m-%d'))

    s3 = get_s3_client(self.path_cred)
    s3_object = s3.get_object(Bucket = self.bucket_path, Key = file_best_model)
    body = s3_object['Body']
    model_and_features = pickle.loads(body.read())

    #data = gral.read_gather_s3(
    #  date_string = self.date.strftime('%Y-%m-%d'), 
    #  folder = "feature-engineering/feature", 
    #  cred_path = self.path_cred,
    #  bucket = self.bucket_path
    #)

    data_model_features = {
      "data": data,
      "best_model": model_and_features["best_model"],
      "features": model_and_features["features"]
    }

    return data_model_features

  def rows(self):

    data_input = self.input()

    data = data_input["data"]
    best_model = data_input["best_model"]
    features = data_input["features"]

    pred = bf.predict(
      df_fe = data, 
      best_model = best_model,
      auto_variables = features
    )
    # Para agregar columna con la fecha
    pred["fecha_load"] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    pred["fecha"] = self.date.strftime('%Y-%m-%d')

    pred = pred[["fecha_load","fecha","inspection_id","label",
                 "score","facility_type","inspection_type"]]

    records = pred.to_records(index=False)
    r = list(records)
    
    for element in r:
      yield element

  """
  def output(self):
          
    s3_c = gral.get_s3_credentials(self.path_cred)
    client_s3 = luigi.contrib.s3.S3Client(
      aws_access_key_id = s3_c["aws_access_key_id"],
      aws_secret_access_key = s3_c["aws_secret_access_key"]
      )
  
    file_type = "bias_fairness/bias_fairness-consecutive-"
  
    if self.initial:
    	file_type = "bias_fairness/bias_fairness-historic-"  
    	
    output_path = "s3://{}/{}{}.pkl".format(cte.BUCKET, file_type, self.date.strftime('%Y-%m-%d'))

    return luigi.contrib.s3.S3Target(path = output_path, client = client_s3)


  def run(self):

    data_input = self.input()

    data = data_input["data"]
    best_model = data_input["best_model"]
    features = data_input["features"]

    pred = bf.predict(
      df_fe = data, 
      best_model = best_model,
      auto_variables = features
    )
    # Para agregar columna con la fecha
    #pred["date_predict"] = self.date.strftime('%Y-%m-%d')

    file_pred = file_name = "predictions/predict" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))

    guardar_ingesta(
      path_cred = self.path_cred, 
      bucket = self.bucket_path, 
      bucket_path = file_pred, 
      data = pred,
      fecha = self.date.strftime('%Y-%m-%d')
    )
"""

