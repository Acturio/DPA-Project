from luigi.contrib.postgres import CopyToTable
from src.pipeline import transformation as transf
from src.pipeline import bias_fairness as bf
from src.pipeline.ingesta_almacenamiento import get_s3_client, guardar_ingesta
from src.utils import general as gral
from src.pipeline.LuigiPredictTestTask import PredictTestTask

import luigi
#import boto3
import pandas as pd
import pickle
import luigi.contrib.s3
import src.utils.constants as cte
import yaml
import psycopg2

class PredictMetadataTask(CopyToTable):

  path_cred = luigi.Parameter(default = 'credentials.yaml')
  initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
  limit = luigi.IntParameter(default = 300000)
  date = luigi.DateParameter(default = None)
  initial_date = luigi.DateParameter(default = None)
  bucket_path = luigi.Parameter(default = cte.BUCKET)
  exercise = luigi.BoolParameter(default=False, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
  #rama = luigi.Parameter(default = 'almacenamiento')# o monitoreo
  date_bestmodel = luigi.DateParameter(default = None)

  with open(cte.CREDENTIALS, 'r') as f:
    config = yaml.safe_load(f)

  credentials = config['db']

  user = credentials['user']
  password = credentials['pass']
  database = credentials['database']
  host = credentials['host']
  port = credentials['port']

  table = 'metadata.predict'

  columns = [("file_name", "VARCHAR"),
             ("data_date", "DATE"),
             ("processing_date", "TIMESTAMPTZ"),
             ("nrows", "INTEGER"),
             ("ncols", "INTEGER"),
             #("label_1", "INTEGER"),
             #("label_0", "INTEGER"),
             #("score_1", "INTEGER"),
             #("score_0", "INTEGER"),
             ("source","VARCHAR"),
             ("dataset", "VARCHAR")
             ]

  def requires(self):
    return PredictTestTask(
      self.path_cred,
      self.initial,
     	self.limit,
      self.date,
      self.initial_date,
      self.bucket_path,
      self.exercise,
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
		
    data = gral.predict_metadata(
    	data = self.input(),
    	data_date = self.date)
    
    print(data)
    records = data.to_records(index=False)
    r = list(records)
    
    for element in r:
    	yield element
