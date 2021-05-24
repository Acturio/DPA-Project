from luigi.contrib.postgres import CopyToTable
from src.utils.general import bias_fairness_metadata, read_yaml_file
from src.utils.utils import load_df
from src.pipeline.LuigiApiTask import ApiTask
from src.pipeline.ingesta_almacenamiento import get_s3_client
import src.utils.constants as cte
import pandas as pd
import luigi
import psycopg2
import yaml
import pickle

class MonitoreoTask(CopyToTable):

  path_cred = luigi.Parameter(default = 'credentials.yaml')
  initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
  limit = luigi.IntParameter(default = 300000)
  date = luigi.DateParameter(default = None)
  initial_date = luigi.DateParameter(default = None)
  bucket_path = luigi.Parameter(default = cte.BUCKET)
  exercise = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
  date_bestmodel = luigi.DateParameter(default = None)

  with open(cte.CREDENTIALS, 'r') as f:
  	config = yaml.safe_load(f)

  credentials = config['db']

  user = credentials['user']
  password = credentials['pass']
  database = credentials['database']
  host = credentials['host']
  port = credentials['port']

  table = 'metadata.bias_fairness'

  columns = [("fecha_load", "DATE"),
             ("fecha", "DATE"),
             ("establecimiento", "VARCHAR"),
             ("label", "INTEGER"),
             ("score","DOUBLE"),
             ("pred_score","DOUBLE"),
             ("facility_type","VARCHAR"),
             ("inspection_type","VARCHAR"),
             ("modelo","VARCHAR"),
             ("id", "VARCHAR")
             ]

  def requires(self):
    return ApiTask(
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
            FROM predict.predictions WHERE fecha = '{}'
        """.format(self.date.strftime('%Y-%m-%d'))
    )
    rows = cur.fetchall()
      
    data = pd.DataFrame(rows)
    data.columns = [desc[0] for desc in cur.description]

    return data


  def rows(self):

    data = self.input()
    #print(data)
    records = data.to_records(index=False)
    r = list(records)
    
    for element in r:
    	yield element
