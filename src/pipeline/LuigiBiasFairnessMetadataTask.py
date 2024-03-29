from luigi.contrib.postgres import CopyToTable
from src.utils.general import bias_fairness_metadata, read_yaml_file
from src.utils.utils import load_df
from src.pipeline.LuigiBiasFairnessTestTask import BiasFairnessTestTask
from src.pipeline.ingesta_almacenamiento import get_s3_client
import src.utils.constants as cte
import pandas as pd
import luigi
import psycopg2
import yaml
import pickle

class BiasFairnessMetadataTask(CopyToTable):

  path_cred = luigi.Parameter(default = 'credentials.yaml')
  initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
  limit = luigi.IntParameter(default = 300000)
  date = luigi.DateParameter(default = None)
  initial_date = luigi.DateParameter(default = None)
  bucket_path = luigi.Parameter(default = cte.BUCKET)
  exercise = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)

  with open(cte.CREDENTIALS, 'r') as f:
  	config = yaml.safe_load(f)

  credentials = config['db']

  user = credentials['user']
  password = credentials['pass']
  database = credentials['database']
  host = credentials['host']
  port = credentials['port']

  table = 'metadata.bias_fairness'

  columns = [("file_name", "VARCHAR"),
             ("data_date", "DATE"),
             ("processing_date", "TIMESTAMPTZ"),
             ("nrows", "INTEGER"),
             ("protected_group","VARCHAR"),
             ("categories_names", "VARCHAR"),
             ("source","VARCHAR"),
             ("dataset", "VARCHAR")
             ]

  def requires(self):
    return BiasFairnessTestTask(
      					self.path_cred,
      					self.initial,
     					  self.limit,
      					self.date,
      					self.initial_date,
      					self.bucket_path,
  							self.exercise
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
            FROM sesgo.bias_fairness
        """
    )
    rows = cur.fetchall()
      
    data = pd.DataFrame(rows)
    data.columns = [desc[0] for desc in cur.description]

    return data


  def rows(self):

    biasfairness_metrics_table = self.input()
		
    data = bias_fairness_metadata(
    	data = self.input(),
    	data_date = self.date)
    print(data)
    records = data.to_records(index=False)
    r = list(records)
    
    for element in r:
    	yield element
