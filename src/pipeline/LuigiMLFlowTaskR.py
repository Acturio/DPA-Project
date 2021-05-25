import luigi
import subprocess
import yaml
import psycopg2
import pandas as pd
import src.utils.constants as cte
from time import gmtime, strftime
from luigi.contrib.postgres import CopyToTable
from src.pipeline.LuigiTrainingMetadataTask import TrainingModelMetadataTask

class MLFlowTaskR(CopyToTable):

  path_cred = luigi.Parameter(default = 'credentials.yaml')
  initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
  limit = luigi.IntParameter(default = 300000)
  date = luigi.DateParameter(default = None)
  initial_date = luigi.DateParameter(default = None)
  bucket_path = luigi.Parameter(default = cte.BUCKET)
  exercise = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)

  def requires(self):
    return TrainingModelMetadataTask(
      self.path_cred,
      self.initial,
      self.limit,
      self.date,
      self.initial_date,
      self.bucket_path,
      self.exercise
    )

  with open(cte.CREDENTIALS, 'r') as f:
    config = yaml.safe_load(f)

  credentials = config['db']

  user = credentials['user']
  password = credentials['pass']
  database = credentials['database']
  host = credentials['host']
  port = credentials['port']

  table = 'metadata.mlflow'

  columns = [
       ("processing_date","TIMESTAMPTZ"),
			 ("data_date","DATE"),
			 ("mlflow","VARCHAR")
  ]

  def rows(self):

    subprocess.call(['Rscript', 'src/pipeline/LuigiMLFlow_R.R'])

    df = {
      "processing_date": strftime("%Y-%m-%d %H:%M:%S", gmtime()),
      "data_date": self.date.strftime('%Y-%m-%d'),
      "mlflow" : "TRUE",
    }
    data = pd.DataFrame(df, index=[0])
      
    records = data.to_records(index=False)
    r = list(records)

    for element in r:
      yield element
    
# PYTHONPATH='.' luigi --module src.pipeline.LuigiMLFlowTaskR MLFlowTaskR --path-cred ./conf/local/credentials.yaml --initial false --limit 2000 --date '2021-05-10' --exercise true

# PYTHONPATH='.' luigi --module src.pipeline.LuigiModelSelectionTask ModelSelectionTask --path-cred ./conf/local/credentials.yaml --initial false --limit 2000 --date '2021-05-11' --exercise true