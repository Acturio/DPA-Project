import luigi
import boto3
import pandas as pd
import pickle
import luigi.contrib.s3
import src.utils.constants as cte

from src.utils import general as gral
from src.pipeline.ingesta_almacenamiento import get_s3_client, guardar_ingesta
from src.pipeline.LuigiTransformationTask import TransformTask
from src.pipeline.transformation import *


class FeatureEngineeringTask(luigi.Task):

  path_cred = luigi.Parameter(default = 'credentials.yaml')
  initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameterEXPLICIT_PARSING)
  limit = luigi.IntParameter(default = 300000)
  date = luigi.DateParameter(default = None)
  initial_date = luigi.DateParameter(default = None)
  bucket_path = luigi.Parameter(default = cte.BUCKET)

  # Se requiere TransformTask
  def requires(self):
    return TransformTask(
      self.path_cred,
      self.initial,
      self.limit,
      self.date,
      self.initial_date,
      self.bucket_path
    )

  # Se carga el archivo sobre el cual se realizará el feature engineering

  def input(self):

    if self.initial:
       file_name = "processed-data/historic-inspections-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))
    else:
       file_name = "processed-data/consecutive-inspections-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))

    s3 = get_s3_client(self.path_cred)
    s3_object = s3.get_object(Bucket = self.bucket_path, Key = file_name)
    body = s3_object['Body']
    my_pickle = pickle.loads(body.read())

    data = pd.DataFrame(my_pickle)
    return data













