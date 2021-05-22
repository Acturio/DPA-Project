import luigi
import boto3
import pandas as pd
import pickle
import luigi.contrib.s3
import src.utils.constants as cte

from src.utils import general as gral
from src.pipeline.ingesta_almacenamiento import get_s3_client, guardar_ingesta
from src.pipeline.LuigiTransformationMetadataTask import TransformationMetadataTask
from src.pipeline.feature_engineering import *

class FeatureEngineeringTask(luigi.Task):

  path_cred = luigi.Parameter(default = 'credentials.yaml')
  initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
  limit = luigi.IntParameter(default = 300000)
  date = luigi.DateParameter(default = None)
  initial_date = luigi.DateParameter(default = None)
  bucket_path = luigi.Parameter(default = cte.BUCKET)

  # Se requiere TransformationMetadataTask
  def requires(self):
    return TransformationMetadataTask(
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
       file_name = "processed-data/clean-historic-inspections-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))
    else:
       file_name = "processed-data/clean-consecutive-inspections-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))

    s3 = get_s3_client(self.path_cred)
    s3_object = s3.get_object(Bucket = self.bucket_path, Key = file_name)
    body = s3_object['Body']
    my_pickle = pickle.loads(body.read())

    data = pd.DataFrame(my_pickle)
    return data


  def output(self):
          
    s3_c = gral.get_s3_credentials(self.path_cred)
    client_s3 = luigi.contrib.s3.S3Client(
      aws_access_key_id = s3_c["aws_access_key_id"],
      aws_secret_access_key = s3_c["aws_secret_access_key"]
      )
  
    if self.initial:
        file_type = "feature-engineering/feature-historic-inspections-"
    else:
        file_type = "feature-engineering/feature-consecutive-inspections-"
          
    output_path = "s3://{}/{}{}.pkl".format(cte.BUCKET, file_type, self.date.strftime('%Y-%m-%d'))

    return luigi.contrib.s3.S3Target(path = output_path, client = client_s3)


  def run(self):

    if self.initial:
        file_type = "feature-engineering/feature-historic-inspections-"
    else:
        file_type = "feature-engineering/feature-consecutive-inspections-"

    data = self.input()
    output_path = "{}{}.pkl".format(file_type, self.date.strftime('%Y-%m-%d'))
    local_path = "results/" + output_path

    feature_data = feature_engineering(data, local_path)

    guardar_ingesta(
      path_cred = self.path_cred, 
      bucket = self.bucket_path, 
      bucket_path = file_type, 
      data = feature_data,
      fecha = self.date.strftime('%Y-%m-%d')
    )







