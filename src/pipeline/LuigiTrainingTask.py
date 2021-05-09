from  src.pipeline import transformation as transf
from  src.pipeline import feature_engineering as fe
from src.pipeline import modeling as mod
from src.pipeline.ingesta_almacenamiento import get_s3_client, guardar_ingesta
from src.utils import general as gral
from src.pipeline.LuigiFeatureEngineeringMetadataTask import FeatureMetadataTask

import luigi
import boto3
import pandas as pd
import pickle
import luigi.contrib.s3
import src.utils.constants as cte

class TrainingModelTask(luigi.Task):

  path_cred = luigi.Parameter(default = 'credentials.yaml')
  initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
  limit = luigi.IntParameter(default = 300000)
  date = luigi.DateParameter(default = None)
  initial_date = luigi.DateParameter(default = None)
  bucket_path = luigi.Parameter(default = cte.BUCKET)
  exercise = luigi.BoolParameter(default=False, parsing = luigi.BoolParameter.EXPLICIT_PARSING)

  def requires(self):
    return FeatureMetadataTask(
      self.path_cred,
      self.initial,
      self.limit,
      self.date,
      self.initial_date,
      self.bucket_path
    )

  def input(self):

    data = gral.read_gather_s3(
      date_string = self.date.strftime('%Y-%m-%d'), 
      folder = "feature-engineering/feature", 
      cred_path = self.path_cred,
      bucket = self.bucket_path
    )

    return data


  def output(self):
          
    s3_c = gral.get_s3_credentials(self.path_cred)
    client_s3 = luigi.contrib.s3.S3Client(
      aws_access_key_id = s3_c["aws_access_key_id"],
      aws_secret_access_key = s3_c["aws_secret_access_key"]
      )
  
    file_type = "models/training-models/food-inspections-models-"
    output_path = "s3://{}/{}{}.pkl".format(cte.BUCKET, file_type, self.date.strftime('%Y-%m-%d'))

    return luigi.contrib.s3.S3Target(path = output_path, client = client_s3)


  def run(self):

    data = self.input()
    models_filename = "results/models/training-models/food-inspections-models-" + self.date.strftime('%Y-%m-%d') + ".pkl"
    file_type = "models/training-models/food-inspections-models-"

    models = mod.training(
      df_fe = data, 
      path_save_models = models_filename, 
      exercise = self.exercise
      )

    guardar_ingesta(
      path_cred = self.path_cred, 
      bucket = self.bucket_path, 
      bucket_path = file_type, 
      data = models,
      fecha = self.date.strftime('%Y-%m-%d')
    )


