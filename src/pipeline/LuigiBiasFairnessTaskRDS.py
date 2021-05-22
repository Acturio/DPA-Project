from luigi.contrib.postgres import CopyToTable
from src.pipeline import transformation as transf
from src.pipeline import bias_fairness as bf
from src.pipeline.ingesta_almacenamiento import get_s3_client, guardar_ingesta
from src.utils import general as gral
from src.pipeline.LuigiModelSelectionMetadataTask import ModelSelectionMetadataTask

import luigi
import boto3
import yaml
import pandas as pd
import pickle
import luigi.contrib.s3
import src.utils.constants as cte

class BiasFairnessTask(CopyToTable):

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

  table = 'sesgo.bias_fairness'
  	          
  columns = [("fecha_load", "DATE"),
             ("fecha", "DATE"),
             ("model_id", "INTEGER"),
             ("score_threshold", "VARCHAR"),
             ("k", "INTEGER"),
             ("attribute_name", "VARCHAR"),
             ("attribute_value","VARCHAR"),
             ("tpr","DOUBLE"),
             ("tnr","DOUBLE"),
             ("f_or","DOUBLE"),
             ("fdr","DOUBLE"),
             ("fpr","DOUBLE"),
             ("fnr","DOUBLE"),
             ("npv","DOUBLE"),
             ("precision","DOUBLE"),
             ("pp","INTEGER"),
             ("pn","INTEGER"),
             ("ppr","DOUBLE"),
             ("pprev","DOUBLE"),
             ("fp","INTEGER"),
             ("fn","INTEGER"),
             ("tn","INTEGER"),
             ("tp","INTEGER"),
             ("group_label_pos","INTEGER"),
             ("group_label_neg","INTEGER"),
             ("group_size","INTEGER"),
             ("total_entities","INTEGER"),
             ("prev","DOUBLE"), 
             ("ppr_disparity","DOUBLE"),
             ("pprev_disparity","DOUBLE"),
             ("precision_disparity","DOUBLE"),
             ("fdr_disparity","DOUBLE"),
             ("for_disparity","DOUBLE"),
             ("fpr_disparity","DOUBLE"),
             ("fnr_disparity","DOUBLE"),
             ("tpr_disparity","DOUBLE"),
             ("tnr_disparity","DOUBLE"),
             ("npv_disparity","DOUBLE"),
             ("ppr_ref_group_value","VARCHAR"),
             ("pprev_ref_group_value","VARCHAR"),
             ("precision_ref_group_value","VARCHAR"),
             ("fdr_ref_group_value","VARCHAR"),
             ("for_ref_group_value","VARCHAR"),
             ("fpr_ref_group_value","VARCHAR"),
             ("fnr_ref_group_value","VARCHAR"),
             ("tpr_ref_group_value","VARCHAR"),
             ("tnr_ref_group_value","VARCHAR"),
             ("npv_ref_group_value","VARCHAR"),
             ("statistical_parity","BOOLEAN"),
             ("impact_parity","BOOLEAN"),
             ("fdr_parity","BOOLEAN"),
             ("fpr_parity","BOOLEAN"),
             ("for_parity","BOOLEAN"),
             ("fnr_parity","BOOLEAN"),
             ("tpr_parity","BOOLEAN"),
             ("tnr_parity","BOOLEAN"),
             ("npv_parity","BOOLEAN"),
             ("precision_parity","BOOLEAN"),
             ("typei_parity","BOOLEAN"),
             ("typeii_parity","BOOLEAN"),
             ("equalized_odds","BOOLEAN"),
             ("unsupervised_fairness","BOOLEAN"),
             ("supervised_fairness","BOOLEAN")
             ]

  def requires(self):
  	#requiere dataframe_fe, mejor modelo y autovariables
    return ModelSelectionMetadataTask(
      self.path_cred,
      self.initial,
      self.limit,
      self.date,
      self.initial_date,
      self.bucket_path,
      self.exercise
    )

  def input(self):
        
    file_best_model = "models/best-models/best-food-inspections-model-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))

    s3 = get_s3_client(self.path_cred)
    s3_object = s3.get_object(Bucket = self.bucket_path, Key = file_best_model)
    body = s3_object['Body']
    model_and_features = pickle.loads(body.read())

    data = gral.read_gather_s3(
      date_string = self.date.strftime('%Y-%m-%d'), 
      folder = "feature-engineering/feature", 
      cred_path = self.path_cred,
      bucket = self.bucket_path
    )

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

    data = bf.bias_fairness(
      df_fe = data, 
      best_model = best_model,
      auto_variables = features,
      inicial = self.initial,
      fecha = self.date.strftime('%Y-%m-%d')
    )

    records = data.to_records(index=False)
    r = list(records)
    
    for element in r:
      yield element

