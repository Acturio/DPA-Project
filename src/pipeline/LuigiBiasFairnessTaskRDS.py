from luigi.contrib.postgres import CopyToTable
from src.pipeline import transformation as transf
from src.pipeline import bias_fairness as bf
from src.pipeline.ingesta_almacenamiento import get_s3_client, guardar_ingesta
from src.utils import general as gral
from src.pipeline.LuigiModelSelectionMetadataTask import ModelSelectionMetadataTask

import luigi
import boto3
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
  exercise = luigi.BoolParameter(default=False, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
  
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
             ("for","DOUBLE"),
             ("fdr","DOUBLE"),
             ("fpr","DOUBLE"),
             ("fnr","DOUBLE"),
             ("npv","DOUBLE"),
             ("precision","DOUBLE"),
             ("pp","DOUBLE"),
             ("pn","DOUBLE"),
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
             ("ppr_ref_group_value",""),
             ("pprev_ref_group_value",""),
             ("precision_ref_group_value",""),
             ("fdr_ref_group_value",""),
             ("for_ref_group_value",""),
             ("fpr_ref_group_value",""),
             ("fnr_ref_group_value",""),
             ("tpr_ref_group_value",""),
             ("tnr_ref_group_value",""),
             ("npv_ref_group_value",""),
             ("Statistical Parity",""),
             ("Impact Parity",""),
             ("FDR Parity",""),
             ("FPR Parity",""),
             ("FOR Parity",""),
             ("FNR Parity",""),
             ("TPR Parity",""),
             ("TNR Parity",""),
             ("NPV Parity",""),
             ("Precision Parity",""),
             ("TypeI Parity",""),
             ("",""),
             ("",""),
             ("",""),
             ("",""),
             ("",""),
             ("","")
             ]
             '','','','Equalized Odds','Unsupervised Fairness','Supervised Fairness'

  def requires(self):
  	#requiere dataframe_fe, mejor modelo y autovariables
    return ModelSelectionMetadataTask(
      self.path_cred,
      self.initial,
      self.limit,
      self.date,
      self.initial_date,
      self.bucket_path
    )

  def input(self):
  
    if self.initial:
    	file_name = "feature-engineering/feature-historic-inspections-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))    	
    else:
    	file_name = "feature-engineering/feature-consecutive-inspections-" + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))
        
    file_best_m = "models/best-models/best-food-inspections-model-"

    s3 = get_s3_client(self.path_cred)
    s3_object = s3.get_object(Bucket = self.bucket_path, Key = file_name)
    body = s3_object['Body']
    my_pickle = pickle.loads(body.read())
		
    data = pd.DataFrame(my_pickle)
    return data


  def rows(self):
    #models_filename = "results/models/training-models/food-inspections-models-metadata-" 
    #models_filename = models_filename + self.date.strftime('%Y-%m-%d') + ".pkl"

    data = bf.bias_fairness(
      df_fe = data, 
      best_model = best,
      auto_variables = autov,
      path_save_models = bias_fair_filename, 
      exercise = self.exercise
      )
    records = data.to_records(index=False)
      r = list(records)
    
    for element in r:
      yield element

