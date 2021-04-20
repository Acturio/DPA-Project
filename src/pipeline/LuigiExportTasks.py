
import luigi
import luigi.contrib.s3
from src.utils import general as gral
from src.pipeline import ingesta_almacenamiento as ing
from src.pipeline.LuigiIngestionTasks import IngestionTask
import src.utils.constants as cte
from datetime import date, timedelta, datetime
import pickle

class ExportFileTask(luigi.Task):
    
    path_cred = luigi.Parameter(default = 'credentials.yaml')
    initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
    limit = luigi.IntParameter(default = 300000)
    date = luigi.DateParameter(default = None)
    initial_date = luigi.DateParameter(default = None)
    bucket_path = luigi.Parameter(default = cte.BUCKET)# Bucket en archivo de constantes
    
    # Se requiere IngestionTask
    def requires(self):
        
        return IngestionMetadata(
            self.path_cred, self.initial, self.limit, self.date, self.initial_date
            )
    
    # Se carga el archivo a ser usado
    def input(self):
        
        if self.initial:
            file_name = cte.BUCKET_PATH_HIST + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))
        else:
            file_name = cte.BUCKET_PATH_CONS + '{}.pkl'.format(self.date.strftime('%Y-%m-%d'))
        
        with open(file_name, 'rb') as f:
            data = pickle.load(f)

        return data

    def output(self):
        
        s3_c = gral.get_s3_credentials(self.path_cred)
        client_s3 = luigi.contrib.s3.S3Client(aws_access_key_id = s3_c["aws_access_key_id"],
                             aws_secret_access_key = s3_c["aws_secret_access_key"])
        
        if self.initial:
            file_type = cte.BUCKET_PATH_HIST 
        else:
            file_type = cte.BUCKET_PATH_CONS
            
        output_path = "s3://{}/{}{}.pkl".format(cte.BUCKET, file_type, self.date.strftime('%Y-%m-%d'))

        return luigi.contrib.s3.S3Target(path = output_path, client = client_s3)
    
    
    def run(self):
        
        if self.initial:
            file_type = cte.BUCKET_PATH_HIST 
        else:
            file_type = cte.BUCKET_PATH_CONS
                    
        data = self.input()
        
        ing.guardar_ingesta(
            path_cred = self.path_cred, 
            bucket = self.bucket_path, 
            bucket_path = file_type, 
            data = data,
            fecha = self.date.strftime('%Y-%m-%d')
        )

    
            
if __name__ == '__main__':
    luigi.run()
