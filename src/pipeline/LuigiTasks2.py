
import luigi
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
    bucket_path = luigi.Parameter(default = 'data-product-architecture-equipo-n')
    
    # Se requiere IngestionTask
    def requires(self):
        return IngestionTask(self.path_cred, self.initial, self.limit, self.date)
    
    # Se carga el archivo a ser usado
    def input(self):
        
        if self.initial:
            file_name = cte.BUCKET_PATH_HIST + '{}.pkl'.format(self.date)
        else:
            file_name = cte.BUCKET_PATH_CONS + '{}.pkl'.format(self.date)
        
        with open(file_name, 'rb') as f:
            data = pickle.load(f)

        return data

    
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
            data = data
        )

    
            
if __name__ == '__main__':
    luigi.run()
