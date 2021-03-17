
import luigi

from src.utils import general as gral
from src.pipeline import ingesta_almacenamiento as ing
import src.utils.constants as cte

from datetime import date, timedelta, datetime
import pickle


class IngestionTask(luigi.Task):
    
    path_cred = luigi.Parameter(default = 'credentials.yaml')
    initial = luigi.BoolParameter(default=True, parsing = luigi.BoolParameter.EXPLICIT_PARSING)
    limit = luigi.IntParameter(default = 300000)
    date = luigi.DateParameter(default = None)
                    
    def output(self):
        
        hoy = datetime.today().strftime('%Y-%m-%d')
        
        if self.initial:
#            type_insp = 'historical'
            file_name = cte.BUCKET_PATH_HIST + '{}.pkl'.format(hoy)
        else:
#            type_insp = 'consecutive'
            file_name = cte.BUCKET_PATH_CONS + '{}.pkl'.format(hoy)
                
#        file_name = 'results/food_inspections/{}/{}-inspections-{}.pkl'.\
#                     format(hoy, type_insp, hoy)
                
        return luigi.local_target.LocalTarget(file_name, format = luigi.format.Nop)
        
    def run(self):
        
        s3_c = gral.get_s3_credentials(self.path_cred)
        my_token = gral.get_api_token(self.path_cred)
        
        cliente = ing.get_client(data_url = cte.DATA_URL, token = my_token)
        
        if self.initial:
            datos = ing.ingesta_inicial(
                cliente, 
                data_set = cte.DATA_SET, 
                limit = self.limit
            )
            
        else:
            datos = ing.ingesta_consecutiva(
                client = cliente, 
                data_set = cte.DATA_SET, 
                fecha = self.date.strftime('%Y-%m-%d'), 
                limit = self.limit
            )

        with self.output().open('wb') as f:
            pickle.dump(datos, f)
        

            
if __name__ == '__main__':
    luigi.run()
