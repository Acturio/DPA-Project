from src.utils import general as gral
import src.utils.constants as cte
from src.pipeline import ingesta_almacenamiento as ing
import pandas as pd
import luigi
import luigi.scheduler
import luigi.worker
import marbles.core

class TestIngestion(marbles.core.TestCase):
    
    def test_get_token_api(self):
        my_token = gral.get_api_token('conf/local/credentials2.yaml')
        token_def = gral.get_api_token(cte.CREDENTIALS)
        #, note = " El archivo credentials.yaml no es correcto"
        self.assertEqual(my_token, token_def)

    def test_get_cliente(self):
        token_def = gral.get_api_token('conf/local/credentials.yaml')
        cliente = ing.get_client(data_url = cte.DATA_URL, token = token_def)
        #, note = " No se logró obtener el cliente"
        self.assertNotEqual(token_def, cliente)

class MyTask(luigi.Task):           
    
    def run(self):
        a = TestIngestion()
        a.test_get_cliente()
        

