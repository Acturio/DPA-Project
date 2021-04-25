from src.utils import general as gral
import src.utils.constants as cte
from src.pipeline import ingesta_almacenamiento as ing
from datetime import date
import pandas as pd
import luigi
import luigi.scheduler
import luigi.worker
import marbles.core
import marbles.mixins

class TestIngestion(marbles.core.TestCase, marbles.mixins.DateTimeMixins):

    def __init__(self, my_path, my_date):
        super().__init__()
        self.path_cred = my_path
        self.date = my_date

    def test_get_token_api(self):
        my_token = gral.get_api_token(self.path_cred)
        token_def = gral.get_api_token(cte.CREDENTIALS)
        self.assertEqual(my_token, token_def, note = "Las credenciales introducidas no cuentan con permisos")


    def test_get_cliente(self):
        token_def = gral.get_api_token(self.path_cred)
        cliente = ing.get_client(data_url = cte.DATA_URL, token = token_def)
        self.assertNotEqual(token_def, cliente, note = " No se logró obtener el cliente")

    def test_get_date_validation(self):
        fecha = self.date
        self.assertDateTimesPast(
            [fecha], 
            strict = True, 
            msg = "La fecha solicitada debe ser menor a la fecha de hoy"
        )

    def test_get_size_file_validation(self):
        cars = {
            'Brand': ['Honda Civic','Toyota Corolla','Ford Focus','Audi A4'],
            'Price': [22000,25000,27000,35000]
        }
        df = pd.DataFrame(cars, columns = ['Brand', 'Price'])
        nrow = df.shape[0]
        self.assertGreater(nrow, 10, note = "El archivo no tiene registros")
        return nrow



class MyTask(luigi.Task):           
    
    path_cred = luigi.Parameter(default = 'credentials.yaml')
    date = luigi.DateParameter(default = None)

    def run(self):

        a = TestIngestion(my_path = self.path_cred, my_date = self.date)
        a.test_get_cliente()
        a.test_get_token_api()
        a.test_get_date_validation()
        nrows = a.test_get_size_file_validation()
        print(nrows)
        
# PYTHONPATH='.' luigi --module luigi_test MyTask --path-cred ./conf/local/credentials.yaml --local-scheduler
