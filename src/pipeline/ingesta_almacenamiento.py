from src.utils.general import get_s3_credentials, get_api_token
from sodapy import Socrata
from datetime import date, timedelta
import src.utils.constants as cte
import boto3
import pickle

def get_client(token):
    """
    Esta función regresa un cliente que se puede conectar a la API de inspecciones
    de establecimiento dándole un token previamente generado.
    :param: none (integrado yam file)
    :return: client API
    """
    # Create the client to point to the API endpoint, with app token created in the prior steps
    client = Socrata(cte.DATA_URL, token)
    return client


def ingesta_inicial(client, limite):
    """
    Esta función recibe como parámetros el cliente con el que nos podemos comunicar con la API,
    y el límite de registros que queremos obtener al llamar a la API.
    Regresa una lista de los elementos que la API regresó.
    :param: Cliente API, límite de datos
    :return: pickle con datos binarios
    """
    # Set the timeout to 60 seconds
    client.timeout = 60
    # Retrieve the first 'limite' results returned as JSON object from the API
    # The SoDaPy library converts this JSON object to a Python list of dictionaries
    results = client.get(cte.DATA_SET, where="inspection_date >= '2021-01-01' ", limit=limite)  
    # Convert pickle
    bd_historica = pickle.dumps(results)

    return bd_historica


def get_s3_resource():
    """
    Esta función regresa un resource de S3 para poder guardar datos
    en el bucket (checar script de aws_boto_s3).
    :param: none (llama función get_s3_credentials para credenciales)
    :return: recurso S3
    """
    # "../../conf/local/credentials.yaml"
    s3_creds = get_s3_credentials("conf/local/credentials.yaml")

    session = boto3.Session(
        aws_access_key_id=s3_creds['aws_access_key_id'],
        aws_secret_access_key=s3_creds['aws_secret_access_key']
    )

    s3 = session.resource('s3')

    return s3


def guardar_ingesta(bucket, bucket_path, data):
    """
    Esta función recibe como parámetros el nombre de tu bucket de S3,
    la ruta en el bucket en donde se guardarán los datos y los datos ingestados en pkl.
    :param: bucket, bucket path, datos pkl y el resurso se obtiene llamando a la función get_s3_resurce
    :return: none(guarda datos s3)
    """
    # Obtiene el recurso S3
    s3_resource = get_s3_resource()

    today = date.today()
    # ingestion/initial/historic-inspections-2020-02-02.pkl
    # ingestion/consecutive/consecutive-inspections-2020-11-03.pkl
    file_name = bucket_path + today.strftime('%Y-%m-%d') + ".pkl"

    s3_resource.Object(bucket, file_name).put(Body=data)

    print("Ingesta guardada")


def ingesta_consecutiva(client, fecha, limite):
    """
    Esta función recibe como parámetros el cliente con el que nos podemos comunicar con la API,
    la fecha de la que se quieren obtener nuevos datos al llamar a la API y
    el límite de registros para obtener de regreso.
    :param: Cliente API, fecha inicial desde donde se extraeran los datos, límite de datos
    :return: pickle con datos binarios
    """
    # Set the timeout to 60 seconds    
    client.timeout = 60
        
    if fecha is None:
        fecha_ini = date.today() - timedelta(7)
        where_cond = "inspection_date >= '" + fecha_ini.strftime('%Y-%m-%d') + "' "
        
    else:
        where_cond = "inspection_date >= '" + fecha + "' "
        
    results = client.get(cte.DATA_SET, where = where_cond, limit = limite)
        
    bd_consecutiva = pickle.dumps(results) 
    
    return bd_consecutiva
