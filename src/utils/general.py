import yaml
import datetime
from time import gmtime, strftime
import pandas as pd
import pickle
import boto3

def read_yaml_file(yaml_file):
    """
    Load yaml cofigurations
    :param: file_name
    :return: cofigurations
    """

    config = None
    try:
        with open(yaml_file, 'r') as f:
            config = yaml.safe_load(f)
    except:
        raise FileNotFoundError('Could not load the file')

    return config


# este va en src/utils general.py
def get_s3_credentials(credentials_file):
    """
    Regresa credenciales de conexión a AWS
    :param: file_name
    :return: S3 credentials
    """
    credentials = read_yaml_file(credentials_file)
    s3_creds = credentials['s3']

    return s3_creds

def get_db_credentials(credentials_file):
    """
    Regresa credenciales de conexión a postgres
    :param: file_name
    :return: db credentials
    """
    credentials = read_yaml_file(credentials_file)
    db_creds = credentials['db']

    return db_creds

def get_db_conn_sql_alchemy(credentials_file):
    """
    Regresa credenciales de conexión a postgres
    :param: file_name
    :return: db credentials
    """
    credentials = read_yaml_file(credentials_file)
    db_creds = credentials['db']

    path_conn = 'postgresql://'  + \
                db_creds["user"] + ":" + \
                db_creds["pass"] + "@" + \
                db_creds["host"] + ":" + \
                db_creds["port"] + "/" + \
                db_creds["database"]

    return path_conn


def get_api_token(credentials_file):
    """
    Regresa token API
    Regresa credenciales de conexión a AWS
    :param: file_name
    :return: token
    """
    credentials = read_yaml_file(credentials_file)
    token = credentials['food_inspections']
    token = token["api_token"]

    return token

def ingestion_metadata(data, file_name, data_date):
    date_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())

    df = {"file_name": file_name,
     "data_date": data_date,
     "processing_data" : date_time,
     "nrows" : data.shape[0],
     "ncols" : data.shape[1],
     "extension" : file_name[-3:],
     "col_names" : ",".join(list(data.columns.values)),
     "source" : "data.cityofchicago.org",
     "dataset" : "4ijn-s7e5"
    }
    return pd.DataFrame(df, index=[0])


def export_metadata(data, data_date, initial):

    if initial:
        file_name = 'historic-inspections-{}.pkl'.format(data_date.strftime('%Y-%m-%d'))
        subfolder = "initial"
        dataset = "historic-inspections"
    else:
        file_name = 'consecutive-inspections-{}.pkl'.format(data_date.strftime('%Y-%m-%d'))
        subfolder = "consecutive"
        dataset = "consecutive-inspections"

    source = "s3://data-product-architecture-equipo-n/ingestion/" + subfolder
    date_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())

    df = {
        "file_name": file_name,
        "data_date": data_date,
        "processing_data" : date_time,
        "nrows" : data.shape[0],
        "ncols" : data.shape[1],
        "extension" : file_name[-3:],
        "col_names" : ",".join(list(data.columns.values)),
        "source" : source,
        "dataset" : dataset
       }

    return pd.DataFrame(df, index=[0])


def transform_metadata(data, data_date, initial):

    if initial:
        file_name = 'clean-historic-inspections-{}.pkl'.format(data_date.strftime('%Y-%m-%d'))
        dataset = "clean-historic-inspections"
    else:
        file_name = 'clean-consecutive-inspections-{}.pkl'.format(data_date.strftime('%Y-%m-%d'))
        dataset = "clean-consecutive-inspections"

    source = "s3://data-product-architecture-equipo-n/processed-data/"
    date_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())

    df = {
        "file_name": file_name,
        "data_date": data_date,
        "processing_data" : date_time,
        "nrows" : data.shape[0],
        "ncols" : data.shape[1],
        "extension" : file_name[-3:],
        "col_names" : ",".join(list(data.columns.values)),
        "source" : source,
        "dataset" : dataset
       }

    return pd.DataFrame(df, index=[0])


def feature_metadata(data, data_date, initial):

    if initial:
        file_name = 'feature-historic-inspections-{}.pkl'.format(data_date.strftime('%Y-%m-%d'))
        dataset = "feature-historic-inspections"
    else:
        file_name = 'feature-consecutive-inspections-{}.pkl'.format(data_date.strftime('%Y-%m-%d'))
        dataset = "feature-consecutive-inspections"

    source = "s3://data-product-architecture-equipo-n/feature-engineering/"
    date_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())

    df = {
        "file_name": file_name,
        "data_date": data_date,
        "processing_data" : date_time,
        "nrows" : data.shape[0],
        "ncols" : data.shape[1],
        "extension" : file_name[-3:],
        "col_names" : ",".join(list(data.columns.values)),
        "source" : source,
        "dataset" : dataset
       }

    return pd.DataFrame(df, index=[0])


def bias_fairness_metadata(data, data_date):

    file_name = 'bias-fairness-metadata-{}'.format(data_date.strftime('%Y-%m-%d'))
    dataset = "bias-and-fairness"
    date_time = strftime("%Y-%m-%d %H:%M:%S", gmtime())

    df = {
        "file_name": file_name,
        "data_date": data_date,
        "processing_data" : date_time,
        "nrows" : data.shape[0],
        "protected_group": data.loc[0,"attribute_name"],
        "categories_names" : ",".join(list(data.attribute_value)),
        "source" : "postgres rds",
        "dataset" : dataset
       }

    return pd.DataFrame(df, index=[0])



def get_s3_resource(path_cred):
    """
    Esta función regresa un resource de S3 para poder guardar datos
    en el bucket (checar script de aws_boto_s3).
    :param: none (llama función get_s3_credentials para credenciales)
    :return: recurso S3
    """
    s3_creds = get_s3_credentials(path_cred)

    session = boto3.Session(
        aws_access_key_id=s3_creds['aws_access_key_id'],
        aws_secret_access_key=s3_creds['aws_secret_access_key']
    )

    s3 = session.resource('s3')
    return s3


def read_gather_s3(date_string, folder, cred_path, bucket):

    s3 = get_s3_resource(cred_path)
    bucket = s3.Bucket(bucket)
    prefix_objs = bucket.objects.filter(Prefix = folder)
    
    prefix_df = []

    for obj in prefix_objs:
        key = obj.key
        data_day = key[-14:-4]
        if data_day > date_string:
            continue
        print(data_day)
        body = obj.get()['Body'].read()
        temp = pickle.loads(body)
        prefix_df.append(temp)
    gather_data = pd.concat(prefix_df)
    return gather_data.drop_duplicates()
