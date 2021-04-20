import yaml
import datetime
import pandas as pd

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
    date_time = datetime.datetime.strptime(date_time_string, '%Y-%m-%d %H:%M')

    data={"file_name": file_name
     "data_date": data_date
     "processing_data" : date_time
     "nrows" : data.shape[0]
     "ncols" : data.shape[1]
     "extension" : file_name[-3:-1]
     "col_names" : ",".join(list(data.colnames))
     "source" : "data.cityofchicago.org"
     "dataset" : "4ijn-s7e5"
    }
    return pd.DataFrame(data)
