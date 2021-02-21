import yaml

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
