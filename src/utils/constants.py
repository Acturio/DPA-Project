# The Host Name for the API endpoint (the https:// part will be added automatically)
DATA_URL = 'data.cityofchicago.org'
# The data set at the API endpoint
DATA_SET = '4ijn-s7e5'
# Nombre del bucket
BUCKET = "data-product-architecture-equipo-n"
# Path de ingesta histrórica
BUCKET_PATH_HIST = "ingestion/initial/historic-inspections-"
# Path de ingesta consecutiva
BUCKET_PATH_CONS = "ingestion/consecutive/consecutive-inspections-"
# Path local para las credenciales
CREDENTIALS = 'conf/local/credentials.yaml'

CREDENTIALS_TEST = './conf/local/credentials2.yaml'