import pandas as pd
import numpy as np
from src.utils import utils as u 
import time


def generate_label(df):
    """
    Crea en el data frame de los datos la variable label que es 1
    cuando el código de cierre es 'Pass','Pass w/ Conditions', 0 en caso de 'Fail'.
    :param: dataframe
    :return: dataframe
    """
    df['label'] = np.where(df.results.isin(['Pass','Pass w/ Conditions']), 0, 1)
    # se considerará como exito el detectar que NO PASO LA INSPECCION
    return df

def date_transformation(col, df):
    """
    Recibe la columna que hay que transformar a DATE y el data frame al que pertenece.
    :param: column, dataframe
    :return: column
    """
    return pd.to_datetime(df[col])

def numeric_transformation(col, df):
    """
    Recibe la columna que hay que transformar a NUMERIC (entera) y el data frame al que pertenece.
    :param: column, dataframe
    :return: column
    """
    return df[col].astype(float)

def int_transformation(col, df):
    """
    Recibe la columna que hay que transformar a NUMERIC (entera) y el data frame al que pertenece.
    :param: column, dataframe
    :return: column
    """
    return df[col].astype(int)

def categoric_trasformation(col, df):
    """
    Recibe la columna que hay que transformar a CATEGORICA y el data frame al que pertenece.
    :param: column, dataframe
    :return: column
    """
    return df[col].astype(str)


def clean(df):
    """
    Recibe un dataframe y le aplica limpieza de datos
    :param: dataframe
    :return: dataframe
    """    
    
    # Nos quedamos sólo con los resultados 'Pass','Pass w/ Conditions', 'Fail
    df = df[df.results.isin(['Pass','Pass w/ Conditions', 'Fail'])]     
    
    # Quitamos NA
    df = df[df['inspection_type'].notna()]
    df = df[df['facility_type'].notna()]
    df = df[df['risk'].notna()]
    df = df[df['city'].notna()]
    df = df[df['state'].notna()]
    df = df[df['zip'].notna()]
    df = df[df['latitude'].notna()]
    df = df[df['longitude'].notna()]
    df = df[df['location'].notna()]
 
    # Generamos el label
    df = generate_label(df)
    
     # Categoricas
    df['facility_type'] = categoric_trasformation('facility_type', df)
    df['zip'] = categoric_trasformation('zip', df)

    # Númericas
    df['label'] = int_transformation('label', df)
    df['latitude'] = numeric_transformation('latitude', df)
    df['longitude'] = numeric_transformation('longitude', df)
    
    # Transformando fechas.
    df['inspection_date'] = date_transformation('inspection_date', df)

        
    # Ordenando datos por fecha de inspeccion
    df = df.sort_values(['inspection_date'])

    # Reseteando indice con datos ordenados por fecha
    df = df.reset_index(drop=True)

    # Nos quedamos con que usaremos para el modelo
    df = df[['inspection_id', 'facility_type', 'inspection_type','risk', 'zip', 'inspection_date','latitude','longitude','label']]  

    return df   

def transform(df ,path_save):
    """
    Recibe la ruta del pickle que hay que transformar y devuelve en una ruta los datos transformados en pickle.
    :param: path
    :return: file
    """
    print("Inicio proceso: Transformación y limpieza")
    start_time = time.time()
    #df = u.load_df(path)
    
    # Limpieza de datos
    df_final = clean(df)
    
    # Se guarda pkl
    u.save_df(df_final, path_save)
    print("Archivo 'pkl_transform.pkl' escrito correctamente")   
    
    print("Finalizó proceso:  Transformación y limpieza en ", time.time() - start_time)
    
    return df_final