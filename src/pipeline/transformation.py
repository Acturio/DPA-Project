from utils.utils import load_df, save_df
import pandas as pd
import numpy as np


def load_ingestion(path='../output/'):
    """
    Recibe el path en donde se encuentra el pickle que generamos durante la ingestión.
    :param: path
    :return: pickle
    """
    df = load_df(path + 'ingest_df.pkl')
    return df


def generate_label(df):
    """
    Crea en el data frame de los datos la variable label que es 1
    cuando el código de cierre es 'Pass','Pass w/ Conditions', 0 en caso de 'Fail'.
    :param: dataframe
    :return: dataframe
    """
    df['label'] = np.where(df.results.isin(['Pass','Pass w/ Conditions']), 1, 0)
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


def split_fecha(col, df):
    """
    Recibe la columna fecha que hay que transformar en 3 columnas: año, mes, dia y
    el data frame al que pertenece.
    :param: column, dataframe
    :return: dataframe con 2 columnas mas (mes, día) y semanas
    """
    # df[col + '_año'] = df[col].dt.year.astype(str)
    df[col + '_mes'] = df[col].dt.month.astype(str)
    df[col + '_dia'] = df[col].dt.day.astype(str)

    # Cambio a enteros
    df[col + '_mes'] = int_transformation(col + '_mes', df)
    df[col + '_dia'] = int_transformation(col + '_dia', df)
    # Como strings
    # df[col + '_mes'] = df[col + '_mes'].apply(two_dig)
    # df[col + '_dia'] = df[col + '_dia'].apply(two_dig)

    df['week'] = df[col].dt.week
    df['day_of_week'] = df[col].dt.day_name()

    return df


def save_transformation(df, path='../output/'):
    """
    Guarda en formato pickle (ver notebook feature_engineering.ipynb)
    el data frame que ya tiene los datos transformados.
    El pickle se debe llamar transformation_df.pkl y se debe guardar
    en la carpeta output.
    :param: dataframe, path
    :return: file save
    """
    # pickle.dump(df, open(path + "transformation_df.pkl", "wb"))
    # utils function, debería guardar el picjle llamado transformation_df.pkl en la carpeta ouput
    save_df(df, path + 'transformation_df_DPA.pkl')


def clean(df):
    """
    Recibe un dataframe y le aplica limpieza de datos
    :param: dataframe
    :return: dataframe
    """
    # Nos quedamos sólo con los resultados 'Pass','Pass w/ Conditions', 'Fail
    df = df[df.results.isin(['Pass', 'Pass w/ Conditions', 'Fail'])]

    # Podemos tomar sólo los de tipo Canvass (Sondeo)
    df = df[df.inspection_type.isin(['Canvass','CANVAS'])]

    # Generamos el label
    df = generate_label(df)

    # Quitamos NA
    df = df[df['facility_type'].notna()]
    df = df[df['risk'].notna()]
    df = df[df['city'].notna()]
    df = df[df['state'].notna()]
    df = df[df['zip'].notna()]
    df = df[df['latitude'].notna()]
    df = df[df['longitude'].notna()]
    df = df[df['location'].notna()]

    # Nos quedamos con las variables para predecir
    df = df[['facility_type', 'risk', 'zip',
             'inspection_date', 'latitude', 'longitude', 'label']]

    # Quitamos duplicados
    df = df.drop_duplicates()

    # Agrupamos establecimientos
    df = agrupa_tipos(df)

    return df


def agrupa_tipos(df_final):
    """
    Agrupa los tipos de establecimientos
    :param: dataframe
    :return: dataframe
    """
    # Se pasa a minúsculas
    df_final["facility_type"] = df_final["facility_type"].str.lower().str.strip()
    # Se hace una copia de la variable
    df_final["type"] = df_final["facility_type"]
    # Comenzamos a agrupar
    df_final['type'] = df_final['type'].replace(["1023",
                                                "1023 childern's service facility",
                                                "1023 childern's service s facility",
                                                "1023 childern's services facility",
                                                "1023 children's services facility",
                                                "1023-children's services facility",
                                                "15 monts to 5 years old",
                                                "adult daycare",
                                                "assissted living",
                                                "assisted living",
                                                "assisted living senior care",
                                                "catering",
                                                "charity aid kitchen",
                                                "childern's service facility",
                                                "childrens services facility",
                                                "children's services facility",
                                                "day care 1023",
                                                "day care 2-14",
                                                "daycare",
                                                "daycare (2 - 6 years)",
                                                "daycare (2 years)",
                                                "daycare (under 2 years)",
                                                "daycare 2 yrs to 12 yrs",
                                                "daycare above and under 2 years",
                                                "daycare combo 1586",
                                                "daycare night",
                                                "long term care",
                                                "long term care facility",
                                                "long-term care",
                                                "long-term care facility",
                                                "non -profit",
                                                "nursing home",
                                                "rehab center",
                                                "senior day care",
                                                "shelter",
                                                "supportive living",
                                                "supportive living facility",
                                                "youth housing",
                                                ],"Assistance_service")

    df_final['type'] = df_final['type'].replace(["banquet",
                                                "banquet dining",
                                                "banquet facility",
                                                "banquet hall",
                                                "banquet hall/catering",
                                                "banquet room",
                                                "banquet rooms",
                                                "banquet/kitchen",
                                                "banquets",
                                                "banquets/room service",
                                                "bowling lanes/banquets",
                                                "catering/banquet",
                                                "church",
                                                "church (special events)",
                                                "church kitchen",
                                                "church/day care",
                                                "church/special event",
                                                "church/special events",
                                                "event space",
                                                "lounge/banquet hall",
                                                "special event",],"BanquetService/Church")

    df_final['type'] = df_final['type'].replace(["bar",
                                                "bar/grill",
                                                "brewery",
                                                "brewpub",
                                                "juice and salad bar",
                                                "juice bar",
                                                "juice bar/grocery",
                                                "liquor/grocery store/bar",
                                                "restuarant and bar",
                                                "smoothie bar",
                                                "tavern",
                                                "wine tasting bar",],"Bar")

    df_final['type'] = df_final['type'].replace(["cafe/store",
                                                "cafeteria",
                                                "catering/cafe",
                                                "coffee  shop",
                                                "coffee cart",
                                                "coffee kiosk",
                                                "coffee shop",
                                                "coffee/tea",
                                                "liquor/coffee kiosk"],"Coffe shop")

    df_final['type'] = df_final['type'].replace(["drug store",
                                                "drug store/grocery",
                                                "drug treatment facility",
                                                "drug/food store",
                                                "grocery/drug store"],"Drug/Grocery")

    df_final['type'] = df_final['type'].replace(["art center",
                                                "blockbuster video",
                                                "boys and girls club",
                                                "day spa",
                                                "movie theater",
                                                "movie theatre",
                                                "music venue",
                                                "night club",
                                                "not-for-profit club",
                                                "pool",
                                                "social club",
                                                "stadium",
                                                "theater",
                                                "theatre",
                                                "vfw hall",
                                                "video store"],"Entertainment services")

    df_final['type'] = df_final['type'].replace(["convenience/gas station",
                                                "gas station",
                                                "gas station /grocery",
                                                "gas station store",
                                                "gas station/ grocery store",
                                                "gas station/convenience store",
                                                "gas station/food",
                                                "gas station/grocery",
                                                "gas station/mini mart",
                                                "gas station/restaurant",
                                                "gas station/store",
                                                "gas station/store grocery",
                                                "grocery store / gas station",
                                                "grocery store/gas station",
                                                "grocery/gas station",
                                                "grocery/service gas station",
                                                "retail food/gas station",
                                                "service gas station"],"Gas station/Grosery")

    df_final['type'] = df_final['type'].replace(["(convenience store)",
                                                "bakery",
                                                "bakery/deli",
                                                "butcher shop",
                                                "candy shop",
                                                "candy store",
                                                "candy/gelato",
                                                "chinese herbs",
                                                "cold/frozen food storage",
                                                "commiasary",
                                                "commissary",
                                                "commissary for soft serve ice cream trucks",
                                                "convenience",
                                                "convenience store",
                                                "convenience/drug store",
                                                "convenient store",
                                                "convnience store",
                                                "deli",
                                                "deli/grocery store",
                                                "distribution center",
                                                "donut shop",
                                                "fish market",
                                                "fitness center",
                                                "french market space",
                                                "gelato shop",
                                                "golf course conncession stand",
                                                "grocery",
                                                "grocery and butcher",
                                                "grocery store",
                                                "grocery/bakery",
                                                "grocery/butcher",
                                                "grocery/deli",
                                                "grocery/dollar store",
                                                "grocery/liquor store",
                                                "gym store",
                                                "ice cream",
                                                "ice cream shop",
                                                "kitchen demo",
                                                "liquor",
                                                "liquor store",
                                                "meat packing",
                                                "packaged liquor",
                                                "paleteria",
                                                "paleteria /icecream shop",
                                                "pantry",
                                                "popcorn shop",
                                                "prepacakaged foods",
                                                "prepackage meal distributor (1006 retail)",
                                                "rest/grocery",
                                                "retail store offers cooking classes",
                                                "shakes/teas",
                                                "slaughter house/ grocery",
                                                "snack shop",
                                                "store",
                                                "warehouse",
                                                "wholesale"],"Grocery/Almacen/Convenience store")

    df_final['type'] = df_final['type'].replace(["health care store",
                                                "health center",
                                                "herbal",
                                                "herbal drinks",
                                                "herbal life shop",
                                                "herbal medicine",
                                                "herbal remedy",
                                                "herbal store",
                                                "herbalife",
                                                "herbalife nutrition",
                                                "herbalife/zumba",
                                                "hospital",
                                                "nutrition store",
                                                "packaged health foods",
                                                "repackaging plant",
                                                "weight loss program",],"Health store")

    df_final['type'] = df_final['type'].replace(["hot dog station",
                                                "kiosk",
                                                "mobile food dispenser",
                                                "mobile food preparer",
                                                "navy pier kiosk",
                                                "newsstand",
                                                "o'hare kiosk"],"Kiosko")

    df_final['type'] = df_final['type'].replace(["bakery/ restaurant",
                                                "bakery/restaurant",
                                                "commisary restaurant",
                                                "dining hall",
                                                "employee kitchen",
                                                "golden diner",
                                                "grocery & restaurant",
                                                "grocery store/ restaurant",
                                                "grocery store/bakery",
                                                "grocery store/taqueria",
                                                "grocery& restaurant",
                                                "grocery(sushi prep)",
                                                "grocery/ restaurant",
                                                "grocery/restaurant",
                                                "grocery/taqueria",
                                                "main kitchen",
                                                "restaurant",
                                                "restaurant(protein shake bar)",
                                                "restaurant.banquet halls",
                                                "restaurant/bakery",
                                                "restaurant/bar",
                                                "restaurant/grocery",
                                                "restaurant/grocery store",
                                                "restaurant/hospital",
                                                "roof top",
                                                "roof tops",
                                                "rooftop",
                                                "rooftops",
                                                "shared kitchen",
                                                "shared kitchen user (long term)",
                                                "shared kitchen user (short term)",
                                                "smokehouse",
                                                "summer feeding",
                                                "summer feeding prep area",
                                                "sushi counter",
                                                "tavern grill",
                                                "tavern/restaurant",
                                                "theater & restaurant",
                                                "wrigley roof top",
                                                "wrigley rooftop"],"Restaurant")

    df_final['type'] = df_final['type'].replace(["after school program",
                                                "a-not-for-profit chef training program",
                                                "before and after school program",
                                                "charter",
                                                "charter school",
                                                "charter school cafeteria",
                                                "city of chicago college",
                                                "college",
                                                "cooking school",
                                                "culinary arts school",
                                                "culinary class rooms",
                                                "culinary school",
                                                "grocery store/cooking school",
                                                "high school kitchen",
                                                "pastry school",
                                                "prep inside school",
                                                "private school",
                                                "public shcool",
                                                "school",
                                                "school cafeteria",
                                                "teaching school",
                                                "university cafeteria"],"School")

    df_final['type'] = df_final['type'].replace(["airport lounge",
                                                "beverage/silverware warehouse",
                                                "cell phone store",
                                                "custom poultry slaughter",
                                                "dollar store",
                                                "dollar store with grocery",
                                                "hostel",
                                                "hotel",
                                                "incubator",
                                                "live poultry",
                                                "northerly island",
                                                "other",
                                                "poultry slaughter",
                                                "regulated business",
                                                "rest/gym",
                                                "rest/rooftop",
                                                "riverwalk",
                                                "room service",
                                                "unused storage"],"Others")

    return df_final


def transform(path):
    """
    Recibe la ruta del pickle que hay que transformar y devuelve en una ruta los datos transformados en pickle.
    :param: path
    :return: file
    """
    print("Inicio proceso transform")
    df_final = load_ingestion(path)
    # Limpieza de datos
    df_final = clean(df_final)
    # Transformaciones
    # Categoricas
    df_final['type'] = categoric_trasformation('type', df_final)
    df_final['facility_type'] = categoric_trasformation('facility_type', df_final)
    df_final['risk'] = categoric_trasformation('risk', df_final)
    df_final['address'] = categoric_trasformation('address', df_final)
    df_final['zip'] = categoric_trasformation('zip', df_final)
    # Númericas
    df_final['label'] = int_transformation('label', df_final)
    df_final['latitude'] = numeric_transformation('latitude', df_final)
    df_final['longitude'] = numeric_transformation('longitude', df_final)
    # Transformando fechas.
    df_final['inspection_date'] = date_transformation('inspection_date', df_final)
    # Ordenando datos por fecha de inspección
    df_final = df_final.sort_values(['inspection_date'])
    # Reseteando indice con datos ordenados por fecha
    df_final = df_final.reset_index(drop=True)

    save_transformation(df_final, path)
    print("Finalizó proceso transformación")