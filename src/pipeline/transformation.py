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
    df = df[df.inspection_type.isin(['Canvass'])]

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
    df = df[['facility_type', 'risk', 'address', 'zip',
             'inspection_date', 'latitude', 'longitude', 'label']]

    # Quitamos duplicados
    df = df.drop_duplicates()

    # Agrupamos establecimientos
    df = agrupa_tipos(df)

    return df


def agrupa_tipos(df_final):
    """
    Agrupa los tipos de establecimientos (300 a 11 tipos)
    :param: dataframe
    :return: dataframe
    """
    # Se pasa a minusculas
    df_final["facility_type"] = df_final["facility_type"].str.lower().str.strip()
    # Se hace una copia de la variable
    df_final["type"] = df_final["facility_type"]
    # Comenzamos a agrupar
    df_final['type'] = df_final['type'].replace(["1023",
                                                 "1023 childern\'s service facility",
                                                 "1023 childern\'s service s facility",
                                                 "1023 childern's services facility",
                                                 "1023 children's services facility",
                                                 "1023-children's services facility",
                                                 "childern's service facility",
                                                 "children's services facility",
                                                 "childrens services facility"], "childern\'s service facility")

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
                                                 "service gas station"], "gas station")

    df_final['type'] = df_final['type'].replace(["restaurant",
                                                 " restaurant\'", "restaurant\'",
                                                 "grocery& restaurant",
                                                 "restaurant.banquet halls",
                                                 "restaurant(protein shake bar)",
                                                 "restaurant/bakery",
                                                 "restaurant/bar",
                                                 "restaurant/grocery",
                                                 "restaurant/grocery store",
                                                 "restaurant/hospital",
                                                 "grocery & restaurant",
                                                 "restuarant and bar"], "restaurant")

    df_final['type'] = df_final['type'].replace(["bakery/ restaurant",
                                                 "bakery/restaurant",
                                                 "bakery",
                                                 "bakery/deli", ], "bakery")

    df_final['type'] = df_final['type'].replace(["deli/grocery store",
                                                 "grocery",
                                                 "grocery and butcher",
                                                 "grocery store",
                                                 "grocery store/ restaurant",
                                                 "grocery store/bakery",
                                                 "grocery store/cooking school",
                                                 "grocery store/taqueria",
                                                 "grocery(sushi prep)",
                                                 "grocery/ restaurant",
                                                 "grocery/bakery",
                                                 "grocery/butcher",
                                                 "grocery/dollar store",
                                                 "grocery/drug store",
                                                 "grocery/liquor store",
                                                 "grocery/restaurant",
                                                 "grocery/taqueria",
                                                 "slaughter house/ grocery"], "grocery")

    df_final['type'] = df_final['type'].replace(["juice and salad bar",
                                                 "juice bar",
                                                 "juice bar/grocery"], "juice bar")

    df_final['type'] = df_final['type'].replace(["assissted living",
                                                 "assisted living",
                                                 "assisted living senior care"], "assisted living")

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
                                                 "lounge/banquet hall"], "banquet")

    df_final['type'] = df_final['type'].replace(["bar", "bar/grill", "smoothie bar",
                                                 "wine tasting bar"], "bar")

    df_final['type'] = df_final['type'].replace(["candy shop",
                                                 "candy store",
                                                 "candy/gelato"], "candy")

    df_final['type'] = df_final['type'].replace(["charter",
                                                 "charter school",
                                                 "charter school cafeteria"], "charter")

    df_final['type'] = df_final['type'].replace(["church",
                                                 "church (special events)",
                                                 "church kitchen",
                                                 "church/day care",
                                                 "church/special events"], "church")

    df_final['type'] = df_final['type'].replace(["(convenience store)",
                                                 "convenience",
                                                 "convenience store",
                                                 "convenience/drug store",
                                                 "convenient store",
                                                 "convnience store"], "convenience")

    df_final['type'] = df_final['type'].replace(["cafe/store",
                                                 "cafeteria",
                                                 "coffee  shop",
                                                 "coffee cart",
                                                 "coffee kiosk",
                                                 "coffee shop",
                                                 "coffee/tea",
                                                 "school cafeteria",
                                                 "university cafeteria"], "cafeteria")

    df_final['type'] = df_final['type'].replace(["catering",
                                                 "catering/banquet",
                                                 "catering/cafe"], "catering")

    df_final['type'] = df_final['type'].replace(["commiasary",
                                                 "commisary restaurant",
                                                 "commissary",
                                                 "commissary for soft serve ice cream trucks"],
                                                "commisary")

    df_final['type'] = df_final['type'].replace(["day care 1023",
                                                 "day care 2-14",
                                                 "daycare",
                                                 "daycare (2 - 6 years)",
                                                 "daycare (2 years)",
                                                 "daycare (under 2 years)",
                                                 "daycare 2 yrs to 12 yrs",
                                                 "daycare above and under 2 years",
                                                 "daycare combo 1586",
                                                 "daycare night",
                                                 "15 monts to 5 years old"], "daycare")

    df_final['type'] = df_final['type'].replace(["adult daycare", "senior day care"], "adult daycare")

    df_final['type'] = df_final['type'].replace(["dollar store", "dollar store with grocery"], "dollar store")

    df_final['type'] = df_final['type'].replace(["health care store", "health center"], "health")

    df_final['type'] = df_final['type'].replace(["herbal",
                                                 "herbal drinks",
                                                 "herbal life shop",
                                                 "herbal medicine",
                                                 "herbal remedy",
                                                 "herbal store"], "herbal")

    df_final['type'] = df_final['type'].replace(["herbalife",
                                                 "herbalife nutrition",
                                                 "herbalife/zumba"], "herbalife")

    df_final['type'] = df_final['type'].replace(["ice cream",
                                                 "ice cream shop",
                                                 "paleteria",
                                                 "paleteria /icecream shop"], "ice cream")

    df_final['type'] = df_final['type'].replace(["liquor",
                                                 "liquor store",
                                                 "liquor/coffee kiosk",
                                                 "liquor/grocery store/bar"], "liquor")

    df_final['type'] = df_final['type'].replace(["long term care",
                                                 "long term care facility",
                                                 "long-term care",
                                                 "long-term care facility"], "long-term care")

    df_final['type'] = df_final['type'].replace(["mobile food dispenser",
                                                 "mobile food preparer"], "mobile food")

    df_final['type'] = df_final['type'].replace(["movie theater", "movie theatre"], "movie theater")

    df_final['type'] = df_final['type'].replace(["non -profit", "not-for-profit club"], "non -profit")

    df_final['type'] = df_final['type'].replace(["rest/grocery",
                                                 "rest/gym",
                                                 "rest/rooftop"], "rest")

    df_final['type'] = df_final['type'].replace(["roof top",
                                                 "roof tops",
                                                 "rooftop",
                                                 "rooftops"], "roof top")

    df_final['type'] = df_final['type'].replace(["shared kitchen",
                                                 "shared kitchen user (long term)",
                                                 "shared kitchen user (short term)"], "shared kitchen")

    df_final['type'] = df_final['type'].replace(["summer feeding", "summer feeding prep area"], "summer feeding")

    df_final['type'] = df_final['type'].replace(["supportive living", "supportive living facility"],
                                                "supportive living")

    df_final['type'] = df_final['type'].replace(["tavern", "tavern grill", "tavern/restaurant"], "tavern")

    df_final['type'] = df_final['type'].replace(["theater", "theater ", "theatre"], "theater")

    df_final['type'] = df_final['type'].replace(["wrigley roof top", "wrigley rooftop"], "wrigley roof top")

    df_final['type'] = df_final['type'].replace(["culinary arts school",
                                                 "culinary class rooms",
                                                 "culinary school"], "culinary")

    df_final['type'] = df_final['type'].replace(["drug store",
                                                 "drug store/grocery",
                                                 "drug treatment facility",
                                                 "drug/food store"], "drug store")

    df_final['type'] = df_final['type'].replace(["gas station", "shelter", "liquor", "wholesale", "nursing home",
                                                 "after school program", "shared kitchen", "incubator", "cafeteria",
                                                 "theater", "tavern", "assisted living", "cooking school", "pool",
                                                 "special event", "candy", "convenience", "kiosk", "commisary",
                                                 "culinary", "banquet", "public shcool", "private school",
                                                 "mobile food", "ice cream", "cold/frozen food storage", "church",
                                                 "summer feeding", "non -profit", "rest", "city of chicago college",
                                                 "a-not-for-profit chef training program", "employee kitchen",
                                                 "room service", "pantry", "main kitchen",
                                                 "prepackage meal distributor (1006 retail)", "fitness center",
                                                 "o'hare kiosk", "live poultry", "pastry school",
                                                 "charity aid kitchen", "snack shop", "newsstand", "butcher shop",
                                                 "dollar store", "bar", "weight loss program", "meat packing",
                                                 "teaching school", "nutrition store", "social club", "health",
                                                 "before and after school program", "adult daycare", "video store",
                                                 "deli", "juice bar", "drug store", "donut shop", "hot dog station",
                                                 "popcorn shop", "boys and girls club", "stadium", "fish market",
                                                 "smokehouse", "college", "chinese herbs", "blockbuster video",
                                                 "wrigley roof top", "roof top", "navy pier kiosk", "herbal",
                                                 "beverage/silverware warehouse", "supportive living",
                                                 "rehab center", "unused storage", "repackaging plant", "day spa",
                                                 "custom poultry slaughter", "night club", "music venue", "other",
                                                 "charter", "movie theater", "retail store offers cooking classes",
                                                 "northerly island", "store", "gelato shop", "packaged liquor",
                                                 "vfw hall", "regulated business", "cell phone store", "hotel",
                                                 "prepacakaged foods", "warehouse", "golf course conncession stand",
                                                 "brewpub", "riverwalk", "kitchen demo", "herbalife", "dining hall",
                                                 "hostel", "theater", "airport lounge", "poultry slaughter",
                                                 "prep inside school", "french market space", "gym store",
                                                 "distribution center", "youth housing", "sushi counter", "brewery",
                                                 "event space", "packaged health foods", "art center",
                                                 "shakes/teas", "theater & restaurant"], "otros")

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
    # Dividiendo fecha en mes y día, y creando semanas
    df_final = split_fecha("inspection_date", df_final)
    # Ordenando datos por fecha de inspección
    df_final = df_final.sort_values(['inspection_date'])
    # Reseteando indice con datos ordenados por fecha
    df_final = df_final.reset_index(drop=True)

    save_transformation(df_final, path)
    print("Finalizó proceso transformación")