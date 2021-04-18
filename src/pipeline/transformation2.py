# from utils.utils import load_df, save_df
import pandas as pd
import numpy as np
from src.utils import utils as u 


def load_ingestion(path):
    """
    Recibe el path en donde se encuentra el pickle que generamos durante la ingestión.
    :param: path
    :return: pickle
    """
    df = u.load_df(path)
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


def save_transformation(df, path):
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
    u.save_df(df, path)


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
        
    # Agrupamos tipo inspecciones AGREGADO
    df = agrupa_inspeciones_tipo(df)  
    
    # Agrupamos establecimientos
    df = agrupa_tipos(df)  
    
    # Quitamos duplicados
    #df = df.drop_duplicates()
    
    # Podemos tomar sólo los de tipo Canvass (Sondeo)
    #df = df[df.inspection_type.isin(['Canvass','CANVAS'])]
    
    # Generamos el label
    df = generate_label(df)
    
    # Nos quedamos con las variables para predecir 
    df = df[['type_insp','type','risk', 'zip',
             'inspection_date','latitude','longitude','label']]  
    
    return df


def agrupa_inspeciones_tipo(df_final):
    """
    Agrupa los tipos de establecimientos
    :param: dataframe
    :return: dataframe
    """
    
    # Se pasa a minúsculas
    df_final["inspection_type"] = df_final["inspection_type"].str.lower().str.strip()
    # Se hace una copia de la variable
    df_final["type_insp"] = df_final["inspection_type"]
    
    # Podemos tomar sólo las que no esten en
    df_final = df_final[~df_final.type_insp.isin(["addendum","business not located","changed court date",
                                "duplicated","error save","no entry","non-inspection",
                                "not ready","out of business","out ofbusiness"])]
        
    # Comenzamos a agrupar
    df_final['type_insp'] = df_final['type_insp'].replace(["canvas",
                                                "canvass",
                                                "canvass for rib fest",
                                                "canvass re inspection of close up",
                                                "canvass re-inspection",
                                                "canvass school/special event",
                                                "canvass special events",
                                                "canvass/special event"],"Canvass")
    
    df_final['type_insp'] = df_final['type_insp'].replace(["close-up/complaint reinspection",
                                                        "complaint",
                                                        "complaint re-inspection",
                                                        "complaint-fire",
                                                        "complaint-fire re-inspection",
                                                        "covid complaint",
                                                        "finish complaint inspection from 5-18-10",
                                                        "fire complaint",
                                                        "fire/complain",
                                                        "no entry-short complaint)",
                                                        "sfp/complaint",
                                                        "short form complaint",
                                                        "short form fire-complaint",
                                                        "smoking complaint"],"Complaint")
    
    
    df_final['type_insp'] = df_final['type_insp'].replace(["consultation"],
                                                          "Consultation")
    
    df_final['type_insp'] = df_final['type_insp'].replace(["1315 license reinspection",
                                                        "day care license renewal",
                                                        "license",
                                                        "license consultation",
                                                        "license daycare 1586",
                                                        "license re-inspection",
                                                        "license renewal for daycare",
                                                        "license renewal inspection for daycare",
                                                        "license request",
                                                        "license task 1474",
                                                        "license task force / not -for-profit clu",
                                                        "license task force / not -for-profit club",
                                                        "license wrong address",
                                                        "license/not ready",
                                                        "owner suspended operation/license",
                                                        "tavern 1470"],"License")
    
    df_final['type_insp'] = df_final['type_insp'].replace(["license-task force",
                                                          "pre-license consultation"],
                                                          "LicenseTaskForce-PreLicenseConsultation")
    
    df_final['type_insp'] = df_final['type_insp'].replace(["package liquor 1474"],
                                                          "Package liquor 1474")
    
    df_final['type_insp'] = df_final['type_insp'].replace(["recent inspection"],
                                                          "Recent inspection")
    
    df_final['type_insp'] = df_final['type_insp'].replace(["special events (festivals)"],
                                                          "Special events (festivals)")
    
    df_final['type_insp'] = df_final['type_insp'].replace(["suspected food poisoning",
                                                           "suspected food poisoning re-inspection"],
                                                          "Suspected food poisoning")
    
    df_final['type_insp'] = df_final['type_insp'].replace(["tag removal"],
                                                          "Tag removal")
    
    df_final['type_insp'] = df_final['type_insp'].replace(["liqour task force not ready",
                                                            "special task force",
                                                            "task force",
                                                            "task force for liquor 1474",
                                                            "task force liquor (1481)",
                                                            "task force liquor 1470",
                                                            "task force liquor 1474",
                                                            "task force liquor 1475",
                                                            "task force liquor catering",
                                                            "task force liquor inspection 1474",
                                                            "task force night",
                                                            "task force not ready",
                                                            "task force package goods 1474",
                                                            "task force package liquor",
                                                            "task force(1470) liquor tavern",
                                                            "taskforce"],
                                                            "Task force")
    
    df_final['type_insp'] = df_final['type_insp'].replace(["citation re-issued",
                                                            "citf",
                                                            "corrective action",
                                                            "expansion",
                                                            "haccp questionaire",
                                                            "illegal operation",
                                                            "kids cafe",
                                                            "kids cafe'",
                                                            "liquor catering",
                                                            "office assignment",
                                                            "possible fbi",
                                                            "recall inspection",
                                                            "reinspection",
                                                            "reinspection of 48 hour notice",
                                                            "re-inspection of close-up",
                                                            "sample collection",
                                                            "sfp",
                                                            "sfp recently inspected",
                                                            "summer feeding",
                                                            "taste of chicago",
                                                            "two people ate and got sick."],"Other")
                                                          
    return df_final
                                                          

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
    df_final['type'] = df_final['type'].replace(["1005 nursing home",
                                                "1023",
                                                "1023 childern's service facility",
                                                "1023 childern's service s facility",
                                                "1023 childern's services facility",
                                                "1023 children's services facility",
                                                "1023-children's services facility",
                                                "15 monts to 5 years old",
                                                "1584-day care above 2 years",
                                                "adult daycare",
                                                "adult family care center",
                                                "assissted living",
                                                "assisted living",
                                                "assisted living senior care",
                                                "catering",
                                                "charity aid kitchen",
                                                "childern activity facility",
                                                "childern's service facility",
                                                "childern's services  facility",
                                                "childrens services facility",
                                                "children's services facility",
                                                "day care",
                                                "day care 1023",
                                                "day care 2-14",
                                                "daycare",
                                                "daycare (2 - 6 years)",
                                                "daycare (2 years)",
                                                "daycare (under 2 years)",
                                                "daycare 1586",
                                                "daycare 2 yrs to 12 yrs",
                                                "daycare 2-6, under 6",
                                                "daycare 6 wks-5yrs",
                                                "daycare above and under 2 years",
                                                "daycare combo",
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
                                                "youth housing"
                                                ],"AssistanceService")

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
                                                "catered events",
                                                "catering/banquet",
                                                "church",
                                                "church (special events)",
                                                "church kitchen",
                                                "church/day care",
                                                "church/special event",
                                                "church/special events",
                                                "event center",
                                                "event space",
                                                "event venu",
                                                "hooka lounge",
                                                "lounge",
                                                "lounge/banquet hall",
                                                "private event space",
                                                "religious",
                                                "special event"],"BanquetService-Church")

    df_final['type'] = df_final['type'].replace(["bar",
                                                "bar/grill",
                                                "brewery",
                                                "brewpub",
                                                "hooka bar",
                                                "juice and salad bar",
                                                "juice bar",
                                                "juice bar/grocery",
                                                "liqour brewery tasting",
                                                "liquor/grocery store/bar",
                                                "liquore store/bar",
                                                "massage bar",
                                                "protein shake bar",
                                                "restuarant and bar",
                                                "service bar/theatre",
                                                "shuffleboard club with bar",
                                                "smoothie bar",
                                                "tap room/tavern/liquor store",
                                                "tavern",
                                                "tavern/bar",
                                                "tavern/liquor",
                                                "tavern/store",
                                                "tavern-liquor",
                                                "theater/bar",
                                                "wine tasting bar",
                                                ],"Bar")

    df_final['type'] = df_final['type'].replace(["cafe",
                                                "cafe/store",
                                                "cafeteria",
                                                "catering/cafe",
                                                "coffee",
                                                "coffee  shop",
                                                "coffee cart",
                                                "coffee kiosk",
                                                "coffee roaster",
                                                "coffee shop",
                                                "coffee/tea",
                                                "kids cafe",
                                                "kids cafe'",
                                                "liquor/coffee kiosk",
                                                "riverwalk cafe"],"Coffe shop")

    df_final['type'] = df_final['type'].replace(["drug store",
                                                "drug store/grocery",
                                                "drug store/w/ food",
                                                "drug treatment facility",
                                                "drug/food store",
                                                "grocery/drug store",
                                                "pharmacy"],"Drug-Grocery")

    df_final['type'] = df_final['type'].replace(["art center",
                                                "art gallery",
                                                "art gallery w/wine and beer",
                                                "blockbuster video",
                                                "boys and girls club",
                                                "day spa",
                                                "movie theater",
                                                "movie theatre",
                                                "museum/gallery",
                                                "music venue",
                                                "night club",
                                                "not-for-profit club",
                                                "pool",
                                                "private club",
                                                "social club",
                                                "spa",
                                                "stadium",
                                                "theater",
                                                "theatre",
                                                "vfw hall",
                                                "video store"],"EntertainmentServices")

    df_final['type'] = df_final['type'].replace(["(gas station)",
                                                "convenience/gas station",
                                                "gas station",
                                                "gas station /grocery",
                                                "gas station /subway mini mart.",
                                                "gas station food store",
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
                                                "grocery(gas station)",
                                                "grocery/gas station",
                                                "grocery/service gas station",
                                                "retail food/gas station",
                                                "service gas station"],"GasStation-Grosery")

    df_final['type'] = df_final['type'].replace(["(convenience store)",
                                                "1475 liquor",
                                                "bakery",
                                                "bakery/deli",
                                                "bakery/grocery",
                                                "butcher shop",
                                                "candy",
                                                "candy maker",
                                                "candy shop",
                                                "candy store",
                                                "candy/gelato",
                                                "catered liquor",
                                                "catering and wholesale",
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
                                                "deli/bakery",
                                                "deli/grocery",
                                                "deli/grocery store",
                                                "distribution center",
                                                "distributor",
                                                "donut shop",
                                                "exercise and nutrition bar",
                                                "fish market",
                                                "fitness center",
                                                "fitness studio",
                                                "food pantry",
                                                "food pantry/church",
                                                "french market space",
                                                "furniture store",
                                                "gelato shop",
                                                "general store",
                                                "golf course conncession stand",
                                                "grocery",
                                                "grocery and butcher",
                                                "grocery delivery service",
                                                "grocery store",
                                                "grocery store /pharmacy",
                                                "grocery store/deli",
                                                "grocery/bakery",
                                                "grocery/butcher",
                                                "grocery/cafe",
                                                "grocery/deli",
                                                "grocery/dollar store",
                                                "grocery/liquor",
                                                "grocery/liquor store",
                                                "gym",
                                                "gym store",
                                                "ice cream",
                                                "ice cream parlor",
                                                "ice cream shop",
                                                "kitchen demo",
                                                "liquor",
                                                "liquor consumption on premises.",
                                                "liquor store",
                                                "meat packing",
                                                "mexican candy store",
                                                "milk tea",
                                                "packaged food distribution",
                                                "packaged liquor",
                                                "paleteria",
                                                "paleteria /icecream shop",
                                                "pantry",
                                                "popcorn corn",
                                                "popcorn shop",
                                                "pre packaged",
                                                "prepacakaged foods",
                                                "prepackage meal distributor (1006 retail)",
                                                "produce stand",
                                                "produce vendor",
                                                "rest/grocery",
                                                "retail",
                                                "retail store",
                                                "retail store offers cooking classes",
                                                "shakes/teas",
                                                "slaughter house/ grocery",
                                                "snack shop",
                                                "store",
                                                "tea store",
                                                "tobacco store",
                                                "vending commissary",
                                                "warehouse",
                                                "watermelon house",
                                                "wholesale",
                                                "wholesale & retail",
                                                "wholesale bakery",
                                                "wine store"],"Grocery-Almacen-ConvenienceStore")

    df_final['type'] = df_final['type'].replace(["health care store",
                                                "health center",
                                                "health food store",
                                                "herabalife",
                                                "herbal",
                                                "herbal drinks",
                                                "herbal life",
                                                "herbal life shop",
                                                "herbal medicine",
                                                "herbal remedy",
                                                "herbal store",
                                                "herbalcal",
                                                "herbalife",
                                                "herbalife nutrition",
                                                "herbalife store",
                                                "herbalife/zumba",
                                                "hospital",
                                                "nutrition shakes",
                                                "nutrition store",
                                                "nutrition/herbalife",
                                                "packaged health foods",
                                                "repackaging plant",
                                                "weight loss program"],"HealthStore")

    df_final['type'] = df_final['type'].replace(["coffee vending machine",
                                                "food booth",
                                                "food vending machines",
                                                "frozen dessert pushcarts",
                                                "frozen desserts dispenser -non motorized",
                                                "frozen desserts dispenser-non-motorized",
                                                "hot dog cart",
                                                "hot dog station",
                                                "kiosk",
                                                "mfd truck",
                                                "mobil food 1315",
                                                "mobile dessert cart",
                                                "mobile dessert vendor",
                                                "mobile desserts vendor",
                                                "mobile food",
                                                "mobile food desserts vendor",
                                                "mobile food dispenser",
                                                "mobile food preparer",
                                                "mobile food truck",
                                                "mobile frozen dessert disp/non-motorized",
                                                "mobile frozen dessert dispenser_non  motorized.",
                                                "mobile frozen dessert vendor",
                                                "mobile frozen desserts dispenser-non- motorized",
                                                "mobile frozen desserts dispenser-non-motor",
                                                "mobile frozen desserts dispenser-non-motorized",
                                                "mobile frozen desserts vendor",
                                                "mobile prepared food vendor",
                                                "mobile push cart",
                                                "mobilprepared food vendor",
                                                "navy pier kiosk",
                                                "newsstand",
                                                "np-kiosk",
                                                "o'hare kiosk",
                                                "peddler",
                                                "pop-up food establishment user-tier ii",
                                                "pop-up food establishment user-tier iii",
                                                "push carts",
                                                "pushcart",
                                                "temporary kiosk",
                                                "truck",
                                                "vending machine"],"Kiosko")

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
                                                "kitchen",
                                                "main kitchen",
                                                "restaurant",
                                                "restaurant(protein shake bar)",
                                                "restaurant.banquet halls",
                                                "restaurant/bakery",
                                                "restaurant/bar",
                                                "restaurant/bar/theater",
                                                "restaurant/grocery",
                                                "restaurant/grocery store",
                                                "restaurant/hospital",
                                                "restaurant/liquor",
                                                "roof top",
                                                "roof tops",
                                                "rooftop",
                                                "rooftop patio",
                                                "rooftops",
                                                "shared kitchen",
                                                "shared kitchen user (long term)",
                                                "shared kitchen user (long trem)",
                                                "shared kitchen user (short term)",
                                                "smokehouse",
                                                "soup kitchen",
                                                "summer feeding",
                                                "summer feeding prep area",
                                                "sushi counter",
                                                "tavern grill",
                                                "tavern/restaurant",
                                                "tent rstaurant",
                                                "theater & restaurant",
                                                "wrigley roof top",
                                                "wrigley rooftop"],"Restaurant")

    df_final['type'] = df_final['type'].replace(["after school care",
                                                "after school program",
                                                "a-not-for-profit chef training program",
                                                "before and after school program",
                                                "charter",
                                                "charter school",
                                                "charter school cafeteria",
                                                "charter school/cafeteria",
                                                "church/after school program",
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

    df_final['type'] = df_final['type'].replace(["",
                                                "airport lounge",
                                                "animal shelter cafe permit",
                                                "beverage/silverware warehouse",
                                                "car wash",
                                                "cell phone store",
                                                "chicago park district",
                                                "clothing store",
                                                "custom poultry slaughter",
                                                "dollar & grocery store",
                                                "dollar store",
                                                "dollar store with grocery",
                                                "dollar tree",
                                                "farmer's market",
                                                "gift shop",
                                                "golf course",
                                                "greenhouse",
                                                "hair salon",
                                                "helicopter terminal",
                                                "hostel",
                                                "hotel",
                                                "illegal vendor",
                                                "incubator",
                                                "internet cafe",
                                                "laundromat",
                                                "linited business",
                                                "live butcher",
                                                "live poultry",
                                                "nail shop",
                                                "non-for profit basement kit",
                                                "northerly island",
                                                "not for profit",
                                                "other",
                                                "pop-up establishment host-tier ii",
                                                "poultry slaughter",
                                                "regulated business",
                                                "rest/gym",
                                                "rest/rooftop",
                                                "riverwalk",
                                                "room service",
                                                "unlicensed facility",
                                                "unused storage",
                                                "urban farm"],"Others")
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
    df_final['type_insp'] = categoric_trasformation('type_insp', df_final)
    df_final['type'] = categoric_trasformation('type', df_final)
    df_final['risk'] = categoric_trasformation('risk', df_final)
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

    save_transformation(df_final, 'pkl_clean.pkl')
    print("Finalizó proceso transformación")
