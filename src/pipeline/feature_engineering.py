from siuba import *
from src.utils import utils as u 
from  src.pipeline import transformation as t
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder, KBinsDiscretizer
from sklearn.compose import ColumnTransformer
from sklearn.base import BaseEstimator,TransformerMixin

import pandas as pd
import numpy as np
import datetime
import time
import warnings

warnings.filterwarnings('ignore')

   
def split_fecha(col, df):
    """
    Recibe la columna fecha que hay que transformar en 3 columnas: año, mes, dia y 
    el data frame al que pertenece.
    :param: column, dataframe
    :return: dataframe con 2 columnas mas (mes, día) y semanas
    """
    df[col + '_anio'] = df[col].dt.year.astype(str) 
    df[col + '_mes'] = df[col].dt.month.astype(str) 
    df[col + '_dia'] = df[col].dt.day.astype(str)     
    
    # Cambio a enteros
    df[col + '_anio'] = t.int_transformation(col + '_anio', df)
    df[col + '_mes'] = t.int_transformation(col + '_mes', df)
    df[col + '_dia'] = t.int_transformation(col + '_dia', df)
    
    df['week'] = df[col].dt.week
    df['day_of_week'] = df[col].dt.day_name()
    
    # Fines de semana 
    df['weekday']=df[col].map(lambda x: 'Y' if x.weekday()<5 else 'N')
    
    return df

def ciclic_variables(col, df):
    """
    Recibe la columna day_no, mes o fecha_creacion y las convierte en variables cíclicas:
    número día de la semana, mes, semana y hora respectivamente.
    :param: column, dataframe
    :return: dataframe con variable cíclica creada corresondientemente
    """
    
    if (col == 'day_of_week'):
        no_dia_semana = {'Sunday':1, 'Monday':2, 'Tuesday':3, 'Wednesday':4, 
                         'Thursday':5, 'Friday':6, 'Saturday':7}
        df['day_no'] = df[col].apply(lambda x: no_dia_semana[x])
        #max_day_no = np.max(df['day_no'])
        max_day_no = 7
        df['sin_day_no'] = np.sin(2*np.pi*df['day_no']/max_day_no)
        df['cos_day_no'] = np.cos(2*np.pi*df['day_no']/max_day_no)
        
    if(col == 'week'):
        # converting the hour into a sin, cos coordinate
        WEEKS = 53
        df['sin_week'] = np.sin(2*np.pi*df[col]/WEEKS)
        df['cos_week'] = np.cos(2*np.pi*df[col]/WEEKS) 
        
    if(col == 'inspection_date_mes'):
        MONTH = 12
        df['sin_month'] = np.sin(2*np.pi*df[col]/MONTH)
        df['cos_month'] = np.cos(2*np.pi*df[col]/MONTH) 
        
    if(col == 'inspection_date_dia'):
        # converting the hour into a sin, cos coordinate
        DAYS = 31
        df['sin_days'] = np.sin(2*np.pi*df[col]/DAYS)
        df['cos_days'] = np.cos(2*np.pi*df[col]/DAYS)    
            
    return df


def get_distance(df):
    """
    Genera centroide de latitud y longitu con medias y calcula distancias
    :param: dataframe
    :return: dataframe
    """
    lat_c = df['latitude'].median()
    lon_c = df['longitude'].median()
    
    df['lat'] = lat_c- df['latitude'].astype(float) 
    df['lon'] = lon_c - df['longitude'].astype(float) 
    
    df['distance'] = np.sqrt(df['lat']**2+df['lon']**2)
    
    df = (df
        >> group_by (_.inspection_date_anio, _.inspection_date_mes) 
        >> mutate(mediana_ym_lat = _.latitude.median(), mediana_ym_lon = _.longitude.median())
        >> ungroup()
        >> mutate(distancia_ym = ( (_.mediana_ym_lat - _.latitude)**2 +  (_.mediana_ym_lon - _.longitude)**2 ) **(1/2) )
        >> mutate(distancia_ym_mht = abs (_.mediana_ym_lat - _.latitude) +  abs(_.mediana_ym_lon - _.longitude) ) )

    df = pd.DataFrame(df.drop(['lat','lon','mediana_ym_lat', 'mediana_ym_lon'], axis=1))
    
    return df

def limpieza_dic(palabra, lista_diccionario):
    laSuma=0
    for i in lista_diccionario: 
        laSuma = laSuma + (i in palabra)
    return laSuma


def clean_dummy(df):
    
    ############# DUMMY TYPE ##################
    dic_cafe = ['coffe', 'cafe', 'tea']
    dic_restaurante = ['restaurant', 'taqueria','kitchen', 'dining','diner', 'roof', 'grill','sushi','feeding','soup']
    dic_bar = ['bar', 'taverna', 'pub','tavern', 'liquor', 'brewery']
    dic_school = [ 'school', 'training program', 'charter school','college' ]
    dic_assistence = ['hospital', "children's services", 'nursing', 'years old', 'day care', 'daycare' ,'care center', 'assissted living', 
                                  'term care', 'rehab','supportive living facility', 'non -profit', 'shelter', ' living', ' facility' ]
    dic_banquet = ['banquet','event', 'catering', 'church','lounge', 'religious' ]
    dic_drug = ['drug, pharmacy']
    dic_gas = ['gas station']
    dic_entertaiment= ['art center', 'gallery', 'movie', 'museum', 'night club', 'pool', 'stadium', 'theater', 'video', ' spa', 'spa ', 'music', 'club0', 'fitness center' ]
    dic_kiosko = ['kiosk', 'mobile']
    dic_grocery = ['bakery', 'grocery', 'foods', 'snack', 'store', 'ice cream', 'candy store', 'liquor store','popcorn', 'shop', 'retail','wholesale', 'market', 'paleteria', 'food', 
                           'packag', 'distribut', 'gelato', 'convenience', 'pantry']
    dic_health = ['nutrition', 'herbal',  'health', 'weight loss' ]
    dic_commisary = ['commissary', 'commiasary']
       
    
    df['tf_cafe_serv']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_cafe) >0  else 0)
    df['tf_restaurant_serv']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_restaurante) >0  else 0)
    df['tf_bar_serv']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_bar) >0  else 0)
    df['tf_school_serv']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_school) >0  else 0)
    df['tf_assistence_serv']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_assistence) >0  else 0)
    df['tf_banquet_serv']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_banquet) >0  else 0)
    df['tf_drug_serv']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_drug) >0  else 0)
    df['tf_gas_station_serv']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_gas) >0  else 0)
    df['tf_entertaiment_serv']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_entertaiment) >0  else 0)
    df['tf_kiosko_mobil_serv']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_kiosko) >0  else 0)
    df['tf_grocery_almacen_conve_serv']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_grocery) >0  else 0)
    df['tf_health_store']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_health) >0  else 0)    
    df['tf_commissary_serv']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_commisary) >0  else 0)    
    
    
    df['BandServ'] = df[['tf_cafe_serv',  'tf_restaurant_serv' ,  'tf_bar_serv' ,  'tf_school_serv' , 'tf_assistence_serv' , 'tf_banquet_serv', 'tf_drug_serv' , 'tf_drug_serv' , 'tf_gas_station_serv', 
                                          'tf_entertaiment_serv' , 'tf_kiosko_mobil_serv' , 'tf_grocery_almacen_conve_serv' , 'tf_health_store', 'tf_commissary_serv' ]].sum(axis=1)
    df['tf_others_serv'] = df['BandServ'].apply(lambda x: 1 if x <1 else 0)
    df.drop(['BandServ'], axis = 'columns',  inplace = True)
    
    ############# DUMMY RIESGO ##################
    df['risk_all'] = df['risk'].apply(lambda x: 1 if x == 'All'   else 0 )
    df['risk_high'] = df['risk'].apply(lambda x: 1 if x =='Risk 1 (High)' else 0 )
    df['risk_medium'] = df['risk'].apply(lambda x: 1 if x =='Risk 2 (Medium)' else 0 )
    df['risk_low'] = df['risk'].apply(lambda x: 1 if x == 'Risk 3 (Low)' else 0 )
    
    df['Bandrisk'] = df[['risk_all','risk_high','risk_medium','risk_low'] ].sum(axis=1)
    df['risk_other'] = df['Bandrisk'].apply(lambda x: 1 if x <1 else 0)
    df.drop(['Bandrisk'], axis = 'columns',  inplace = True)
    
    ############# DUMMY INSPECTION TYPE ##################
    dic_canvass = ['canvas']    
    dic_license = ['license','tag removal','license task force / not -for-profit clu']    
    dic_licuor = ['task 1474','liquor','task force','taskforce','tavern 1470']      
    dic_complaint = ['complain','two people ate and got sick.']    
    dic_reinsp  = ['re-inspection','recent inspection','reinspection','recall inspection','re inspection',
                   'recently inspected','short form fire-complaint','changed court date','citation re-issued'] 
    dic_illegal = ['illegal operation']           
    dic_notready = ['not ready']        
    dic_outofbussines = ['out of business','consultation','no entry','business not located','out ofbusiness'] 
    dic_prelicense = ['pre-license consultation']
           
    df['type_canvass']=df['inspection_type'].apply(lambda x: 1 if limpieza_dic(x,dic_canvass) >0  else 0)    
    df['type_license']=df['inspection_type'].apply(lambda x: 1 if limpieza_dic(x,dic_license) >0  else 0)    
    df['type_licuor']=df['inspection_type'].apply(lambda x: 1 if limpieza_dic(x,dic_licuor) >0  else 0)    
    df['type_complaint']=df['inspection_type'].apply(lambda x: 1 if limpieza_dic(x,dic_complaint) >0  else 0)    
    df['type_reinsp']=df['inspection_type'].apply(lambda x: 1 if limpieza_dic(x,dic_reinsp) >0  else 0)  
    df['type_illegal']=df['inspection_type'].apply(lambda x: 1 if limpieza_dic(x,dic_illegal) >0  else 0)    
    df['type_not_ready']=df['inspection_type'].apply(lambda x: 1 if limpieza_dic(x,dic_notready) >0  else 0)    
    df['type_out_of_buss']=df['inspection_type'].apply(lambda x: 1 if limpieza_dic(x,dic_outofbussines) >0 else 0)
    df['type_prelicense']=df['facility_type'].apply(lambda x: 1 if limpieza_dic(x,dic_prelicense) >0  else 0)
        
    df['BandType'] = df[['type_canvass','type_license','type_licuor','type_complaint','type_reinsp',
                         'type_illegal','type_not_ready','type_out_of_buss','type_prelicense']].sum(axis=1)
    
    df['type_others'] = df['BandType'].apply(lambda x: 1 if x <1 else 0)
    df.drop(['BandType'], axis = 'columns',  inplace = True)
    
    
    return df


# Para conservar el mismo valor
class NoTransformer(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        assert isinstance(X, pd.DataFrame)
        return X


def feature_generation(data): 
    """
    Recibe el data frame que contiene las variables a partir de las cuales 
    crearemos nuevas variables. Estas nuevas variables se guardarán en este 
    mismo data frame.
    """    
    # Creación de variables
    # Dividiendo fecha en mes y día, y creando semanas
    data = split_fecha("inspection_date", data)
    
    # Para crear variables ciclícas
    data = ciclic_variables('day_of_week', data)
    data = ciclic_variables('week', data)
    data = ciclic_variables('inspection_date_mes', data)
    data = ciclic_variables('inspection_date_dia', data)
    
    # Medidas de distancia generales y agrupadas 
    data = get_distance(data)
 

    # Se limpian y se crean variables dummy para variables particulares de la base
    data = clean_dummy(data)

    # Variables a transformar
    data_input = pd.DataFrame(data,
                              columns=[ 'weekday',  'sin_day_no', 'cos_day_no', 'sin_week', 'cos_week', 'sin_month', 'cos_month', 'sin_days', 'cos_days', 
                                               'distance','distancia_ym', 'distancia_ym_mht', 
                                               'tf_cafe_serv', 'tf_restaurant_serv', 'tf_bar_serv', 'tf_school_serv', 'tf_assistence_serv', 'tf_banquet_serv', 'tf_drug_serv', 'tf_gas_station_serv',
                                               'tf_entertaiment_serv',  'tf_kiosko_mobil_serv', 'tf_grocery_almacen_conve_serv', 'tf_health_store', 'tf_commissary_serv', 'tf_others_serv',
                                               'risk_all', 'risk_high', 'risk_medium', 'risk_low', 'risk_other',
                                               'type_canvass', 'type_license', 'type_licuor',  'type_complaint', 'type_reinsp', 'type_illegal', 'type_not_ready',
                                               'type_out_of_buss', 'type_prelicense', 'type_others'])
    
    
    # Transformaciones
    transformers_2 = [('one_hot', OneHotEncoder(), ['weekday']),
                      ('scale', MinMaxScaler(), ['distance','distancia_ym_mht','distancia_ym' ]),
                      ('', NoTransformer(), ['sin_day_no','cos_day_no','sin_week','cos_week',
                                               'sin_month','cos_month','sin_days','cos_days',
                                               'tf_cafe_serv', 'tf_restaurant_serv', 'tf_bar_serv', 'tf_school_serv', 'tf_assistence_serv', 'tf_banquet_serv', 'tf_drug_serv', 'tf_gas_station_serv',
                                               'tf_entertaiment_serv',  'tf_kiosko_mobil_serv', 'tf_grocery_almacen_conve_serv', 'tf_health_store', 'tf_commissary_serv', 'tf_others_serv',
                                               'risk_all', 'risk_high', 'risk_medium', 'risk_low', 'risk_other',
                                               'type_canvass', 'type_license', 'type_licuor',  'type_complaint', 'type_reinsp', 'type_illegal', 'type_not_ready',
                                               'type_out_of_buss', 'type_prelicense', 'type_others'])]

    col_trans_2 = ColumnTransformer(transformers_2, remainder="drop", n_jobs=-1, verbose=True)
    col_trans_2.fit(data_input)
    input_vars = col_trans_2.transform(data_input) 

    input_vars = col_trans_2.transform(data_input)   
    cols = ['not_weekday', 'not_weekday',  
            'distance','distancia_ym_mht','distancia_ym',
            'sin_day_no','cos_day_no','sin_week','cos_week',
            'sin_month','cos_month','sin_days','cos_days',
            'tf_cafe_serv', 'tf_restaurant_serv', 'tf_bar_serv', 'tf_school_serv', 'tf_assistence_serv', 'tf_banquet_serv', 'tf_drug_serv', 'tf_gas_station_serv',
            'tf_entertaiment_serv',  'tf_kiosko_mobil_serv', 'tf_grocery_almacen_conve_serv', 'tf_health_store', 'tf_commissary_serv', 'tf_others_serv',
            'risk_all', 'risk_high', 'risk_medium', 'risk_low', 'risk_other',
            'type_canvass', 'type_license', 'type_licuor',  'type_complaint', 'type_reinsp', 'type_illegal', 'type_not_ready',
            'type_out_of_buss', 'type_prelicense', 'type_others']
    
    # Columnas del dataframe final
    df_final = pd.DataFrame(input_vars)
    df_final.columns = cols    

    df_final['label']= data['label']
    
       
    return df_final


def feature_engineering(df_transform, path_save= '../results/pkl_fe.pkl'):
    """
    Guarda en formato pickle (ver notebook feature_engineering.ipynb) el data frame
    que ya tiene los features que ocuparemos. El pickle se debe llamar fe_df.pkl y
    se debe guardar en la carpeta output.
    """
    print("Inicio proceso: feature_engineering")
    # Cargamos pickle de transformación
    #df_transform= u.load_df(path)
    
    # Realizamos el feature generation
    
    fe_df = feature_generation(df_transform)
   
    u.save_df(fe_df, path_save)
    print("Archivo 'pkl_fe.pkl' escrito correctamente")   
    print("Finalizó proceso: Feature engineering")
    
    return fe_df

    