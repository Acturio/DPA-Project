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
from siuba import *

warnings.filterwarnings('ignore')


def load_transformation(path):
    """
    Recibe el path en donde se encuentra el pickle que generamos
    durante la transformación.
    """
    df = u.load_df(path)
    return df


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
    
    # Distancia al centroide
    data = get_distance(data)
    
    # Variables posibles a transformar
    """
    'type_insp','type','risk','zip','weekday','distance',
    'sin_day_no','cos_day_no','sin_week','cos_week',
    'sin_month','cos_month','sin_days','cos_days'
    """
    # Variables a transformar
    data_input = pd.DataFrame(data,
                              columns=['type_insp','type','risk','zip','weekday',
                                       'distance','distancia_ym_mht','distancia_ym',
                                       'sin_day_no','cos_day_no','sin_week','cos_week',
                                       'sin_month','cos_month','sin_days','cos_days'])
    # Transformaciones
    transformers_2 = [('one_hot', OneHotEncoder(), ['type_insp','type','risk','zip','weekday']),
                      ('min_max', MinMaxScaler(), ['distance','distancia_ym_mht','distancia_ym' ]),
                      ('r_original', NoTransformer(), ['sin_day_no','cos_day_no','sin_week','cos_week',
                                                       'sin_month','cos_month','sin_days','cos_days'])]

    col_trans_2 = ColumnTransformer(transformers_2, remainder="drop", n_jobs=-1, verbose=True)
    col_trans_2.fit(data_input)

    input_vars = col_trans_2.transform(data_input)   
    # Solo para medir metricas
    #pickle.dump(input_vars, open("output/feature_selection_input_vars_DPA.pkl", "wb"))

    cols = ['Canvass', 'Complaint', 'Consultation', 'License',
           'LicenseTaskForce-PreLicenseConsultation', 'Other',
           'Package liquor 1474', 'Recent inspection',
           'Special events (festivals)', 'Suspected food poisoning', 'Tag removal','Task force',
            
            'AssistanceService', 'BanquetService-Church', 'Bar', 'CoffeShop',
            'Drug-Grocery', 'EntertainmentServices', 'GasStation-Grosery',
            'Grocery-Almacen-ConvenienceStore', 'HealthStore', 'Kiosko', 'Others',
            'Restaurant', 'School',
            'All','Risk 1 (High)', 'Risk 2 (Medium)', 'Risk 3 (Low)',
            
            '60148', '60406', '60501', '60601', '60602', '60603', '60604', '60605',
           '60606', '60607', '60608', '60609', '60610', '60611', '60612', '60613',
           '60614', '60615', '60616', '60617', '60618', '60619', '60620', '60621',
           '60622', '60623', '60624', '60625', '60626', '60627', '60628', '60629',
           '60630', '60631', '60632', '60633', '60634', '60636', '60637', '60638',
           '60639', '60640', '60641', '60642', '60643', '60644', '60645', '60646',
           '60647', '60649', '60651', '60652', '60653', '60654', '60655', '60656',
           '60657', '60659', '60660', '60661', '60666', '60707', '60827',
            
            #'Fail', 'Pass', 'Pass w/ Conditions',
            #'latitude','longitude',
            'N','Y',
            'distance','distancia_ym_mht','distancia_ym',
            'sin_day_no','cos_day_no','sin_week','cos_week',
            'sin_month','cos_month','sin_days','cos_days']

    # Información del dataframe final
    df_final = pd.DataFrame(input_vars.todense())
    df_final.columns = cols    
        
    #df_final['inspection_date']= data['inspection_date']
    df_final['label']= data['label']
    
    return df_final
   
    
def feature_selection(data): 
    """
    Recibe el data frame que contiene las variables de las cuales haremos
    una selección.
    """
    X = data
    y = data.label
    X = pd.DataFrame(X.drop(['label'], axis=1))
    
    np.random.seed(20201124)

    ## Dividiendo datos en train y test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, shuffle=False, random_state=None)

    # ocuparemos un RF
    classifier = RandomForestClassifier(oob_score=True, random_state=1234)

    # Definicion de los hiperparametros que queremos probar
    hyper_param_grid = {'n_estimators': [100, 300],
                        'max_depth': [1, 10, 15],
                        'min_samples_split': [2, 5]}

    tscv = TimeSeriesSplit(n_splits=3)
    
    # Ocupemos grid search!
    gs = GridSearchCV(classifier,
                      hyper_param_grid,
                      scoring='precision',
                      cv = tscv,
                      n_jobs = 3)

    # ejecutando el RF
    start_time = time.time()
    gs.fit(X_train, y_train)
    print("El proceso en segundos duro: ", time.time() - start_time)
    print("Mejores parámetros: " + str(gs.best_params_))
    print("Score:" + str(print(gs.best_score_)))
    best_e = gs.best_estimator_
    print("Mejor estimador: " + str(best_e))
    print("Mejor estimador observado: " + str(gs.best_estimator_.oob_score_))
    
    cols = ['Canvass', 'Complaint', 'Consultation', 'License',
           'LicenseTaskForce-PreLicenseConsultation', 'Other',
           'Package liquor 1474', 'Recent inspection',
           'Special events (festivals)', 'Suspected food poisoning', 'Tag removal','Task force',
            
            'AssistanceService', 'BanquetService-Church', 'Bar', 'CoffeShop',
            'Drug-Grocery', 'EntertainmentServices', 'GasStation-Grosery',
            'Grocery-Almacen-ConvenienceStore', 'HealthStore', 'Kiosko', 'Others',
            'Restaurant', 'School',
            'All','Risk 1 (High)', 'Risk 2 (Medium)', 'Risk 3 (Low)',
            
            '60148', '60406', '60501', '60601', '60602', '60603', '60604', '60605',
           '60606', '60607', '60608', '60609', '60610', '60611', '60612', '60613',
           '60614', '60615', '60616', '60617', '60618', '60619', '60620', '60621',
           '60622', '60623', '60624', '60625', '60626', '60627', '60628', '60629',
           '60630', '60631', '60632', '60633', '60634', '60636', '60637', '60638',
           '60639', '60640', '60641', '60642', '60643', '60644', '60645', '60646',
           '60647', '60649', '60651', '60652', '60653', '60654', '60655', '60656',
           '60657', '60659', '60660', '60661', '60666', '60707', '60827',
            
            #'Fail', 'Pass', 'Pass w/ Conditions',
            #'latitude','longitude',
            'N','Y',
            'distance','distancia_ym_mht','distancia_ym',
            'sin_day_no','cos_day_no','sin_week','cos_week',
            'sin_month','cos_month','sin_days','cos_days']



    # Importancia de los parámetros
    feature_importance = pd.DataFrame({'importance': best_e.feature_importances_,
                                       'feature': list(cols)})
    print("Importancia de los parámetros")
    print(feature_importance.sort_values(by="importance", ascending=False))

    # Salvando el mejor modelo obtenido
    #save_fe(best_e, path='../output/feature_selection_model_DPA.pkl')
    save_fe(best_e, path='output/feature_selection_model_DPA.pkl')

    # Regresando dataframe con los features que ocuparemos.
    # En este caso las variables que aportan más del 7% de información son:
    final_df = data[[
                            # Variables que aportan 7%
                            'distance','distancia_ym_mht','distancia_ym',
                            'sin_days','cos_days',
                            'sin_week','cos_week',    
                            # Variables que aportan 4% y 3%
                            'sin_day_no','cos_day_no','sin_month','cos_month',
                            # Variables que aportan 1.5% aprox
                            'All','Risk 1 (High)', 'Risk 2 (Medium)', 'Risk 3 (Low)',
                            
                            'Canvass', 'Complaint', 'Consultation', 'License',
           'LicenseTaskForce-PreLicenseConsultation', 'Other',
           'Package liquor 1474', 'Recent inspection',
           'Special events (festivals)', 'Suspected food poisoning', 'Tag removal','Task force',
        
                            'AssistanceService', 'BanquetService-Church', 'Bar', 'CoffeShop',
                            'Drug-Grocery', 'EntertainmentServices', 'GasStation-Grosery',
                            'Grocery-Almacen-ConvenienceStore', 'HealthStore', 'Kiosko', 'Others',
                            'Restaurant', 'School',
        
                            'label'
                         ]]    

    return final_df

    
def save_fe(df, path='fe_df.pkl'):
    """
    Guarda en formato pickle (ver notebook feature_engineering.ipynb) el data frame 
    que ya tiene los features que ocuparemos. El pickle se debe llamar fe_df.pkl y 
    se debe guardar en la carpeta output.
    """    
    #pickle.dump(df, open(path, "wb"))##en jupyter
    # utils function, debería guardar el picjle llamado fe_df.pkl en la carpeta ouput
    u.save_df(df, path)# en .py
    

# --------------------- Funciones Auxiliares ---------------------------   
# Para conservar el mismo valor
class NoTransformer(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        return self

    def transform(self, X):
        assert isinstance(X, pd.DataFrame)
        return X
    
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

#     df['lat'] = lat_c.astype(float) - df['latitude'].astype(float) 
#     df['lon'] = lon_c.astype(float) - df['longitude'].astype(float) 
    
    df['distance'] = np.sqrt(df['lat']**2+df['lon']**2)
    
    df = (df
        >> group_by (_.inspection_date_anio, _.inspection_date_mes) 
        >> mutate(mediana_ym_lat = _.latitude.median(), mediana_ym_lon = _.longitude.median())
        >> ungroup()
        >> mutate(distancia_ym = ( (_.mediana_ym_lat - _.latitude)**2 +  (_.mediana_ym_lon - _.longitude)**2 ) **(1/2) )
        >> mutate(distancia_ym_mht = abs (_.mediana_ym_lat - _.latitude) +  abs(_.mediana_ym_lon - _.longitude) ) )

    df = pd.DataFrame(df.drop(['lat','lon','mediana_ym_lat', 'mediana_ym_lon'], axis=1))
    
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
    


def feature_engineering(path):
    """
    Guarda en formato pickle (ver notebook feature_engineering.ipynb) el data frame
    que ya tiene los features que ocuparemos. El pickle se debe llamar fe_df.pkl y
    se debe guardar en la carpeta output.
    """
    print("Inicio proceso feature_engineering")
    # Cargamos pickle de transformación
    df_transform = load_transformation(path)
    # Realizamos el feature generation
    fe_df = feature_generation(df_transform)
    # Corremos el modelo y nos quedamos con los mejores parametros y
    # las variables que tienen más del 7%
#     df = feature_selection(fe_df)
    # Se salva el dataframe con los features
    save_fe(fe_df, 'fe_df.pkl')
    print("Finalizó proceso con éxito y de manera bonita : feature_engineering")
