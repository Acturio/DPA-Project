from src.utils import utils as u 

import pandas as pd
import numpy as np
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit, train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder, KBinsDiscretizer
from sklearn.compose import ColumnTransformer
from sklearn.base import BaseEstimator,TransformerMixin
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from siuba import *
import time
from time import gmtime, strftime



def train_test(df, path_save_train = 'pkl_train.pkl', path_save_test='pkl_test.pkl'   ): 
    """
    Recibe el data frame del cual se elegiran muestran de test y train
    """
    print('Se inicia el proceso de muestreo:train/test')   
    start_time = time.time()
    X = df
    y = df.label
    y_id = df.inspection_id 
    X = pd.DataFrame(X.drop(['label'], axis=1))
    
    np.random.seed(2021)
    X_train_id, X_test_id, y_train, y_test = train_test_split(X, y, test_size=0.2,stratify=y )
    print('Muestreo estratificado train/test completado satisfactoriamente en ', time.time() - start_time)
          


    return (X_train_id, X_test_id, y_train,y_test)



def sampling(df, path_save_train_sampling = 'pkl_train.pkl'   ): 
    """
    Recibe el data frame que contiene las variables de las cuales haremos  una selección demuestra pequeña para ejercicio en clase 
    """
    print('Se inicia el proceso de muestreo para ejercicio')
    start_time = time.time()
    
    n= int(round(df.shape[0] *.1,0))
    
    X = df
    y = df.label
    y_id = df.inspection_id 
    X = pd.DataFrame(X.drop(['label'], axis=1))
    
    np.random.seed(2021)
    X_train_id, X_test_id, y_train, y_test = train_test_split(X, y, train_size= n ,stratify=y )
    print('Muestreo estratificado completado satisfactoriamente en ', time.time() - start_time, 'segundos' )
          
    return (X_train_id, y_train)

def auto_selection_variables (X_train_id, y_train):
    start_time = time.time()
    classifier = RandomForestClassifier(oob_score=True, random_state=1234)

     # Definicion de los hiperparametros que queremos probar
    hyper_param_grid = { 'n_estimators': [200], # Numero de arboles
                                            'max_depth': [ 10],     #Profundidad
                                            'min_samples_split': [ 5]}  #El minimo para un nuevo nodo

    tscv = TimeSeriesSplit(n_splits=3)

    gs = GridSearchCV(classifier,
                          hyper_param_grid,
                          scoring='recall',
                          cv = tscv,
                          n_jobs = 3)

    X_train = X_train_id.drop(columns=['inspection_id'])
   
    gs.fit(X_train, y_train)
    best_e = gs.best_estimator_
    cols= X_train.columns

    feature_importance = pd.DataFrame({'importance': best_e.feature_importances_,
                                           'feature': list(cols)}).sort_values(by="importance", ascending=False)

    auto_selection_variables = feature_importance[feature_importance.importance > 0.0001 ]['feature'].unique()
    
    
    print("Selección de variables completada satisfactoriamente en ", time.time() - start_time, ' segundos')
    
    return (auto_selection_variables)



def train_models(X_train_id, y_train, auto_variables):

    X_train = X_train_id[auto_variables]

    algorithms_dict = {'tree': 'tree_grid_search',
                          'random_forest': 'rf_grid_search',
                          'logistic': 'logistic_grid_search'}

    grid_search_dict = {
        'tree_grid_search': {
            'max_depth': [5, 10, 15],
            'min_samples_leaf': [3, 5, 7]
        },
        'rf_grid_search': {
            'n_estimators': [30, 50, 100],
            'max_depth': [5, 10, 15],
            'min_samples_leaf': [3, 5, 10]
        },
        'logistic_grid_search':{
            'C':np.logspace(-3,3,7),
            'penalty':['l2']
        }
    }

    estimators_dict = {'tree': DecisionTreeClassifier(random_state=1111),
                       'random_forest': RandomForestClassifier(oob_score=True, random_state=2222),
                       'logistic': LogisticRegression(random_state=3333) 
                       } 
    
    scoring_met= 'recall'
    algorithms = ['tree', 'random_forest','logistic']
    models = []
    start_time = time.time()
    models_list = []
   
    for algorithm in algorithms:
        
        estimator = estimators_dict[algorithm]
        grid_search_to_look = algorithms_dict[algorithm]
        grid_params = grid_search_dict[grid_search_to_look]
        tscv = TimeSeriesSplit(n_splits=5)
        
        
        gs = GridSearchCV(estimator, 
                          grid_params, scoring = scoring_met, 
                          cv = tscv,  n_jobs= - 2 )
        
        # train
        model_gs= gs.fit(X_train, y_train)
       
        models.append(model_gs)
                   
            
    return (models)



### TRAINING FUNCTION ###
def training(df_fe, exercise=True) :
    
    print("Inicia proceso de entrenamiento de modelos")
    start_time = time.time()
    
    if exercise: 
        X_train_id, y_train = sampling(df_fe)
        auto_variables = auto_selection_variables(X_train_id, y_train)
        models = train_models(X_train_id, y_train, auto_variables)
        print("Se concluye proceso de entrenamiento muestral para ejercicio en ", time.time() - start_time, ' segundos')
    
    else : 
        X_train_id = df_fe.drop(['label'], axis = 1)
        y_train = df_fe.label
        auto_variables = auto_selection_variables(X_train_id, y_train)
        models = train_models(X_train_id, y_train, auto_variables)
        print("Se concluye proceso de entrenamiento con datos completos en  ", time.time() - start_time, ' segundos')

    model_and_features = {'models': models, 'features': auto_variables}

        
    return model_and_features



### METADATA TRAINING FUNCTION ###
def metadata_models(models_ejercicio, date= ''):
    
    cv_results_f = pd.DataFrame([])

    for i in range(len(models_ejercicio['models'])):

        cv_results = pd.DataFrame(models_ejercicio['models'][i].cv_results_) 
        cv_results['estimator']= models_ejercicio['models'][i].estimator
        cv_results['scoring']= models_ejercicio['models'][i].scoring
        cv_results['processing_date']= strftime("%Y-%m-%d %H:%M:%S", gmtime())
        cv_results['data_date']= date
        
        cv_results = cv_results[['processing_date', 'data_date','estimator', 'scoring', 'params','mean_test_score','rank_test_score']]

        cv_results_f = pd.concat([cv_results_f, cv_results], axis=0)
    
    # Parámetros por modelo
    md_id= cv_results_f.reset_index()
    models_results_params= pd.DataFrame([])

    for i in range(len (md_id)): 

        registro = md_id[['index',  'processing_date', 'data_date', 'estimator', 'scoring' ,  'mean_test_score', 'rank_test_score']][md_id.index == i]
        registro['index']= i
        dic_parameter= md_id['params'][i]
        key_list = list(dic_parameter.keys())
        val_list = list(dic_parameter.values())
        dic_new= {'parameter': key_list, 'value': val_list, 'index': i}
        params= pd.DataFrame(dic_new)

        df_parameter= pd.merge( registro,params, how= 'left', on='index' )

        models_results_params = pd.concat([models_results_params, df_parameter], axis=0)
    
    models_results_params.rename(columns = {'index':'num_model'}, inplace = True)

    models_results_params['value'] = (models_results_params['value']).astype('str').str.replace('l','').astype('float')

    return (models_results_params)




## BEST MODEL SELECCION ##
def best_model(models_ejercicio): 
    
    scores = []
    best_estimator = []
    for i in range(len(models_ejercicio['models'])):
            scores.append(models_ejercicio['models'][i].best_score_) 
            best_estimator.append(models_ejercicio['models'][i].best_estimator_) 

    max_score = max(scores)  
    max_score_index = scores.index(max_score)
    best_model = {
        'best_model': best_estimator[max_score_index],
        'features': models_ejercicio['features']
    }
    

    return best_model


### METADATA BEST MODEL  ###
def metadata_best_model(best_model, data_date) : 
    
    df = pd.DataFrame(columns=['processing_date','data_date','value_params'])
    
    df['processing_date'] = [strftime("%Y-%m-%d %H:%M:%S", gmtime())]
    df['data_date']= [data_date]
    df['value_params'] = (best_model.get_params)
        

    return (df)










        
        


