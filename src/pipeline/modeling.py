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

def auto_selection_variables ( X_train_id,  y_train ):
    start_time = time.time()
    classifier = RandomForestClassifier(oob_score=True, random_state=1234)

     # Definicion de los hiperparametros que queremos probar
    hyper_param_grid = { 'n_estimators': [200], # Numero de arboles
                                            'max_depth': [ 10],     #Profundidad
                                            'min_samples_split': [ 5]}  #El minimo para un nuevo nodo

    tscv = TimeSeriesSplit(n_splits=3)

    gs = GridSearchCV(classifier,
                          hyper_param_grid,
                          scoring='precision',
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



def train_models(X_train_id, y_train, auto_variables, path_save_models):

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
    
    scoring_met= 'precision'
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
                   
    u.save_df(models, path_save_models)
            
    return (models)



### TRAINING FUNCTION ###
def training(df_fe, path_save_models, exercise=True) :
    
    start_time = time.time()
    
    if exercise == True : 
        X_train_id, y_train= sampling(df_fe)
        auto_variables = auto_selection_variables(X_train_id, y_train)
        models = train_models(X_train_id, y_train, auto_variables, path_save_models)
        print("Se concluye proceso de entrenamiento para ejercicio en ", time.time() - start_time, ' segundos')
    
    else : 
        X_train_id = df_fe.drop(['label'], axis=1)
        y_train = df_fe.label
        auto_variables = auto_selection_variables(X_train_id, y_train)
        models = train_models(X_train_id, y_train, auto_variables, path_save_models)
        print("Se concluye proceso de entrenamiento con datos completos en  ", time.time() - start_time, ' segundos')
        
    return (models)



### METADATA TRAINING FUNCTION ###
def metadata_models(models_ejercicio, date= ''):
    
    cv_results_f = pd.DataFrame([])

    for i in range(len(models_ejercicio)):

        cv_results = pd.DataFrame(models_ejercicio[i].cv_results_) 
        cv_results['estimator']= models_ejercicio[i].estimator
        cv_results['scoring']= models_ejercicio[i].scoring
        cv_results['processing_date']= strftime("%Y-%m-%d %H:%M:%S", gmtime())
        cv_results['data_date']= date
        
        
        cv_results = cv_results[['processing_date', 'data_date','estimator', 'scoring', 'params','mean_test_score','rank_test_score']]

        cv_results_f = pd.concat([cv_results_f, cv_results], axis=0)
    
    return (cv_results_f)




## BEST MODEL SELECCION ##
def best_model(models_ejercicio): 
    
    scores = []
    best_estimator = []
    for i in range(len(models_ejercicio)):
            scores.append(models_ejercicio[i].best_score_) 
            best_estimator.append(models_ejercicio[i].best_estimator_) 

    max_score = max(scores)  
    max_score_index = scores.index(max_score)
    best_model = best_estimator[max_score_index]
    
    return best_model


### METADATA BEST MODEL  ###
def metadata_best_model(best_model, data_date) : 
    
    df = pd.DataFrame(columns=['processing_date','data_date', 'base_estimator','params', 'value_params', 'num_features', 'oob_score'])
    
    df['processing_date'] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    df['data_date']= [fecha]
    df['base_estimator']= ([best_model.base_estimator])
    df['params']=([best_model.estimator_params ])
    df['value_params'] = (best_model.get_params)
    df['num_features'] = (best_model.n_features_)
    df['oob_score'] = (best_model.oob_score_)
        
    return (df)










        
        


