from utils.utils import load_df, save_df
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler, OneHotEncoder, KBinsDiscretizer
from sklearn.compose import ColumnTransformer

import pandas as pd
import numpy as np
import datetime
import time
import warnings

warnings.filterwarnings('ignore')


def load_transformation(path='../output/'):
    """
    Recibe el path en donde se encuentra el pickle que generamos
    durante la transformación.
    """
    df = load_df(path + 'transformation_df.pkl')
    return df


def feature_generation(data):
    """
    Recibe el data frame que contiene las variables a partir de las cuales
    crearemos nuevas variables. Estas nuevas variables se guardarán en este
    mismo data frame.
    """
    # Para crear variables ciclícas
    data = ciclic_variables('day_of_week', data)
    data = ciclic_variables('week', data)
    data = ciclic_variables('inspection_date_mes', data)
    data = ciclic_variables('inspection_date_dia', data)

    # Variables posibles a transformar
    """
    'type','risk',zip','latitude','longitude',
    'sin_day_no','cos_day_no','sin_week','cos_week',
    'sin_month','cos_month','sin_days','cos_days'
    """

    # Variables a transformar
    data_input = pd.DataFrame(data,
                              columns=['type', 'risk', 'zip',  # 'results',
                                       'latitude', 'longitude',
                                       'sin_day_no', 'cos_day_no', 'sin_week', 'cos_week',
                                       'sin_month', 'cos_month', 'sin_days', 'cos_days'])
    # Transformaciones
    transformers_2 = [('one_hot', OneHotEncoder(), ['type', 'risk', 'zip']),  # ,'results']),
                      ('min_max', MinMaxScaler(), ['latitude', 'longitude',
                                                   'sin_day_no', 'cos_day_no', 'sin_week', 'cos_week',
                                                   'sin_month', 'cos_month', 'sin_days', 'cos_days'])]

    col_trans_2 = ColumnTransformer(transformers_2, remainder="drop", n_jobs=-1, verbose=True)
    col_trans_2.fit(data_input)

    input_vars = col_trans_2.transform(data_input)
    # Solo para medir metricas
    #pickle.dump(input_vars, open("output/feature_selection_input_vars_DPA.pkl", "wb"))

    cols = ['bakery', 'catering', 'childern\'s service facility', 'daycare', 'golden diner', 'grocery', 'hospital',
            'long-term care', 'otros', 'restaurant', 'school',
            'Risk 1 (High)', 'Risk 2 (Medium)', 'Risk 3 (Low)',
            '60601', '60602', '60603', '60604', '60605', '60606', '60607', '60608',
            '60609', '60610', '60611', '60612', '60613', '60614', '60615', '60616',
            '60617', '60618', '60619', '60620', '60621', '60622', '60623', '60624',
            '60625', '60626', '60627', '60628', '60629', '60630', '60631', '60632',
            '60633', '60634', '60636', '60637', '60638', '60639', '60640', '60641',
            '60642', '60643', '60644', '60645', '60646', '60647', '60649', '60651',
            '60652', '60653', '60654', '60655', '60656', '60657', '60659', '60660',
            '60661', '60666', '60707', '60827',
            # 'Fail', 'Pass', 'Pass w/ Conditions',
            'latitude', 'longitude',
            'sin_day_no', 'cos_day_no', 'sin_week', 'cos_week',
            'sin_month', 'cos_month', 'sin_days', 'cos_days']

    # Información del dataframe final
    df_final = pd.DataFrame(input_vars.todense())
    df_final.columns = cols

    # df_final['inspection_date']= data['inspection_date']
    df_final['label'] = data['label']

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
                      cv=tscv,
                      n_jobs=3)

    # ejecutando el RF
    start_time = time.time()
    gs.fit(X_train, y_train)
    print("El proceso en segundos duro: ", time.time() - start_time)
    print("Mejores parámetros: " + str(gs.best_params_))
    print("Score:" + str(print(gs.best_score_)))
    best_e = gs.best_estimator_
    print("Mejor estimador: " + str(best_e))
    print("Mejor estimador observado: " + str(gs.best_estimator_.oob_score_))

    cols = ['bakery', 'catering', 'childern\'s service facility', 'daycare', 'golden diner', 'grocery', 'hospital',
            'long-term care', 'otros', 'restaurant', 'school',
            'Risk 1 (High)', 'Risk 2 (Medium)', 'Risk 3 (Low)',
            '60601', '60602', '60603', '60604', '60605', '60606', '60607', '60608',
            '60609', '60610', '60611', '60612', '60613', '60614', '60615', '60616',
            '60617', '60618', '60619', '60620', '60621', '60622', '60623', '60624',
            '60625', '60626', '60627', '60628', '60629', '60630', '60631', '60632',
            '60633', '60634', '60636', '60637', '60638', '60639', '60640', '60641',
            '60642', '60643', '60644', '60645', '60646', '60647', '60649', '60651',
            '60652', '60653', '60654', '60655', '60656', '60657', '60659', '60660',
            '60661', '60666', '60707', '60827',
            # 'Fail', 'Pass', 'Pass w/ Conditions',
            'latitude', 'longitude',
            'sin_day_no', 'cos_day_no', 'sin_week', 'cos_week',
            'sin_month', 'cos_month', 'sin_days', 'cos_days']

    # Importancia de los parámetros
    feature_importance = pd.DataFrame({'importance': best_e.feature_importances_,
                                       'feature': list(cols)})
    print("Importancia de los parámetros")
    print(feature_importance.sort_values(by="importance", ascending=False))

    # Salvando el mejor modelo obtenido
    # save_fe(best_e, path='../output/feature_selection_model_DPA.pkl')
    save_fe(best_e, path='output/feature_selection_model_DPA.pkl')

    # Regresando dataframe con los features que ocuparemos.
    # En este caso las variables que aportan más del 7% de información son:
    final_df = data[[
        # Variables que aportan 7%
        'latitude', 'longitude', 'sin_days', 'cos_days',
        'sin_week', 'cos_week',
        # Variables que aportan 4% y 3%
        'sin_day_no', 'cos_day_no', 'sin_month', 'cos_month',
        # Variables que aportan 1.5% aprox
        'Risk 1 (High)', 'Risk 2 (Medium)', 'Risk 3 (Low)',
        'bakery', 'catering', 'childern\'s service facility',
        'daycare', 'golden diner', 'grocery', 'hospital',
        'long-term care', 'otros', 'restaurant', 'school',
        'label'
    ]]

    return final_df

def save_fe(df, path):
    """
    Guarda en formato pickle (ver notebook feature_engineering.ipynb) el data frame
    que ya tiene los features que ocuparemos. El pickle se debe llamar fe_df.pkl y
    se debe guardar en la carpeta output.
    """
    # pickle.dump(df, open(path, "wb"))
    # utils function, debería guardar el picjle llamado fe_df.pkl en la carpeta ouput
    save_df(df, path)


# --------------------- Funciones Auxiliares ---------------------------
def ciclic_variables(col, df):
    """
    Recibe la columna day_no, mes o fecha_creacion y las convierte en variables cíclicas:
    número día de la semana, mes, semana y hora respectivamente.
    :param: column, dataframe
    :return: dataframe con variable cíclica creada corresondientemente
    """

    if (col == 'day_of_week'):
        no_dia_semana = {'Sunday': 1, 'Monday': 2, 'Tuesday': 3, 'Wednesday': 4,
                         'Thursday': 5, 'Friday': 6, 'Saturday': 7}
        df['day_no'] = df[col].apply(lambda x: no_dia_semana[x])
        # max_day_no = np.max(df['day_no'])
        max_day_no = 7
        df['sin_day_no'] = np.sin(2 * np.pi * df['day_no'] / max_day_no)
        df['cos_day_no'] = np.cos(2 * np.pi * df['day_no'] / max_day_no)

    if (col == 'week'):
        # converting the hour into a sin, cos coordinate
        WEEKS = 53
        df['sin_week'] = np.sin(2 * np.pi * df[col] / WEEKS)
        df['cos_week'] = np.cos(2 * np.pi * df[col] / WEEKS)

    if (col == 'inspection_date_mes'):
        MONTH = 12
        df['sin_month'] = np.sin(2 * np.pi * df[col] / MONTH)
        df['cos_month'] = np.cos(2 * np.pi * df[col] / MONTH)

    if (col == 'inspection_date_dia'):
        # converting the hour into a sin, cos coordinate
        DAYS = 31
        df['sin_days'] = np.sin(2 * np.pi * df[col] / DAYS)
        df['cos_days'] = np.cos(2 * np.pi * df[col] / DAYS)

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
    df = feature_selection(fe_df)
    # Se salva el dataframe con los features
    save_fe(df, '../output/fe_df.pkl')
    print("Finalizó proceso feature_engineering")