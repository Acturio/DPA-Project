from utils.utils import load_df, save_df
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit, RandomizedSearchCV
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
import pandas as pd
import time


def load_features(path='../output/'):
    """
    Recibe el path en donde se encuentra el pickle que generamos
    durante el feature_engineering.
    :param: file_name
    :return: data frame
    """
    df = load_df(path + 'fe_df.pkl')
    return df


def magic_loop(algorithms, df):
    """
    Evaluación de metodología Magic Loop en la etapa de modeling
    :param: Algoritmos a evaluar y el dataframe
    :return: mejor modelo
    """
    # Procesamiento de datos
    X = df
    y = df.label
    X = pd.DataFrame(X.drop(['label'], axis=1))
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, shuffle=False, random_state=None)

    # Magic Loop
    algorithms_dict = {'tree': 'tree_grid_search',
                       'random_forest': 'rf_grid_search'}
    grid_search_dict = {'tree_grid_search': {'max_depth': [5, 10, 15, None],
                                             'min_samples_leaf': [3, 5, 7]},
                        'rf_grid_search': {'n_estimators': [30, 50, 100],
                                           'max_depth': [5, 10],
                                           'min_samples_leaf': [3, 5]}}

    estimators_dict = {'tree': DecisionTreeClassifier(random_state=1111),
                       'random_forest': RandomForestClassifier(oob_score=True, random_state=2222)}

    # Empezar proceso
    best_estimators = []
    start_time = time.time()
    for algorithm in algorithms:
        estimator = estimators_dict[algorithm]
        grid_search_to_look = algorithms_dict[algorithm]
        grid_params = grid_search_dict[grid_search_to_look]

        tscv = TimeSeriesSplit(n_splits=5)
        gs = GridSearchCV(estimator, grid_params, scoring='precision', cv=tscv, n_jobs=3)

        # train
        gs.fit(X_train, y_train)
        # best estimator
        best_estimators.append(gs)

    print("Tiempo de ejecución: ", time.time() - start_time)

    # Seleccionar el de mejor desempeño
    models_develop = [best_estimators[0].best_score_, best_estimators[1].best_score_]
    max_score = max(models_develop)
    max_index = models_develop.index(max_score)
    best_model = best_estimators[max_index]

    print('Mejor modelo: ', best_model.best_estimator_)
    print('Mejor desempeño: ', best_model.best_score_)
    return best_model

def save_models(model, path = '../output/model_loop_DPA.pkl'):
    save_df(model, path)

def modeling(path):
    print("Iniciando modelado")
    start_time = time.time()
    # Cargamos datos
    df = load_features(path)
    algorithms = ['tree', 'random_forest']
    # Obtenemos el modelo
    model = magic_loop(algorithms, df)
    # Guardamos el modelo
    save_models(model)
    print("Terminó modelado en ", time.time() - start_time)





