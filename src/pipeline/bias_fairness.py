from src.utils import utils as u 
from src.pipeline import modeling as e
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_recall_curve , roc_auc_score, roc_curve, PrecisionRecallDisplay
from aequitas.group import Group
from aequitas.bias import Bias
from aequitas.fairness import Fairness
from src.pipeline.ingesta_almacenamiento import get_s3_client
import src.utils.constants as cte
import pickle
from datetime import datetime
import numpy as np
import pandas as pd
import time


def predict(df_fe, best_model, auto_variables, inicial, date_input):
    """
    Recibe el data frame a predecir, regresa los labesl y scores predichos
    """
    X_train_id, X_test_id, y_train,y_test = e.train_test(df_fe)
    predicted_labels = best_model.predict(X_test_id[auto_variables])
    predicted_scores = best_model.predict_proba(X_test_id[auto_variables])
    
    # Punto de corte con recall
    fpr, tpr, thresholds_roc = roc_curve(y_test, predicted_scores[:,1], pos_label=1)
    s1=pd.Series(thresholds_roc,name='threshold')
    s2=pd.Series(fpr,name='false_pr')
    s3=pd.Series(tpr ,name='true_pr')
    df_threshold_roc = pd.concat([s1,s2,s3], axis=1)
    recall = 0.8
    threshold_recall = round(df_threshold_roc[df_threshold_roc.false_pr == df_threshold_roc[df_threshold_roc.true_pr >= recall ].false_pr.min()].threshold, 2).max()
    
    # Resultados 
    predict_proba = pd.DataFrame(predicted_scores[:,1])
    terminos_threshold = predict_proba > threshold_recall

    score = terminos_threshold[0]
    score = score.replace(True,1).replace(False,0)
    score = score.to_numpy()

    #print(predicted_scores)
    print(predict_proba)

    results = pd.DataFrame(y_test)
    results['score'] = score 
    results['pred_score'] = predict_proba.values
    
    results_confusion_matrix =  pd.DataFrame(results[['label', 'score']].value_counts()).sort_values('label')
    results_confusion_matrix
    
    # Regresando a una sola columna el one hot encoding de la variable tipo de inspección
    X_test_id['type_inspection_limpia'] = X_test_id[['type_canvass','type_license','type_licuor','type_complaint',
                                          'type_reinsp','type_illegal','type_not_ready','type_out_of_buss',
                                          'type_prelicense','type_others']].idxmax(axis=1) 
    print(results)                                  
    results_conjunto = pd.concat([results,X_test_id], axis=1)
    
    #Leyendo variables de inicio
    if inicial:
         file_name = 'processed-data/clean-historic-inspections-{}.pkl'.format(date_input.strftime('%Y-%m-%d'))
    else:
         file_name = 'processed-data/clean-consecutive-inspections-{}.pkl'.format(date_input.strftime('%Y-%m-%d'))

    
    s3 = get_s3_client(cte.CREDENTIALS)
    s3_object = s3.get_object(Bucket = cte.BUCKET, Key = file_name)
    body = s3_object['Body']
    my_pickle = pickle.loads(body.read())

    df_clean = pd.DataFrame(my_pickle)
    

    df_clean_results = df_clean[['inspection_id', 'dba_name', 'facility_type', 'inspection_type']]
    results_conjunto_original = results_conjunto.merge(df_clean_results, on='inspection_id', how='left')
    
    return results_conjunto_original


def bias_fairness(df_fe, best_model, auto_variables, inicial, fecha= ''):
    
    print('Generación de análisis de sesgo e inequidad en base a la metodología de Aequitas ')
    start_time = time.time()
    # Cargamos features, mejor modelo y variables
    df = predict(df_fe, best_model, auto_variables, inicial, fecha)
    
        
    # Realizamos análisis de sesgo e inequidad
    df_s_i = df[['score','label','type_inspection_limpia']]
    df_s_i.rename(columns = {'label':'label_value'}, inplace = True)
    
    #Group
    g = Group()
    xtab, attrbs = g.get_crosstabs(df_s_i)
    # Bias
    bias = Bias()    
    bdf = bias.get_disparity_predefined_groups(xtab, original_df=df_s_i, 
                                           ref_groups_dict={'type_inspection_limpia':'type_canvass'}, 
                                           alpha=0.05)
    # Fairness
    fair = Fairness()
    fdf = fair.get_group_value_fairness(bdf)
    
    # Si se va a base de datos------------------------------------------------------------------------------
    if fecha != '':
        fdf['fecha_load'] = datetime.today().strftime('%Y-%m-%d')
        fdf['fecha'] = fecha
        fdf = fdf[
            ['fecha_load',
             'fecha',
             'model_id',
             'score_threshold',
             'k',
             'attribute_name',
             'attribute_value',
             'tpr',
             'tnr',
             'for',
             'fdr',
             'fpr',
             'fnr',
             'npv',
             'precision',
             'pp',
             'pn',
             'ppr',
             'pprev',
             'fp',
             'fn',
             'tn',
             'tp',
             'group_label_pos',
             'group_label_neg',
             'group_size',
             'total_entities',
             'prev',
             'ppr_disparity',
             'pprev_disparity',
             'precision_disparity',
             'fdr_disparity',
             'for_disparity',
             'fpr_disparity',
             'fnr_disparity',
             'tpr_disparity',
             'tnr_disparity',
             'npv_disparity',
             'ppr_ref_group_value',
             'pprev_ref_group_value',
             'precision_ref_group_value',
             'fdr_ref_group_value',
             'for_ref_group_value',
             'fpr_ref_group_value',
             'fnr_ref_group_value',
             'tpr_ref_group_value',
             'tnr_ref_group_value',
             'npv_ref_group_value',
             'Statistical Parity',
             'Impact Parity',
             'FDR Parity',
             'FPR Parity',
             'FOR Parity',
             'FNR Parity',
             'TPR Parity',
             'TNR Parity',
             'NPV Parity',
             'Precision Parity',
             'TypeI Parity',
             'TypeII Parity',
             'Equalized Odds',
             'Unsupervised Fairness',
             'Supervised Fairness'
             ]]
        dwb_col = fdf.columns.str.replace('\s+', '_')
        fdf.columns = map(str.lower, dwb_col)
        fdf.rename(columns = {'for':'f_or'}, inplace = True)
    #-------------------------------------------------------------------------------------------------------
    
    fdf = fdf.where(pd.notnull(fdf),None)
    
    #u.save_df(fdf, path_save)
    
    print("Proceso sesgo e inequidad finalizado en ", time.time() - start_time)

    return fdf









