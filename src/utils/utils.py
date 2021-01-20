
# Functions for EDA and GEDA Analysis

import pandas as pd 
import seaborn as sns 
import numpy as np
import matplotlib.pyplot as plt 
from matplotlib.ticker import FuncFormatter

# Función number_formater

def number_formatter(number, pos = None):
    """Convert a number into a human readable format."""
    magnitude = 0
    while abs(number) >= 1000:
        magnitude += 1
        number /= 1000.0
    return '%.1f%s' % (number, ['', 'K', 'M', 'B', 'T', 'Q'][magnitude])



# Función integer_formatter

def int_formatter(number, pos=None):
    return int(number)



# Función get_repeated_values

def get_repeated_values(df, col, top):
    """Return top 't' of repeated values."""
    top_5 = df.groupby([col])[col]\
                    .count()\
                    .sort_values(ascending = False)\
                    .head(5)
    indexes_top_5 = top_5.index
    
    if ((top == 1) and (len(indexes_top_5) > 0)):
        return indexes_top_5[0]
    elif ((top == 2) and (len(indexes_top_5) > 1)):
        return indexes_top_5[1]
    elif ((top == 3) and (len(indexes_top_5) > 2)):
        return indexes_top_5[2]
    else: 
        return 'undefined'

    
    
# Función categoric_profiling

def categoric_profiling(df_o, col):
    """
    Profiling for categoric columns. 
    
    :param: column to analyze
    :return: dictionary
    """
    profiling = {}
    
    # eliminate missing values
    df = df_o.copy()
    #df = df[df[col].notna()]

    profiling.update({'mode': df[col].mode().values,
                     'uniques': df[col].nunique(),
                     'missings': df[col].isnull().sum(),
                     'top1_repeated': get_repeated_values(df, col, 1),
                     'top2_repeated': get_repeated_values(df, col, 2),
                     'top3_repeated': get_repeated_values(df, col, 3)})
    
    return profiling



# Función dates_profiling

def dates_profiling(df_o, col):
    """
    Profiling for dates and hours columns. 
    
    :param: column to analyze
    :return: dictionary
    """
    profiling = {}
    
    # eliminate missing values
    df = df_o.copy()
   # df = df[df[col].notna()]

    profiling.update({'max': df[col].max(),
                     'min': df[col].min(),
                     'missings': df[col].isnull().sum(),
                     'uniques': df[col].nunique(),
                     'top1_repeated': get_repeated_values(df, col, 1),
                     'top2_repeated': get_repeated_values(df, col, 2),
                     'top3_repeated': get_repeated_values(df, col, 3)})
    
    return profiling


# Función para crear gráficas de barra

def barplots(x,y,df,title, order, xlim, x_label, y_label):
    a = sns.barplot(x=x,y=y, data=df, order = df[order])
    a.xaxis.set_major_formatter(FuncFormatter(number_formatter))
    a.set_xlabel(x_label)
    a.set_ylabel(y_label)
    a.set_xlim(0,xlim)
    a.set_title(title)
    
    

# Función para crear gráficas de barra desagregadas por variable categórica
    
def facet_grids(x,y,data,col):
    """
    :param: x categorical variable
    :param: y response variable
    
    """
    a = sns.FacetGrid(data=data, col=col, col_wrap=4, sharex=False, sharey=False,
                 height=4.5, aspect=1.5)
    a.map_dataframe(sns.barplot, x=x, y=y)
    for ax in a.axes.flat:
        for label in ax.get_xticklabels():
            label.set_size(7)
            label.set_rotation(90)
        ax.yaxis.set_major_formatter(FuncFormatter(number_formatter))
