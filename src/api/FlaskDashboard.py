import dash
import dash_core_components as dcc
import dash_html_components as html

import src.utils.constants as cte
import pandas as pd
import psycopg2
import yaml

app = dash.Dash()

with open(cte.CREDENTIALS, 'r') as f:
  config = yaml.safe_load(f)

credentials = config['db']
user = credentials['user']
password = credentials['pass']
database = credentials['database']
host = credentials['host']

conn = psycopg2.connect(
    dbname=database,
    user=user,
    host=host,
    password=password
)

cur = conn.cursor()
cur.execute(
    """ SELECT *
        FROM monitor.predictions
    """
)
rows = cur.fetchall()
df = pd.DataFrame(rows)
df.columns = [desc[0] for desc in cur.description]

print(df.head())
modelo = df.iloc[0,8]

df1= pd.DataFrame(df.value_counts(['inspection_type', 'score']))
df1= df1.reset_index()
df1['inspections'] = df1.iloc[:,2]

df1_1 = df1.loc[df1.score == 1]
df1_0= df1.loc[df1.score == 0]


colors = {
    'background': '#111111',
    'text': '#7FDBFF',
    'divbackground' : '#273746'
}

app.layout = html.Div(style={'backgroundColor': colors['divbackground']}, children=[
    html.H1(
        children='Model Monitoring ',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),
    
      html.Div(children=modelo, style={
        'textAlign': 'center',
        'color': colors['text']
    }),
    
        
    html.Div(children='Scores', style={
        'textAlign': 'left',
        'color': colors['text']
    }),
    
    dcc.Graph(
        id='histograma_scores',
        figure={
            'data': [
                {
                    'x': df['pred_score'],
#                     'text': df['facility_type'],
#                     'customdata': df['facility_type'],
                    'name': 'Scores',
                    'type': 'histogram'
                },

            ],
            'layout': {
                'title': 'Distribution of predicted scores',
                'legendPosition': True ,
                'plot_bgcolor': colors['background'],
                'paper_bgcolor': colors['background'],
                'font': {
                    'color': colors['text']
                }}
        }
    ),
    
     html.Div(children='Results by Inspection type', style={
        'textAlign': 'left',
        'color': colors['text']
    }),
    
dcc.Graph(
        id='Resultsby Inspection type',
        figure={
            'data': [
                {'x': df1_1['inspection_type'], 
                 'y': df1_1['inspections'], 
                 'type': 'bar', 
                 'name': 'Not pass inspection'},
                
                {'x':  df1_0['inspection_type'], 
                 'y': df1_0['inspections'], 
                 'type': 'bar', 
                 'name': u'Pass inspection'} ,
            ],
                
       
            'layout': {
                 'title': ' Results by Inspection type',
                'plot_bgcolor': colors['background'],
                'paper_bgcolor': colors['background'],
                'font': {
                'color': colors['text']
                }
            }
        }
    )
    
    
    
] )    

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', debug=True, use_reloader=False)