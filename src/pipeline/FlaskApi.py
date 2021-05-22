from flask_sqlalchemy import SQLAlchemy
from flask_restplus import Api, Resource, fields

from src.utils.general import get_db_conn_sql_alchemy


# Connecting to db
db_conn_str = get_db_conn_sql_alchemy("conf/local/credentials.yaml")

# create flask app
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = db_conn_str
api = Api(app)

db = SQLAlchemy(app)


# Tabla deploy.mochup match_api
class Match(db.Model):
  __table_args__ = {'schema': 'deploy'}
  __tablename__ = 'mochup_match_api'

  id_establecimiento = db.Column(db.Integer, primary_key=True)
  nombre_establecimiento = db.Column(db.String)
  tipo_inspeccion = db.Column(db.String)
  tipo_establecimiento = db.Column(db.String)
  prediccion = db.Column(db.Integer)
  fecha_prediccion = db.Column(db.Date)

  def __repr__(self):
    return(u'<{self.__clas__.__name__}: {self.id}>'.format{self=self})


model = api.model("prediccion_match_establecimiento", {
  "nombre_establecimiento": fields.String,
  "tipo_inspeccion": fields.String,
  "tipo_establecimiento": fields.String,
  "prediccion": fields.Integer,
  "fecha_prediccion": fields.Date
})

model_list = api.model('prediccion_match_output', {
  "id_establecimiento": fields.String,
  "establecimiento": fields.Nested(model)
})

# endpoints
@api.route('/predictions/<string:id_establecimiento>')
class Predictions(Resource):
  @api.marshal_with(model_list, as_list=True)
  def get(self, id_establecimiento):
    match = Match.query.filter_by(id_establecimiento=id_establecimiento).\
                  order_by(Match.fecha_prediccion.desc()).\
                  limit(10).\
                  all()
    inspecciones = []
    for element in match:
      inspecciones.append({
        "nombre_establecimiento": element.nombre_establecimiento,
        "tipo_inspeccion": element.tipo_inspeccion,
        "tipo_establecimiento": element.tipo_establecimiento,
        "prediccion": element.prediccion,
        "fecha_prediccion": element.fecha_prediccion
      })

    return {"id_establecimiento": id_establecimiento, 
            "inspecciones": inspecciones
            }

id __name__ = "__main__":
  app.run(debug=True)
