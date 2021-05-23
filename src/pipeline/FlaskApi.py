from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from flask_restplus import Api, Resource, fields

from src.utils.general import get_db_conn_sql_alchemy


# Connecting to db
db_conn_str = get_db_conn_sql_alchemy("./conf/local/credentials.yaml")

# create flask app
app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = db_conn_str
api = Api(app)

db = SQLAlchemy(app)


# Tabla predict.predictions match_api
class Match(db.Model):
  __table_args__ = {'schema': 'predict'}
  __tablename__ = 'predictions'

  id = db.Column(db.Integer, primary_key=True)
  establecimiento = db.Column(db.String)
  inspection_type = db.Column(db.String)
  facility_type = db.Column(db.String)
  score = db.Column(db.Float)
  pred_score = db.Column(db.Float)
  fecha = db.Column(db.Date)

  def __repr__(self):
    return(u'<{self.__clas__.__name__}: {self.id}>'.format(self=self))


model = api.model("score_establecimiento", {
  "id": fields.Integer,
  "inspection_type": fields.String,
  "facility_type": fields.String,
  "score": fields.Float,
  "pred_score": fields.Float,
  "fecha": fields.Date
})

model_list = api.model('score_output', {
  "establecimiento": fields.String,
  "predicciones": fields.Nested(model)
})

# endpoints
@api.route("/predictions/<string:establecimiento>")
class Predictions(Resource):
  @api.marshal_with(model_list, as_list=True)
  def get(self, establecimiento):
    match = Match.query.filter_by(establecimiento=establecimiento).\
                  limit(10).\
                  all()
    predicciones = []
    for element in match:
      predicciones.append({
        "id": element.id,
        "inspection_type": element.inspection_type,
        "facility_type": element.facility_type,
        "score": element.score,
        "pred_score": element.pred_score,
        "fecha": element.fecha
      })

    return {"establecimiento": establecimiento, 
            "predicciones": predicciones
            }


model_date = api.model("score_date", {
  "id": fields.Integer,
  "establecimiento": fields.String,
  "inspection_type": fields.String,
  "facility_type": fields.String,
  "score": fields.Float,
  "pred_score": fields.Float,
})

model_date_list = api.model('score_date_output', {
  "fecha": fields.Date,
  "establecimientos": fields.Nested(model_date)
})

# endpoints
@api.route("/dates/<string:fecha>")
class Dates(Resource):
  @api.marshal_with(model_date_list, as_list=True)
  def get(self, fecha):
    match = Match.query.filter_by(fecha=fecha).\
                  all()
    establecimientos = []
    for element in match:
      establecimientos.append({
        "id": element.id,
        "establecimiento": element.establecimiento,
        "inspection_type": element.inspection_type,
        "facility_type": element.facility_type,
        "score": element.score,
        "pred_score": element.pred_score,
      })

    return {"fecha": fecha, 
            "establecimientos": establecimientos
            }

if __name__ == "__main__":
  app.run(host='0.0.0.0', debug=True)
