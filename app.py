from logging.config import dictConfig
from flask import Flask, render_template, jsonify
from config import SQLALCHEMY_DATABASE_URI
from utils.error import ServiceError
from utils.orm import db
from cube import view

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(module)s %(lineno)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
app.register_blueprint(view.bp)

@app.cli.command('init_db')
def init_db():
    """
    With {flask init_db} to init the metadata schema
    :return:
    """
    db.create_all()

@app.cli.command('drop_db')
def init_db():
    """
    With {flask init_db} to init the metadata schema
    :return:
    """
    db.drop_all()

@app.errorhandler(ServiceError)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


if __name__ == '__main__':
    app.run(debug=True)
