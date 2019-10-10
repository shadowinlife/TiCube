from celery import Celery
from flask import Flask, jsonify
from config import SQLALCHEMY_DATABASE_URI, CELERY_BROKER
from utils.error import ServiceError
from utils.orm import db
from cube import view

# Init flask environment
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
app.register_blueprint(view.bp)

# Init celery environment
celery_app = Celery('tasks', broker=CELERY_BROKER)


@app.cli.command('init_db')
def init_db():
    """
    With {flask init_db} to init the metadata schema
    :return:
    """
    db.create_all()


@app.cli.command('drop_db')
def drop_db():
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
