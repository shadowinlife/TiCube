import datetime
from enum import Enum

from utils.orm import db, CreateUpdateMixin


class CubeStatus(Enum):
    EMPTY = 0
    READY = 1
    RUNNING = 2
    DELETED = 3
    ERROR = 4


class MeasureAction(Enum):
    SUM = 0
    AVG = 1
    COUNT = 2
    CountDistinct = 3
    FIRST = 4
    MAX = 5
    MIN = 6
    MEAN = 7


class Cube(CreateUpdateMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(100), nullable=False)
    status = db.Column(db.Enum(CubeStatus), nullable=False)
    table = db.Column(db.String(255), nullable=False)
    createdAt = db.Column(db.DateTime, default=datetime.datetime.utcnow, nullable=False)
    updatedAt = db.Column(db.DateTime, default=datetime.datetime.utcnow, nullable=False)
    measure = db.relationship('Measure', backref='cube', lazy=True)



class Measure(CreateUpdateMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    cubeId = db.Column(db.Integer, db.ForeignKey('cube.id'), nullable=False)
    col = db.Column(db.String(255), nullable=False)
    status = db.Column(db.Enum(MeasureAction), nullable=False)
    createdAt = db.Column(db.DateTime, default=datetime.datetime.utcnow, nullable=False)
    updatedAt = db.Column(db.DateTime, default=datetime.datetime.utcnow, nullable=False)

