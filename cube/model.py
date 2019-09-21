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
    name = db.Column(db.String(100), nullable=False, comment="cube name")
    status = db.Column(db.Enum(CubeStatus), nullable=False)
    table = db.Column(db.String(255), nullable=False)
    desc = db.Column(db.String(255), nullable=True)
    createdAt = db.Column(db.DateTime, default=datetime.datetime.utcnow, nullable=False)
    updatedAt = db.Column(db.DateTime, default=datetime.datetime.utcnow, nullable=False)
    measure = db.relationship('Measure', backref='cube', lazy=True)
    dimension = db.relationship('Dimension', backref='cube', lazy=True)


class Measure(CreateUpdateMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    cubeId = db.Column(db.Integer, db.ForeignKey('cube.id'), nullable=False)
    col = db.Column(db.String(255), nullable=False, comment="measure col name")
    colType = db.Column(db.String(255), nullable=True, comment="measure col type")
    action = db.Column(db.Enum(MeasureAction), nullable=False, comment="group function on measure")
    desc = db.Column(db.String(255), nullable=True)
    createdAt = db.Column(db.DateTime, default=datetime.datetime.utcnow, nullable=True)
    updatedAt = db.Column(db.DateTime, default=datetime.datetime.utcnow, nullable=True)


class Dimension(CreateUpdateMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    cubeId = db.Column(db.Integer, db.ForeignKey('cube.id'), nullable=False)
    table = db.Column(db.String(255), nullable=False, comment="table name")
    col = db.Column(db.String(255), nullable=False, comment="table column name, cube table for foreign table")
    alias = db.Column(db.String(255), nullable=False, comment="dimension column alias")
    colType = db.Column(db.String(255), nullable=True, comment="dimension column type")
    func = db.Column(db.String(255), nullable=True, comment="function on dimension column")
    groupId = db.Column(db.BigInteger, nullable=True, comment="comment columns own same group id")
    parentId = db.Column(db.Integer, nullable=True, comment="subset point to parent set")
    desc = db.Column(db.String(255), nullable=True)
    createdAt = db.Column(db.DateTime, default=datetime.datetime.utcnow, nullable=True)
    updatedAt = db.Column(db.DateTime, default=datetime.datetime.utcnow, nullable=True)
