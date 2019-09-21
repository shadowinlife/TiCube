import json
from datetime import datetime
from decimal import Decimal
from enum import Enum
from json import JSONEncoder
from sqlalchemy.ext.declarative import DeclarativeMeta
from flask_sqlalchemy import SQLAlchemy

# hold the db connection instance
db = SQLAlchemy()


# To serialize SQLalchemy objects
def row_to_dict(obj):
    if isinstance(obj.__class__, DeclarativeMeta):
        model_fields = {}
        for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata']:
            data = obj.__getattribute__(field)
            try:
                if isinstance(data, datetime):
                    model_fields[field] = data.strftime("%Y-%m-%d %H:%M:%S")
                elif isinstance(data, str):
                    model_fields[field] = data
                elif isinstance(data, int):
                    model_fields[field] = data
                elif isinstance(data, Enum):
                    model_fields[field] = str(data.name)
                elif isinstance(data, float) or isinstance(data, Decimal):
                    model_fields[field] = float(data)
            except Exception:
                continue
        return model_fields
    return None


class CreateUpdateMixin(object):
    def save(self):
        """
        add createdAt and updatedAt to the row by default
        :return:
        """
        self.createdAt = datetime.now()
        self.updatedAt = datetime.now()
        db.session.add(self)
        db.session.commit()
        return self

    def update(self, values):
        """
        change updatedAt by default
        :param values:
        :return:
        """
        for attr in self.__mapper__.columns.keys():
            if attr in values:
                setattr(self, attr, values[attr])
        self.updatedAt = datetime.now()
        return self

