import json
from datetime import datetime
from flask import Blueprint, request, jsonify, current_app
from cube.model import Cube, CubeStatus
from utils.orm import row_to_dict
from utils.error import ServiceError
from utils.logger import logger as LOG
from cube import service

bp = Blueprint('cube', __name__, url_prefix='/cube')


@bp.route('/list', methods=['GET', 'POST'])
def cube_list():
    if request.method == "GET":
        page_num = 0
        page_size = 15
        show_del = False

    elif request.method == "POST":
        page_num = request.json['page']
        page_size = request.json['size']
        show_del = request.json['show_del']

    if show_del:
        rows = Cube.query.limit(page_size).offset(page_num * page_size)
    else:
        rows = Cube.query.filter(Cube.status != CubeStatus.DELETED).limit(page_size).offset(page_num * page_size)

    cubes = [row_to_dict(row) for row in rows]
    return jsonify(cubes)


@bp.route('/del', methods=['POST'])
def cube_del():
    try:
        cube_id = request.json['id']
        cube = Cube.query.get(cube_id)
        cube.update(dict(status=CubeStatus.DELETED, updatedAt=datetime.now()))
        return jsonify(True)
    except Exception as e:
        LOG.exception()
        raise ServiceError('Cube List Failed', status_code=501)


@bp.route('/add', methods=['POST'])
def cube_add():
    try:
        cube = Cube(name=request.json['name'], table=request.json['table'], status=CubeStatus.EMPTY)
        cube = cube.init()
        return jsonify(dict(cubeId=cube.id))
    except Exception:
        LOG.exception()
        raise ServiceError('Cube Save Failed', status_code=502)


@bp.route('/load', methods=['POST'])
def suggest():
    try:
        cube_id = request.json['id']
        cube = Cube.query.get(cube_id)
        if cube.status == CubeStatus.EMPTY:
            cube_config = service.suggest(Cube.query.get(cube_id).name)
        else:
            cube_config = None
        return jsonify(cube_config)
    except Exception as e:
        LOG.exception()
        raise ServiceError('Cube Load Failed', status_code=503)


@bp.route('/measues', methods=['GET', 'POST'])
def measure_add():
    if request.method == 'GET':
        try:
            return jsonify(True)
        except Exception as e:
            LOG.exception()
            raise ServiceError('Measure List Failed', status_code=504)
    else:
        try:
            return jsonify(True)
        except Exception as e:
            LOG.exception()
            raise ServiceError('Measure Save Failed', status_code=505)
