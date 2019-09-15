from flask import Blueprint, request, jsonify
from utils.error import ServiceError
from utils.logger import logger as LOG
from cube import service

bp = Blueprint('cube', __name__, url_prefix='/cube')


@bp.route('/list', methods=['GET', 'POST'])
def cube_list():
    """
    REF:
    :return:
    """
    if request.method == "GET":
        page_num = 0
        page_size = 15
        show_del = False
    elif request.method == "POST":
        page_num = request.json['page']
        page_size = request.json['size']
        show_del = request.json['show_del']
    return jsonify(service.list_cube(page_num, page_size, show_del))


@bp.route('/del', methods=['POST'])
def cube_del():
    """
    REF:
    :return:
    """
    cube_id = request.json['id']
    exec_status = service.del_cube(cube_id)
    return jsonify(exec_status)


@bp.route('/save', methods=['POST'])
def cube_add():
    """
    REF:
    :return:
    """
    cube_id = service.save_cube(name=request.json['name'], table=request.json['table'], desc= request.json['desc'])
    return jsonify(dict(cubeId=cube_id))


@bp.route('/dimension/list', methods=['POST'])
def dimension_list():
    """
    REF:
    :return:
    """
    cube_id = request.json['id']
    return jsonify(service.list_dimentions(cube_id))


@bp.route('/measure/list', methods=['POST'])
def measure_list():
    """
    REF:
    :return:
    """
    cube_id = request.json['id']
    return jsonify(service.list_measures(cube_id))


@bp.route('/dimension/add', methods=['POST'])
def dimension_add():
    """
    REF:
    :return:
    """
    cube_id = request.json['id']
    return jsonify(service.list_dimentions(cube_id))


@bp.route('/measure/add', methods=['POST'])
def measure_add():
    """
    REF:
    :return:
    """
    cube_id = request.json['id']
    return jsonify(service.list_dimentions(cube_id))
