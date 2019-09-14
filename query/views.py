from flask import Blueprint, render_template, request

bp = Blueprint('query', __name__, url_prefix='/query')

@bp.route('/', methods=('GET'))
def list():
    if request.method == 'GET':
        return render_template("query.html")


@bp.route('/sql', methods=('POST'))
def sql():
    param = request.json
    sql_str = param['sql']

