import uuid

from sqlalchemy import text
from cube.model import Cube, CubeStatus, Dimension, Measure, MeasureAction
from utils.orm import row_to_dict, db
from utils.logger import logger as LOG

_FK_SQL = '''
SELECT
    CONCAT(REFERENCED_TABLE_SCHEMA, '.', REFERENCED_TABLE_NAME) AS fk_table,
    REFERENCED_COLUMN_NAME AS fk_col,
    COLUMN_NAME AS col
FROM
    information_schema.key_column_usage
WHERE
    REFERENCED_TABLE_NAME IS NOT NULL AND TABLE_SCHEMA='{0}' AND TABLE_NAME='{1}'
'''

_SHOW_UNIQUE = '''
SHOW INDEXES FROM {} WHERE non_unique=0
'''

_DESC_SQL = '''
DESC {0}
'''


def _analyse_table(cube_id):
    """
    Analysis the cube, give out suggest dimensions and measures
    :param cube_id:
    :return:
    """
    cube = Cube.query.get(cube_id)
    # used to track foreign key  in the cube table
    fk_col = []
    # used to track the key in the foreign table
    fk_dim = {}

    # for not empy cube, just reutrn
    if cube.status != CubeStatus.EMPTY:
        return

    table_db = cube.table.split('.')[0]
    table_name = cube.table.split('.')[1]

    # search all the foreign key in this table, only trace one level
    rs = db.engine.execute(text(_FK_SQL.format(table_db, table_name)))

    # parser fk col
    for row in rs:
        fk_col.append(row['col'])
        # generator fk_table->[col1, col2, col3]
        if row['fk_table'] in fk_dim:
            fk_dim[row['fk_table']].append(row['fk_col'])
        else:
            fk_dim[row['fk_table']] = [row['fk_col']]

    # check the fk_table, column that used in the foreign key must be unique key
    for fk_table_name in fk_dim:
        rs = db.engine.execute(text(_SHOW_UNIQUE.format(fk_table_name)))

        # all the reference key must be the unique in the foreign table, to keep the snowflake structure
        unique_col_list = [row['Column_name'] for row in rs]
        if not (all(x in unique_col_list for x in fk_dim[fk_table_name])):
            LOG.info("{}:{} not fit fk", fk_table_name, fk_dim[fk_table_name])
            continue

        # all the
        rs = db.engine.execute(text(_DESC_SQL.format(fk_table_name)))
        for row in rs:
            db.session.add(Dimension(table=fk_table_name, cubeId=cube_id, col=row[0], colType=row[1]))

    # add non numberic column
    rs = db.engine.execute(text(_DESC_SQL.format(cube.table)))
    for row in rs:
        if row[0] in fk_col:
            # skip all the reference column
            continue
        elif 'int' in row[1] or 'float' in row[1] or 'decimal' in row[1] or 'double' in row[1]:
            # numeric col is used as measure, default action is summary
            db.session.add(Measure(action=MeasureAction.SUM, cubeId=cube_id, col=row[0], colType=row[1]))
        elif 'date' in row[1] or 'timestamp' in row[1]:
            # date filed can be extend to day->month->year
            db.session.add(Dimension(table=cube.table, cubeId=cube_id, col=row[0], colType=row[1], func="DATE"))
            db.session.add(Dimension(table=cube.table, cubeId=cube_id, col=row[0], colType=row[1], func="MONTH"))
            db.session.add(Dimension(table=cube.table, cubeId=cube_id, col=row[0], colType=row[1], func="YEAR"))
        else:
            # others used as dimension
            db.session.add(Dimension(table=cube.table, cubeId=cube_id, col=row[0], colType=row[1]))

    # add default row count to the measure
    db.session.add(Measure(action=MeasureAction.COUNT, cubeId=cube_id, col='1', colType='DEFAULT'))
    return


def list_cube(page_num, page_size, show_del):
    """
    Fetch cubes in the TiDB and paginator them
    :param page_num:
    :param page_size:
    :param show_del:
    :return: list of cubes
    """
    if show_del:
        rows = Cube.query.limit(page_size).offset(page_num * page_size)
    else:
        rows = Cube.query.filter(Cube.status != CubeStatus.DELETED).limit(page_size).offset(page_num * page_size)
    rs = [row_to_dict(row) for row in rows]
    return rs


def del_cube(cube_id):
    """
    Delete the cube, just mark it as deleted
    :param cube_id:
    :return:
    """
    try:
        cube = Cube.query.get(cube_id)
        cube.update(dict(status=CubeStatus.DELETED))
        return True
    except Exception as e:
        return False


def save_cube(name, table, desc):
    """
    Init a new cube, set the status to empty
    :param name:
    :param table:
    :return:
    """
    cube = Cube(name=name, table=table, desc=desc, status=CubeStatus.EMPTY)
    cube = cube.save()
    return cube.id


def list_dimentions(cube_id):
    """
    Fetch cube dimestion config
    - Suggest dimension for empty cube
        - If table own foreign key, use them as the dimension
        - If table own DATETIME / DATE / TIMESTAMP column, use them as dimension
        - If table own non numeric column, use them as dimension
    - Fetch manual config for ready cube
    :param cube_id:
    :return:
    """
    cube = Cube.query.get(cube_id)
    if cube.status == CubeStatus.EMPTY:
        _analyse_table(cube.id)
        cube.update(dict(status=CubeStatus.READY))
        db.session.commit()
    # for ready cube, just return the list
    rs = Dimension.query.filter_by(cubeId=cube_id).all()
    return [row_to_dict(row) for row in rs]


def list_measures(cube_id):
    """
    Fetch cube measure config
    - Suggest dimension and measures for empty cube
    - Fetch manual config for ready cube
    :param cube_id:
    :return:
    """
    cube = Cube.query.get(cube_id)
    if cube.status == CubeStatus.EMPTY:
        _analyse_table(cube.id)
        cube.update(status=CubeStatus.READY)
        db.session.commit()
    # for ready cube, just return the list
    rs = Measure.query.filter_by(cubeId=cube_id).all()
    return [row_to_dict(row) for row in rs]


def save_measure(cube_id, measure_list):
    """
    save user define measures
    - delete old config
    - save new config
    :param cube_id: cube id
    :param measure_list:
    :return:
    """
    Measure.query.filter_by(cubeId=cube_id).delete()
    for item in measure_list:
        db.session.add(
            Measure(action=item['action'], cubeId=cube_id, col=item['col'], colType=item['colType'], desc=item['desc']))
    db.session.commit()
    return True


def save_dimension(cube_id, measure_list):
    """
    save user define measures
    - delete old config
    - save new config
    :param cube_id: cube id
    :param measure_list:
    :return:
    """
    Dimension.query.filter_by(cubeId=cube_id).delete()
    for item in measure_list:
        db.session.add(
            Dimension(cubeId=cube_id, table=item['table'], col=item['col'], colType=item['colType'], func=item['func'],
                      desc=item['desc']))
    db.session.commit()
    return True


def save_dimension_struct(cube_id, dimension_struct):
    """
    persist the dimention relation in the database
    - group means that all the column in the group own one-to-one mapping
    - hierarchy means that column own subset-parent relation mapping

    :param cube_id:
    :param dimension_struct:
    :return:
    """

    # for group struct, mark all the column with unique group id
    if 'group' in dimension_struct:
        # iterator all the group
        for item_list in dimension_struct['group']:
            # init an unique group id for all the col in this group
            group_id = uuid.uuid1().int
            for dim_id in item_list:
                Dimension.query.get(dim_id).update(dict(groupId=group_id))

    # for hierarchy structure, document the parent relation at at database
    if 'hierarchy' in dimension_struct:
        for item_tuple in dimension_struct:
            Dimension.query.get(item_tuple[0]).update(dict(parentId=item_tuple[1]))

    db.session.commit()
