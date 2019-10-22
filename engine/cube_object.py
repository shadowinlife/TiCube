from sqlalchemy import text

from utils.orm import db
from utils.logger import logger as LOG


class PlanNodeUtils:
    def __init__(self, cube_id):
        """
        Tools to convert plan node to basic sqls
        :param cube_id:
        """
        self._cube_id = cube_id
        self._measure_cache = []
        self._dimension_cache = {}

    def get_measure(self, plan_node):
        """
        Get the Metric SQL
        :return:
        """

        # for root plan node , the measure is build from origin table
        if plan_node.get_parent_node() is None:
            _MEASURE_SQL = '''
                              SELECT 
                                  col, action
                              FROM TiCube.measure 
                                  WHERE cubeId={0} 
                       '''.format(self._cube_id)
            rs = db.engine.execute(text(_MEASURE_SQL))
            for row in rs:
                self._measure_cache.append('{0}_{1}'.format(row['action'], row['col']))
                yield '{0}({1}) AS {0}_{1}'.format(row['action'], row['col'])

        # for child plan node, the measure is a column in fact cube table
        else:
            for measure in self._measure_cache:
                yield measure

    def get_dimension(self, plan_node):

        """
        Get the Dimension SQL
        :return:
        """

        # for root plan node, it need to build all the dimensions from origin table
        if plan_node.get_parent_node() is None:
            _DIMENSION_SQL = '''SELECT id, `table`, col, func FROM TiCube.dimension WHERE cubeId={0} AND id IN ({1})'''.format(
                self._cube_id, ','.join([str(dim_id) for dim_id in plan_node.get_dim()]))
            rs = db.engine.execute(text(_DIMENSION_SQL))

            for row in rs:
                if row['func'] is None:
                    dimension_name = (row['table'] + '_' + row['col']).replace('.', '_')
                    self._dimension_cache[row['id']] = dimension_name
                    yield '{0}.{1} AS {2}'.format(row['table'], row['col'], dimension_name)
                else:
                    dimension_name = (row['table'] + '_' + row['col'] + '_' + row['func']).replace('.', '_')
                    self._dimension_cache[row['id']] = dimension_name
                    yield '{2}({0}.{1}) AS {3}'.format(row['table'], row['col'], row['func'], dimension_name)

        # for child plan node, the dimension is a column in fact cube table
        else:
            for dim_id in plan_node.get_dim():
                yield self._dimension_cache[dim_id]

    def get_table(self, plan_node):

        if plan_node.get_parent_node() is None:
            table_name = None
            fk_list = []

            # find the pivot table name
            _rs = db.engine.execute(text('SELECT `table` FROM TiCube.cube WHERE id={}'.format(self._cube_id)))
            for row in _rs:
                table_name = row['table']

            # find all the dimension table name
            _rs = db.engine.execute(
                text('SELECT DISTINCT `table` FROM TiCube.dimension WHERE cubeId={}'.format(self._cube_id)))
            for row in _rs:
                fk_list.append(row['table'])

            # find out the foreign key between pivot table and dimension
            FK_TABLE_SQL = '''
                       SELECT
                           REFERENCED_COLUMN_NAME AS fk_col,
                           COLUMN_NAME AS col,
                           CONCAT(REFERENCED_TABLE_SCHEMA, '.', REFERENCED_TABLE_NAME) AS fk_name
                       FROM
                           information_schema.key_column_usage
                       WHERE
                           TABLE_SCHEMA='{1}' AND TABLE_NAME='{2}' AND 
                           REFERENCED_TABLE_NAME IN ({3}) ORDER BY information_schema.key_column_usage.REFERENCED_TABLE_NAME
               '''.format(table_name, table_name.split('.')[0], table_name.split('.')[1],
                          ','.join(["'%s'" % fk.split('.')[1] for fk in fk_list]))
            LOG.info("CubeId: %s, FK SQL:%s", self._cube_id, FK_TABLE_SQL)

            _rs = db.engine.execute(text(FK_TABLE_SQL))

            # join the pivot and dimension to generate wide table
            current_fk = None
            plan_table_sql = ''
            for row in _rs:
                if current_fk != row['fk_name']:
                    plan_table_sql += ' JOIN {1} ON {0}.{2}={1}.{3}'.format(table_name, row['fk_name'], row['col'],
                                                                            row['fk_col'])
                    current_fk = row['fk_name']
                else:
                    plan_table_sql += ' AND {0}.{2}={1}.{3}'.format(table_name, row['fk_name'], row['col'],
                                                                    row['fk_col'])

            return table_name + plan_table_sql


class PlanNode:
    def __init__(self, dim_list, parent_node=None):
        """
        Used to generate a build tree in fast cube algorithm
        :param dim_list: the dimension id list
        :param parent_node: parent plan node
        """
        self.child_list = []
        self.dim_list = dim_list
        self.parent_node = parent_node

    def get_dim(self):
        return self.dim_list

    def get_children(self):
        return self.child_list

    def get_low_level_children(self):
        for link in self.child_list:
            yield link

    def add_child(self, plan_node):
        self.child_list.append(plan_node)

    def get_dim_length(self):
        return len(self.dim_list)

    def get_combine_list(self):
        for item in self.dim_list:
            child_combine = self.dim_list.copy()
            child_combine.remove(item)
            yield child_combine

    def get_parent_node(self):
        return self.parent_node

    def set_sql(self, sql_str):
        self.sql = sql_str

    def get_sql(self):
        return self.sql
