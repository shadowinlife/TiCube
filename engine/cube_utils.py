from sqlalchemy import text
from utils.orm import db
from utils.logger import logger as LOG


class CubeUtils:
    def __init__(self, cube_id):
        """
        Tools to convert plan node to basic sqls
        :param cube_id:
        """
        self._cube_id = cube_id
        # find the pivot table name
        _rs = db.engine.execute(text('SELECT `table` FROM TiCube.cube WHERE id={}'.format(self._cube_id)))
        for row in _rs:
            self.table_name = row['table']

        # used to generate sql for plan node
        self._measure_cache = {}
        self._dimension_cache = {}

        # used to generate schema for fact table
        self._measure_col = {}
        self._dimension_col = {}

        self._build_meausre_name()
        self._build_dimension_name()



    def _build_meausre_name(self):
        """
        build the measure column name and relate sql
        :return:
        """
        _MEASURE_SQL = 'SELECT col, action, colType FROM TiCube.measure WHERE cubeId={0}'.format(self._cube_id)
        rs = db.engine.execute(text(_MEASURE_SQL))
        for row in rs:
            key = '{0}_{1}'.format(row['action'], row['col'])
            value = '{0}.{1} AS {2}'.format(self.table_name, row['col'], key)
            self._measure_cache[key] = value
            self._measure_col[key] = row['colType']

    def _build_dimension_name(self):
        """
        build dimension column name and relate sql
        :return:
        """
        _DIMENSION_SQL = 'SELECT id, `table`, col, func, colType FROM TiCube.dimension WHERE cubeId={0}'.format(
            self._cube_id)
        rs = db.engine.execute(text(_DIMENSION_SQL))

        for row in rs:
            # for column that do not need to run function
            if row['func'] is None:
                dimension_name = (row['table'] + '_' + row['col']).replace('.', '_')
                dimension_sql = '{0}.{1} AS {2}'.format(row['table'], row['col'], dimension_name)
            # for column that need function
            else:
                dimension_name = (row['table'] + '_' + row['col'] + '_' + row['func']).replace('.', '_')
                dimension_sql = '{2}({0}.{1}) AS {3}'.format(row['table'], row['col'], row['func'], dimension_name)
            self._dimension_cache[row['id']] = [dimension_name, dimension_sql]
            self._dimension_col[dimension_name] = row['colType']

    def get_measure(self, plan_node):
        """
        Get the Metric SQL
        :return:
        """

        # for root plan node , the measure is build from origin table
        if plan_node.get_parent_node() is None:
            for measure_sql in self._measure_cache.values():
                yield measure_sql

        # for child plan node, the measure is a column in fact cube table
        else:
            for measure_name in self._measure_cache.keys():
                yield measure_name

    def get_dimension(self, plan_node):

        """
        Get the Dimension SQL
        :return:
        """

        # for root plan node, it need to build all the dimensions from origin table
        if plan_node.get_parent_node() is None:
            for dim_id in plan_node.get_dim():
                yield self._dimension_cache[dim_id][1]

        # for child plan node, the dimension is a column in fact cube table
        else:
            for dim_id in plan_node.get_dim():
                yield self._dimension_cache[dim_id][0]

    def build_fact_table(self, plan_node):
        """
        build the sql that join the pivot table and dimension table
        :param plan_node:
        :return:
        """
        if plan_node.get_parent_node() is None:
            table_name = None
            fk_list = []

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

    def get_schema(self):
        """
        return the schema tuple of the cube
        :return:
        """
        measure_schema = ["{} {}".format(col_name, col_type) for col_name, col_type in self._measure_col.items()]
        dimension_schema = ["{} {}".format(col_name, col_type) for col_name, col_type in self._dimension_col.items()]

        table_create_schema = """
            CREATE TABLE IF NOT EXISTS TiCube.CUBE_{table_name} (
                ColLevel BIGINT NOT NULL,
                {measure_col},
                {dimension_col},
                INDEX({dimension_name})
            ) PARTITION BY HASH(ColLevel)
        """.format(
            table_name=self.table_name.replace('.', '_'),
            measure_col = ','.join(measure_schema),
            dimension_col = ','.join(dimension_schema),
            dimension_name = ','.join(self._dimension_col.keys())
        )
        return table_create_schema
