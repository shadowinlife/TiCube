from sqlalchemy import text
from utils.orm import db
from utils.logger import logger as LOG


class PlanNode:
    """
    DAG plan node
    """

    def __init__(self, dim_list, parent_node=None):
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


class CubeBuildPlan:
    def __init__(self, cube_id):
        """
        Init object by Cube Id
        :param cube_id:
        """
        self._cube_id = cube_id
        self._group_relation_dict = {}
        self._cube_hash_buffer = []

    def _level_build(self, root_node):
        """
        N -> N-1 -> N-2 .... 1 level build the plan
        :param root_node:
        :return:
        """

        # have reach the deepest level
        if root_node.get_dim_length == 0:
            return root_node

        for child_dim_list in root_node.get_combine_list():
            # have own this dimension combination in the plan
            hash_code = hash(str(sorted(child_dim_list)))
            if hash_code in self._cube_hash_buffer:
                continue
            # a new combine
            else:
                LOG.info("CubeId: %s, PlanNode: %s", self._cube_id, child_dim_list)
                self._cube_hash_buffer.append(hash_code)
                # deep search child_node
                child_node = self._level_build(root_node=PlanNode(dim_list=child_dim_list, parent_node=root_node))
                root_node.add_child(child_node)

        return root_node

    def _fast_cube(self):
        """
        Build the root node, which own every leaf dimension
        :return:
        """
        # find all the leaf
        _LEAF_DIM_SQL = '''
        SELECT 
            id, groupId
        FROM TiCube.dimension 
            WHERE cubeId={0} 
                AND 
            id NOT IN 
                (SELECT parentId FROM TiCube.dimension WHERE cubeId={0} AND parentId IS NOT NULL)
            ORDER BY id
        '''.format(self._cube_id)
        rs = db.engine.execute(text(_LEAF_DIM_SQL))

        # build the root node dimension list
        root_dim_list = []
        for row in rs:
            # no group relation for this dimension
            if row['groupId'] is None:
                root_dim_list.append(row['id'])

            # the lowest id dimension for this group
            elif row['groupId'] not in self._group_relation_dict:
                root_dim_list.append(row['id'])
                self._group_relation_dict[row['groupId']] = [row['id']]

            # add dimension to its group
            else:
                self._group_relation_dict[row['groupId']].append(row['id'])
        LOG.info("CubeId: %s, Root Dimensions: %s", self._cube_id, root_dim_list)
        root_node = PlanNode(dim_list=root_dim_list)

        # build the origin plan
        self._build_plan = self._level_build(root_node=root_node)

    def _get_measure(self, plan_node):
        """
        Get the Metric SQL
        :return:
        """

        _MEASURE_SQL = '''
               SELECT 
                   col, action
               FROM TiCube.measure 
                   WHERE cubeId={0} 
        '''.format(self._cube_id)

        rs = db.engine.execute(text(_MEASURE_SQL))

        # return the measure col and alias
        if plan_node.get_parent_node() is None:
            for row in rs:
                yield '{0}({1}) AS {0}_{1}'.format(row['action'], row['col'])
        else:
            for row in rs:
                yield '{0}_{1}'.format(row['action'], row['col'])

    def _get_dimension(self, plan_node):

        """
        Get the Dimension SQL
        :return:
        """
        _DIMENSION_SQL = '''SELECT id, `table`, col, func FROM TiCube.dimension WHERE cubeId={0} AND id IN ({1})'''.format(self._cube_id, ','.join([str(dim_id) for dim_id in plan_node.get_dim()]))

        rs = db.engine.execute(text(_DIMENSION_SQL))

        # return the measure col and alias
        dimension_list = []
        for row in rs:
            if row['func'] is None:
                yield '{0}.{1} AS {2}'.format(row['table'], row['col'], (row['table'] + '_' +  row['col']).replace('.', '_'))
            else:
                yield '{2}({0}.{1}) AS {3}'.format(row['table'], row['col'], row['func'], (row['table'] + '_' + row['col'] + '_' + row['func']).replace('.', '_'))


    def _get_table(self, plan_node):
        """
        return the table used in the plan
        :param dim_list:
        :param parent_node:
        :return:
        """

        if plan_node.get_parent_node() is None:
            table_name = None
            fk_list = []

            _rs = db.engine.execute(text('SELECT `table` FROM TiCube.cube WHERE id={}'.format(self._cube_id)))
            for row in _rs:
                table_name = row['table']

            _rs = db.engine.execute(text('SELECT DISTINCT `table` FROM TiCube.dimension WHERE cubeId={}'.format(self._cube_id)))
            for row in _rs:
                fk_list.append(row['table'])

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
            '''.format(table_name, table_name.split('.')[0], table_name.split('.')[1], ','.join(["'%s'"%fk.split('.')[1] for fk in fk_list]))
            LOG.info("CubeId: %s, FK SQL:%s", self._cube_id, FK_TABLE_SQL)

            _rs = db.engine.execute(text(FK_TABLE_SQL))
            current_fk = None

            # sql that create table for this plan node
            plan_table_sql = ''
            for row in _rs:
                if current_fk != row['fk_name']:
                    plan_table_sql += ' JOIN {1} ON {0}.{2}={1}.{3}'.format(table_name, row['fk_name'], row['col'], row['fk_col'])
                    current_fk = row['fk_name']
                else:
                    plan_table_sql += ' AND {0}.{2}={1}.{3}'.format(table_name, row['fk_name'], row['col'], row['fk_col'])

            return table_name + plan_table_sql

    def _plan_to_sql(self, plan_node):
        # build the sql
        # this is the root node
        measure_sql = ', '.join(self._get_measure(plan_node))
        LOG.info("CubeId: %s, Measure SQL:%s", self._cube_id, measure_sql)

        dimension_sql = ','.join(self._get_dimension(plan_node))
        LOG.info("CubeId: %s, Dimension SQL:%s", self._cube_id, dimension_sql)

        table_sql = self._get_table(plan_node)
        LOG.info("CubeId: %s, Table SQL:%s", self._cube_id, table_sql)

        plan_sql = 'SELECT {0}, {1} FROM {2} GROUP BY {3}'.format(dimension_sql, measure_sql, table_sql, ','.join(str(i) for i in range(1, plan_node.get_dim_length() + 1)))
        LOG.info("CubeId: %s, Plan SQL:%s", self._cube_id, plan_sql)

        plan_node.set_sql(plan_sql)

        # iterator all the child level sql
        for child_node in plan_node.child_list():
            self._plan_to_sql(child_node)

    def get_plan(self):
        # make the plan
        self._fast_cube()
        # generate sql from the plan node
        self._plan_to_sql(self._build_plan)
        return self._build_plan
