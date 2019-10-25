from sqlalchemy import text

from engine.cube_utils import CubeUtils
from engine.cube_plan_model import PlanNode
from utils.logger import logger as LOG
from utils.orm import db


class CubeExecute:
    def __init__(self, cube_id):
        self._cube_id = cube_id
        self._cube_utils = CubeUtils(cube_id)

    def _origin_table_sql(self, plan_node: PlanNode):
        # build the sql

        # find all the measures
        measure_sql = ', '.join(self._cube_utils.get_measure(plan_node))
        LOG.info("CubeId: %s, Measure SQL:%s", self._cube_id, measure_sql)

        # find all the dimensions
        dimension_sql = ','.join(self._cube_utils.get_dimension(plan_node))
        LOG.info("CubeId: %s, Dimension SQL:%s", self._cube_id, dimension_sql)

        # find the sql that can build the talbe
        table_sql = self._cube_utils.get_table(plan_node)
        LOG.info("CubeId: %s, Table SQL:%s", self._cube_id, table_sql)

        # generate the sql that can create origin fact table without aggragate
        table_build_sql = 'SELECT {0}, {1} FROM {2}'.format(dimension_sql, measure_sql, table_sql, ','.join(
            str(i) for i in range(1, plan_node.get_dim_length() + 1)))
        LOG.info("CubeId: %s, Plan SQL:%s", self._cube_id, table_build_sql)

        return table_build_sql

    def build_origin_table(self, spark_client):
        return

    def create_cube_schema(self):
        cube_table_schema = self._cube_utils.get_schema()
        db.engine.execute(text(cube_table_schema))
        LOG.info('Cube Schenma: %s', cube_table_schema)
