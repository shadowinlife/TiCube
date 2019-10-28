from sqlalchemy import text

from cube.model import Cube, CubePlan
from engine.cube_build_plan import CubeBuildPlan
from engine.cube_utils import CubeUtils
from engine.cube_plan_model import PlanNode
from utils.logger import logger as LOG
from utils.orm import db


class CubeExecute:
    def __init__(self, cube_id):
        self._cube_id = cube_id
        self._cube_utils = CubeUtils(cube_id)

    def _plan_to_sql(self, plan_node: PlanNode):
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
        cube_build_sql = 'SELECT {0}, {1}, {2} AS ColLevel FROM {3} GROUP BY {4}'\
            .format(dimension_sql,
                    measure_sql,
                    plan_node.get_col_level(),
                    table_sql,
                    ','.join(str(i) for i in range(1, plan_node.get_dim_length() + 1)))
        LOG.info("CubeId: %s, Plan SQL:%s", self._cube_id, cube_build_sql)
        return cube_build_sql

    def init_cube_build_plan(self):
        # init the cube build object
        cube_build_plan = CubeBuildPlan(cube_id=self._cube_id)

        # generate the plan node tree
        root_plan_node = cube_build_plan.get_basic_plan()

        # save plan sql to meta db
        CubePlan.query.filter_by(cubeId=self._cube_id).delete()
        self._persist_plan(plan_node=root_plan_node)

        db.session.commit()

        return

    def init_cube_table(self):
        # generate the sql that create the cube table
        init_cube_sql = self._cube_utils.get_schema()

        # save the sql str to the table to debug
        cube_dao = Cube.query.get(self._cube_id)
        cube_dao.update({"sqlStr": init_cube_sql})
        db.session.commit()

        # create the table
        db.engine.execute(text(init_cube_sql))
        LOG.info('Cube Schenma: %s', init_cube_sql)

    def _persist_plan(self, plan_node: PlanNode, parent_id=None, ):
        sql_str = self._plan_to_sql(plan_node)
        cube_plan = CubePlan(sqlStr=sql_str, cubeId=self._cube_id, parentPlanId=parent_id)

        db.session.add(cube_plan)
        db.session.flush()
        cube_plan_id = cube_plan.id

        LOG.info("Persist Plan: %s", cube_plan_id)

        for child_node in plan_node.get_children():
            self._persist_plan(plan_node=child_node, parent_id=cube_plan_id)
        return
