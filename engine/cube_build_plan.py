from sqlalchemy import text

from engine.cube_plan_model import PlanNode
from utils.orm import db
from utils.logger import logger as LOG


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

    def get_basic_plan(self):
        # make the plan
        self._fast_cube()
        return self._build_plan

    def get_group_plan(self):
        # make the plan
        self._fast_cube()
        return self._build_plan

    def get_hierarchy_plan(self):
        # make the plan
        self._fast_cube()
        return self._build_plan
