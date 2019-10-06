from sqlalchemy import text
from utils.orm import db
from utils.logger import logger as LOG


class PlanNode:
    def __init__(self, dim_list):
        self.child_list = []
        self.dim_list = dim_list

    def get_dimensions(self):
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


class CubeBuildPlan:
    def __init__(self, cube_id):
        """
        Init object by Cube Id
        :param cube_id:
        """
        self._cube_id = cube_id
        self._group_relation_dict = {}
        self._cube_hash_buffer = []

    def _level_build(self, plan_node):
        """
        N -> N-1 -> N-2 .... 1 level build the plan
        :param plan_node:
        :return:
        """
        # have reach the deepest level
        if plan_node.get_dim_length == 0:
            return plan_node

        for child_dim_list in plan_node.get_combine_list():
            # have own this dimension combination in the plan
            hash_code = hash(str(sorted(child_dim_list)))
            if hash_code in self._cube_hash_buffer:
                continue
            # a new combine
            else:
                LOG.info("CubeId: %s, PlanNode: %s", self._cube_id, child_dim_list)
                self._cube_hash_buffer.append(hash_code)
                # deep search child_node
                child_node = self._level_build(plan_node = PlanNode(dim_list=child_dim_list))
                plan_node.add_child(child_node)

        return plan_node

    def _fast_cube_plan_group(self):
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
        self._build_plan = self._level_build(plan_node=root_node)

    def get_plan(self):
        self._fast_cube_plan_group()
        return self._build_plan
