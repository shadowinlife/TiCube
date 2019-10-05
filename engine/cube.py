from cube.model import Dimension, Cube


class CubePlan:
    def __init__(self, cube_id):
        """
        Init object by Cube Id
        :param cube_id:
        """
        self.cube_id = cube_id

    def fast_cube_plan(self):
        """
        fast cube build plan, same as Apache Kylin
        :return:
        """
        # scan dimensions, convert it to a struct structure
        rs = Dimension.query.filter_by(cubeId=self.cube_id).all()

        dim_struct ={}









