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

    def get_col_level(self):
        return hash('_'.join(str(dim_id) for dim_id in self.dim_list))

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
