import my_spy
class Record(object):
    def __init__(self, trace_op):
        # field, op, req, inst, kind_str, point_set
        self.trace = trace_op
    def print_trace(self):
        points = set()
        print("Field, treeID, taskID, point_set, parent point set")
        for item in self.trace:
            field, op, req, inst, kind_str, point_set = item
            for point in point_set.points:
                points.add(point)
            print(f"{field}, {req.logical_node.tree_id}, " +
                f"{op.index_owner.uid if op.index_owner != None else op.uid}, "
                f"{point_set}, {req.index_node.parent.point_set if req.index_node.parent is not None else point_set}")
        for point in points:
            print(f"{point}, {point.shape}")

class Algo(object):
    def __init__(self):
        self.history_bvh = []
    def generate_bvh(self, list_point_set, all_point_set, fieldID, treeID, taskID):
        # list_point_set: the list of smallest points that each point task access
        # all_point_set: all points that the index launch accesses
        pass