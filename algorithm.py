import my_spy
import pprint
class Record(object):
    def __init__(self, trace_op):
        # field, op, req, inst, kind_str, point_set
        self.trace = trace_op
        self.grp_trace, self.grp_parent = self.group_trace(trace_op)
        self.field_trace, self.field_parent = self.group_by_field(self.grp_trace, self.grp_parent)
    
    def extract_item(self, item):
        # field, region_tree_id, task_id, point_set, parent_point_set
        field, op, req, inst, kind_str, point_set = item
        return field, req.logical_node.tree_id, op.index_owner.uid if op.index_owner != None else op.uid, \
            point_set, req.index_node.parent.point_set if req.index_node.parent is not None else point_set
    def extract_points(self):
        points = set()
        for item in self.trace:
            field, op, req, inst, kind_str, point_set = item
            for point in point_set.points:
                points.add(point)
        return points
    def print_trace(self):
        print("field, region_tree_id, task_id, point_set, parent_point_set")
        for item in self.trace:
            field, region_tree_id, task_id, point_set, parent_point_set = self.extract_item(item)
            print(f"{field}, {region_tree_id}, {task_id}, {point_set}, {parent_point_set}")
        points = self.extract_points()
        for point in points:
            print(f"{point}, {point.shape}")
    def group_trace(self, trace):
        grp_trace = {}
        grp_parent = {}
        for item in trace:
            field, region_tree_id, task_id, point_set, parent_point_set = self.extract_item(item)
            key = (field, region_tree_id, task_id)
            value_trace = point_set
            value_parent = parent_point_set
            if key in grp_trace.keys():
                grp_trace[key].append(value_trace)
                assert grp_parent[key] == value_parent
            else:
                grp_trace[key] = [value_trace]
                grp_parent[key] = value_parent
        return grp_trace, grp_parent
    def group_by_field(self, grp_trace, grp_parent):
        field_trace = {}
        field_parent = {}
        for key in grp_trace.keys():
            field, region_tree_id, task_id = key
            newkey = (field, region_tree_id)
            if newkey in field_trace.keys():
                field_trace[newkey].append(grp_trace[key])
                assert field_parent[newkey] == grp_parent[key]
            else:
                field_trace[newkey] = [grp_trace[key]]
                field_parent[newkey] = grp_parent[key]
        return field_trace, field_parent
    def eval_algo(self, algo):
        pprint.pprint(self.field_trace)
        pprint.pprint(self.field_parent)
        # for key in self.grp_trace.keys():
        #     trace_pointset_list = self.grp_trace[key]
        #     parent_point_set = self.grp_parent[key]

class Algo(object):
    def __init__(self):
        self.history_bvh = []
    @staticmethod
    def union_point_set(point_set_list):
        union = set()
        for point_set in point_set_list:
            for point in point_set:
                union.add(point)
        return union
    def generate_bvh(self, trace_points_list, parent_point_set):
        # trace_points_list: the list of smallest points that each point task access
        # parent_point_set: all points that the index launch accesses
        if len(self.history_bvh) == 0:
            if Algo.union_point_set(trace_points_list) == parent_point_set:
                self.history_bvh.append(trace_points_list)
            else:
                assert False
        return self.history_bvh[0]
