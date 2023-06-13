import my_spy
import pprint
class Record(object):
    def __init__(self, trace_op):
        # field, op, req, inst, kind_str, point_set
        self.trace = trace_op
        self.grp_trace, self.grp_parent = self.group_trace(trace_op)
        self.field_trace, self.field_parent = self.group_by_field(self.grp_trace, self.grp_parent)
    @staticmethod
    def extract_item(item):
        # field, region_tree_id, task_id, point_set, parent_point_set
        field, op, req, inst, kind_str, point_set = item
        return field, req.logical_node.tree_id, op.index_owner.uid if op.index_owner != None else op.uid, \
            point_set, req.parent.index_space.point_set
            # req.index_node.parent.point_set
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
                # same task launch (same task_id) should have the same parent index space for regions
                assert grp_parent[key] == value_parent, f"{key}: {grp_parent[key]} versus {value_parent}"
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
                # different task launch can have different index spaces for regions; union them
                field_parent[newkey] = field_parent[newkey] | grp_parent[key]
            else:
                field_trace[newkey] = [grp_trace[key]]
                field_parent[newkey] = grp_parent[key]
        return field_trace, field_parent
    @staticmethod
    def compute_access_cost(trace_pointset_list, cur_bvh):
        # trace_pointset_list: list of [program access]; each access is a list of PointSet, representing point tasks
        cost_per_access = 1
        access_cost = 0
        for point_task_pset in trace_pointset_list:
            for bvh_pset in cur_bvh:
                if len(point_task_pset & bvh_pset) > 0:
                    access_cost += cost_per_access
        return access_cost
    @staticmethod
    def compute_contention_cost(trace_pointset_list, cur_bvh):
        return 1
    @staticmethod
    def compute_switch_cost(prev_bvh, cur_bvh):
        return 1
    def eval_algo(self, algo):
        for key in self.field_trace:
            prev_bvh = None
            access_cost = 0
            contention_cost = 0
            switch_cost = 0
            trace_pointset_list_all = self.field_trace[key]
            parent_point_set = self.field_parent[key]
            for trace_pointset_list in trace_pointset_list_all:
                cur_bvh = algo.generate_bvh(trace_pointset_list, parent_point_set)
                print(f"{key}, access={trace_pointset_list}, bvh_view={cur_bvh}")
                access_cost += self.compute_access_cost(trace_pointset_list, cur_bvh)
                contention_cost += self.compute_contention_cost(trace_pointset_list, cur_bvh)
                if prev_bvh != None:
                    switch_cost += self.compute_switch_cost(prev_bvh, cur_bvh)
                prev_bvh = cur_bvh
            print(f"{key}, access = {access_cost}, contention = {contention_cost}, switch = {switch_cost}")
            # different field should have different analysis
            algo.clear()
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
            for point in point_set.points:
                union.add(point)
        return union
    def generate_bvh(self, trace_points_list, parent_point_set):
        # trace_points_list: the list of smallest points that each point task access
        # parent_point_set: all points that the index launch accesses
        if len(self.history_bvh) == 0:
            if Algo.union_point_set(trace_points_list) == parent_point_set.points:
                self.history_bvh.append(trace_points_list)
            else:
                first_half = list(parent_point_set.points)[:len(parent_point_set.points)//2]
                second_half = list(parent_point_set.points)[len(parent_point_set.points)//2:]
                self.history_bvh.append([first_half, second_half])
        return self.history_bvh[0]
    def clear(self):
        self.history_bvh = []
