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
        # trace_pointset_list: list of [program access]; each access is a list of PointSet, representing concurrent point tasks
        cost_per_access = 1
        access_cost = 0
        for point_task_pset in trace_pointset_list:
            for bvh_pset in cur_bvh:
                if len(point_task_pset & bvh_pset) > 0:
                    access_cost += cost_per_access
        return access_cost
    @staticmethod
    def compute_contention_cost(trace_pointset_list, cur_bvh):
        cost_per_contention = 1
        contention_cost = 0
        contention_dict = {}
        for point_task_pset in trace_pointset_list:
            for i in range(len(cur_bvh)):
                bvh_pset = cur_bvh[i]
                if len(point_task_pset & bvh_pset) > 0:
                    contention_dict[i] = contention_dict.get(i, 0) + 1
        for key in contention_dict.keys():
            contention_cost += cost_per_contention * contention_dict[key] * contention_dict[key]
        # print(f"{trace_pointset_list}, {cur_bvh}, {contention_cost}")
        return contention_cost
    @staticmethod
    def compute_switch_cost(prev_bvh, cur_bvh):
        if prev_bvh == cur_bvh:
            return 0
        cost_per_switch = 10
        exchange_time = 0
        for prev_pset in prev_bvh:
            for cur_pset in cur_bvh:
                if len(prev_pset & cur_pset) > 0:
                    exchange_time += 1
        return exchange_time * cost_per_switch
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
    def find_optimal_bvh(self):
        best_record = {}
        for key in self.field_trace:
            min_total_cost = 1e20
            best_bvh = None
            trace_pointset_list_all = self.field_trace[key]
            parent_point_set = self.field_parent[key]
            # enumerate all possible disjoint&complete partitions
            all_bvh = self.generate_all_bvh(parent_point_set)
            # enumerate all possible orderings up to a given length (length of program trace)
            all_ordering_bvh = self.generate_all_ordering_bvh(all_bvh, len(trace_pointset_list_all))
            assert len(all_ordering_bvh) == pow(len(all_bvh), len(trace_pointset_list_all))
            for cur_ordering_bvh in all_ordering_bvh:
                access_cost = 0
                contention_cost = 0
                switch_cost = 0
                cur_total_cost = 0
                prev_bvh = None
                assert len(cur_ordering_bvh) == len(trace_pointset_list_all)
                for i in range(len(trace_pointset_list_all)):
                    trace_pointset_list = trace_pointset_list_all[i]
                    cur_bvh = cur_ordering_bvh[i]
                    access_cost += self.compute_access_cost(trace_pointset_list, cur_bvh)
                    contention_cost += self.compute_contention_cost(trace_pointset_list, cur_bvh)
                    if prev_bvh != None:
                        switch_cost += self.compute_switch_cost(prev_bvh, cur_bvh)
                    prev_bvh = cur_bvh
                    cur_total_cost += access_cost + contention_cost + switch_cost
                if cur_total_cost < min_total_cost:
                    min_total_cost = cur_total_cost
                    best_bvh = cur_ordering_bvh
            best_record[key] = (min_total_cost, best_bvh)
        pprint.pprint(best_record)

    @staticmethod
    def partition(collection):
        if len(collection) == 1:
            yield [ collection ]
            return

        first = collection[0]
        for smaller in Record.partition(collection[1:]):
            # insert `first` in each of the subpartition's subsets
            for n, subset in enumerate(smaller):
                yield smaller[:n] + [[ first ] + subset]  + smaller[n+1:]
            # put `first` in its own subset
            yield [ [ first ] ] + smaller

    @staticmethod
    def generate_all_bvh(point_set):
        all_res = []
        for each_partition in Record.partition(list(point_set.points)):
            point_sets = []
            for points in each_partition:
                point_set = my_spy.PointSet()
                point_set.points = points
                point_sets.append(point_set)
            all_res.append(point_sets)
        return all_res

    @staticmethod
    def generate_all_ordering_bvh(all_bvh, length):
        # todo: orderings of bvh
        pass

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
    @staticmethod
    def disjoint_point_sets(point_set_list):
        for i in range(len(point_set_list)):
            for j in range(i+1, len(point_set_list)):
                # has intersection
                if len(point_set_list[i] & point_set_list[j]) > 0:
                    return False
        return True
    def generate_bvh(self, trace_points_list, parent_point_set):
        # trace_points_list: the list of smallest points that each point task access
        # parent_point_set: all points that the index launch accesses
        if len(self.history_bvh) == 0:
            if Algo.union_point_set(trace_points_list) == parent_point_set.points and self.disjoint_point_sets(trace_points_list):
                self.history_bvh.append(trace_points_list)
            else:
                first_half = list(parent_point_set.points)[:len(parent_point_set.points)//2]
                second_half = list(parent_point_set.points)[len(parent_point_set.points)//2:]
                self.history_bvh.append([first_half, second_half])
        return self.history_bvh[-1]
    def clear(self):
        self.history_bvh = []

class Algo2(object):
    def __init__(self):
        self.history_bvh = []
    def generate_bvh(self, trace_points_list, parent_point_set):
        # trace_points_list: the list of smallest points that each point task access
        # parent_point_set: all points that the index launch accesses
        res = []
        for point in parent_point_set.points:
            a = my_spy.PointSet()
            a.add_point(point)
            res.append(a)
        return res
    def clear(self):
        self.history_bvh = []

if __name__ == "__main__":
    for item in Record.partition([1, 2, 3, 4, 5]):
        print(item)
