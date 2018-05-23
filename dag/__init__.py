from copy import copy, deepcopy
from collections import deque

from . import six_subset as six

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict


class DAGValidationError(Exception):
    pass


class DAG(object):
    """ Directed acyclic graph implementation. """

    def __init__(self):
        """ Construct a new DAG with no nodes or edges. """
        self.reset_graph()

    def __str__(self):
        """
        String representation of DAG
        """
        return "DAG(" + repr(self.graph) + ")"

    def add_node(self, node_name, graph=None):
        """ Add a node if it does not exist yet, or error out. """
        if not graph:
            graph = self.graph
        if node_name in graph:
            raise KeyError('node %s already exists' % node_name)
        graph[node_name] = set()

    def add_node_if_not_exists(self, node_name, graph=None):
        try:
            self.add_node(node_name, graph=graph)
        except KeyError:
            pass

    def delete_node(self, node_name, graph=None):
        """ Deletes this node and all edges referencing it. """
        if not graph:
            graph = self.graph
        if node_name not in graph:
            raise KeyError('node %s does not exist' % node_name)
        graph.pop(node_name)

        for node, edges in six.iteritems(graph):
            if node_name in edges:
                edges.remove(node_name)

    def delete_node_if_exists(self, node_name, graph=None):
        try:
            self.delete_node(node_name, graph=graph)
        except KeyError:
            pass

    def add_edge(self, ind_node, dep_node, graph=None, dependency_type=None):
        """
        Add an edge (dependency) between the specified nodes.
        Optionally, an edge label can be supplied with a keyword argument:
          dependency_type='something'
        """
        if not graph:
            graph = self.graph
        if ind_node not in graph or dep_node not in graph:
            raise KeyError('one or more nodes do not exist in graph')
        test_graph = deepcopy(graph)
        test_graph[ind_node].add((dep_node, dependency_type))
        is_valid, message = self.validate(test_graph)
        if is_valid:
            graph[ind_node].add((dep_node, dependency_type))
        else:
            msg = "ERROR: Egde from {} to {} would create a cycle!".format(ind_node,dep_node)
            raise DAGValidationError(msg)

    def delete_edge(self, ind_node, dep_node, graph=None, dependency_type=None):
        """
        Delete an edge from the graph, optionally constrained to nodes with edge
        types matching dependency_type='something'.
        """
        if not graph:
            graph = self.graph
        if (dep_node, dependency_type) not in graph.get(ind_node, []):
            raise KeyError('this edge does not exist in graph')
        graph[ind_node].remove((dep_node, dependency_type))

    def rename_edges(self, old_node_name, new_node_name, graph=None):
        """ Change references to a node in existing edges. """
        if not graph:
            graph = self.graph
        for node, edges in graph.items():

            if node == old_node_name:
                graph[new_node_name] = copy(edges)
                del graph[old_node_name]

            else:
                if old_node_name in edges:
                    edges.remove(old_node_name)
                    edges.add(new_node_name)
                tuple_edges = [e for e in edges if isinstance(e, tuple)]
                if old_node_name in list(zip(*tuple_edges))[0]:
                    for edge in [e for e in tuple_edges if e[0] == old_node_name]:
                        edge_type = edge[1]
                        edge = (new_node_name, edge_type)

    def predecessors(self, node, graph=None, dependency_type=None):
        """
        Returns a list of all predecessors of the given node, optionally
        constrained to nodes with edge types matching dependency_type='something'
        """
        if graph is None:
            graph = self.graph
        if dependency_type:
            return [key for key in graph if node in [e[0] for e in graph[key] if e[1] == dependency_type]]
        else:
            return [key for key in graph if node in [e[0] for e in graph[key]]]

    def downstream(self, node, graph=None, dependency_type=None):
        """ Returns a list of all nodes this node has edges towards. """
        if graph is None:
            graph = self.graph
        if node not in graph:
            raise KeyError('node %s is not in graph' % node[0])
        if dependency_type:
            return [edge[0] for edge in graph[node] if edge[1] == dependency_type]
        else:
            return list(set([edge[0] for edge in graph[node]]))

    def all_downstreams(self, node, graph=None, dependency_type=None):
        """
        Returns a list of all nodes ultimately downstream
        of the given node in the dependency graph, in
        topological order, optionally constrained to follow edge types matching
        dependency_type='something'.
        """
        if graph is None:
            graph = self.graph
        nodes = [node]
        nodes_seen = set()
        i = 0
        while i < len(nodes):
            downstreams = self.downstream(nodes[i], graph, dependency_type=dependency_type)
            for downstream_node in downstreams:
                if downstream_node not in nodes_seen:
                    nodes_seen.add(downstream_node)
                    nodes.append(downstream_node)
            i += 1
        return list(
            filter(
                lambda node: node in nodes_seen,
                self.topological_sort(graph=graph)
            )
        )

    def all_leaves(self, graph=None):
        """ Return a list of all leaves (nodes with no downstreams) """
        if graph is None:
            graph = self.graph
        return [key for key in graph if not [e[0] for e in graph[key]]]

    def from_dict(self, graph_dict):
        """ Reset the graph and build it from the passed dictionary.

        The dictionary takes the form of {node_name: [directed edges]}

        directed edges are represented as 2-tuples in the form of
          ('dependent node', 'edge label')
        when edges are labeled to support multigraph DAG edges.

        directed edges can be any hashable (immutable) object otherwise
        """

        self.reset_graph()
        for new_node in six.iterkeys(graph_dict):
            self.add_node(new_node)
        for ind_node, dep_nodes in six.iteritems(graph_dict):
            if not isinstance(dep_nodes, list):
                raise TypeError('dict values must be lists')
            for dep_node in dep_nodes:
                if isinstance(dep_node, tuple) and len(dep_node) == 2:
                    self.add_edge(ind_node, dep_node[0], dependency_type=dep_node[1])
                if isinstance(dep_node, tuple) and len(dep_node) > 2:
                    raise ValueError("%s is not a tuple in the form of %s" %
                        repr(dep_node), "('dependent node', 'edge label')")
                else:
                    self.add_edge(ind_node, str(dep_node))

    def reset_graph(self):
        """ Restore the graph to an empty state. """
        self.graph = OrderedDict()

    def ind_nodes(self, graph=None):
        """ Returns a list of all nodes in the graph with no dependencies. """
        if graph is None:
            graph = self.graph

        dependent_nodes = set(
            edge[0] for dependents in six.itervalues(graph) for edge in dependents
        )
        return [node for node in graph.keys() if node not in dependent_nodes]

    def validate(self, graph=None):
        """ Returns (Boolean, message) of whether DAG is valid. """
        graph = graph if graph is not None else self.graph
        if len(self.ind_nodes(graph)) == 0:
            return (False, 'no independent nodes detected')
        try:
            self.topological_sort(graph)
        except ValueError:
            return (False, 'failed topological sort')
        return (True, 'valid')

    def topological_sort(self, graph=None):
        """ Returns a topological ordering of the DAG.

        Raises an error if this is not possible (graph is not valid).
        """
        if graph is None:
            graph = self.graph

        in_degree = {}
        for u in graph:
            in_degree[u] = 0

        for u in graph:
            for v in graph[u]:
                in_degree[v[0]] += 1

        queue = deque()
        for u in in_degree:
            if in_degree[u] == 0:
                queue.appendleft(u)

        l = []
        while queue:
            u = queue.pop()
            l.append(u)
            for v in graph[u]:
                in_degree[v[0]] -= 1
                if in_degree[v[0]] == 0:
                    queue.appendleft(v[0])

        if len(l) == len(graph):
            return l
        else:
            raise ValueError('graph is not acyclic')

    def size(self):
        return len(self.graph)
