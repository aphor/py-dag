""" Tests on the DAG implementation """

from nose import with_setup
from nose.tools import nottest, raises
from dag import DAG, DAGValidationError

dag = None


@nottest
def blank_setup():
    global dag
    dag = DAG()


@nottest
def start_with_graph():
    global dag
    dag = DAG()
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['b'],
                   'd': []})


@with_setup(blank_setup)
def test_add_node():
    dag.add_node('a')
    assert dag.graph == {'a': set()}


@with_setup(blank_setup)
def test_add_edge():
    dag.add_node('a')
    dag.add_node('b')
    dag.add_edge('a', 'b')
    assert dag.graph == {'a': set([('b', None)]), 'b': set()}
    print(dag.graph['b'])
    assert ('b', None) in dag.graph['a']
    assert 'b' in dag.graph
    assert len(dag.graph['b']) == 0


@with_setup(blank_setup)
def test_from_dict():
    dag.from_dict({'a': ['b', 'c'],
                   'b': ['d'],
                   'c': ['d'],
                   'd': []})
    assert dag.graph == {'a': set([('b', None), ('c', None)]),
                         'b': set([('d',None)]),
                         'c': set([('d', None)]),
                         'd': set()}
    assert ('b',None) in dag.graph['a'] and ('c', None) in dag.graph['a']
    assert ('d', None) in dag.graph['b'] and len(dag.graph['b']) == 1
    assert ('d', None) in dag.graph['c'] and len(dag.graph['c']) == 1
    assert len(dag.graph['d']) == 0


@with_setup(blank_setup)
def test_reset_graph():
    dag.add_node('a')
    assert dag.graph == {'a': set()}
    dag.reset_graph()
    assert dag.graph == {}


@with_setup(start_with_graph)
def test_ind_nodes():
    assert list(dag.ind_nodes(dag.graph)) == ['a']


@with_setup(blank_setup)
def test_topological_sort():
    dag.from_dict({'a': [],
                   'b': ['a'],
                   'c': ['b']})
    assert list(dag.topological_sort()) == ['c', 'b', 'a']


@with_setup(start_with_graph)
def test_successful_validation():
    assert dag.validate()[0] is True


@raises(DAGValidationError)
@with_setup(blank_setup)
def test_failed_validation():
    dag.from_dict({'a': ['b'],
                   'b': ['a']})


@with_setup(start_with_graph)
def test_downstream():
    nodes_downstream_of_a = dag.downstream('a')
    print(repr(dag.graph['a']))
    assert 'b' in nodes_downstream_of_a
    assert 'c' in nodes_downstream_of_a
    assert len(nodes_downstream_of_a) == 2


@with_setup(start_with_graph)
def test_all_downstreams():
    assert list(dag.all_downstreams('a')) == ['c', 'b', 'd']
    assert list(dag.all_downstreams('b')) == ['d']
    assert list(dag.all_downstreams('d')) == []


@with_setup(start_with_graph)
def test_all_downstreams_pass_graph():
    dag2 = DAG()
    dag2.from_dict({'a': ['c'],
                    'b': ['d'],
                    'c': ['d'],
                    'd': []})
    all_nodes_downstream_of_a = list(dag.all_downstreams('a', dag2.graph))
    all_nodes_downstream_of_a.sort()
    assert all_nodes_downstream_of_a == ['c', 'd']
    assert list(dag.all_downstreams('b', dag2.graph)) == ['d']
    assert list(dag.all_downstreams('d', dag2.graph)) == []


@with_setup(start_with_graph)
def test_predecessors():
    assert set(dag.predecessors('a')) == set([])
    assert set(dag.predecessors('b')) == set(['a', 'c'])
    assert set(dag.predecessors('c')) == set(['a'])
    assert set(dag.predecessors('d')) == set(['b'])


@with_setup(start_with_graph)
def test_all_leaves():
    assert list(dag.all_leaves()) == ['d']


@with_setup(start_with_graph)
def test_size():
    assert dag.size() == 4
    dag.delete_node('a')
    assert dag.size() == 3
