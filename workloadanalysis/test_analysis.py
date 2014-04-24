from query_analysis import check_child_matches
from query_analysis import get_hash


def test_subtree_comparison():
    tree = {
        'operator': 'foo',
        'filters': frozenset([1, 2, 3]),
        'columns': frozenset([10, 11, 12]),
        'children': []
    }

    # same
    matches = [{
        'operator': 'foo',
        'filters': frozenset([1, 2, 3]),
        'columns': frozenset([10, 11, 12]),
        'children': []
    }]
    assert check_child_matches(tree, matches)

    # more cols
    matches = [{
        'operator': 'foo',
        'filters': frozenset([1, 2, 3]),
        'columns': frozenset([10, 11, 12, 13]),
        'children': []
    }]
    assert check_child_matches(tree, matches)

    # fewer filters
    matches = [{
        'operator': 'foo',
        'filters': frozenset([1, 2]),
        'columns': frozenset([10, 11, 12]),
        'children': []
    }]
    assert check_child_matches(tree, matches)

    # fewer cols
    matches = [{
        'operator': 'foo',
        'filters': frozenset([1, 2, 3]),
        'columns': frozenset([10, 11]),
        'children': []
    }]
    assert not check_child_matches(tree, matches)

    # more filters
    matches = [{
        'operator': 'foo',
        'filters': frozenset([1, 2, 3, 4]),
        'columns': frozenset([10, 11, 12]),
        'children': []
    }]
    assert not check_child_matches(tree, matches)

    # different operator
    matches = [{
        'operator': 'bar',
        'filters': frozenset([1, 2, 3]),
        'columns': frozenset([10, 11, 12]),
        'children': []
    }]
    assert not check_child_matches(tree, matches)

    # only one match
    matches = [{
        'operator': 'bar',
        'filters': frozenset([1, 2, 3]),
        'columns': frozenset([10, 11, 12]),
        'children': []
    },
    {
        'operator': 'foo',
        'filters': frozenset([1, 2, 3]),
        'columns': frozenset([10, 11, 12]),
        'children': []
    },
    {
        'operator': 'bar',
        'filters': frozenset([1, 2, 3]),
        'columns': frozenset([10, 11, 12]),
        'children': []
    }]
    assert check_child_matches(tree, matches)

def test_comparison_children():
    tree = {
        'operator': 'foo',
        'filters': frozenset([1, 2, 3]),
        'columns': frozenset([10, 11, 12]),
        'children': [{
            'operator': 'bar',
            'filters': frozenset([5, 6]),
            'columns': frozenset([14, 15]),
            'children': [{
                'operator': 'foobar',
                'filters': frozenset([42]),
                'columns': frozenset([19, 20]),
                'children': []
            }]
        }, {
            'operator': 'baz',
            'filters': frozenset([8, 9]),
            'columns': frozenset([16, 17]),
            'children': []
        }]
    }

    # same
    matches = [{
        'operator': 'foo',
        'filters': frozenset([1, 2, 3]),
        'columns': frozenset([10, 11, 12]),
        'children': [{
            'operator': 'bar',
            'filters': frozenset([5, 6]),
            'columns': frozenset([14, 15]),
            'children': [{
                'operator': 'foobar',
                'filters': frozenset([42]),
                'columns': frozenset([19, 20]),
                'children': []
            }]
        }, {
            'operator': 'baz',
            'filters': frozenset([8, 9]),
            'columns': frozenset([16, 17]),
            'children': []
        }]
    }]
    assert check_child_matches(tree, matches)

    # correct sets
    matches = [{
        'operator': 'foo',
        'filters': frozenset([1, 2]),
        'columns': frozenset([10, 11, 12, 13]),
        'children': [{
            'operator': 'bar',
            'filters': frozenset([5]),
            'columns': frozenset([14, 15, 16]),
            'children': [{
                'operator': 'foobar',
                'filters': frozenset([]),
                'columns': frozenset([19, 20, 21]),
                'children': []
            }]
        }, {
            'operator': 'baz',
            'filters': frozenset([8]),
            'columns': frozenset([16, 17, 22]),
            'children': []
        }]
    }]
    assert check_child_matches(tree, matches)

    print "test"

    # error is foobar
    matches = [{
        'operator': 'foo',
        'filters': frozenset([1, 2, 3]),
        'columns': frozenset([10, 11, 12]),
        'children': [{
            'operator': 'bar',
            'filters': frozenset([5, 6]),
            'columns': frozenset([14, 15]),
            'children': [{
                'operator': 'foobar',
                'filters': frozenset([42]),
                'columns': frozenset([19]),  # here
                'children': []
            }]
        }, {
            'operator': 'baz',
            'filters': frozenset([8, 9]),
            'columns': frozenset([16, 17]),
            'children': []
        }]
    }]
    assert not check_child_matches(tree, matches)


def test_same():
    a = {'operator': u'Inner Join', 'total': 0.0131481, 'filters': frozenset(['"fGetNearbyObjEq.objID"']), 'columns': frozenset([]), 'children': [{'operator': u'Inner Join', 'total': 0.00985909, 'filters': frozenset(['"SpecObjAll.specObjID"']), 'columns': frozenset([]), 'children': [{'operator': u'Inner Join', 'total': 0.00657181, 'filters': frozenset(['"fGetNearbyObjEq.objID"']), 'columns': frozenset([]), 'children': [{'operator': u'Table Scan', 'total': 0.0032831, 'filters': frozenset([]), 'columns': frozenset([u'fGetNearbyObjEq.objID']), 'children': []}, {'operator': u'Index Seek', 'total': 0.00328317, 'filters': frozenset(['"myskyserver.dbo.specobjall.specclass=3 or myskyserver.dbo.specobjall.specclass=4"', '"myskyserver.dbo.specobjall.scienceprimary=1"', '"myskyserver.dbo.fgetnearbyobjeq.objid"']), 'columns': frozenset([u'SpecObjAll.specObjID']), 'children': []}]}, {'operator': u'Clustered Index Seek', 'total': 0.0032831, 'filters': frozenset(['"myskyserver.dbo.specobjall.z<5.000000000000000e-001"', '"myskyserver.dbo.specobjall.specobjid"', '"myskyserver.dbo.specobjall.z>0.000000000000000e+000"']), 'columns': frozenset([u'SpecObjAll.z']), 'children': []}]}, {'operator': u'Clustered Index Seek', 'total': 0.0032831, 'filters': frozenset(['"myskyserver.dbo.photoobjall.mode=1 or myskyserver.dbo.photoobjall.mode=2"', '"myskyserver.dbo.fgetnearbyobjeq.objid"']), 'columns': frozenset([u'PhotoObjAll.dec', u'PhotoObjAll.objID', u'PhotoObjAll.ra']), 'children': []}]}
    b = {'operator': u'Inner Join', 'total': 0.0131481, 'filters': frozenset(['"fGetNearbyObjEq.objID"']), 'columns': frozenset([]), 'children': [{'operator': u'Inner Join', 'total': 0.00985909, 'filters': frozenset(['"SpecObjAll.specObjID"']), 'columns': frozenset([]), 'children': [{'operator': u'Inner Join', 'total': 0.00657181, 'filters': frozenset(['"fGetNearbyObjEq.objID"']), 'columns': frozenset([]), 'children': [{'operator': u'Table Scan', 'total': 0.0032831, 'filters': frozenset([]), 'columns': frozenset([u'fGetNearbyObjEq.objID']), 'children': []}, {'operator': u'Index Seek', 'total': 0.00328317, 'filters': frozenset(['"myskyserver.dbo.specobjall.specclass=3 or myskyserver.dbo.specobjall.specclass=4"', '"myskyserver.dbo.specobjall.scienceprimary=1"', '"myskyserver.dbo.fgetnearbyobjeq.objid"']), 'columns': frozenset([u'SpecObjAll.specObjID']), 'children': []}]}, {'operator': u'Clustered Index Seek', 'total': 0.0032831, 'filters': frozenset(['"myskyserver.dbo.specobjall.z<5.000000000000000e-001"', '"myskyserver.dbo.specobjall.specobjid"', '"myskyserver.dbo.specobjall.z>0.000000000000000e+000"']), 'columns': frozenset([u'SpecObjAll.z']), 'children': []}]}, {'operator': u'Clustered Index Seek', 'total': 0.0032831, 'filters': frozenset(['"myskyserver.dbo.photoobjall.mode=1 or myskyserver.dbo.photoobjall.mode=2"', '"myskyserver.dbo.fgetnearbyobjeq.objid"']), 'columns': frozenset([u'PhotoObjAll.dec', u'PhotoObjAll.objID', u'PhotoObjAll.ra']), 'children': []}]}

    assert get_hash(a) == get_hash(b)
    assert check_child_matches(a, [b])
    assert check_child_matches(b, [a])
