from query_analysis import check_child_matches


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
