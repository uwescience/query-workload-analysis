#!/usr/bin/env python
from collections import defaultdict

from lxml import etree as ET


# From http://homework.nwsnet.de/products/45be_remove-namespace-in-an-xml-document-using-elementtree
def remove_namespace(doc, namespace):
    """Remove namespace in the passed document in place."""
    namespace = u'{%s}' % (namespace)
    ns_len = len(namespace)
    for elem in doc.getiterator():
        if elem.tag.startswith(namespace):
            elem.tag = elem.tag[ns_len:]
    return doc


def remove_subnodes_named(tree, tag):
    """Remove subnodes with the given tag"""
    parent = tree.find('.//' + tag + '/..')
    while parent is not None:
        child = parent.find(tag)
        while child is not None:
            parent.remove(child)
            child = parent.find(tag)
        parent = tree.find('.//' + tag + '/..')


def print_rel_op_tags(root, depth=0):
    """Prints the relational operators in a hierarchical fashion"""
    if root.tag == 'RelOp':
        print "%s%s" % (' ' * depth, root.attrib['PhysicalOp'])
        depth += 1
    for child in root:
        print_rel_op_tags(child, depth)


def get_query_plans(tree, cost=False):
    """Returns a list of the query plans in the given XML tree

    :param cost: Show costs and operator configs
    :type cost: bool
    """
    qplans = tree.findall('.//QueryPlan')
    return [operator_tree(qplan, cost) for qplan in qplans]


def flatten(plan):
    """Flattens the plan list so that it is a single list of dicts"""
    if isinstance(plan, dict):
        return [plan]
    if len(plan) == 0:
        return plan
    return flatten(plan[0]) + flatten(plan[1:])


def operator_tree(root, cost):
    """Returns a tree as a list of plan dictionaries, stored recursively. Each
    node has a value (its name) and a list of children, possibly empty."""
    if root is None or root == []:
        return None
    children = [operator_tree(child, cost) for child in root]
    children = [x for x in children if x is not None]
    children = flatten(children)

    if root.tag == 'RelOp':
        tables = defaultdict(list)
        refs = root.xpath('.//ColumnReference')
        not_ref = root.xpath('.//RelOp//ColumnReference')
        for ref in set(refs) - set(not_ref):
            if 'Table' in ref.attrib:
                name = ref.attrib['Table'].strip('[').strip(']')
                tables[name].append(ref.attrib['Column'])
        ret = {
            'operator': root.attrib['PhysicalOp'],
            'children': children,
            'columns': tables
        }
        if cost:
            ret.update({
                'cpu': float(root.attrib['EstimateCPU']),
                'io': float(root.attrib['EstimateIO']),
                'total': float(root.attrib['EstimatedTotalSubtreeCost']),
                'numRows': float(root.attrib['EstimateRows']),
                'rowSize': float(root.attrib['AvgRowSize']),
            })
        return ret

    if len(children) == 1:
        children = children[0]

    return children


def indent(elem, level=0):
    """pretty indent from http://effbot.org/zone/element-lib.htm#prettyprint"""
    i = "\n" + level * "  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for elem in elem:
            indent(elem, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i


def get_hash(root, t):
    stmt = root.xpath('.//StmtSimple')[0]
    if t == 'query':
        return stmt.attrib['QueryHash']
    elif t == 'plan':
        return stmt.attrib['QueryPlanHash']


def clean(xml_string):
    XMLNS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"

    full_tree = ET.fromstring(xml_string)
    tree = remove_namespace(ET.ElementTree(full_tree), XMLNS)

    remove_subnodes_named(tree, 'UDF')
    remove_subnodes_named(tree, 'OutputList')

    return tree
