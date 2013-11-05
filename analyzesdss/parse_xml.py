#!/usr/bin/env python

try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import json


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
    """Prints the relational operators in a hierachical fashion"""
    if root.tag == 'RelOp':
        print "%s%s" % (' ' * depth, root.attrib['PhysicalOp'])
        depth += 1
    for child in root:
        print_rel_op_tags(child, depth)


def get_query_plans(tree):
    """Returns a list of the query plans in the given XML tree"""
    qplans = tree.findall('.//QueryPlan')
    return [operator_tree(qplan) for qplan in qplans]


def flatten(plan):
    """Flattens the plan list so that it is a single list of dicts"""
    if isinstance(plan, dict):
        return [plan]
    if len(plan) == 0:
        return plan
    return flatten(plan[0]) + flatten(plan[1:])


def operator_tree(root):
    """Returns a tree as a list of plan dictionaries, stored recursively. Each
    node has a value (its name) and a list of children, possibly empty."""
    if root is None or root == []:
        return None
    children = [operator_tree(child) for child in root]
    children = [x for x in children if x is not None]
    children = flatten(children)

    if root.tag == 'RelOp':
        return {
            'value': root.attrib['PhysicalOp'],
            'children': children
        }

    if len(children) == 1:
        children = children[0]

    return children


def json_pretty(obj):
    """A simple wrapper for the json pretty string given in the json docs"""
    return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))


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


def clean(xml_string):
    XMLNS = "http://schemas.microsoft.com/sqlserver/2004/07/showplan"

    full_tree = ET.fromstring(xml_string)
    tree = remove_namespace(ET.ElementTree(full_tree), XMLNS)

    remove_subnodes_named(tree, 'UDF')
    remove_subnodes_named(tree, 'OutputList')

    print_rel_op_tags(tree.getroot())

    print json_pretty(get_query_plans(tree))

    indent(tree.getroot())
    tree.write('clean.xml')
