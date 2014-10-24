#!/usr/bin/env python
from collections import defaultdict
import re

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


def get_physical_op_count(root, count):
    """Prints the relational operators in a hierarchical fashion"""
    if root.tag == 'RelOp':
        count[0] += 1
    for child in root:
        get_physical_op_count(child, count)


def get_query_plans(tree, cost=False, show_filters=False, consts=True):
    """Returns a list of the query plans in the given XML tree

    :param cost: Show costs and operator configs
    :type cost: bool
    """
    qplans = tree.findall('.//QueryPlan')
    refs = tree.xpath('.//QueryPlan/ParameterList/ColumnReference')
    if consts:
        parameters = [(x.attrib['Column'],
                      x.attrib['ParameterCompiledValue']) for x in refs]
    else:
        parameters = [(x.attrib['Column'],
                      'CONST') for x in refs]
        parameters.extend([(x.attrib['ConstValue'].strip("(").strip(")"), 'CONST') for x in tree.xpath('.//Const')])

    parameters.sort(key=lambda x: -len(x[0]))
    return [operator_tree(
        qplan, cost, show_filters, parameters) for qplan in qplans]


def flatten(plan):
    """Flattens the plan list so that it is a single list of dicts"""
    if isinstance(plan, dict):
        return [plan]
    if len(plan) == 0:
        return plan
    return flatten(plan[0]) + flatten(plan[1:])


def operator_tree(root, cost, show_filters, parameters):
    """Returns a tree as a list of plan dictionaries, stored recursively. Each
    node has a value (its name) and a list of children, possibly empty."""
    if root is None or root == []:
        return None
    children = [operator_tree(
        child, cost, show_filters, parameters) for child in root]
    children = [x for x in children if x is not None]
    children = flatten(children)

    if root.tag == 'RelOp':
        tables = defaultdict(set)
        filters = []

        # add column references
        refs = root.xpath('.//ColumnReference')
        not_ref = root.xpath('.//RelOp//ColumnReference')
        not_ref += root.xpath('.//Predicate//ColumnReference')
        not_ref += root.xpath('.//SeekPredicates//ColumnReference')
        for ref in set(refs) - set(not_ref):
            if 'Table' in ref.attrib:
                name = ref.attrib['Table'].strip('[').strip(']')
                if "join" in root.attrib['LogicalOp'].lower():
                    # add join attribute as filter
                    filters.append(name + "." + ref.attrib['Column'])
                else:
                    tables[name].add(ref.attrib['Column'])

        if show_filters:
            def repl(s):
                    s = s.replace('(', '').replace(')', '')
                    for p in parameters:
                        s = s.replace(p[0], p[1], 2)
                    return s.lower()

            # if the rel op is top, use the (constant) expression as filter
            if root.attrib['LogicalOp'] == "Top":
                for x in root.xpath('Top/TopExpression//Const'):
                    filters.append(repl(x.attrib['ConstValue']))

            # set row count as filter for top sort
            if root.attrib['LogicalOp'] == "TopN Sort":
                filters.append(repl(root.xpath('TopSort')[0].attrib['Rows']))

            # extract scalar strings (where clause expression) from predicates
            predicates = root.xpath('.//SeekPredicates')
            not_predicates = root.xpath('.//RelOp//SeekPredicates')

            predicates += root.xpath('.//Predicate')
            not_predicates += root.xpath('.//RelOp//Predicate')

            predicates += root.xpath('.//StreamAggregate')
            not_predicates += root.xpath('.//RelOp//StreamAggregate')

            if root.attrib['LogicalOp'] == "Compute Scalar":
                predicates += root.xpath('.//DefinedValue')
                not_predicates += root.xpath('.//RelOp//DefinedValue')

            for pred in set(predicates) - set(not_predicates):
                s = ''

                sos = pred.xpath('.//ScalarOperator')
                nosos = pred.xpath('.//ScalarOperator//ScalarOperator')

                sos = list(set(sos) - set(nosos))

                if all('ScalarString' in so.attrib for so in sos):
                    # TPCH 1
                    s = ' AND '.join(so.attrib['ScalarString'] for so in sos)

                if not s:
                    # objects to compare
                    objects = []

                    ref = pred.xpath('.//ColumnReference')
                    no_ref = pred.xpath('.//ColumnReference//ColumnReference')

                    for ref in list(set(ref) - set(no_ref)):
                        if 'Column' in ref.attrib and ref.attrib['Column'].startswith('Const') and ref.xpath('.//ColumnReference'):
                            objects.append(ref.xpath('.//ColumnReference')[0].attrib['Column'])
                            continue
                        attribs = [v for k, v in ref.attrib.iteritems() if k != 'Alias']
                        objects.append('.'.join(sorted(attribs)))

                    consts = pred.xpath('.//Const')
                    for const in consts:
                        objects.append(const.attrib['ConstValue'].strip("(").strip(")"))

                    objects = [x for x in objects if x]

                    # operation
                    operator = None
                    op = pred.xpath('.//Compare')
                    if op:
                        operator = op[0].attrib['CompareOp']
                    op = pred.xpath('.//Prefix')
                    if op:
                        operator = op[0].attrib['ScanType']

                    available = {
                        'EQ': '=',
                        'GT': '>',
                        'LT': '<',
                        'GE': '>',
                        'LE': '<',
                        'NE': '<>'
                    }

                    if operator in available:
                        operator = available[operator]
                    elif operator:
                        operator = ' {} '.format(operator)
                    else:
                        operator = ' '

                    if not len(objects) % 2 == 0:
                        print "==> unexpected state"
                        print objects
                        print operator

                    a = []
                    b = []
                    for obj in objects:
                        a.append(obj)
                        if len(a) == 2:
                            b.append(operator.join(a))
                            a = []
                    s = ' AND '.join(b)

                s = repl(s)

                # replace , if not inside ()
                news = []
                bc = 0  # nesting level
                for i, c in enumerate(s):
                    if c == '(':
                        bc += 1
                    elif c == ')':
                        bc -= 1
                    if c == ',' and bc == 0:
                        news.append(' and ')
                    else:
                        news.append(c)
                s = ''.join(news)
                s = s.lower().replace('[', '').replace(']', '').replace("'", '')
                s = s.replace('>=', '>').replace('<=', '<')
                s = s.replace('CONSTCONST', 'CONST')
                fs = s.split(' and ')
                fs = [re.sub(r' as [\w|\.]+', r'', x) for x in fs]
                fs = [' or '.join(set(x.split(' or '))) for x in fs]

                filters.extend([x.strip() for x in fs])

            filters = list(set(filters))

            # extract the parameter list as filters from table valued functions
            tvf = root.xpath('TableValuedFunction')
            if tvf:
                tbl = tvf[0].xpath(
                    'Object')[0].attrib['Table'].strip('[').strip(']')
                consts = tvf[0].xpath('ParameterList//Const')
                tables[tbl] = []

                filters.append({tbl: set([repl(x.attrib['ConstValue']) for x in consts])})

        ret = {
            'operator': root.attrib['LogicalOp'],
            'physicalOp': root.attrib['PhysicalOp'],
            'children': children,
            'columns': tables
        }

        if show_filters:
            ret['filters'] = filters

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


def get_expression_operators(root):
    """Returns all expression operators in the tree

    # http://technet.microsoft.com/en-us/library/ms174318(v=sql.105).aspx
    #
    # Logical Operation="AND"
    # Compare CompareOp="GT"
    # Const ConstValue="(7)"
    # Intrinsic FunctionName="sin"
    # Aggregate AggType="countstar" Distinct="false"
    # Arithmetic Operation="MULT"
    # Sequence FunctionName="row_number"
    """
    ops = []
    for op in root.xpath('.//Arithmetic'):
        ops.append({'class': 'arithmetic', 'operator': op.attrib['Operation']})
    for op in root.xpath('.//Logical'):
        ops.append({'class': 'logical', 'operator': op.attrib['Operation']})
    for op in root.xpath('.//Compare'):
        ops.append({'class': 'compare', 'operator': op.attrib['CompareOp']})
    for op in root.xpath('.//Const'):
        ops.append({'class': 'const', 'value': op.attrib['ConstValue'].strip('(').strip(')')})
    for op in root.xpath('.//Intrinsic'):
        ops.append({'class': 'intrinsic', 'operator': op.attrib['FunctionName']})
    for op in root.xpath('.//Aggregate'):
        ops.append({'class': 'aggregate', 'operator': op.attrib['AggType'], 'distinct': op.attrib['Distinct']})
    for op in root.xpath('.//Sequence'):
        # skip if sequence operator and not sequence expression
        if op.attrib != []:
            ops.append({'class': 'sequence', 'operator': op.attrib['FunctionName']})
    for op in ops:
        if 'value' not in op.keys():
            op['value'] = None
        if 'operator' not in op.keys():
            op['operator'] = None
    return ops


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
