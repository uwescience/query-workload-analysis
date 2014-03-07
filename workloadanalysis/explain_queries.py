import json
import dataset
import sqlalchemy as sa
import parse_xml
import query_analysis
import utils

EXAMPLE = [{'query': '''
SELECT  top 1   p.objID, p.run,
p.rerun, p.camcol, p.field, p.obj,
   p.type, p.ra, p.dec, p.u,p.g,p.r,p.i,p.z,
   p.Err_u, p.Err_g, p.Err_r,p.Err_i,p.Err_z
   FROM fGetNearbyObjEq(195,2.5,0.5) n, PhotoPrimary p
   WHERE n.objID=p.objID
'''}]

def explain_sqlshare(config, database, quiet):
    db = dataset.connect(database)

    errors = []
    table = db['sqlshare_logs']
    queries = list(db.query('SELECT * FROM sqlshare_logs'))
    views = list(db.query('SELECT * FROM sqlshare_logs WHERE isView = 1'))
    for i, query in enumerate(queries):
        print "Explain query", i
        op_count = [0]
        #see if the plan is valid
        if query['plan'][0] != '<':
            errors.append(query['plan'])
            continue

        xml_string = "".join([x for x in query['plan']])
        
        # print operators
        #parse_xml.get_physical_op_count(tree.getroot(), op_count)
         
        # get the simplified query plan as dictionary
        try:
            tree = parse_xml.clean(xml_string)
            query_plans = parse_xml.get_query_plans(
                tree, cost=True, show_filters=True)
            if len(query_plans) == 0:
                errors.append("No query plan found")
                print 'no query_plan'
                continue
            if len(query_plans) > 1:
                errors.append("Found two query plans")
                print 'multiple query_plan'
                continue
        except:
            print 'get query_plan error...'
            continue

        query_plan = query_plans[0]
        if not quiet:
            print utils.json_pretty(query_plan)
        query['plan'] = json.dumps(query_plan, cls=utils.SetEncoder)

        # indent tree and export as xml file
        parse_xml.indent(tree.getroot())
        #tree.write('clean_{}.xml'.format(i))

        simple_query_plan = parse_xml.get_query_plans(
            tree, cost=False, show_filters=False)[0]
        query['simple_plan'] = json.dumps(
            simple_query_plan, cls=utils.SetEncoder)

        query['estimated_cost'] = query_plan['total']

        ####Getting all referenced views now#####
        ref_views = []
        for q in views:
            if q['view'] in query['query']:
                ref_views.append(str(q['id']) + ',')
        query['ref_views'] = ''.join([x for x in ref_views])


        ####Getting all the ops#####        
        ops = query_analysis.get_logical_operators(query_plan)
        if ops:
            query['expanded_plan_ops'] = ','.join([x for x in ops])
        else:
            print 'no ops is view'
        query['has_plan'] = True
        table.update(query, ['id'])

    print "Errors", errors
    print "Error: {0} \%".format(len(errors)*100.0/len(queries))


def explain(config, database, quiet):
    """Explain queries and store the results in database
    """
    connection_string = 'mssql+pymssql://%s:%s@%s:%s/%s?charset=UTF-8' % (
                        config['user'],
                        config['password'],
                        config['server'],
                        config['port'],
                        config['db'])

    db = sa.create_engine(connection_string, echo=(not quiet))

    with db.connect() as connection:
        connection.execute('set showplan_xml on')
        connection.execute('set noexec on')

        if database:
            db = dataset.connect(database)
            queries = list(db.query(
            'SELECT * FROM logs WHERE db = "BestDR5" AND error != "" GROUP BY query'))
        else:
            queries = EXAMPLE

        errors = []
        table = db['logs']

        for i, query in enumerate(queries):
            print "Explain query", i
            query = dict(query)
            try:
                res = connection.execute(query['query'].replace('[','"').replace(']','"')).fetchall()[0]
            except Exception as e:
                errors.append(str(e))
                print str(e)
                print 'execute error'
                continue

            xml_string = "".join([x for x in res])
            tree = parse_xml.clean(xml_string)

            # print operators
            parse_xml.print_rel_op_tags(tree.getroot())

            # get the simplified query plan as dictionary
            query_plans = parse_xml.get_query_plans(
                tree, cost=True, show_filters=True)
            if len(query_plans) == 0:
                errors.append("No query plan found")
                print 'no query_plan'
                continue
            if len(query_plans) > 1:
                errors.append("Found two query plans")
                print 'multiple query_plan'
                continue

            query_plan = query_plans[0]
            if not quiet:
                print utils.json_pretty(query_plan)
            query['plan'] = json.dumps(query_plan, cls=utils.SetEncoder)

            # indent tree and export as xml file
            parse_xml.indent(tree.getroot())
            #tree.write('clean_{}.xml'.format(i))

            simple_query_plan = parse_xml.get_query_plans(
                tree, cost=False, show_filters=False)[0]
            query['simple_plan'] = json.dumps(
                simple_query_plan, cls=utils.SetEncoder)

            query['estimated_cost'] = query_plan['total']
            query['has_plan'] = True

            print query
            table.update(query, ['id'])
        connection.execute('set showplan_xml off')
        connection.execute('set noexec off')

        print "Errors", errors
