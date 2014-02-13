import json
import dataset
import sqlalchemy as sa
import parse_xml
import utils

EXAMPLE = [{'query': '''
SELECT  top 1   p.objID, p.run,
p.rerun, p.camcol, p.field, p.obj,
   p.type, p.ra, p.dec, p.u,p.g,p.r,p.i,p.z,
   p.Err_u, p.Err_g, p.Err_r,p.Err_i,p.Err_z
   FROM fGetNearbyObjEq(195,2.5,0.5) n, PhotoPrimary p
   WHERE n.objID=p.objID
'''}]


def explain(config, database, sdss):
    """Explain queries and store the results in database
    """
    db = sa.create_engine(
        'mssql+pymssql://%s:%s@%s:%s/%s?charset=UTF-8' % (
            config['user'],
            config['password'],
            config['server'],
            config['port'],
            config['db']
        ),
        echo=True
    )

    with db.connect() as connection:
        connection.execute('set showplan_xml on')
        connection.execute('set noexec on')

        if database:
            db = dataset.connect(database)
            queries = list(db.query(
                'SELECT * FROM logs WHERE db = "BestDR5" AND error != ""'))
        else:
            queries = EXAMPLE

        errors = []
        table = db['logs']

        for i, query in enumerate(queries):
            print "Explain query", i
            query = dict(query)

            try:
                res = connection.execute(query['query']).fetchall()[0]
            except Exception as e:
                errors.append(str(e))
                continue

            xml_string = "".join([x for x in res])
            tree = parse_xml.clean(xml_string)

            # print operators
            # parse_xml.print_rel_op_tags(tree.getroot())

            # get the simplified query plan as dictionary
            query_plans = parse_xml.get_query_plans(
                tree, cost=True, show_filters=True)
            assert(len(query_plans) == 1)

            query_plan = query_plans[0]
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
            table.update(query, ['id'])
        connection.execute('set showplan_xml off')
        connection.execute('set noexec off')

        print "Errors", errors