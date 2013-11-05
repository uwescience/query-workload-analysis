import sqlalchemy as sa
import parse_xml
from utils import json_pretty


def explain(config):
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

        query = '''
        SELECT  top 1   p.objID, p.run,
        p.rerun, p.camcol, p.field, p.obj,
           p.type, p.ra, p.dec, p.u,p.g,p.r,p.i,p.z,
           p.Err_u, p.Err_g, p.Err_r,p.Err_i,p.Err_z
           FROM fGetNearbyObjEq(195,2.5,0.5) n, PhotoPrimary p
           WHERE n.objID=p.objID
        '''

        res = connection.execute(query).fetchall()[0]

        xml_string = "".join([x for x in res])
        tree = parse_xml.clean(xml_string)

        # print operators
        # parse_xml.print_rel_op_tags(tree.getroot())

        # get the simplified query plan as dictionary
        query_plan = parse_xml.get_query_plans(tree, details=True)
        print json_pretty(query_plan)

        # indent tree and export as xml file
        # parse_xml.indent(tree.getroot())
        # tree.write('clean.xml')

        connection.execute('set showplan_xml off')
        connection.execute('set noexec off')
