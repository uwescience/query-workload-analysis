"""Query explainer.

Usage:
  explain_queries.py <config>
  explain_queries.py (-h | --help)
  explain_queries.py --version

Options:
  -h --help     Show this screen.
  --version     Show version.
"""
import sqlalchemy as sa
from docopt import docopt


def analyze(config):
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

    db.engine.execute('set showplan_xml on')
    db.engine.execute('set noexec on')

    query = '''
    SELECT  top 1   p.objID, p.run,
    p.rerun, p.camcol, p.field, p.obj,
       p.type, p.ra, p.dec, p.u,p.g,p.r,p.i,p.z,
       p.Err_u, p.Err_g, p.Err_r,p.Err_i,p.Err_z
       FROM fGetNearbyObjEq(195,2.5,0.5) n, PhotoPrimary p
       WHERE n.objID=p.objID
    '''

    res = db.engine.execute(query)

    for line in res:
        print line

    db.engine.execute('set showplan_xml off')
    db.engine.execute('set noexec off')


def main():
    arguments = docopt(__doc__, version='Query explainer')
    print arguments
    d = {}
    with open(arguments['<config>']) as f:
        for line in f:
            key, val = line.split('=')
            d[key.strip()] = val.strip()
    print d
    analyze(d)

if __name__ == '__main__':
    main()
