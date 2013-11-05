import sqlalchemy as sa


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
