def by_point(point, conn):
    sql = "SELECT  statefp, sldlst \
          FROM  tl_2013_42_sldl\
          WHERE ST_Contains(geom, ST_GeomFromText('POINT(%(lon)s %(lat)s)'))"
    with conn.cursor() as cur:
        cur.execute(sql, point)
        row = cur.fetchone()

    statefp = row[0]
    cdfp = row[1]

    state_id = "/state/" + statefp;
    cd_id = state_id + "/lower-house-district/" + cdfp

    ret = {
        'state': {
            state_id: {
                'lower-house-district': cd_id,
            }
        }
    }
    
    return ret

def lookup(statefp, sldlst, conn):
    sql = "SELECT statefp, sldlst, lsad, mtfcc, funcstat \
           FROM tl_2013_42_sldl \
           WHERE statefp = %(statefp)s AND sldlst = %(sldlst)s"
    state_id = "/state/" + statefp
    lh_id = state_id + "/lower-house-district/" + sldlst
    
    with conn.cursor() as cur:
        cur.execute(sql, {'statefp':statefp, 'sldlst':sldlst}); 
        row = cur.fetchone()

    lsad = row[2]
    mtfcc = row[3]
    funcstat = row[4]

    ret = {
        'state': {
            state_id: {
                "lower-house-district": {
                    lh_id: {
                        'lsad': "/lsad/" + lsad,
                        'mtfcc': "/mtfcc/" + mtfcc,
                        'lsad': "/funcstat/" + funcstat,
                    }
                }
            }
        }
    }

    return ret

def methods():
    return ['counties']

def counties(statefp, sldlst, conn):
    sql = "SELECT cty.statefp, cty.countyfp \
           FROM tl_2013_us_county AS cty, tl_2013_42_sldl as cd \
           WHERE cd.cd113fp = %(cd113fp)s AND cd.statefp = %(statefp)s AND \
                 ST_Intersects(cty.geom, cd.geom) AND NOT ST_Touches(cty.geom, cd.geom)"
    state_id = "/state/" + statefp
    sldlst_id = state_id + "/lower-house-district/" + sldlst
    ret = {
        "state": {
            state_id: {
                "counties": [],
            }
        }
    }

    with conn.cursor() as cur:
        cur.execute(sql, {"statefp": statefp, "sldlst": sldlst }); 
        for row in cur:
            ret['state'][state_id]['counties'].append(state_id + "/county/" + row[1])

    return ret
