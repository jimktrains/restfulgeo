def by_point(point, conn):
    sql = "SELECT  statefp, sldust \
          FROM  tl_2013_42_sldu\
          WHERE ST_Contains(geom, ST_GeomFromText('POINT(%(lon)s %(lat)s)'))"
    cur = conn.cursor()
    cur.execute(sql, point)
    row = cur.fetchone()

    statefp = row[0]
    cdfp = row[1]

    state_id = "/state/" + statefp;
    cd_id = state_id + "/lower-house-district/" + cdfp

    ret = {
        'state': {
            'id': state_id,
            'lower-house-district': {
                'id': cd_id,
            }
        }
    }
    
    return ret

def lookup(statefp, sldust, conn):
    sql = "SELECT statefp, sldust, lsad, mtfcc, funcstat \
           FROM tl_2013_42_sldu \
           WHERE statefp = %(statefp)s AND sldust = %(sldust)s"
    state_id = "/state/" + statefp
    lh_id = state_id + "/lower-house-district/" + cd113fp
    
    cur = conn.cursor()
    cur.execute(sql, (statefp,cd113fp)); 
    row = cur.fetchone()

    lsad = row[2]
    mtfcc = row[3]
    funcstat = row[4]

    ret = {
        'state': {
            "id": state_id,
            "lower-house-district": {
                'id': cd113_id,
                'lsad': "/lsad/" + lsad,
                'mtfcc': "/mtfcc/" + mtfcc,
                'lsad': "/funcstat/" + funcstat,
            },
        }
    }

    return ret

def methods():
    return ['counties']

def counties(statefp, sldust, conn):
    sql = "SELECT cty.statefp, cty.countyfp \
           FROM tl_2013_us_county AS cty, tl_2013_42_sldu as cd \
           WHERE cd.cd113fp = %(cd113fp)s AND cd.statefp = %(statefp)s AND \
                 ST_Intersects(cty.geom, cd.geom) AND NOT ST_Touches(cty.geom, cd.geom)"
    state_id = "/state/" + statefp
    sldust_id = state_id + "/lower-house-district/" + sldust
    ret = {
        "state": {
            "id": state_id,
            "counties": [],
            "lower-house-district": sldust_id,
        }
    }

    cur = conn.cursor()
    cur.execute(sql, {"statefp": statefp, "sldust": sldust }); 
    for row in cur:
        ret['state']['counties'].append(state_id + "/county/" + row[1])

    return ret
