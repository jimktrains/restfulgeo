def by_point(point, conn):
    sql = "SELECT  statefp, cd113fp \
          FROM tl_rd13_42_cd113 \
          WHERE ST_Contains(the_geom, ST_GeomFromText('POINT(%(lon)s %(lat)s)'))" # PGis uses lon lat
    with conn.cursor() as cur:
        cur.execute(sql, point); 
        row = cur.fetchone()

    statefp = row[0]
    cd113fp = row[1]
    state_id = "/state/" + statefp;
    cd113_id = state_id + "/congressional-district/" + cd113fp
    ret = {'state': { 'id': state_id, "congressional-district": cd113_id } }

    return ret

def lookup(statefp, cd113fp, conn):
    sql = "SELECT statefp, cd113fp, lsad, mtfcc, funcstat \
           FROM tl_rd13_42_cd113 \
           WHERE statefp = %s AND cd113fp = %s"
    state_id = "/state/" + statefp
    cd113_id = state_id + "/congressional-district/" + cd113fp
    
    with conn.cursor() as cur:
        cur.execute(sql, (statefp,cd113fp)); 
        row = cur.fetchone()

    lsad = row[2]
    mtfcc = row[3]
    funcstat = row[4]

    ret = {
        'state': {
            state_id: {
                "congressional-district": {
                    cd113_id: {
                        'lsad': '/lsad/' + lsad,
                        'mtfcc': '/mtfcc/' + mtfcc,
                        'lsad': '/funcstat/' + funcstat
                    }
                }
            },
        }
    }

    return ret

def methods():
    return ['counties']

def counties(statefp, cd113fp, conn):
    sql = "SELECT cty.statefp, cty.countyfp \
           FROM tl_2013_us_county AS cty, tl_rd13_42_cd113 as cd \
           WHERE cd.cd113fp = %(cd113fp)s AND cd.statefp = %(statefp)s AND \
                 ST_Intersects(cty.geom, cd.the_geom) AND NOT ST_Touches(cty.geom, cd.the_geom)"
    state_id = "/state/" + statefp
    cd113_id = state_id + "/congressional-district/" + cd113fp
    ret = {
        "state": {
            state_id: {
                "counties": [],
            }
        }
    }

    with conn.cursor() as cur:
        cur.execute(sql, {"statefp": statefp, "cd113fp": cd113fp}); 
        for row in cur:
            ret['state'][state_id]['counties'].append(state_id + "/county/" + row[1])

    return ret
