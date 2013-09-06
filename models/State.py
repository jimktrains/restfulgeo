def by_point(point, conn):
    sql = "SELECT  statefp \
          FROM  tl_2013_us_state \
          WHERE ST_Contains(geom, ST_GeomFromText('POINT(%(lon)s %(lat)s)'))"
    cur = conn.cursor()
    cur.execute(sql, point); # PGis uses lon lat
    row = cur.fetchone()

    statefp = row[0]
    state_id = "/state/" + statefp;
    ret = {'state': { 'id': state_id } }

    return ret


def lookup(statefp, conn):
    sql = "SELECT region, division, statefp, statens, geoid, stusps, name, lsad, mtfcc, funcstat, aland, awater \
           FROM tl_2013_us_state \
           WHERE statefp = %s"
    statefp = str(statefp)
    state_id = "/state/" + statefp
    cur = conn.cursor()
    cur.execute(sql, (statefp,)); 
    row = cur.fetchone()

    region = row[0]
    division = row[1]
    statefp = row[2]
    statens = row[3]
    geoid = row[4]
    stusps = row[5]
    name = row[6]
    lsad = row[7]
    mtfcc = row[8]
    funcstat = row[9]
    aland = row[10]
    awater = row[11]

    ret = {
        'id': state_id,
        'name': name,
        'division': division,
        'region': region,
        'counties': state_id + "/county",
        'school-districts': state_id + "/school-district",
        'congressional-districts': state_id + "/congressional-district",
        'lower-house-districts': state_id + "/lower-house-district",
        'upper-house-districts': state_id + "/upper-house-district",
        'area': {
            'land': aland,
            'water': awater,
        },
        'abbr':    stusps,
        'mtfcc':    "/mtfcc/" + mtfcc,
        'lsad':     "/lsad/" + lsad,
        'functional_status': "/funcstat/" + funcstat,
    }
    return ret

def methods():
    return []
