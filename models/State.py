import models
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
        'state':{
            'id': state_id,
            'name': name,
            'division': division,
            'region': region,
            'area': {
                'land': aland,
                'water': awater,
            },
            'abbr':    stusps,
            'mtfcc':    "/mtfcc/" + mtfcc,
            'lsad':     "/lsad/" + lsad,
            'functional_status': "/funcstat/" + funcstat,
        }
    }
    for method in models.State.methods():
        ret['state'][method] = ret['state']['id'] + "/" + method
    return ret

def methods():
    return ['school-districts','congressional-districts', 'lower-house-districts', 'upper-house-districts']

def upper_house_districts(statefp, conn):
    sql = "SELECT lh.statefp, lh.sldust \
           FROM tl_2013_42_sldu as lh\
           WHERE lh.statefp = %(statefp)s"
    state_id = "/state/" + statefp
    ret = {
        "state": {
            "id": state_id,
            "upper-house-districts": [],
        }
    }

    cur = conn.cursor()
    cur.execute(sql, {"statefp": statefp}); 
    for row in cur:
        ret['state']['upper-house-districts'].append(state_id + "/upper-house-district/" + row[1])

    return ret

def lower_house_districts(statefp, conn):
    sql = "SELECT lh.statefp, lh.sldlst \
           FROM tl_2013_42_sldl as lh\
           WHERE lh.statefp = %(statefp)s"
    state_id = "/state/" + statefp
    ret = {
        "state": {
            "id": state_id,
            "lower-house-districts": [],
        }
    }

    cur = conn.cursor()
    cur.execute(sql, {"statefp": statefp}); 
    for row in cur:
        ret['state']['lower-house-districts'].append(state_id + "/lower-house-district/" + row[1])

    return ret

def congressional_districts(statefp, conn):
    sql = "SELECT cd.statefp, cd.cd113fp \
           FROM tl_rd13_42_cd113 as cd \
           WHERE cd.statefp = %(statefp)s"
    state_id = "/state/" + statefp
    ret = {
        "state": {
            "id": state_id,
            "congressional-districts": [],
        }
    }

    cur = conn.cursor()
    cur.execute(sql, {"statefp": statefp}); 
    for row in cur:
        ret['state']['congressional-districts'].append(state_id + "/congressional-district/" + row[1])

    return ret

def school_districts(statefp, conn):
    sql = "SELECT sd.statefp, sd.unsdlea \
           FROM tl_2013_42_unsd as sd \
           WHERE sd.statefp = %(statefp)s "
    state_id = "/state/" + statefp
    ret = {
        "state": {
            "id": state_id,
            "school-districts": []
        }
    }

    cur = conn.cursor()
    cur.execute(sql, {"statefp": statefp}); 
    for row in cur:
        ret['state']['school-districts'].append(state_id + "/school-districts/" + row[1])

    return ret
