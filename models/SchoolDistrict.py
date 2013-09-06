import models

def by_point(point, conn):
    sql = "SELECT  statefp, unsdlea \
          FROM tl_2013_42_unsd \
          WHERE ST_Contains(geom, ST_GeomFromText('POINT(%(lon)s %(lat)s)'))"
    cur = conn.cursor()
    cur.execute(sql, point)
    row = cur.fetchone()

    statefp = row[0]
    unsdlea = row[1]
    
    state_id = "/state/" + statefp;
    unsdlea_id = state_id + "/school-district/" + unsdlea;


    ret = {
        'state':{
            'id': state_id,
            'school-district': {
                'id': unsdlea_id,
            }
        }
    }

    return ret

def lookup(statefp, unsdlea, conn):
    sql = "SELECT  statefp, unsdlea, name, lsad, lograde, higrade, mtfcc, sdtyp, funcstat \
          FROM tl_2013_42_unsd \
          WHERE statefp = %(statefp)s AND unsdlea = %(unsdlea)s"
    cur = conn.cursor()
    cur.execute(sql, {'statefp': statefp, 'unsdlea': unsdlea})
    row = cur.fetchone()

    statefp = row[0]
    unsdlea = row[1]
    name = row[2]
    lsad = row[3]
    lograde = row[4]
    higrade = row[5]
    mtfcc = row[6];
    sdtype = row[7]
    funcstat = row[8]
    
    state_id = "/state/" + statefp;
    unsdlea_id = state_id + "/school-district/" + unsdlea;


    ret ={ 
        'state': {
            'id': state_id,
            'school-district': {
                'id': unsdlea_id,
                'lo-grade': lograde,
                'high-grade': higrade,
                'name': name,
                'mtfcc':    "/mtfcc/" + mtfcc,
                'lsad':     "/lsad/" + lsad,
                'functional_status': "/funcstat/" + funcstat,
            }
        }
    }

    for method in models.SchoolDistrict.methods():
        ret['state']['school-district'][method] = ret['state']['school-district']['id'] + "/" + method

    return ret

def methods():
    return ['counties']

def counties(statefp, unsdlea, conn):
    sql = "SELECT cty.statefp, cty.countyfp \
           FROM tl_2013_us_county AS cty, tl_2013_42_unsd as sd \
           WHERE sd.unsdlea = %(unsdlea)s AND sd.statefp = %(statefp)s AND \
                 ST_Intersects(cty.geom, sd.geom) AND NOT ST_Touches(cty.geom, sd.geom)"
    state_id = "/state/" + statefp
    unsdlea_id = state_id + "/school-district/" + unsdlea
    ret = {
        "state": {
            "id": state_id,
            "counties": [],
            "school-district": unsdlea_id,
        }
    }

    cur = conn.cursor()
    cur.execute(sql, {"statefp": statefp, "unsdlea": unsdlea}); 
    for row in cur:
        ret['state']['counties'].append(state_id + "/county/" + row[1])

    return ret
