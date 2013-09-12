import psycopg2.extras
import models 

def by_point(point, conn):
    sql = "SELECT statefp, countyfp \
          FROM tl_2013_us_county \
          WHERE ST_Contains(geom, ST_GeomFromText('POINT(%(lon)s %(lat)s)'))";
    with conn.cursor() as cur:
        cur.execute(sql, point);
        row = cur.fetchone()

    ret = {}

    statefp = row[0]
    countyfp = row[1]
    cousubfp  = row[2]
    
    state_id = "/state/" + statefp;
    county_id = state_id + "/county/" + countyfp;

    ret['state'] = {
        'id':  state_id,
        'county': {
            [county_id,]
        }
    }
    return ret

def lookup(statefp, countyfp, conn):
    sql = "SELECT statefp, countyfp, name, lsad, mtfcc, funcstat \
          FROM tl_2013_us_county \
          WHERE statefp = %(statefp)s AND countyfp = %(countyfp)s "
    with conn.cursor() as cur:
        cur.execute(sql, {'statefp':statefp, 'countyfp': countyfp})
        row = cur.fetchone()

    ret = {}

    if row is None:
        raise models.NotFound.NotFound('county', {'statefp':statefp, 'countyfp':countyfp})

    statefp = row[0]
    countyfp = row[1]
    name = row[2]
    lsad = row[3]
    mtfcc = row[4];
    funcstat = row[5]
    
    state_id = "/state/" + statefp;
    county_id = state_id + "/county/" + countyfp;

    ret = {
        'state': {
            state_id:{
                'county': {
                    county_id: {
                        'name':    name,
                        'mtfcc':    "/mtfcc/" + mtfcc,
                        'lsad':     "/lsad/" + lsad,
                        'functional_status': "/funcstat/" + funcstat,
                    }
                }
            }
        }
    }

    for method in models.County.methods():
        ret['state'][state_id]['county'][county_id][method] = county_id + "/" + method

    return ret

def methods():
    return ['school-districts','congressional-districts', 'lower-house-districts', 'upper-house-districts', 'subdivisions', 'acs2010-5e']

def acs2010_5e(statefp, countyfp, conn, extra = None):
    if extra is None:
        return models.acs20105e.get_all_stats(statefp=statefp, countyfp=countyfp, conn=conn)
    else:
        
        return models.acs20105e.get_one_stat(statefp=statefp, countyfp=countyfp, stat=extra, conn=conn)

def subdivisions(statefp, countyfp, conn):
    sql = "SELECT sd.statefp, sd.cousubfp, sd.countyfp \
           FROM tl_2013_us_county AS cnty, tl_2013_42_cousub as sd\
           WHERE cnty.countyfp = %(countyfp)s AND cnty.statefp = %(statefp)s AND \
                 ST_Intersects(sd.geom, cnty.geom) AND NOT ST_Touches(sd.geom, cnty.geom)"
    state_id = "/state/" + statefp
    county_id = state_id + "/county/" + countyfp
    ret = {
        "state": {
            state_id: {
                "county": {
                    county_id: {
                        "subdivisions": [],
                    }
                }
            }
        }
    }

    cur = conn.cursor()
    cur.execute(sql, {"statefp": statefp, "countyfp": countyfp}); 
    for row in cur:
        ret['state'][state_id]['county'][county_id]['subdivisions'].append(county_id + "/subdivision/" + row[1])

    return ret
def upper_house_districts(statefp, countyfp, conn):
    sql = "SELECT lh.statefp, lh.sldust \
           FROM tl_2013_us_county AS cnty, tl_2013_42_sldu as lh\
           WHERE cnty.countyfp = %(countyfp)s AND cnty.statefp = %(statefp)s AND \
                 ST_Intersects(lh.geom, cnty.geom) AND NOT ST_Touches(lh.geom, cnty.geom)"
    state_id = "/state/" + statefp
    county_id = state_id + "/county/" + countyfp
    ret = {
        "state": {
            state_id: {
                "upper-house-districts": [],
            }
        }
    }

    cur = conn.cursor()
    cur.execute(sql, {"statefp": statefp, "countyfp": countyfp}); 
    for row in cur:
        ret['state'][state_id]['upper-house-districts'].append(state_id + "/upper-house-district/" + row[1])

    return ret
def lower_house_districts(statefp, countyfp, conn):
    sql = "SELECT lh.statefp, lh.sldlst \
           FROM tl_2013_us_county AS cnty, tl_2013_42_sldl as lh\
           WHERE cnty.countyfp = %(countyfp)s AND cnty.statefp = %(statefp)s AND \
                 ST_Intersects(lh.geom, cnty.geom) AND NOT ST_Touches(lh.geom, cnty.geom)"
    state_id = "/state/" + statefp
    county_id = state_id + "/county/" + countyfp
    ret = {
        "state": {
            state_id: {
                "lower-house-districts": [],
            }
        }
    }

    cur = conn.cursor()
    cur.execute(sql, {"statefp": statefp, "countyfp": countyfp}); 
    for row in cur:
        ret['state'][state_id]['lower-house-districts'].append(state_id + "/lower-house-district/" + row[1])

    return ret
def congressional_districts(statefp, countyfp, conn):
    sql = "SELECT cd.statefp, cd.cd113fp \
           FROM tl_2013_us_county AS cnty, tl_rd13_42_cd113 as cd \
           WHERE cnty.countyfp = %(countyfp)s AND cnty.statefp = %(statefp)s AND \
                 ST_Intersects(cd.the_geom, cnty.geom) AND NOT ST_Touches(cd.the_geom, cnty.geom)"
    state_id = "/state/" + statefp
    county_id = state_id + "/county/" + countyfp
    ret = {
        "state": {
            state_id: {
                "congressional-districts": [],
            }
        }
    }

    cur = conn.cursor()
    cur.execute(sql, {"statefp": statefp, "countyfp": countyfp}); 
    for row in cur:
        ret['state'][state_id]['congressional-districts'].append(state_id + "/congressional-district/" + row[1])

    return ret

def school_districts(statefp, countyfp, conn):
    sql = "SELECT sd.statefp, sd.unsdlea \
           FROM tl_2013_us_county AS cnty, tl_2013_42_unsd as sd \
           WHERE cnty.countyfp = %(countyfp)s AND cnty.statefp = %(statefp)s AND \
                 ST_Intersects(sd.geom, cnty.geom) AND NOT ST_Touches(sd.geom, cnty.geom)"
    state_id = "/state/" + statefp
    county_id = state_id + "/county/" + countyfp
    ret = {
        "state": {
            state_id: {
                "school-districts": []
            }
        }
    }

    cur = conn.cursor()
    cur.execute(sql, {"statefp": statefp, "countyfp": countyfp}); 
    for row in cur:
        ret['state'][state_id]['school-districts'].append(state_id + "/school-districts/" + row[1])

    return ret
