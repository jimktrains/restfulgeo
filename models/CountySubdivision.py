def by_point(point, conn):
    sql = "SELECT statefp, countyfp, cousubfp \
          FROM tl_2013_42_cousub \
          WHERE ST_Contains(geom, ST_GeomFromText('POINT(%(lon)s %(lat)s)'))";
    cur = conn.cursor()
    cur.execute(sql, point);
    row = cur.fetchone()

    ret = {}

    statefp = row[0]
    countyfp = row[1]
    cousubfp  = row[2]
    
    state_id = "/state/" + statefp;
    county_id = state_id + "/county/" + countyfp;
    cousub_id = county_id + "/subdivision/" + cousubfp

    ret['state'] = {
        'id':  state_id,
        'county': {
            'id': county_id,
            'subdivision': cousub_id,
        }
    }
    return ret

def lookup(statefp, countyfp, cousubfp, conn):
    sql = "SELECT statefp, countyfp, cousubfp, name, lsad, classfp, mtfcc, funcstat \
          FROM tl_2013_42_cousub \
          WHERE statefp = %(statefp)s AND countyfp = %(countyfp)s AND cousubfp = %(cousubfp)s"
    cur = conn.cursor()
    cur.execute(sql, {'statefp':statefp, 'countyfp':countyfp, 'cousubfp':cousubfp})
    row = cur.fetchone()

    ret = {}

    statefp = row[0]
    countyfp = row[1]
    cousubfp  = row[2]
    name = row[3]
    lsad = row[4]
    classfp = row[5]
    mtfcc = row[6];
    funcstat = row[7]
    
    state_id = "/state/" + statefp;
    county_id = state_id + "/county/" + countyfp;
    cousub_id = county_id + "/subdivision/" + cousubfp

    ret['state'] = {
        'id':  state_id,
        'county': {
            'id': county_id,
            'subdivision': {
                'id':      cousub_id,
                'name':    name,
                'mtfcc':    "/mtfcc/" + mtfcc,
                'lsad':     "/lsad/" + lsad,
                'classfp':  "/classfp/" + classfp,
                'functional_status': "/funcstat/" + funcstat,
            }
        }
    }

    return ret

def methods():
    return []
