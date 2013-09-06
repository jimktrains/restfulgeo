import psycopg2
import json
import re
import tornado.ioloop
import tornado.web


PORT = 8000

conn = psycopg2.connect("dbname=jim user=jim password=jim")


fips = {
    "statefp": {
        "42": "Pennsylvania",
    },
    "lsad": {
        "00": "Blank",
        "20": "barrio",
        "21": "borough",
        "22": "census county division",
        "23": "census subarea",
        "24": "census subdistrict",
        "25": "city",
        "26": "county",
        "27": "district",
        "28": "District (prefix)",
        "29": "precinct",
        "30": "Precinct (prefix)",
        "31": "gore",
        "32": "grant",
        "36": "location",
        "39": "plantation",
        "41": "barrio-pueblo",
        "42": "purchase",
        "43": "town",
        "44": "township",
        "45": "Township (prefix)",
        "46": "Unorganized Territory (UT)",
        "47": "village",
        "49": "charter township",
        "86": "Reservation"
    },
    "classfp": {
        "C2":  "An active incorporated place that is legally coextensive with an county subdivision but treated as independent of any county subdivision",
        "C5":   "An active incorporated place that is independent of any county subdivision and serves as a county subdivision equivalent",
        "C7":   "An incorporated place that is independent of any county",
        "S1":   "A nonfunctioning county subdivision that is coextensive with a census designated place",
        "S2":   "A statistical county subdivision that is coextensive with a census designated place",
        "S3":   "A statistical county subdivision that is coextensive with a legal American Indian, Alaska Native, or Native Hawaiian area",
        "T1":   "An active county subdivision that is not coextensive with an incorporated place",
        "T2":   "An active county subdivision that is coextensive with a census designated place",
        "T5":   "An active county subdivision that is coextensive with an incorporated place",
        "T9":   "An inactive county subdivision",
        "Z1":   "A nonfunctioning county subdivision",
        "Z2":   "A county subdivision that is coextensive with an American Indian, Alaska Native, or Native Hawaiian area and legally is independent of any other county subdivision",
        "Z3":   "A county subdivision defined as an unorganized territory",
        "Z5":   "A statistical county subdivision",
        "Z7":   "A county subdivision that is coextensive with a county or equivalent feature or all or part of an incorporated place that the Census Bureau recognizes separately",
        "Z9":   "Area of a county or equivalent, generally in territorial sea, where no county subdivision exists",
    },
    "mtfcc": {
        "G4040": "County Subdivision",
        "G5420": "School District",
        "G5200": "Congressional District"
    },
    "funcstat": {
        "A":   "Active government providing primary general-purpose functions",
        "B":   "Active government that is partially consolidated with another government but with separate officials providing primary general-purpose functions",
        "C":   "Active government consolidated with another government with a single set of officials",
        "E":   "NEED TO FIGURE OUT",
        "F":   "Fictitious entity created to fill the Census Bureau geographic hierarchy",
        "G":   "Active government that is subordinate to another unit of government.",
        "I":   "Inactive governmental unit that has the power to provide primary special-purpose functions",
        "N":   "Nonfunctioning legal entity",
        "S":   "Statistical entity",
    }
}

def get_congressional_district(point):
    sql = "SELECT  statefp, cd113fp \
          FROM  tl_rd13_42_cd113 \
          WHERE ST_Contains(the_geom, ST_GeomFromText('POINT(%s %s)'))"
    cur = conn.cursor()
    cur.execute(sql, (point[1], point[0])); # PGis uses lon lat
    row = cur.fetchone()

    ret = {}

    statefp = row[0]
    cdfp = row[1]

    state_id = "/state/" + statefp;
    cd_id = state_id + "/congressional-district/" + cdfp

    ret['state'] = {
        'id': state_id,
        'congressional-district': {
            'id': cd_id,
        }
    }
    
    return ret

def get_state_lower_house(point):
    sql = "SELECT  statefp, sldlst \
          FROM  tl_2013_42_sldl\
          WHERE ST_Contains(geom, ST_GeomFromText('POINT(%s %s)'))"
    cur = conn.cursor()
    cur.execute(sql, (point[1], point[0])); # PGis uses lon lat
    row = cur.fetchone()

    ret = {}

    statefp = row[0]
    cdfp = row[1]

    state_id = "/state/" + statefp;
    cd_id = state_id + "/lower-house-district/" + cdfp

    ret['state'] = {
        'id': state_id,
        'lower-house-district': {
            'id': cd_id,
        }
    }
    
    return ret

def get_state_upper_house(point):
    sql = "SELECT  statefp, sldust \
          FROM  tl_2013_42_sldu\
          WHERE ST_Contains(geom, ST_GeomFromText('POINT(%s %s)'))"
    cur = conn.cursor()
    cur.execute(sql, (point[1], point[0])); # PGis uses lon lat
    row = cur.fetchone()

    ret = {}

    statefp = row[0]
    cdfp = row[1]

    state_id = "/state/" + statefp;
    cd_id = state_id + "/upper-house-district/" + cdfp

    ret['state'] = {
        'id': state_id,
        'upper-house-district': {
            'id': cd_id,
        }
    }
    
    return ret

def get_school_district(point):
    sql = "SELECT  STATEFP, UNSDLEA, NAME, LSAD, LOGRADE, HIGRADE, MTFCC, SDTYP, FUNCSTAT \
          FROM tl_2013_42_unsd \
          WHERE ST_Contains(geom, ST_GeomFromText('POINT(%s %s)'))"
    cur = conn.cursor()
    cur.execute(sql, (point[1], point[0])); # PGis uses lon lat
    row = cur.fetchone()

    ret = {}

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


    ret['state'] = {
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

    return ret


def get_county_subdivision(point):
    pass
    sql = "SELECT statefp, countyfp, cousubfp, name, lsad, classfp, mtfcc, funcstat \
          FROM tl_2013_42_cousub \
          WHERE ST_Contains(geom, ST_GeomFromText('POINT(%s %s)'))";

    cur = conn.cursor()
    cur.execute(sql, (point[1], point[0])); # PGis uses lon lat
    row = cur.fetchone()

    ret = {}

    statefp = row[0]
    countyfp = row[1]
    countysubfp  = row[2]
    name = row[3]
    lsad = row[4]
    classfp = row[5]
    mtfcc = row[6];
    funcstat = row[7]
    
    state_id = "/state/" + statefp;
    county_id = state_id + "/county/" + countyfp;
    cousub_id = county_id + "/subdivision/" + countysubfp

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

def dmerge(a, b):
    for k, v in b.items():
        if isinstance(v, dict) and k in a:
            dmerge(a[k], v)
        else:
            a[k] = v 
    return a

class Point():
    @staticmethod
    def lookup(lat, lon):
        point = {"lat":float(lat), "lon":float(lon)}
        ret = State.by_point(point)
        ret = dmerge(ret, CD.by_point(point))

        return ret

    @staticmethod
    def methods():
        return []

class State:
    @staticmethod
    def by_point(point):
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
    

    @staticmethod
    def lookup(statefp):
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

    @staticmethod
    def methods():
        return []
    
class CD:
    @staticmethod
    def by_point(point):
        sql = "SELECT  statefp, cd113fp \
              FROM tl_rd13_42_cd113 \
              WHERE ST_Contains(the_geom, ST_GeomFromText('POINT(%(lon)s %(lat)s)'))"
        cur = conn.cursor()
        cur.execute(sql, point); # PGis uses lon lat
        row = cur.fetchone()

        statefp = row[0]
        cd113fp = row[1]
        state_id = "/state/" + statefp;
        cd113_id = state_id + "/congressional-district/" + cd113fp
        ret = {'state': { 'id': state_id, "congressional-district": cd113_id } }

        return ret
    @staticmethod
    def lookup(statefp, cd113fp):
        sql = "SELECT statefp, cd113fp, lsad, mtfcc, funcstat \
               FROM tl_rd13_42_cd113 \
               WHERE statefp = %s AND cd113fp = %s"
        state_id = "/state/" + statefp
        cd113_id = state_id + "/congressional-district/" + cd113fp
        ret = {
            'state': {
                "id": state_id,
                "congressional-district": {
                    'id': cd113_id
                },
            }
        }
        
        cur = conn.cursor()
        cur.execute(sql, (statefp,cd113fp)); 
        row = cur.fetchone()

        lsad = row[2]
        mtfcc = row[3]
        funcstat = row[4]

        ret['state']['congressional-district']['lsad'] = "/lsad/" + lsad
        ret['state']['congressional-district']['mtfcc'] = "/mtfcc/" + mtfcc
        ret['state']['congressional-district']['lsad'] = "/funcstat/" + funcstat

        return ret

    @staticmethod
    def methods():
        return ['counties']

    @staticmethod
    def counties(statefp, cd113fp):
        sql = "SELECT cty.statefp, cty.countyfp \
               FROM tl_2013_us_county AS cty, tl_rd13_42_cd113 as cd \
               WHERE cd.cd113fp = %(cd113fp)s AND cd.statefp = %(statefp)s AND \
                     ST_Intersects(cty.geom, cd.the_geom) AND NOT ST_Touches(cty.geom, cd.the_geom)"
        state_id = "/state/" + statefp
        cd113_id = state_id + "/congressional-district/" + cd113fp
        ret = {
            "state": {
                "id": state_id,
                "counties": [],
                "congressional-district": cd113_id,
            }
        }

        cur = conn.cursor()
        cur.execute(sql, {"statefp": statefp, "cd113fp": cd113fp}); 
        for row in cur:
            ret['state']['counties'].append(state_id + "/county/" + row[1])
    
        return ret

class GeoHandler(tornado.web.RequestHandler):
    def get(self, lookup, extra, method, **kwargs):
        ret = None
        klass = None
        if lookup == "congressional-district":
            klass = CD
        elif lookup == "state":
            klass = State
        elif lookup == "point":
            klass = Point

        if klass == None:
            ret = {"error_code": 400, "error": "How'd you get here?"}
        elif  method in klass.methods():
            ret = getattr(klass, method)(**kwargs)
        elif method == None:
            ret = klass.lookup(**kwargs)
        else:
            ret = {"error_code": 404, "error": method + " is not valid"}

        if "error_code" in ret:
            self.send_error(ret['error_code'])
            self.write(json.dumps(ret))
            return
        self.set_status(200)
        self.set_header("Content-type", "application/json; charset=utf-8")
        return self.write(json.dumps(ret))
        

FLOAT = "[-+]?\d+\.\d+"
application = tornado.web.Application([
    (r"^/(?P<lookup>state)/(?P<statefp>\d+)(?P<extra>/(?P<method>.+))?$", GeoHandler),
    (r"^/state/(?P<statefp>\d+)/(?P<lookup>congressional-district)/(?P<cd113fp>\d+)(?P<extra>/(?P<method>.+))?$", GeoHandler),
    (r"^/(?P<lookup>point)/(?P<lat>"+FLOAT+")\s*,\s*(?P<lon>" + FLOAT + ")(?P<extra>/(?P<method>.+))?$", GeoHandler),
])

application.listen(8000)
tornado.ioloop.IOLoop.instance().start()
