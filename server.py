import psycopg2
import sys
import http.server
import socketserver
import json
import socket

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

def get_data(point):
    cousub = get_county_subdivision(point)
    sd = get_school_district(point)
    lh = get_state_lower_house(point)
    uh = get_state_upper_house(point)
    cd = get_congressional_district(point)

    ret = dmerge(cousub, sd)
    ret = dmerge(ret, lh)
    ret = dmerge(ret, uh)
    ret = dmerge(ret, cd)

    ret = rm_all_but_id(ret)

    return ret

def rm_all_but_id(a):
    for k in list(a.keys()):
        if isinstance(a[k], dict):
            rm_all_but_id(a[k])
        elif k != 'id':
            del a[k]
    return a

def dmerge(a, b):
    for k, v in b.items():
        if isinstance(v, dict) and k in a:
            dmerge(a[k], v)
        else:
            a[k] = v 
    return a

class HTTPHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        point = [40.455025, -79.938073]
        ret = json.dumps(get_data(point))
        self.send_response(200)
        self.send_header("Content-type", "application/json; charset=utf-8")
        self.end_headers()

        self.wfile.write(bytes(ret, 'UTF-8'))

while 1:
    try:
        httpd = socketserver.TCPServer(("", PORT), HTTPHandler)
        print("serving at port", PORT)
        httpd.serve_forever()
    except socket.error:
        PORT += 1
