import psycopg2
import json
import re
import tornado.ioloop
import tornado.web
import fips
from models import *

lookup_to_class = {
    'congressional-district': CongressionalDistrict,
    'county': County,
    'subdivision': CountySubdivision,
    'lower-house-district': LowerHouse,
    'point': Point,
    'school-district': SchoolDistrict,
    'state': State,
    'upper-house-district': UpperHouse
}

PORT = 8000

conn = psycopg2.connect("dbname=jim user=jim password=jim")

class GeoHandler(tornado.web.RequestHandler):
    def initialize(self, conn):
        self.conn = conn

    def get(self, lookup, extra, method, **kwargs):
        ret = None
        klass = None

        if lookup in lookup_to_class:
            klass = lookup_to_class[lookup]

        kwargs['conn'] = self.conn

        
        if klass == None:
            ret = {"error_code": 400, "error": "How'd you get here?"}
        elif method == None:
            ret = klass.lookup(**kwargs)
        elif method in klass.methods():
            ret = getattr(klass, method.replace("-","_"))(**kwargs)
        else:
            ret = {"error_code": 400, "error": method + " is not valid"}

        if "error_code" in ret:
            self.send_error(ret['error_code'])
            return
        self.set_status(200)
        self.set_header("Content-type", "application/json; charset=utf-8")
        return self.write(json.dumps(ret))

class FIPSHandler(tornado.web.RequestHandler):
    def get(self, key, val, extra):
        ret = None
        if key not in fips.fips:
            ret = {"error_code":404}
        elif val not in fips.fips[key]:
            ret = {key: fips.fips[key]}
        else:
            ret = {key: {val: fips.fips[key][val]}}

        if "error_code" in ret:
            self.send_error(ret['error_code'])
            return
        self.set_status(200)
        self.set_header("Content-type", "application/json; charset=utf-8")
        return self.write(json.dumps(ret))
FLOAT = "[-+]?\d+\.\d+"
application = tornado.web.Application([
    (r"^/(?P<lookup>point)/(?P<lat>"+FLOAT+")\s*,\s*(?P<lon>" + FLOAT + ")(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/state/(?P<statefp>\d+)/(?P<lookup>congressional-district)/(?P<cd113fp>\d+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/state/(?P<statefp>\d+)/county/(?P<countyfp>\d+)/(?P<lookup>subdivision)/(?P<cousubfp>\d+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/state/(?P<statefp>\d+)/(?P<lookup>county)/(?P<countyfp>\d+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/state/(?P<statefp>\d+)/(?P<lookup>lower-house-district)/(?P<sldlst>\d+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/state/(?P<statefp>\d+)/(?P<lookup>upper-house-district)/(?P<sldust>\d+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/state/(?P<statefp>\d+)/(?P<lookup>school-district)/(?P<unsdlea>\d+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/(?P<lookup>state)/(?P<statefp>\d+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/(?P<key>lsad|mtfcc)(?P<extra>/(?P<val>.*))?", FIPSHandler)
])

application.listen(8000)
tornado.ioloop.IOLoop.instance().start()
