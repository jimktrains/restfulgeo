import psycopg2
import json
import re
import tornado.ioloop
import tornado.web
import fips
from geocoder import *
from models import *

lookup_to_class = {
    'congressional-district': CongressionalDistrict,
    'county': County,
    'subdivision': CountySubdivision,
    'lower-house-district': LowerHouse,
    'point': Point,
    'address': Address,
    'school-district': SchoolDistrict,
    'state': State,
    'upper-house-district': UpperHouse
}
lookup_to_type = {
    'congressional-district': 'congressional-district',
    'county': 'county',
    'subdivision': 'county-subdivision',
    'lower-house-district': 'state-lower-house',
    'point': 'point',
    'address': 'point',
    'school-district': 'school-district',
    'state': 'state',
    'upper-house-district': 'state-upper-house' 
}

PORT = 8000

conn = psycopg2.connect("dbname=jim user=jim password=jim")

class GeoHandler(tornado.web.RequestHandler):
    def initialize(self, conn):
        self.conn = conn

    def get(self, lookup, extra, method, **kwargs):
        ret = None
        klass = None
        types = None

        if lookup in lookup_to_class:
            klass = lookup_to_class[lookup]
        if lookup in lookup_to_type:
            types = lookup_to_type[lookup]

        kwargs['conn'] = self.conn

        if method is not None and method.find("/") > -1:
            x = method.split("/",1)
            method = x[0]
            extra = x[1].split("/")
            kwargs['extra'] = extra
        
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
        self.set_header("Content-type", "x-resfulgeo/x-%s" % (types,)) 
        self.set_header("Content-encoding", "application/json; charset=utf-8")
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
        self.set_header("Content-type", "x-resfulgeo/x-fips-%s" % (key,)) 
        self.set_header("Content-encoding", "application/json; charset=utf-8")
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
    (r"^/(?P<lookup>address)/(?P<address>.+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/(?P<key>funcstat|classfp|lsad|mtfcc)(?P<extra>/(?P<val>.*))?", FIPSHandler)
])

port = 8000

application.listen(port)
print("Listening on %d" % port)
tornado.ioloop.IOLoop.instance().start()
