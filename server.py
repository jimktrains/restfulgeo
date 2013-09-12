from copy import deepcopy
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
lookup_to_key = {
    'congressional-district': 'cd113fp',
    'county': 'countyfp',
    'subdivision': 'cousubfp',
    'lower-house-district': 'sldlst' ,
    'point': ('lat', 'lon'),
    'address': 'address',
    'school-district': 'unsdlea',
    'state': 'statefp',
    'upper-house-district': 'sldust'
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

conn = psycopg2.connect("dbname=jim user=jim password=jim", async=False)

class GeoHandler(tornado.web.RequestHandler):
    def initialize(self, conn):
        self.conn = conn

    @staticmethod
    def dmerge(a, b):
        for k, v in b.items():
            if isinstance(v, dict) and k in a:
                GeoHandler.dmerge(a[k], v)
            elif isinstance(v, list) and k in a:
                for i in v:
                    a[k].append(i)
            else:
                a[k] = v 
        return a


    def get(self, lookup, extra, method, **kwargs):
        ret = {}
        klass = None
        types = None
        key = None
        reqs = []

        if lookup in lookup_to_class:
            klass = lookup_to_class[lookup]
        if lookup in lookup_to_type:
            types = lookup_to_type[lookup]
        if lookup in lookup_to_key:
            key = lookup_to_key[lookup]


        if method is not None and ('/' in method):
            x = method.split("/",1)
            method = x[0]
            extra = x[1].split("/")
            kwargs['extra'] = extra

        def dup_req(key, vals):
            for i in vals:
                x = deepcopy(kwargs)
                x[key] = i
                reqs.append(x)

        if ' ' in kwargs[key]:
            dup_req(key, kwargs[key].split(' '))
        elif '+' in kwargs[key]:
            dup_req(key, kwargs[key].split('+'))
        elif ',' in kwargs[key]:
            dup_req(key, kwargs[key].split(','))
        else:
            dup_req(key, [kwargs[key]])
            

        
        for req in reqs:
            req['conn'] = self.conn
            if klass == None:
                raise tornado.web.HTTPError(400, "How'd you get here?")
            elif method == None:
                ret = GeoHandler.dmerge(ret, klass.lookup(**req))
            elif method in klass.methods():
                ret = GeoHandler.dmerge(ret, getattr(klass, method.replace("-","_"))(**req))
            else:
                raise tornado.web.HTTPError(400, ("%s is not valid" % method))


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
    (r"^/state/(?P<statefp>[\d\+]+)/(?P<lookup>congressional-district)/(?P<cd113fp>[\d\+]+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/state/(?P<statefp>[\d\+]+)/county/(?P<countyfp>[\d\+]+)/(?P<lookup>subdivision)/(?P<cousubfp>[\d\+]+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/state/(?P<statefp>[\d\+]+)/(?P<lookup>county)/(?P<countyfp>[\d\+]+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/state/(?P<statefp>[\d\+]+)/(?P<lookup>lower-house-district)/(?P<sldlst>[\d\+]+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/state/(?P<statefp>[\d\+]+)/(?P<lookup>upper-house-district)/(?P<sldust>[\d\+]+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/state/(?P<statefp>[\d\+]+)/(?P<lookup>school-district)/(?P<unsdlea>[\d\+]+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/(?P<lookup>state)/(?P<statefp>[\d\+]+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/(?P<lookup>address)/(?P<address>.+)(?P<extra>/(?P<method>.+))?$", GeoHandler,dict(conn=conn)),
    (r"^/(?P<key>funcstat|classfp|lsad|mtfcc)(?P<extra>/(?P<val>.*))?", FIPSHandler)
])

port = 8000

application.listen(port)
print("Listening on %d" % port)
tornado.ioloop.IOLoop.instance().start()
