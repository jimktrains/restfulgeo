import models
from geocoder import *

def lookup(address, conn):
    point = geocode(address, conn)
    return models.Point.lookup(conn=conn, **point)
