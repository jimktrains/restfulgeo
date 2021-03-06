import models

def dmerge(a, b):
    for k, v in b.items():
        if isinstance(v, dict) and k in a:
            dmerge(a[k], v)
        elif isinstance(v, list) and k in a:
            for i in v:
                a[k].append(i)
        else:
            a[k] = v 
    return a

def lookup(lat, lon, conn):
    point = {"lat":float(lat), "lon":float(lon)}
    ret = models.State.by_point(point, conn)
    ret = models.Point.dmerge(ret, models.CongressionalDistrict.by_point(point, conn))
    ret = models.Point.dmerge(ret, models.CountySubdivision.by_point(point, conn))
    ret = models.Point.dmerge(ret, models.SchoolDistrict.by_point(point, conn))
    ret = models.Point.dmerge(ret, models.LowerHouse.by_point(point, conn))
    ret = models.Point.dmerge(ret, models.UpperHouse.by_point(point, conn))

    return ret

def methods():
    return []
