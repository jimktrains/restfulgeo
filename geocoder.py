import math
import re
import psycopg2
from address_parser import *

def geocode(address, conn):
    address = string_to_address(address, conn)

    sql = "SELECT to_number(addr.lfromhn, '9999999'), to_number(addr.ltohn, '9999999'), to_number(addr.rfromhn, '9999999'), to_number(addr.rtohn, '9999999'), St_AsText(ST_StartPoint(ST_LineMerge(addr.geom))) as start, ST_AsText(ST_EndPoint(ST_LineMerge(addr.geom))) as end \
           FROM tl_2013_42_addfeat as addr \
               INNER JOIN tl_2013_42_featnames as feat on feat.tlid = addr.tlid \
           WHERE (addr.zipr = %(zipcode)s OR addr.zipl = %(zipcode)s) AND \
               ( levenshtein(%(street)s, feat.name) < 3 OR difference(%(street)s, feat.name) > 2) AND \
               ( (to_number(addr.lfromhn, '9999999') <= to_number(%(street_number)s, '9999999') AND to_number(addr.ltohn, '9999999') >= to_number(%(street_number)s, '9999999')) OR \
                 (to_number(addr.rfromhn, '9999999') <= to_number(%(street_number)s, '9999999') AND to_number(addr.rtohn, '9999999') >= to_number(%(street_number)s, '9999999')) OR  \
                 (to_number(addr.lfromhn, '9999999') >= to_number(%(street_number)s, '9999999') AND to_number(addr.ltohn, '9999999') <= to_number(%(street_number)s, '9999999')) OR  \
                 (to_number(addr.rfromhn, '9999999') >= to_number(%(street_number)s, '9999999') AND to_number(addr.rtohn, '9999999') <= to_number(%(street_number)s, '9999999')) )"

    cur = conn.cursor()
    cur.execute(sql, address)

    row = cur.fetchone()

    l1 = int(row[0])
    l2 = int(row[1])

    r1 = int(row[2])
    r2 = int(row[3])

    n = int(re.sub(r"\D", '', address['street_number']))

    s1 = None
    s2 = None
    # which side?
    if (n <= l1 and m >= l2) or (n >= l1 and n <= l2):
        s1 = min(l1, l2)
        s2 = max(l1, l2)
    else:
        s1 = min(r1, r2)
        s2 = max(r1, r2)

    # Small enough scale that the sphericalness isn't a big deal
    match = re.search(r"([+-]?\d+\.\d+)\s+([+-]?\d+\.\d+)", row[4])
    x1 = float(match.group(1))
    y1 = float(match.group(2))

    match = re.search(r"([+-]?\d+\.\d+)\s+([+-]?\d+\.\d+)", row[5])
    x2 = float(match.group(1))
    y2 = float(match.group(2))

    c = math.hypot(x2-x1, y2-y1)
    a = math.hypot(x2-x1, 0)
    theta = math.acos(a/c)

    length = 1 - ( (float(s2) - float(n))/float(s1) )
    xf = x1 + math.sin(theta)*length*c
    yf = y1 + math.cos(theta)*length*c
    
    return {'lon':xf, 'lat': yf}

                
