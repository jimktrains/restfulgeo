from copy import deepcopy
import sys
import psycopg2
import psycopg2.extras
import json

filename='Sequence_Number_and_Table_Number_Lookup_NOT_PERFECT.json'

base_fields = None
             
with open(filename, 'r') as f:
    base_fields = json.loads(f.read())

def fields_to_names(fields, fid, val):
    for field in fields:
        if field['id'].upper() == fid.upper():
            field['value'] = val
        if 'fields' in field and field['fields'] is not None:
            x = fields_to_names(field['fields'], fid, val)
            if x is not None:
                x['value'] = val
    return None

# NOTE: fields is modified
def get_stat(conn, stat, fields, statefp, countyfp = None, cousubfp = None):
    sql = "SELECT stat.* \
           FROM "+stat+" as stat \
               INNER JOIN geoheader ON stat.logrecno = geoheader.logrecno AND stat.stusab = geoheader.stusab \
           WHERE geoheader.state = %(statefp)s "

    if countyfp:
        sql += "AND geoheader.county = %(countyfp)s "
    else:
        sql += "AND geoheader.county IS NULL "
    if cousubfp:
        sql += "AND geoheader.cousub = %(cousubfp)s "
    else:
        sql += "AND geoheader.cousub IS NULL "

    sql += "AND geoheader.place is NULL \
        AND geoheader.tract IS NULL \
        AND geoheader.blkgrp IS NULL \
        AND geoheader.concit IS NULL  \
        AND geoheader.aianhh IS NULL  \
        AND geoheader.aianhhfp IS NULL  \
        AND geoheader.aihhtli IS NULL  \
        AND geoheader.aitsce IS NULL  \
        AND geoheader.aits IS NULL  \
        AND geoheader.anrc IS NULL  \
        AND geoheader.cbsa IS NULL  \
        AND geoheader.csa IS NULL  \
        AND geoheader.metdiv IS NULL  \
        AND geoheader.macc IS NULL  \
        AND geoheader.memi IS NULL  \
        AND geoheader.necta IS NULL  \
        AND geoheader.cnecta IS NULL  \
        AND geoheader.nectadiv IS NULL  \
        AND geoheader.ua IS NULL  \
        AND geoheader.blank1 IS NULL  \
        AND geoheader.cdcurr IS NULL  \
        AND geoheader.sldu IS NULL  \
        AND geoheader.sldl IS NULL  \
        AND geoheader.blank2 IS NULL  \
        AND geoheader.blank3 IS NULL  \
        AND geoheader.blank4 IS NULL  \
        AND geoheader.submcd IS NULL  \
        AND geoheader.sdelm IS NULL  \
        AND geoheader.sdsec IS NULL  \
        AND geoheader.sduni IS NULL  \
        AND geoheader.ur IS NULL  \
        AND geoheader.pci IS NULL  \
        AND geoheader.blank5 IS NULL  \
        AND geoheader.blank6 IS NULL  \
        AND geoheader.puma5 IS NULL  \
        AND geoheader.blank7 IS NULL"


    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, { "statefp": statefp, "countyfp": countyfp, "cousubfp": cousubfp } )
    row = cur.fetchone()

   
    for r in row:
        fields_to_names(fields, r, row[r])
    # Edits fields and therefor returns nothing


#conn = psycopg2.connect("dbname=jim user=jim password=jim")
def get_all_stats(conn, statefp, countyfp = None, cousubfp = None):
    if cousubfp is None:
        cousubfp = ''
    if countyfp is None:
        countyfp = ''
    fields = deepcopy(base_fields)
    for cat in fields:
        for table in fields[cat]:
            try:
                get_stat(stat=table, fields=fields[cat][table]['fields'], statefp='42', countyfp='003', cousubfp='', conn=conn)
            except psycopg2.ProgrammingError as e:
                print(e, file=sys.stderr)
            except Exception as e:
                print(e, file=sys.stderr)
                conn.rollback()
    return fields
