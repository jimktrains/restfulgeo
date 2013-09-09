import psycopg2
import psycopg2.extras

SEX_BY_AGE = "B01001"

all_stats = [SEX_BY_AGE]

fields = {
    SEX_BY_AGE: {
        "b01001001":"Total",
        "b01001002":"Male",
        "b01001003":"m_u5",
        "b01001004":"m_5_9",
        "b01001005":"m_10_14",
        "b01001006":"m_15_17",
        "b01001007":"m_18_19",
        "b01001008":"m_20",
        "b01001009":"m_21",
        "b01001010":"m_22_24",
        "b01001011":"m_25_29",
        "b01001012":"m_30_34",
        "b01001013":"m_35_39",
        "b01001014":"m_40_44",
        "b01001015":"m_45_49",
        "b01001016":"m_50_54",
        "b01001017":"m_55_59",
        "b01001018":"m_60_61",
        "b01001019":"m_62_64",
        "b01001020":"m_65_66",
        "b01001021":"m_67_69",
        "b01001022":"m_70_74",
        "b01001023":"m_75_79",
        "b01001024":"m_80_84",
        "b01001025":"m_85_Over",
        "b01001026":"Female",
        "b01001027":"f_U5",
        "b01001028":"f_5_9",
        "b01001029":"f_10_14",
        "b01001030":"f_15_17",
        "b01001031":"f_18_19",
        "b01001032":"f_20",
        "b01001033":"f_21",
        "b01001034":"f_22_24",
        "b01001035":"f_25_29",
        "b01001036":"f_30_34",
        "b01001037":"f_35_39",
        "b01001038":"f_40_44",
        "b01001039":"f_45_49",
        "b01001040":"f_50_54",
        "b01001041":"f_55_59",
        "b01001042":"f_60_61",
        "b01001043":"f_62_64",
        "b01001044":"f_65_66",
        "b01001045":"f_67_69",
        "b01001046":"f_70_74",
        "b01001047":"f_75_79",
        "b01001048":"f_80_84",
        "b01001049":"f_o85",
    }
}

def sex_by_age(conn, stat, statefp, countyfp = None, cousubfp = None):
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

    print(cur.query)

    for k in row:
        print(k)
        print(k in fields[stat])
        if k in fields[stat]:
            row[fields[stat][k]] = row[k];
            del row[k]
    print("---------------")
    print(row)


conn = psycopg2.connect("dbname=jim user=jim password=jim")
sex_by_age(stat=SEX_BY_AGE, statefp='42', countyfp='003', cousubfp='', conn=conn)
