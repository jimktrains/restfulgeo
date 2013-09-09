import csv
import re
import json

filename = "Sequence_Number_and_Table_Number_Lookup.txt"

stats = {}

cur_table = None
cur_prefix = ''

invalid_prefix = re.compile(r"(^\s*Total\s*:)|(^\s*Universe.*:)")

cnt = 0
with open(filename, 'r') as csvfile:
    csvfile = csv.DictReader(csvfile)
    for row in csvfile:
        if row['cells'] != '':
            cur_table = row['Long Table Title']
            cur_prefix = ''
            stats[row['Long Table Title']]  = {
                'table': row['Table ID'],
                'seq': row['seq'],
                'fields': {}
            }
        elif row['Line Number Decimal M Lines'] != '':
            if not invalid_prefix.match(row['Long Table Title']):
                if row['Long Table Title'].find(":") > 0:
                    cur_prefix = row['Long Table Title']
                    stats[cur_table]['fields'][row['Long Table Title']] = {
                       'line': row['Line Number Decimal M Lines'],
                       'fields': {}
                    }
                else:
                    if cur_prefix == '':
                        stats[cur_table]['fields'][row['Long Table Title']] = row['Line Number Decimal M Lines']
                    else:
                        stats[cur_table]['fields'][cur_prefix]['fields'][row['Long Table Title']] = row['Line Number Decimal M Lines']
            else:
                stats[cur_table]['fields'][row['Long Table Title']] = row['Line Number Decimal M Lines']

print(json.dumps(stats))
