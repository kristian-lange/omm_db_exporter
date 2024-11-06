#!/usr/bin/env python3

"""
Exports job data into a file in CSV format

Usage: ./export.py studyId
Parameter: studyId - the ID of the study
"""

import mysql.connector
import sys, inspect, csv, json
from datetime import datetime

DB_HOST = "localhost"
DB_USER = "ommuser"
DB_PASSWORD = "abc123"
DB_DATABASE = "omm2"

# Parses the JSON string that belongs to the given key of a dictionary and turns it into a nested dictionary.
# If the value is 'null' the element belonging to the key is removed.
def parse_json_element(my_dict, key):
    if my_dict[key] != 'null':
        my_dict[key] = json.loads(my_dict[key])
    else:
        del my_dict[key]
    return my_dict

# Flattens a dictionary - a nested dictionary is pulled to the first level; works only with second level dictionaries
def flatten(my_dict):
    result = {}
    for key, value in my_dict.items():
        if isinstance(value, dict):
            result.update(value)
        else:
            result[key] = value
    return result

# Writes the given data to a file in CSV format
def write_to_file(rows):
    keys = set()
    for row in rows:
        keys.update(row.keys())

    filename = "data_" + datetime.today().strftime('%Y%m%d%H%M%S') + ".csv"
    with open(filename, "w", newline="") as f:
        w = csv.DictWriter(f, keys)
        w.writeheader()
        for row in rows:
            w.writerow(row)
    print(f'Filename: {filename}')
    print(f'Number keys: {len(keys)}')
    print(f'Number rows: {len(rows)}')
    print(f'Keys: {keys}')


# Check we have the study ID as an argument
if len(sys.argv) <= 1:
    print("Missing argument study ID")
    sys.exit()
studyId = sys.argv[1]



dataBase = mysql.connector.connect(host = DB_HOST, user = DB_USER, passwd = DB_PASSWORD, database = DB_DATABASE)
cursorObject = dataBase.cursor(dictionary=True)

query = f'''
SELECT
    study_id, data
FROM job_results
WHERE study_id = {studyId};
;'''

cursorObject.execute(query)

rows = cursorObject.fetchall()

# Parse JSON element 'data' and 'meta'
rows_data_parsed = map(lambda d: parse_json_element(d, 'data'), rows)
# Flatten the dictionary in the 'data' field so all fields are on the same level
rows_flattened = list(map(flatten, rows_data_parsed))
#print(list(rows_flattened))
# Write data to CSV file
write_to_file(rows_flattened)

dataBase.close()
