import mysql.connector
import sys, inspect, csv, json
from datetime import datetime

# Usage: python export.py studyId

# Parses the JSON string that belongs to the given key of a dictionary and turns it into a nested dictionary.
# If the value is 'null' the element belonging to the key is removed.
def parse_json_element(my_dict, key):
    if my_dict[key] != 'null':
        my_dict[key] = json.loads(my_dict[key])
    else:
        del my_dict[key]
    return my_dict

# Flattens a dictionary - a nested dictionary is pulled to the first level; works only for a second level dictionary
def flatten(my_dict):
    result = {}
    for key, value in my_dict.items():
        if isinstance(value, dict):
            result.update(value)
        else:
            result[key] = value
    return result

# Check we have the study ID as an argument
if len(sys.argv) <= 1:
    print("Missing argument study ID")
    sys.exit()
studyId = sys.argv[1]

dataBase = mysql.connector.connect(
  host ="localhost",
  user ="ommuser",
  passwd ="abc123",
  database = "omm_live"
)
 
cursorObject = dataBase.cursor(dictionary=True)

# const jobVariables = Database.select('jobs.id as job_id', aggVars)
#   .from('jobs')
#   .leftJoin('job_variable', 'jobs.id', 'job_variable.job_id')
#   .leftJoin('variables', 'variables.id', 'job_variable.variable_id')
#   .where('jobs.study_id', this.id)
#   .groupBy('jobs.id')

#    return await Database
#      .select(
#        'jobs.id as job_id', 'jobs.position', 'jobs.study_id',
#        'job_statuses.name as status', 'participants.identifier as participant',
#        'job_states.data', 'job_variables.trial_vars', 'participants.meta')
#      .from('jobs')
#      .leftJoin('job_states', 'jobs.id', 'job_states.job_id')
#      .leftJoin('participants', 'job_states.participant_id', 'participants.id')
#      .leftJoin('job_statuses', 'job_states.status_id', 'job_statuses.id')
#      .leftJoin(jobVariables.as('job_variables'), 'jobs.id', 'job_variables.job_id')
#      .where('jobs.study_id', this.id)
#      .whereNotNull('job_states.data')

query = f'''
SELECT 
    jobs.id AS 'job_id', jobs.position, jobs.study_id, 
    job_statuses.name AS status, participants.identifier AS participant,
    participants.meta, job_states.data, job_variable.value
FROM jobs
LEFT JOIN job_states ON jobs.id = job_states.job_id AND job_states.data IS NOT NULL
LEFT JOIN participants ON job_states.participant_id = participants.id
LEFT JOIN job_statuses ON job_states.status_id = job_statuses.id
LEFT JOIN job_variable ON jobs.id = job_variable.job_id
WHERE jobs.study_id = {studyId} AND job_states.data IS NOT NULL
;
'''

cursorObject.execute(query)
rows = cursorObject.fetchall()

# Parse JSON elements 'meta' and 'data'
rows = map(lambda d: parse_json_element(d, 'meta'), rows)
#rows = map(lambda d: parse_json_element(d, 'data'), rows)
# Flatten the dictionary
rows_cleaned = list(map(flatten, rows))

# Write data to CSV file
filename = "data_" + datetime.today().strftime('%Y%m%d%H%M%S') + ".csv"
with open(filename, "w", newline="") as f:
    w = csv.DictWriter(f, rows_cleaned[0].keys())
    w.writeheader()
    for row in rows_cleaned:
        print(row)
        w.writerow(row)

dataBase.close()



