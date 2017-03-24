import json
import confidence

def load_discharge_json(entry, log):
    tsp = entry['HOSP_DISCH_TIME']
    dept = entry['DischargeDepartment']
    disposition = entry['DischargeDisposition']
    result = [tsp,
              json.dumps({'department':dept, 'disposition': disposition}),
              confidence.NO_TRANSFORM]
    return result