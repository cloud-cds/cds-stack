import sys,os
import json

def convert_salmeterol_dose(entries, log):
    cur_path = os.path.realpath(__file__)
    transform_path = os.path.abspath(os.path.join(cur_path, os.pardir, os.pardir))

    sys.path.append(transform_path)
    from transform import default_convert_dose

    results = default_convert_dose(entries, log)
    for result in results:
        dose_json = json.loads(result[1])
        if float(dose_json['dose']) > 4.0:
            dose_json['dose'] = "4.0"
            result[1] = json.dumps(dose_json)
    return results