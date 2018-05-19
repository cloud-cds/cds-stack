""" transform.py
list all transform functions
"""

import json, sys, traceback
import etl.mappings.confidence as confidence
from datetime import datetime, timedelta
from etl.transforms.primitives.row.load_discharge_json import *
from etl.transforms.primitives.row.convert_gender_to_int import *
from etl.transforms.primitives.row.convert_proc_to_boolean import *
from collections import OrderedDict
from datetime import datetime as dt
import pandas as pd

MED_ROUTE_CONTINUOUS = ['Intravenous']
# GIVEN_ACTIONS = ['Given', 'New Bag', 'Restarted', 'Bolus from Bag',
#                  'Anesthesia Bolus', 'Given by Other', 'Started During Downtime']
GIVEN_ACTIONS = ['Given', 'Given by Other', 'Given During Downtime', 'New Bag/Given']
IV_START_ACTIONS = ['New Bag', 'Restart', 'Restarted', 'New Bag/Given']
STOPPED_ACTIONS = ['Stopped', "D/C'd"]
RATE_ACTIONS = ['Rate Change', 'Rate Verify', 'Rate/Dose Verify',
                'Rate/Dose Change']
CHECKPOINT_ACTIONS = ['Handoff', 'Handoff ']

IGNORED_ACTIONS = ['Rate Change', 'Stopped', 'Missed', 'Canceled Entry',
                   'Anesthesia Volume Adjustment', 'Paused', 'MAR Hold',
                   'MAR Unhold', 'Rate Verify', 'Handoff', 'Refused',
                   'Rate/Dose Verify ', 'Automatically Held', 'Override Pull',
                   'Rate/Dose Change ']

MIMIC_GIVEN_ACTIONS = ['GIVEN', 'givne', 'gvien', 'givrn', 'goven', 'givenod',
'YES', 'yes', 'fiven', 'Given', 'adm', 'on', 'pplied', 'applied', 'Applied',
'pplied.', 'DONE', 'done', 'vigor', 'vig ', 'present', 'Sent']
MIMIC_GIVEN_ACTIONS_PREFIX = ['given', '<']


FLUID_DUR = 3600 # normal fluid duration is 1 hour

VENT_DURATION = 5 # default vent duration is 5 hours

def transform(fid, func_id, entry, output_data_type, log):
    """
    ouput: [value, confidence]
    """
    if func_id:
        # print "transform"
        this_mod = sys.modules[__name__]
        func = getattr(this_mod, func_id, log)
        try:
            return func(entry, log)
        except Exception as e:
            log.exception('transform function %s: Invalid data entry %s' \
                % (func_id, entry))
            log.error(e, exc_info=True)
            # traceback.print_exc()
            # return None
    else:
        """ dummy function"""
        try:
            value = None
            if isinstance(entry, dict):
                value = entry['value']
            else:
                if len(entry) == 5:
                    # entry = [enc_id, tsp, name, value, unit]
                    # value is the second last item in entry
                    value = entry[-2]
                elif len(entry) == 6:
                    # entry = [enc_id, tsp, name, value, unit, action]
                    # value is the second last item in entry
                    value = entry[-3]
                else:
                    # value is always the last item
                    value = entry[-1]
            try:
                if value is None:
                    return None
                elif output_data_type == 'Integer':
                    value = int(value)
                elif output_data_type == 'Real':
                    value = float(value)
                elif output_data_type == 'Boolean':
                    value = bool(value)
            except:
                log.warn("transform function dummy: CastError %s %s" \
                    % (output_data_type, value))
                return None
            return [value, confidence.NO_TRANSFORM]
        except:
            log.exception(\
                'transform function dummy: Invalid data entry %s' % entry)
            return None

def log_assert(log, _bool, message):
    try:
        assert _bool, message
    except:
        log.exception("AssertError: " + message)

def get_diastolic(entry, log):
    if entry is None:
        return None
    value = entry[-1]
    if value is not None:
        log_assert(log, '/' in value, \
            'The format of blood pressure is not systolic/diastolic')
        value_int = int(value[value.index('/')+1:])

        result = threshold_nbp_dias(value_int, log)
        if result is not None:
            return result

def get_systolic(entry, log):
    if entry is None:
        return None
    value = entry[-1]
    if value is not None:
        log_assert(log, '/' in value, \
            'The format of blood pressure is not systolic/diastolic')
        value_int = int(value[:value.index('/')])
        result = threshold_nbp_sys(value_int, log)
        if result is not None:
                return result

def convert_ounces_to_kg(entry, log):
    value = entry[-1]
    if value is None:
        return None
    else:
        result = threshold_weight(float(value)*0.0283495231, log)
        if result:
            if result[1] == confidence.NO_TRANSFORM:
                result[1] = confidence.UNIT_TRANSFORMED
            return result

def convert_age_to_int(entry, log):
    value = entry[-1]
    if value is not None:
        if value.endswith("+"):
            # for example, if value is 90+, return 90
            return [int(value[:-1]), confidence.VALUE_TRANSFORMED]
        else:
            return [int(value), confidence.NO_TRANSFORM]


def cast_string_to_real(entry, log):
    value = entry[-1]
    if value is not None:
        try:
            if value.startswith('<') or value.startswith('>'):
                return [float(value[1:]), confidence.VALUE_TRANSFORMED]
            else:
                return [float(value), confidence.NO_TRANSFORM]
        except:
            log.warn("Invalid string_to_real entry: %s" % entry)
            # traceback.print_exc(file=sys.stdout)
            return None

def convert_lab_value_to_real(entry, log):
    value = entry['ResultValue']
    try:
        if value.startswith('<') or value.startswith('>'):
            return [float(value[1:]), confidence.VALUE_TRANSFORMED]
        else:
            return [float(value), confidence.NO_TRANSFORM]
    except:
        log.warn("Invalid convert_lab_value_to_real entry: %s" % entry)
        # traceback.print_exc(file=sys.stdout)
        return None

def convert_lymph_abs_to_real(entry, log):
    value = entry['ResultValue']
    unit = entry['REFERENCE_UNIT']
    try:
        if unit == 'cells/uL':
            return [float(value/1000.0), confidence.VALUE_TRANSFORMED]
        else:
            return [float(value), confidence.NO_TRANSFORM]
    except:
        log.warn("Invalid convert_lab_value_to_real entry: %s" % entry)
        # traceback.print_exc(file=sys.stdout)
        return None

def convert_to_ngdl(entry, log):
    value = entry['ResultValue']
    unit = entry['REFERENCE_UNIT']
    value_num = 0
    conf = confidence.NO_TRANSFORM
    try:
        if value.startswith('<') or value.startswith('>'):
            value_num = float(value[1:])
            conf = confidence.VALUE_TRANSFORMED
        else:
            value_num = float(value)
        if unit == 'ng/mL' or unit == 'ng/dL':
            if unit == 'ng/dL':
                value_num /= 100
            if value_num > 0:
                return [value_num, conf]
        else:
            log.warn("convert_to_ngdl: Unknown unit %s" % unit)
    except:
        log.warn("Invalid string_to_real entry %s" % entry)
        # traceback.print_exc(file=sys.stdout)
        return None

def convert_arterial_ph_to_real(entry, log):
    result = cast_string_to_real(entry, log)
    if result:
        value_and_conf = threshold_arterial_ph(result[0], log)
        if value_and_conf:
            return result


def convert_bun_to_real(entry, log):
    result = cast_string_to_real(entry, log)
    if result:
        value_and_conf = threshold_bun(result[0], log)
        if value_and_conf:
            return result

def transform_potassium(entry, log):
    result = convert_to_mmol(entry, log)
    if result:
        value_and_conf = threshold_potassium(result[0], log)
        if value_and_conf:
            return result

def transform_ddimer(entry, log):
    result = convert_ddimer_unit(entry, log)
    if result:
        return result

def transform_sodium(entry, log):
    result = convert_to_mmol(entry, log)
    if result:
        value_and_conf = threshold_sodium(result[0], log)
        if value_and_conf:
            return result


def convert_co2_to_real(entry, log):
    result = cast_string_to_real(entry, log)
    if result:
        value_and_conf = threshold_co2(result[0], log)
        if value_and_conf:
            return result


def convert_lactate_to_real(entry, log):
    result = cast_string_to_real(entry, log)
    if result:
        value_and_conf = threshold_lactate(result[0], log)
        if value_and_conf:
            return result


def convert_inr_to_real(entry, log):
    result = cast_string_to_real(entry, log)
    if result:
        value_and_conf = threshold_inr(result[0], log)
        if value_and_conf:
            return result


def convert_paco2_to_real(entry, log):
    result = cast_string_to_real(entry, log)
    if result:
        value_and_conf = threshold_paco2(result[0], log)
        if value_and_conf:
            return result


def convert_pao2_to_real(entry, log):
    result = cast_string_to_real(entry, log)
    if result:
        value_and_conf = threshold_pao2(result[0], log)
        if value_and_conf:
            return result


def convert_creatinine_to_real(entry, log):
    result = cast_string_to_real(entry, log)
    if result:
        value_and_conf = threshold_creatinine(result[0], log)
        if value_and_conf:
            return result


AMOXIL = {
    "amoxicillin (AMOXIL) 125 mg/5 mL suspension 500 mg" : 125,
    "amoxicillin (AMOXIL) 250 mg/5 mL suspension 500 mg" : 250,
    "amoxicillin (AMOXIL) 250 mg/5 mL suspension 900 mg" : 250,
    "amoxicillin (AMOXIL) capsule 1,000 mg" : 1000,
    "amoxicillin (AMOXIL) capsule 2,000 mg" : 2000,
    "amoxicillin (AMOXIL) capsule 250 mg" : 250,
    "amoxicillin (AMOXIL) capsule 500 mg" : 500,
    "amoxicillin (AMOXIL) tablet 875 mg" : 875,
    "amoxicillin-clavulanate (AUGMENTIN) 125-31.25 mg/5 mL suspension 500 mg" : 125,
    "amoxicillin-clavulanate (AUGMENTIN) 200-28.5 mg per chewable tablet 900 mg" : 200,
    "amoxicillin-clavulanate (AUGMENTIN) 250-125 mg per tablet 1 tablet" : 250,
    "amoxicillin-clavulanate (AUGMENTIN) 250-125 mg per tablet 2 tablet" : 250,
    "amoxicillin-clavulanate (AUGMENTIN) 400-57 mg per chewable tablet 400 mg" : 400,
    "amoxicillin-clavulanate (AUGMENTIN) 400-57 mg/5 mL suspension 10.9 mL" : 400,
    "amoxicillin-clavulanate (AUGMENTIN) 400-57 mg/5 mL suspension 875 mg" : 400,
    "amoxicillin-clavulanate (AUGMENTIN) 500-125 mg per tablet 1 tablet" : 500,
    "amoxicillin-clavulanate (AUGMENTIN) 875-125 mg per tablet 1 tablet" : 875,
    "amoxicillin-clavulanate (AUGMENTIN) 875-125 mg per tablet 875 mg" : 875,
    "amoxicillin-clavulanate (AUGMENTIN) 250-125 mg per tablet: Dose: 1 tablet" : 250,
    "amoxicillin-clavulanate (AUGMENTIN) 875-125 mg per tablet: Dose: 1 tablet" : 875
}

def convert_amoxicillin_dose_to_mg(entries, log):
    global AMOXIL
    global GIVEN_ACTIONS
    results = []
    for entry in entries:
        name = entry['display_name']
        tsp = entry['TimeActionTaken']
        dose = entry['Dose']
        unit = entry['MedUnit']
        action = entry['ActionTaken']
        order_tsp = entry['ORDER_INST']
        if action in GIVEN_ACTIONS:
            if unit == 'tablet':
                results.append([tsp, \
                    json.dumps({'dose':AMOXIL[name]*int(dose), \
                        'order_tsp': order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                        'action': action}), \
                    confidence.UNIT_TRANSFORMED])
            else:
                result = _convert_to_mg(entry, log)
                if result:
                    results.append(result)
        else:
            log.warn(\
                "convert_amoxicillin_dose_to_mg: non given action: %s" % \
                    action)
    return results

def convert_dose_to_mg_and_medroute(entries, log):
    global GIVEN_ACTIONS
    results = []

    for entry in entries:
        name = entry['display_name']
        tsp  = entry['TimeActionTaken']
        dose = entry['Dose']
        unit = entry['MedUnit']
        order_tsp = entry['ORDER_INST']
        action = entry['ActionTaken']
        if 'injection' in name:
            medroute = 'Injection'
        elif 'capsule' in name:
            medroute = 'Capsule'
        elif 'tablet' in name:
            medroute = 'Tablet'
        else:
            medruote = ''

        if action in GIVEN_ACTIONS:
            if dose is None:
                results.append(None)
                log_assert(log, unit == 'kg' or unit == 'g' or unit == 'mg' or unit == 'mcg', "Unknown unit %s" % unit)
            elif unit == 'kg':
                results.append([tsp,
                        json.dumps({'dose': 1000*1000*float(dose), \
                            'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                            'action': entry['ActionTaken'],
                            'med_route': medroute}),
                        confidence.UNIT_TRANSFORMED])
            elif unit == 'g':
                results.append([tsp,
                        json.dumps({'dose':1000*float(dose), \
                            'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                            'action': entry['ActionTaken'],
                            'med_route': medroute}),
                        confidence.UNIT_TRANSFORMED])
            elif unit == 'mg':
                results.append([tsp,
                        json.dumps({'dose':float(dose), \
                            'order_tsp': order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                            'action': entry['ActionTaken'],
                            'med_route': medroute}),
                        confidence.NO_TRANSFORM])
            elif unit == 'mcg':
                results.append([tsp,
                        json.dumps({'dose':0.001*float(dose), \
                            'order_tsp': order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                            'action': entry['ActionTaken'],
                            'med_route': medroute}),
                        confidence.NO_TRANSFORM])
        else:
            log.warn(\
                "convert_dose_to_mg_and_medroute: non given action: %s" % \
                    action)
    return results


def convert_penicillin_dose(entries, log):
    # convert mg to million unit
    global GIVEN_ACTIONS, IV_START_ACTIONS
    intake_actions = GIVEN_ACTIONS + IV_START_ACTIONS
    results = []
    for entry in entries:
        name = entry['display_name']
        tsp = entry['TimeActionTaken']
        dose = entry['Dose']
        unit = entry['MedUnit']
        action = entry['ActionTaken']
        order_tsp = entry['ORDER_INST']
        if action in intake_actions:
            if dose:
                if unit == 'mg':
                    dose_mu = float(dose) / 250 * 0.4
                    dose_json = json.dumps({'dose':dose_mu, \
                        'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                        'action': entry['ActionTaken']})


                    results.append([tsp, dose_json, confidence.NO_TRANSFORM])
                elif unit == 'Million Units':
                    dose_json = json.dumps({'dose':dose, \
                        'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                        'action': entry['ActionTaken']})


                    results.append([tsp, dose_json, confidence.NO_TRANSFORM])
                else:
                    self.warn("Invalid unit convert_penicillin_dose: %s" % unit)
    return results

def convert_penicillin_g_dose(entries, log):
    global IV_START_ACTIONS
    results = []
    for entry in entries:
        name = entry['display_name']
        tsp = entry['TimeActionTaken']
        dose = entry['Dose']
        unit = entry['MedUnit']
        action = entry['ActionTaken']
        order_tsp = entry['ORDER_INST']
        if action in IV_START_ACTIONS:
            if dose:
                log_assert(log, unit == "Million Units", "Invalid Unit %s" % unit)
                dose_json = json.dumps({'dose':dose, \
                    'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                    'action': entry['ActionTaken']})

                results.append([tsp, dose_json, confidence.NO_TRANSFORM])
    return results

def _process_paused_med_action(start_event, pause_event, log):
    log_assert(log, start_event['Dose'], "No dose!")
    log_assert(log, start_event['mar_duration'], "No duration!")
    log_assert(log, start_event['MAR_DURATION_UNIT'], "No duration unit!")
    real_duration_secs = (pause_event['TimeActionTaken'] \
        - start_event['TimeActionTaken']).total_seconds()
    if start_event["MAR_DURATION_UNIT"] == "minutes":
        duration_secs = float(start_event['mar_duration']) * 60
    elif start_event["MAR_DURATION_UNIT"] == "hours":
        duration_secs = float(start_event['mar_duration']) * 3600
    if real_duration_secs < duration_secs:
        # Record is read only
        start_event = OrderedDict(start_event)
        start_event['Dose'] = real_duration_secs / duration_secs \
            * float(start_event['Dose'])
    return start_event

def convert_clindamycin_dose_to_mg(entries, log):
    global STOPPED_ACTIONS
    global GIVEN_ACTIONS
    global IV_START_ACTIONS
    results = []
    entry_pre = None
    for entry in entries:
        if entry_pre:
            # check current entry
            if entry['ActionTaken'] in STOPPED_ACTIONS and \
                (entry_pre['ActionTaken'] in GIVEN_ACTIONS or \
                    entry_pre['ActionTaken'] in IV_START_ACTIONS):
                # modify entry_pre if it stopped earlier than expect
                entry_pre = _process_paused_med_action(entry_pre, entry, log)
            if entry_pre['ActionTaken'] in GIVEN_ACTIONS or \
                entry_pre['ActionTaken'] in IV_START_ACTIONS:
                result = _convert_to_mg(entry_pre, log)
                if result:
                    results.append(result)
        entry_pre = entry
    # last one
    if entry_pre['ActionTaken'] in GIVEN_ACTIONS or \
        entry_pre['ActionTaken'] in IV_START_ACTIONS:
        result = _convert_to_mg(entry_pre, log)
        if result:
            results.append(result)
    return results


def convert_vancomycin_dose_to_mg(entries, log):
    global STOPPED_ACTIONS
    global GIVEN_ACTIONS
    global IV_START_ACTIONS
    results = []
    entry_pre = None
    for entry in entries:
        if entry_pre:
            # check current entry
            if entry['ActionTaken'] in STOPPED_ACTIONS and \
                (entry_pre['ActionTaken'] in GIVEN_ACTIONS or \
                    entry_pre['ActionTaken'] in IV_START_ACTIONS):
                # modify entry_pre if it stopped earlier than expect
                entry_pre = _process_paused_med_action(entry_pre, entry, log)
            if entry_pre['ActionTaken'] in GIVEN_ACTIONS or \
                entry_pre['ActionTaken'] in IV_START_ACTIONS:
                result = _convert_to_mg(entry_pre, log)
                if result:
                    results.append(result)
        entry_pre = entry
    # last one
    if entry_pre['ActionTaken'] in GIVEN_ACTIONS or \
        entry_pre['ActionTaken'] in IV_START_ACTIONS:
        result = _convert_to_mg(entry_pre, log)
        if result:
            results.append(result)
    return results

def piperacillin_tazobac_dose_to_mg(entries, log):
    global STOPPED_ACTIONS
    global GIVEN_ACTIONS
    global IV_START_ACTIONS
    results = []
    entry_pre = None
    for entry in entries:
        if entry_pre:
            # check current entry
            if entry['ActionTaken'] in STOPPED_ACTIONS and \
                (entry_pre['ActionTaken'] in GIVEN_ACTIONS or \
                    entry_pre['ActionTaken'] in IV_START_ACTIONS):
                # modify entry_pre if it stopped earlier than expect
                entry_pre = _process_paused_med_action(entry_pre, entry, log)
            if entry_pre['ActionTaken'] in GIVEN_ACTIONS or \
                entry_pre['ActionTaken'] in IV_START_ACTIONS:
                if entry_pre['MedUnit'] == 'mg of piperacillin' and \
                    float(entry_pre['Dose']) > 0:
                    dose_json = json.dumps({'dose':float(entry_pre['Dose']), \
                        'order_tsp':entry_pre['ORDER_INST'].strftime("%Y-%m-%d %H:%M:%S"),
                        'action': entry_pre['ActionTaken']})
                    results.append([entry_pre['TimeActionTaken'], \
                        dose_json, confidence.UNIT_TRANSFORMED])
                else:
                    result = _convert_to_mg(entry_pre, log)
                    if result:
                        results.append(result)
        entry_pre = entry
    # last one
    if entry_pre['ActionTaken'] in GIVEN_ACTIONS or \
        entry_pre['ActionTaken'] in IV_START_ACTIONS:
        if entry_pre['MedUnit'] == 'mg of piperacillin' and \
            entry_pre['Dose'] > 0:
            dose_json = json.dumps({'dose':float(entry_pre['Dose']), \
                'order_tsp':entry_pre['ORDER_INST'].strftime("%Y-%m-%d %H:%M:%S"),
                'action': entry_pre['ActionTaken']})
            results.append([entry_pre['TimeActionTaken'], dose_json, \
                confidence.UNIT_TRANSFORMED])
        else:
            result = _convert_to_mg(entry_pre, log)
            if result:
                results.append(result)
    return results

def convert_salmeterol_dose(entries, log):
    results = default_convert_dose(entries, log)
    for result in results:
        dose_json = json.loads(result[1])
        if float(dose_json['dose']) > 4.0:
            dose_json['dose'] = "4.0"
            result[1] = json.dumps(dose_json)
    return results

def default_convert_dose(entries, log):
    global GIVEN_ACTIONS, IV_START_ACTIONS
    results = []
    for entry in entries:
        action = entry['ActionTaken']
        if action in GIVEN_ACTIONS or IV_START_ACTIONS:
            result = _default_convert_dose(entry, log)
            if result:
                results.append(result)
        else:
            log.warn("convert_to_mg: non given action: %s" % action)
    return results

def _default_convert_dose(entry, log):
    name = entry['display_name']
    tsp = entry['TimeActionTaken']
    dose = entry['Dose']
    unit = entry['MedUnit']
    order_tsp = entry['ORDER_INST']
    action = entry['ActionTaken']
    if dose is None:
        return None
    else:
        return [tsp,
                json.dumps({'dose':dose, \
                    'order_tsp': order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                    'action': entry['ActionTaken']}),
                confidence.NO_TRANSFORM]


def convert_to_mg(entries, log):
    global GIVEN_ACTIONS, IV_START_ACTIONS
    results = []
    for entry in entries:
        action = entry['ActionTaken']
        if action in GIVEN_ACTIONS or IV_START_ACTIONS:
            result = _convert_to_mg(entry, log)
            if result:
                results.append(result)
        else:
            log.warn("convert_to_mg: non given action: %s" % action)
    return results

def convert_inch_to_cm(entry, log):
    results = []
    value=entry['Value']
    height_cm = float(value) * 2.54
    tsp = entry['TimeTaken']
    results.append([tsp, height_cm,confidence.UNIT_TRANSFORMED])

    return results

def convert_gentamicin_dose_to_mg(entries, log):
    global GIVEN_ACTIONS
    global IV_START_ACTIONS
    intake_actions = GIVEN_ACTIONS + IV_START_ACTIONS
    results = []
    for entry in entries:
        name = entry['display_name']
        tsp = entry['TimeActionTaken']
        dose = entry['Dose']
        unit = entry['MedUnit']
        action = entry['ActionTaken']
        if action in intake_actions:
            result = _convert_to_mg(entry, log)
            if result:
                results.append(result)
        else:
            log.warn("convert_to_mg: non given action: %s" % action)
    return results

def convert_to_mcg_kg_min(entries, log):
    global GIVEN_ACTIONS
    global IV_START_ACTIONS
    global RATE_ACTIONS
    global CHECKPOINT_ACTIONS
    global STOPPED_ACTIONS
    intake_actions = GIVEN_ACTIONS + IV_START_ACTIONS + RATE_ACTIONS \
        + CHECKPOINT_ACTIONS
    results = []
    for entry in entries:
        name = entry['display_name']
        tsp = entry['TimeActionTaken']
        dose = entry['Dose']
        unit = entry['MedUnit']
        action = entry['ActionTaken']
        order_tsp = entry['ORDER_INST']
        if name.lower().startswith('dopamine'):
            # dopamine
            if action in intake_actions:
                if dose:
                    if unit == "mcg/kg/min":
                        dose_json = json.dumps({'dose':dose, \
                            'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                            'action': action})
                        results.append([tsp, dose_json, confidence.NO_TRANSFORM])
                    elif unit == "mcg":
                        if dose > 100000: # fix a typo for dopamine entries
                            dose_json = json.dumps({'dose':dose/100000, \
                                'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                                'action': action})
                            results.append([tsp, dose_json, \
                                confidence.VALUE_TRANSFORMED])
                        else:
                            dose_json = json.dumps({'dose':dose, \
                                'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                                'action': action})
                            results.append([tsp, dose_json, confidence.UNIT_TRANSFORMED])
            elif action in STOPPED_ACTIONS:
                dose_json = json.dumps({'dose':0, \
                    'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                    'action': action})
                results.append([tsp, dose_json, confidence.NO_TRANSFORM])
        else:
            if action in intake_actions:
                if dose:
                    if unit == "mcg/kg/min":
                        dose_json = json.dumps({'dose':dose, \
                            'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                            'action': action})
                        results.append([tsp, dose_json, confidence.NO_TRANSFORM])
            elif action in STOPPED_ACTIONS:
                dose_json = json.dumps({'dose':0, \
                    'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                    'action': action})
                results.append([tsp, dose_json, confidence.NO_TRANSFORM])

    return results


def convert_vasopressin_to_unit(entries, log):
    # update on 9/1/2016
    global GIVEN_ACTIONS
    global IV_START_ACTIONS
    global RATE_ACTIONS
    global CHECKPOINT_ACTIONS
    global STOPPED_ACTIONS
    intake_actions = GIVEN_ACTIONS + IV_START_ACTIONS + RATE_ACTIONS \
        + CHECKPOINT_ACTIONS
    results = []
    start_tsp = None
    for entry in entries:
        name = entry['display_name']
        tsp = entry['TimeActionTaken']
        dose = entry['Dose']
        unit = entry['MedUnit']
        action = entry['ActionTaken']
        order_tsp = entry['ORDER_INST']
        log_assert(log, unit == 'Units' or unit == 'Units/min', "Unknown unit %s" % unit)

        if unit == 'Units':
            # for epinephrine_dose
            dose_json = json.dumps({'dose':dose, \
                'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                'action': entry['ActionTaken']})
            results.append([tsp, dose_json, confidence.NO_TRANSFORM])
        elif unit == 'Units/min':
            # use rate to calculate dose
            if action in intake_actions:
                if start_tsp:
                    _convert_vasopressin_to_unit(entry, results, start_tsp, start_dose, start_action, False, log)
                start_tsp = tsp
                start_dose = float(dose)*60 # the unit is Unit/hr
                start_action = action
            elif action in STOPPED_ACTIONS:
                if start_tsp:
                    _convert_vasopressin_to_unit(entry, results, start_tsp, start_dose, start_action, True,log)
                start_tsp = None
                start_action = None
    return results

def _convert_vasopressin_to_unit(entry, results, start_tsp, dose, start_action, stopped, log):
    name = entry['display_name']
    tsp = entry['TimeActionTaken']
    order_tsp = entry['ORDER_INST']
    curr_tsp = start_tsp + timedelta(hours=1)
    while curr_tsp < tsp:
        # duration is larger than 1 hour
        dose_json = json.dumps({'dose':dose, \
            'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
            'action': start_action,
            })
        results.append([start_tsp, dose_json, confidence.VALUE_TRANSFORMED])
        start_tsp = curr_tsp
        start_action = 'Rate added by CDM'
        curr_tsp += timedelta(hours=1)
    dose = dose * (tsp + timedelta(hours=1) - curr_tsp).total_seconds()/3600
    dose_json = json.dumps({'dose':dose, \
        'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
        'action': start_action,
        })
    results.append([start_tsp, dose_json, confidence.VALUE_TRANSFORMED])
    if stopped:
        dose_json = json.dumps({'dose':0, \
            'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
            'action': 'Stopped',
            })
        results.append([tsp, dose_json, confidence.NO_TRANSFORM])


def convert_propofol_to_mg(entries, log):
    # update on 9/1/2016
    global GIVEN_ACTIONS
    global IV_START_ACTIONS
    global RATE_ACTIONS
    global CHECKPOINT_ACTIONS
    global STOPPED_ACTIONS
    intake_actions = GIVEN_ACTIONS + IV_START_ACTIONS + RATE_ACTIONS \
        + CHECKPOINT_ACTIONS
    results = []
    start_tsp = None
    for entry in entries:
        name = entry['display_name']
        tsp = entry['TimeActionTaken']
        dose = entry['Dose']
        unit = entry['MedUnit']
        action = entry['ActionTaken']
        order_tsp = entry['ORDER_INST']
        if unit == 'mg':
            # for epinephrine_dose
            dose_json = json.dumps({'dose':dose, \
                'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                'action': entry['ActionTaken']})
            results.append([tsp, dose_json, confidence.UNIT_TRANSFORMED])
        elif unit == 'mcg/kg/min':
            # use rate to calculate dose
            if action in intake_actions:
                if start_tsp:
                    _convert_propofol_to_mg(entry, results, start_tsp, \
                        start_rate, start_action, False, log)
                start_tsp = tsp
                if entry['INFUSION_RATE'] is not None:
                    start_rate = float(entry['INFUSION_RATE']) # the unit is mL/hr
                else:
                    start_rate = float(dose)
                start_action = action
            elif action in STOPPED_ACTIONS:
                if start_tsp:
                    _convert_propofol_to_mg(entry, results, start_tsp, \
                        start_rate, start_action, True, log)
                start_tsp = None
                start_rate = None
                start_action = None
    return results

def _convert_propofol_to_mg(entry, results, start_tsp, rate, start_action, stopped, log):
    name = entry['display_name']
    tsp = entry['TimeActionTaken']
    rate *= 10 # the unit is mg/hr (because it is 10 mg/mL infusion)
    order_tsp = entry['ORDER_INST']
    curr_tsp = start_tsp + timedelta(hours=1)
    while curr_tsp < tsp:
        # duration is larger than 1 hour
        dose_json = json.dumps({'dose':rate, \
            'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
            'action': start_action,
            })
        results.append([start_tsp, dose_json, confidence.VALUE_TRANSFORMED])
        start_tsp = curr_tsp
        start_action = 'Rate added by CDM'
        curr_tsp += timedelta(hours=1)
    rate = rate * (tsp + timedelta(hours=1) - curr_tsp).total_seconds()/3600
    dose_json = json.dumps({'dose':rate, \
        'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
        'action': start_action,
        })
    results.append([start_tsp, dose_json, confidence.VALUE_TRANSFORMED])
    if stopped:
        dose_json = json.dumps({'dose':0, \
            'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
            'action': 'Stopped',
            })
        results.append([tsp, dose_json, confidence.NO_TRANSFORM])



def convert_to_mcg_min(entries, log):
    # used by levophed_dose and epinephrine_dose
    # update on 3/7/2016
    global GIVEN_ACTIONS
    global IV_START_ACTIONS
    global RATE_ACTIONS
    global CHECKPOINT_ACTIONS
    global STOPPED_ACTIONS
    intake_actions = GIVEN_ACTIONS + IV_START_ACTIONS + RATE_ACTIONS \
        + CHECKPOINT_ACTIONS
    results = []
    for entry in entries:
        name = entry['display_name']
        tsp = entry['TimeActionTaken']
        dose = float(entry['Dose'])
        unit = entry['MedUnit']
        action = entry['ActionTaken']
        order_tsp = entry['ORDER_INST']
        if action in intake_actions:
            if dose:
                if unit == "mcg/min":
                    dose_json = json.dumps({'dose':dose, \
                        'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                        'action': entry['ActionTaken']})
                    results.append([tsp, dose_json, confidence.NO_TRANSFORM])
                elif unit == 'mg' or unit == 'ml':
                    # for epinephrine_dose
                    dose_json = json.dumps({'dose':dose*1000, \
                        'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                        'action': entry['ActionTaken']})
                    results.append([tsp, dose_json, confidence.UNIT_TRANSFORMED])
        elif action in STOPPED_ACTIONS:
            dose_json = json.dumps({'dose':0, \
                'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                'action': action})
            results.append([tsp, dose_json, confidence.NO_TRANSFORM])
    return results

def convert_dose_to_binary(entries, log):
    global GIVEN_ACTIONS, IV_START_ACTIONS, RATE_ACTIONS
    global STOPPED_ACTIONS
    actions = GIVEN_ACTIONS + IV_START_ACTIONS + RATE_ACTIONS
    results = []
    for entry in entries:
        name = entry['display_name']
        tsp = entry['TimeActionTaken']
        dose = entry['Dose']
        unit = entry['MedUnit']
        action = entry['ActionTaken']
        order_tsp = entry['ORDER_INST']
        if action in actions:
            if dose is None:
                log.warn("Dose is None: %s" % name)
            elif float(dose) > 0:
                dose_json = json.dumps({'dose':dose, \
                    'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                    'action': entry['ActionTaken']})
                results.append([tsp, dose_json, confidence.VALUE_TRANSFORMED])
        elif action in STOPPED_ACTIONS:
            dose_json = json.dumps({'dose':0, \
                'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                'action': action})
            results.append([tsp, dose_json, confidence.NO_TRANSFORM])

    return results

def _convert_to_mg(entry, log):
    name = entry['display_name']
    tsp = entry['TimeActionTaken']
    dose = entry['Dose']
    unit = entry['MedUnit']
    order_tsp = entry['ORDER_INST']
    action = entry['ActionTaken']
    if dose is None:
        return None
    log_assert(log, unit == 'kg' or unit == 'g' or unit == 'mg' or unit == 'mcg', "Unknown unit %s" % unit   )
    if unit == 'kg':
        return [tsp,
                json.dumps({'dose': 1000*1000*float(dose), \
                    'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                    'action': entry['ActionTaken']}),
                confidence.UNIT_TRANSFORMED]
    elif unit == 'g':
        return [tsp,
                json.dumps({'dose':1000*float(dose), \
                    'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                    'action': entry['ActionTaken']}),
                confidence.UNIT_TRANSFORMED]
    elif unit == 'mg':
        return [tsp,
                json.dumps({'dose':float(dose), \
                    'order_tsp': order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                    'action': entry['ActionTaken']}),
                confidence.NO_TRANSFORM]
    elif unit == 'mcg':
        return [tsp,
                json.dumps({'dose':0.001*float(dose), \
                    'order_tsp': order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                    'action': entry['ActionTaken']}),
                confidence.NO_TRANSFORM]

def convert_to_mmol(entry, log):
    csn_id = entry[0]
    name = entry[1]
    tsp = entry[2]
    dose = entry[3]
    unit = entry[4]
    unit = unit.lower()
    if dose is None or dose == 'Test not performed':
        return None
    log_assert(log, unit == 'meq/l' or unit == 'mmol/l', "Unknown unit %s" % unit   )
    if not dose == 'see below':
        if unit == 'mmol/l':
            return [float(dose), confidence.NO_TRANSFORM]
        elif unit == 'meq/l':
            return [float(dose), confidence.UNIT_TRANSFORMED]

def convert_ddimer_unit(entry, log):
    csn_id = entry[0]
    name = entry[1]
    tsp = entry[2]
    val = entry[3]
    unit = entry[4]
    log_assert(log, unit == "mg/L FEU" or unit == "mg/L", "Unknown unit %s for ddimer" % unit   )
    if unit == "mg/L FEU":
        return [float(val), confidence.NO_TRANSFORM]
    elif unit == 'mg/L':
        return [float(val)*2, confidence.UNIT_TRANSFORMED]


def convert_vent_to_binary(entry, log):
    value = entry[-1]
    if value is not None:
        if value == 'Vent Stop':
            return [False, confidence.NO_TRANSFORM]
        elif value == 'Vent Start':
            return [True, confidence.NO_TRANSFORM]
    return None


def convert_care_unit_hc_epic(entry, log):
    dept = entry[-1]
    event = entry[1]
    if event == 'Discharge':
        return [event, confidence.NO_TRANSFORM]
    else:
        return [dept, confidence.NO_TRANSFORM]



def extract_fluids_intake(entries, log):
    '''
    simplified version
    '''
    global STOPPED_ACTIONS
    global GIVEN_ACTIONS
    global IV_START_ACTIONS
    global RATE_ACTIONS
    # print "extract_fluids_intake"
    on_actions = GIVEN_ACTIONS + IV_START_ACTIONS + RATE_ACTIONS
    volumes = []
    entry_pre = None
    remain_vol = None
    recent_dose = None
    recent_unit = None
    for entry in entries:
        if entry['ActionTaken'] in on_actions:
            tsp = entry['TimeActionTaken']
            if entry['Dose'] is not None:
                dose = entry['Dose']
                volumes.append([tsp, dose, confidence.VALUE_TRANSFORMED])
            elif entry['INFUSION_RATE'] is not None:
                dose = entry['INFUSION_RATE']
                volumes.append([tsp, dose, confidence.VALUE_TRANSFORMED])
    return volumes


def _get_max_vol_ml(med):
    items = med.split(' ')
    for i, item in enumerate(items):
        if item == 'mL':
            item_num = items[i-1].replace(",","")
            # print item_num
            if '.' in item_num:
                return float(item_num)
            else:
                return int(item_num)
    return None

def _calculate_volume_in_ml(volumes, entry_cur, entry_nxt, remain_vol_pre, \
    recent_dose, recent_unit, log, df=False):
    # print "_calculate_volume_in_ml"
    global FLUID_DUR
    global RATE_ACTIONS
    unit = entry_cur['MedUnit'] if not df else entry_cur['dose_unit']
    dose = float(entry_cur['Dose']) if not df else float(entry_cur['dose'])
    tsp = entry_cur['TimeActionTaken'] if not df else entry_cur['tsp']
    med = entry_cur['display_name'] if not df else entry_cur['full_name']
    max_vol_ml = _get_max_vol_ml(med)
    infusion_rate = entry_cur['INFUSION_RATE'] if not df else entry_cur['rate_value']
    infusion_rate_unit = entry_cur['MAR_INF_RATE_UNIT'] if not df else entry_cur['rate_unit']

    if med.startswith('albumin human') and unit == 'g':
        unit = 'mL'

    if med.startswith('sodium chloride 0.9') and unit == 'mg':
        unit = 'mL'

    if unit is None and infusion_rate is not None \
        and infusion_rate_unit == 'mL/hr':
        # case when display name is
        # sodium bicarbonate 150 mEq in sodium chloride 0.9 % 1,000 mL infusion
        unit = infusion_rate_unit
        dose = infusion_rate

    if unit == 'mg' and infusion_rate is not None \
        and infusion_rate_unit == 'mL/hr':
        # vancomycin (VANCOCIN) 1,250 mg in sodium chloride 0.9 % 250 mL IVPB
        unit = infusion_rate_unit
        dose = infusion_rate

    if unit is None and recent_unit is not None:
        unit = recent_unit
        dose = recent_dose
        if unit == 'mL' and max_vol_ml and max_vol_ml != dose:
            dose = max_vol_ml

    if dose is None and infusion_rate is None and max_vol_ml is not None:
        dose = max_vol_ml
        unit = 'mL'

    if unit == "mL/kg/hr" and infusion_rate is not None:
        dose = infusion_rate
        unit = infusion_rate_unit

    if unit == 'mL':
        duration = entry_cur['mar_duration']
        duration_unit = entry_cur['MAR_DURATION_UNIT']
        if duration is None and infusion_rate is not None and infusion_rate > 0:
            log_assert(log, infusion_rate_unit == 'mL/hr', \
                "Invalid infusion rate unit %s" % infusion_rate_unit)
            duration = dose / infusion_rate
            duration_unit = 'HOURS'
        if duration and duration_unit == 'HOURS':
            dose_per_hour = dose/duration
            add_hour = 0
            remain_vol = dose
            while add_hour < duration:
                if remain_vol > dose_per_hour:
                    volumes.append([tsp + timedelta(hours = add_hour), \
                        dose_per_hour, confidence.NO_TRANSFORM])
                    remain_vol -= dose_per_hour
                else:
                    volumes.append([tsp + timedelta(hours = add_hour), \
                        remain_vol, confidence.NO_TRANSFORM])
                add_hour += 1
        elif duration and duration_unit == 'minutes':
            log_assert(log, duration <= 60, "Invalid duration in minutes %s" % duration )
            volumes.append([tsp, dose, confidence.NO_TRANSFORM])
        else:
            volumes.append([tsp, dose, confidence.NO_TRANSFORM])
    elif unit == 'mL/hr':
        if entry_cur['ActionTaken'] in RATE_ACTIONS and remain_vol_pre:
            log_assert(log, remain_vol_pre > 0, "Invalid remain_vol_pre")
            max_vol_ml = remain_vol_pre
        else:
            max_vol_ml =  _get_max_vol_ml(med)
        # print "max_vol_ml", max_vol_ml
        if entry_nxt:
            # entry is not None
            duration_secs = (entry_nxt['TimeActionTaken'] - tsp).total_seconds()
            if duration_secs < FLUID_DUR:
                # less than the max interval
                dose_ml = duration_secs / 3600 * dose
                if max_vol_ml and dose_ml > max_vol_ml:
                    dose_ml = max_vol_ml
                volumes.append([tsp, dose_ml, confidence.UNIT_TRANSFORMED])
                if max_vol_ml:
                    return max_vol_ml - dose_ml
            else:
                int_start = tsp
                int_end = int_start + timedelta(hours=1)
                sum_vol_ml = 0
                while int_start < entry_nxt['TimeActionTaken']:
                    dose_ml = (int_end - int_start).total_seconds()/3600*dose
                    if max_vol_ml and dose_ml + sum_vol_ml > max_vol_ml:
                        dose_ml = max_vol_ml - sum_vol_ml
                    sum_vol_ml += dose_ml
                    if dose_ml > 0:
                        volumes.append([int_start, dose_ml, \
                            confidence.UNIT_TRANSFORMED])
                    int_start = int_end
                    int_end = int_start + timedelta(hours=1)
                    if int_end > entry_nxt['TimeActionTaken']:
                        int_end = entry_nxt['TimeActionTaken']
                if max_vol_ml:
                    return max_vol_ml - sum_vol_ml
        else:
            # no entry exists
            if max_vol_ml:
                # if we know the max volume
                int_start = tsp
                int_end = int_start + timedelta(hours=1)
                sum_vol_ml = 0
                while sum_vol_ml < max_vol_ml:
                    dose_ml = (int_end - int_start).total_seconds()/3600 * dose
                    sum_vol_ml += dose_ml
                    if sum_vol_ml > max_vol_ml:
                        dose_ml -= (sum_vol_ml - max_vol_ml)
                    if dose_ml > 0:
                        volumes.append([int_start, dose_ml, \
                            confidence.UNIT_TRANSFORMED])
                    int_start = int_end
                    int_end = int_start + timedelta(hours=1)
            else:
                # if we don't know the max volume
                duration = entry_cur['mar_duration']
                duration_unit = entry_cur['MAR_DURATION_UNIT']
                if duration:
                    if duration_unit == 'HOURS':
                        volumes.append([tsp, dose*duration, \
                            confidence.UNIT_TRANSFORMED])
                    elif duration_unit == 'minutes':
                        volumes.append([tsp, dose*duration*60, \
                            confidence.UNIT_TRANSFORMED])
                    else:
                        log.warn("Invalid Duration Unit: %s" % duration_unit)
                else:
                    volumes.append([tsp, dose*FLUID_DUR/3600, \
                        confidence.UNIT_TRANSFORMED])
    else:
        log.warn("Invalid unit: %s" % unit)

def convert_vancomycin_to_real(entry, log):
    value = entry['ResultValue']
    tsp = entry['RESULT_TIME']

    if value.startswith('<') or value.startswith('>'):
        return [tsp, float(value[1:]), confidence.NO_TRANSFORM]
    else:
        try:
            value = float(value)
            return [tsp, float(value), confidence.NO_TRANSFORM]
        except:
            log.warn("Invalid vancomycin_trough entry: %s" % entry)
            return None





#############################################################################
########  FUNCTIONS FOR threshold
#############################################################################
def threshold(entry, lower, upper, log):
    try:
        if entry is not None:
            if 'Record' in str(type(entry)) or isinstance(entry, list):
                # entry is a list
                entry = entry[-1] # last item is the value
            # entry is a number item
            if isinstance(entry, str) and (entry.startswith('<') or entry.startswith('>')):
                entry = entry[1:]
            value = entry
            if value is not None:
                value = float(value)
                if lower <= value <= upper:
                    return [value, confidence.NO_TRANSFORM]
    except Exception as e:
        log.warn("%s for threshold function: %s, lower: %s, upper: %s" % (e, entry, lower, upper))

# def threshold_fio2(entry, log):
#     return threshold(entry, 0.1, 1, log)


def threshold_abp_dias(entry, log):
    return threshold(entry, 8 , 150, log)

def threshold_abp_mean(entry, log):
    return threshold(entry, 20 , 170, log)

def threshold_abp_sys(entry, log):
    return threshold(entry, 30 , 300, log)

def threshold_nbp_dias(entry, log):
    return threshold(entry, 8 , 150, log)

def threshold_nbp_mean(entry, log):
    return threshold(entry, 20, 250, log)

def threshold_nbp_sys(entry, log):
    return threshold(entry, 30 , 250, log)

def threshold_arterial_ph(entry, log):
    return threshold(entry, 6.5, 8.5, log)

def threshold_bilirubin(entry, log):
    return threshold(entry, 0, 50, log)

def threshold_bun(entry, log):
    return threshold(entry, 0.1, 180, log)

def threshold_co2(entry, log):
    return threshold(entry, 0.1, 55, log)

def threshold_creatinine(entry, log):
    return threshold(entry, 0.1, 40, log)

def threshold_hematocrit(entry, log):
    return threshold(entry, 15, 60, log)

def threshold_hemoglobin(entry, log):
    return threshold(entry, 4 , 20, log)

def threshold_inr(entry, log):
    return threshold(entry, 0.01, 12, log)

def threshold_lactate(entry, log):
    return threshold(entry, 0.2, 40, log)

def threshold_paco2(entry, log):
    return threshold(entry, 5, 100, log)

def threshold_pao2(entry, log):
    return threshold(entry, 0.1, 500, log)

def threshold_platelets(entry, log):
    return threshold(entry, 0.1, 1200, log)

def threshold_potassium(entry, log):
    return threshold(entry, 1, 10, log)

def threshold_sodium(entry, log):
    return threshold(entry, 115, 160, log)

def threshold_wbc(entry, log):
    return threshold(entry, 0.01, 70, log)

def threshold_fluids_intake(entry, log):
    return threshold(entry, 0, 500000, log)

def threshold_fio2(entry, log):
    return threshold(entry, 20, 100, log)

def threshold_gcs(entry, log):
    return threshold(entry, 3, 15, log)

def threshold_heart_rate(entry, log):
    return threshold(entry, 20, 300, log)

def threshold_temperature(entry, log):
    return threshold(entry, 80, 110, log)

def threshold_weight(entry, log):
    return threshold(entry, 20, 300, log)

###############################################################
#        functions for mimic
###############################################################
def transform_fio2_mimic(entry, log):
    try:
        return threshold_fio2(float(entry[-1])*100, log)
    except:
        log.warn("threshold_fio2 invalid entry: %s" % entry)

def transform_weight_mimic(entry, log):
    try:
        return threshold_weight(entry[-2], log)
    except:
        log.warn('transform_weight_mimic error: %s' % entry)

def convert_status_to_dose(entry, log):
    value1 = entry['value1']
    value1num = entry['value1num']
    stopped = entry['stopped']
    global MIMIC_GIVEN_ACTIONS, MIMIC_GIVEN_ACTIONS_PREFIX
    dose = None
    if stopped is None or stopped not in ["D/C'd", "Stopped"]:
        if value1 is not None:
            if value1 in MIMIC_GIVEN_ACTIONS:
                return [json.dumps({'dose': 1,
                                    'action': value1,
                                    'stopped': stopped}),
                        confidence.VALUE_TRANSFORMED]
            else:
                for prefix in MIMIC_GIVEN_ACTIONS_PREFIX:
                    if value1.startswith(prefix):
                        return [json.dumps({'dose': 1, \
                                           'action': value1,
                                           'stopped': stopped}),
                                confidence.VALUE_TRANSFORMED]
            if value1num is not None and value1num > 0:
                return [json.dumps({'dose': 1,
                                    'action': value1,
                                    'stopped': stopped}),
                        confidence.VALUE_TRANSFORMED]


def check_med_status(entry, log):
    dose = entry['dose']
    stopped = entry['stopped']
    return [json.dumps({'dose': dose,
                        'action': stopped}),
            confidence.NO_TRANSFORM]

def quantify_rikersas(entry, log):
    if entry['value1']:
        value1 = entry['value1'].lower()
        value = 0
        if value1 == 'unarousable':
            value = 1
        elif value1 == 'very sedated':
            value = 2
        elif value1 == 'sedated':
            value = 3
        elif value1 == "calm/cooperative":
            value = 4
        elif value1 == 'agitated':
            value = 5
        elif value1 == 'very agitated':
            value = 6
        elif value1 == 'danger agitation':
            value = 7
        return [value, confidence.VALUE_TRANSFORMED]


def convert_vent_mimic(entries, log):
    # entries are ordered by icustay_id and tsp
    global VENT_DURATION
    tsp_start = None
    tsp_end = None
    vent_intervals = []
    for entry in entries:
        tsp = entry['tsp']
        if tsp_start:
            if tsp > tsp_end:
                # save current interval, start new interval
                vent_intervals.append([tsp_start, True, \
                    confidence.NO_TRANSFORM])
                vent_intervals.append([tsp_end, False, \
                    confidence.NO_TRANSFORM])
                tsp_start = tsp
                tsp_end = tsp + timedelta(hours=VENT_DURATION)
            else:
                # expand the current interval
                tsp_end = tsp + timedelta(hours=VENT_DURATION)
        else:
            tsp_start = tsp
            tsp_end = tsp + timedelta(hours=VENT_DURATION)
    if tsp_start:
        vent_intervals.append([tsp_start, True, confidence.NO_TRANSFORM])
        vent_intervals.append([tsp_end, False, confidence.NO_TRANSFORM])

    return vent_intervals


###############################################################
#        functions for hcgh_v1
###############################################################

def convert_catheter_to_binary(entry, log):
    place_tsp = entry['PLACEMENT_INSTANT']
    rm_tsp = entry['REMOVAL_DTTM']
    results = []
    if place_tsp:
        results.append([place_tsp, True, confidence.NO_TRANSFORM])
    if rm_tsp:
        results.append([rm_tsp, False, confidence.NO_TRANSFORM])
    if len(results) == 0:
        return None
    else:
        return results

def convert_dialysis_to_binary(entry, log):
    try:
        value = float(entry['Value'])
        if value > 0:
            return [True, confidence.NO_TRANSFORM]
    except TypeError as te:
        log.warn('convert_dialysis_to_binary typeError: ' + str(type(entry['Value'])))

def convert_dialysis_to_json(entry, log):
    proc_str = json.dumps({"name":entry['DISP_NAME'], "Value":entry['Value']})
    return [proc_str, confidence.NO_TRANSFORM]


def convert_inhosp_to_json(entry, log):
    inhosp_str = json.dumps({"diagname":entry['diagname'],
                            "ischronic":entry['chronic'],
                            "present on admission":entry['presentonadmission']})
    return [inhosp_str, confidence.NO_TRANSFORM]

def convert_proc_to_json(entry, log):
    status = entry['OrderStatus']
    if status != 'Canceled':
        proc_str = json.dumps({"name":entry['display_name'], "status":status})
        return [proc_str, confidence.NO_TRANSFORM]

def convert_surgery_to_json(entry, log):

    sched_status_dict = {
        1: 'Scheduled',
        2: 'Canceled',
        3: 'Not Scheduled',
        4: 'Missing Information',
        5: 'Voided',
        6: 'Pending',
        7: 'Arrived',
        8: 'Completed',
        9: 'No Show',
        10: 'Pending Unscheduled',
    } # from the clarity data dictionary

    entry_dict = dict(entry)
    surgery_dict = {}
    time_cols = ['case_begin_instant','case_end_instant','enter_or_room_instant','leave_or_room_instant']

    for col in time_cols:
        surgery_dict[col] = str(entry_dict[col])

    cols_2_add = ['preformed_yn','scheduled_yn','procedure_display_name','procedure_name']

    for col in cols_2_add:
        surgery_dict[col] = entry_dict[col]

    surgery_dict['sched_status_c'] = sched_status_dict[entry_dict['sched_status_c']]

    surgery_str = json.dumps(surgery_dict)
    return [surgery_str, confidence.NO_TRANSFORM]

def convert_lda_to_binary(entry, log):
    place_tsp = entry['PLACEMENT_INSTANT']
    rm_tsp = entry['REMOVAL_DTTM']
    results = []
    if place_tsp:
        results.append([place_tsp, True, confidence.NO_TRANSFORM])
    if rm_tsp:
        results.append([rm_tsp, False, confidence.NO_TRANSFORM])
    if len(results) == 0:
        return None
    else:
        return results

def convert_order_question_to_json(entry, log):
    question = entry['quest_name']
    response = entry['ord_quest_resp']
    quest_str = json.dumps({"question":question, "response":response})
    return [quest_str, confidence.NO_TRANSFORM]

def extract_fluids_intake_json(entries, log):
    global STOPPED_ACTIONS
    global GIVEN_ACTIONS
    global IV_START_ACTIONS
    global RATE_ACTIONS
    # print "extract_fluids_intake"
    on_actions = GIVEN_ACTIONS + IV_START_ACTIONS + RATE_ACTIONS
    volumes = []
    entry_pre = None
    remain_vol = None
    recent_dose = None
    recent_unit = None
    recent_type = None
    for entry in entries:
        if entry_pre:
            if entry_pre['ActionTaken'] in on_actions:
                if remain_vol:
                    remain_vol = _calculate_volume_in_ml_json(volumes, entry_pre, \
                        entry, remain_vol, recent_dose, recent_unit, log)
                else:
                    remain_vol = _calculate_volume_in_ml_json(volumes, entry_pre, \
                        entry, None, recent_dose, recent_unit, log)
        entry_pre = entry
        if entry['ActionTaken'] in on_actions and entry['Dose'] is not None and \
            float(entry['Dose']) > 0:
            recent_dose = float(entry['Dose'])
            recent_unit = entry['MedUnit']
            recent_type = entry['display_name']
    # last one
    if entry_pre['ActionTaken'] in on_actions:
        if remain_vol:
            _calculate_volume_in_ml_json(volumes, entry_pre, None, remain_vol, \
                recent_dose, recent_unit, log)
        else:
            _calculate_volume_in_ml_json(volumes, entry_pre, None, None, \
                recent_dose, recent_unit, log)
    return volumes

def _calculate_volume_in_ml_json(volumes, entry_cur, entry_nxt, remain_vol_pre, \
    recent_dose, recent_unit, log):
    # print "_calculate_volume_in_ml"
    global FLUID_DUR
    global RATE_ACTIONS
    unit = entry_cur['MedUnit']
    dose = entry_cur['Dose']
    tsp = entry_cur['TimeActionTaken']
    med = entry_cur['display_name']
    max_vol_ml = _get_max_vol_ml(med)
    infusion_rate = entry_cur['INFUSION_RATE']
    infusion_rate_unit = entry_cur['MAR_INF_RATE_UNIT']

    if med.startswith('albumin human') and unit == 'g':
        unit = 'mL'

    if med.startswith('sodium chloride 0.9') and unit == 'mg':
        unit = 'mL'

    if unit is None and infusion_rate is not None \
        and infusion_rate_unit == 'mL/hr':
        # case when display name is
        # sodium bicarbonate 150 mEq in sodium chloride 0.9 % 1,000 mL infusion
        unit = infusion_rate_unit
        dose = infusion_rate

    if unit == 'mg' and infusion_rate is not None \
        and infusion_rate_unit == 'mL/hr':
        # vancomycin (VANCOCIN) 1,250 mg in sodium chloride 0.9 % 250 mL IVPB
        unit = infusion_rate_unit
        dose = infusion_rate



    if unit is None and recent_unit is not None:
        unit = recent_unit
        dose = recent_dose
        if unit == 'mL' and max_vol_ml and max_vol_ml != dose:
            dose = max_vol_ml

    if dose is None and infusion_rate is None and max_vol_ml is not None:
        dose = max_vol_ml
        unit = 'mL'

    if unit == "mL/kg/hr" and infusion_rate is not None:
        dose = infusion_rate
        unit = infusion_rate_unit

    if unit == 'mL':
        duration = entry_cur['mar_duration']
        duration_unit = entry_cur['MAR_DURATION_UNIT']
        if duration is None and infusion_rate is not None and infusion_rate > 0:
            log_assert(log, infusion_rate_unit == 'mL/hr', \
                "Invalid infusion rate unit %s" % infusion_rate_unit)
            duration = dose / infusion_rate
            duration_unit = 'HOURS'
        if duration and duration_unit == 'HOURS':
            dose_per_hour = dose/duration
            add_hour = 0
            remain_vol = dose
            while add_hour < duration:
                if remain_vol > dose_per_hour:
                    volumes.append([tsp + timedelta(hours = add_hour), \
                        json.dumps({'dose':dose_per_hour, \
                        'type': med}), confidence.NO_TRANSFORM])
                    remain_vol -= dose_per_hour
                else:
                    volumes.append([tsp + timedelta(hours = add_hour), \
                        json.dumps({'dose':remain_vol, \
                        'type': med}), confidence.NO_TRANSFORM])
                add_hour += 1
        elif duration and duration_unit == 'minutes':
            log_assert(log, duration <= 60, "Invalid duration in minutes %s" % duration )
            volumes.append([tsp, json.dumps({'dose':dose, \
                        'type': med}), confidence.NO_TRANSFORM])
        else:
            volumes.append([tsp, json.dumps({'dose':dose, \
                        'type': med}), confidence.NO_TRANSFORM])
    elif unit == 'mL/hr':
        if entry_cur['ActionTaken'] in RATE_ACTIONS and remain_vol_pre:
            log_assert(log, remain_vol_pre > 0, "Invalid remain_vol_pre")
            max_vol_ml = remain_vol_pre
        else:
            max_vol_ml =  _get_max_vol_ml(med)
        # print "max_vol_ml", max_vol_ml
        if entry_nxt:
            # entry is not None
            duration_secs = (entry_nxt['TimeActionTaken'] - tsp).total_seconds()
            if duration_secs < FLUID_DUR:
                # less than the max interval
                dose_ml = duration_secs / 3600 * dose
                if max_vol_ml and dose_ml > max_vol_ml:
                    dose_ml = max_vol_ml
                volumes.append([tsp, json.dumps({'dose':dose_ml, \
                        'type': med}), confidence.UNIT_TRANSFORMED])
                if max_vol_ml:
                    return max_vol_ml - dose_ml
            else:
                int_start = tsp
                int_end = int_start + timedelta(hours=1)
                sum_vol_ml = 0
                while int_start < entry_nxt['TimeActionTaken']:
                    dose_ml = (int_end - int_start).total_seconds()/3600*dose
                    if max_vol_ml and dose_ml + sum_vol_ml > max_vol_ml:
                        dose_ml = max_vol_ml - sum_vol_ml
                    sum_vol_ml += dose_ml
                    if dose_ml > 0:
                        volumes.append([int_start, json.dumps({'dose':dose_ml, \
                        'type': med}), confidence.UNIT_TRANSFORMED])
                    int_start = int_end
                    int_end = int_start + timedelta(hours=1)
                    if int_end > entry_nxt['TimeActionTaken']:
                        int_end = entry_nxt['TimeActionTaken']
                if max_vol_ml:
                    return max_vol_ml - sum_vol_ml
        else:
            # no entry exists
            if max_vol_ml:
                # if we know the max volume
                int_start = tsp
                int_end = int_start + timedelta(hours=1)
                sum_vol_ml = 0
                while sum_vol_ml < max_vol_ml:
                    dose_ml = (int_end - int_start).total_seconds()/3600 * dose
                    sum_vol_ml += dose_ml
                    if sum_vol_ml > max_vol_ml:
                        dose_ml -= (sum_vol_ml - max_vol_ml)
                    if dose_ml > 0:
                        volumes.append([int_start, json.dumps({'dose':dose_ml, \
                        'type': med}),  confidence.UNIT_TRANSFORMED])
                    int_start = int_end
                    int_end = int_start + timedelta(hours=1)
            else:
                # if we don't know the max volume
                duration = entry_cur['mar_duration']
                duration_unit = entry_cur['MAR_DURATION_UNIT']
                if duration:
                    if duration_unit == 'HOURS':
                        volumes.append([tsp, json.dumps({'dose':dose*duration, \
                        'type': med}),confidence.UNIT_TRANSFORMED])
                    elif duration_unit == 'minutes':
                        volumes.append([tsp, json.dumps({'dose':dose*duration*60, \
                        'type': med}), confidence.UNIT_TRANSFORMED])
                    else:
                        log.warn("Invalid Duration Unit {}".format(duration_unit))
                else:
                    volumes.append([tsp, json.dumps({'dose':dose*FLUID_DUR/3600, \
                        'type': med}), confidence.UNIT_TRANSFORMED])
    else:
        log.warn("Invalid unit: %s" % unit)

def convert_to_ml(entries, log):
    global GIVEN_ACTIONS, IV_START_ACTIONS
    results = []
    for entry in entries:
        action = entry['ActionTaken']
        if action in GIVEN_ACTIONS or IV_START_ACTIONS:
            result = _convert_to_ml(entry, log)
            if result:
                results.append(result)
        else:
            log.warn("convert_to_ml: non given action: %s" % action)
    return results

def _convert_to_ml(entry, log):
    name = entry['display_name']
    tsp = entry['TimeActionTaken']
    dose = entry['Dose']
    unit = entry['MedUnit']
    order_tsp = entry['ORDER_INST']
    action = entry['ActionTaken']
    if dose is None:
        return None
    log_assert(log, unit == 'L' or unit == 'mL', "Unknown unit %s" % unit   )
    if unit == 'L':
        return [tsp,
                json.dumps({'dose': 1000*float(dose), \
                    'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                    'action': entry['ActionTaken']}),
                confidence.UNIT_TRANSFORMED]
    elif unit == 'mL':
        return [tsp,
                json.dumps({'dose':float(dose), \
                    'order_tsp':order_tsp.strftime("%Y-%m-%d %H:%M:%S"),
                    'action': entry['ActionTaken']}),
                confidence.UNIT_TRANSFORMED]

