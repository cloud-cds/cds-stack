import falcon
# from Crypto.Cipher import AES
from pkcs7 import PKCS7Encoder
import os
import base64
import hashlib
import binascii
import urllib
import data_example
import ujson as json
import dashan_query as query
import logging
import json
import time
import datetime
import pandas as pd
import numpy as np
import logging
import pprint
import copy
import re
import calendar
import os

from prometheus_client import Histogram, Counter
from prometheus_client import CollectorRegistry, push_to_gateway
prom_job = os.environ['db_name'].replace("_", "-")
prom_gateway_url = 'a99cd27870d1011e78bbf0a73a2cd36e-1301450500.us-east-1.elb.amazonaws.com:9091'
registry = CollectorRegistry()
trews_api_request_latency = Histogram('trews_api_request_latency', 'Time spent processing API request', ['actionType'], registry=registry)
trews_api_request_counts = Counter('trews_api_request_counts', 'Number of requests per seconds', ['actionType'], registry=registry)

THRESHOLD = 0.85
logging.basicConfig(format='%(levelname)s|%(message)s', level=logging.INFO)
#hashed_key = 'C8ED911A8907EFE4C1DE24CA67DF5FA2'
#hashed_key = '\xC8\xED\x91\x1A\x89\x07\xEF\xE4\xC1\xDE\x24\xCA\x67\xDF\x5F\xA2'
#hashed_key = 'e7cde81226f1d5e03c2681035692964d'
# hashed_key = '\xe7\xcd\xe8\x12\x26\xf1\xd5\xe0\x3c\x26\x81\x03\x56\x92\x96\x4d'
# IV = '\x00' * 16
# MODE = AES.MODE_CBC

DECRYPTED = False


def temp_f_to_c(f):
    return (f - 32) * .5556

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, np.int64):
            return int(obj)
        else:
            print "encoder:", obj
            print type(obj)
            return super(NumpyEncoder, self).default(obj)

class TREWSAPI(object):
    def on_get(self, req, resp):
        """
        See example in test_decrpyt.py
        """
        resp.content_type = 'text/html'
        resp.status = falcon.HTTP_200  # This is the default status
        body += "TREWS API"
        resp.body = (body)

    # def decrypt(self, encrypted_text):
    #     encrypted_text = urllib.unquote(encrypted_text)
    #     decodetext =  base64.b64decode(encrypted_text)
    #     aes = AES.new(hashed_key, MODE, IV)
    #     cipher = aes.decrypt(decodetext)
    #     encoder = PKCS7Encoder()
    #     pad_text = encoder.decode(cipher)
    #     return pad_text

    # match and test the consistent API for overriding
    def take_action(self, actionType, actionData, eid, uid):

        # Match pollNotifications first since this is the most common action.
        if actionType == u'pollNotifications':
            notifications = query.get_notifications(eid)
            return {'notifications': notifications}

        elif actionType == u'pollAuditlist':
            auditlist = query.get_criteria_log(eid)
            return {'auditlist': auditlist}

        elif actionType == u'override':
            action_is_clear = 'clear' in actionData and actionData['clear']
            logging.info('override_criteria action %(clear)s: %(v)s' % {'v': json.dumps(actionData), 'clear': action_is_clear})
            if action_is_clear:
                query.override_criteria(eid, actionData['actionName'], clear=True, user=uid)
            else:
                logging.info('override_criteria value: %(name)s %(val)s' % {'name': actionData['actionName'], 'val': actionData['value']})
                query.override_criteria(eid, actionData['actionName'], value=actionData['value'], user=uid)

        elif actionType == u'suspicion_of_infection':
            if 'value' in actionData and actionData['value'] == 'Reset':
                query.override_criteria(eid, actionData['actionName'], clear=True, user=uid)
            else:
                if "other" in actionData:
                    value = '[{ "text": "%(val)s", "other": true }]' % {'val': actionData['other']}
                    query.override_criteria(eid, actionType, value=value, user=uid)
                else:
                    value = '[{ "text": "%(val)s" }]' % {'val': actionData['value']}
                    query.override_criteria(eid, actionType, value=value, user=uid)

        elif actionType == u'notification':
            if 'id' in actionData and 'read' in actionData:
                query.toggle_notification_read(eid, actionData['id'], actionData['read'])
            else:
                msg = 'Invalid notification update action data' + json.dumps(actionData)
                logging.error(msg)
                return {'message': msg}

        elif actionType == u'place_order':
            query.override_criteria(eid, actionData['actionName'], value='[{ "text": "Ordered" }]', user=uid)

        elif actionType == u'complete_order':
            query.override_criteria(eid, actionData['actionName'], value='[{ "text": "Completed" }]', user=uid)

        elif actionType == u'order_not_indicated':
            query.override_criteria(eid, actionData['actionName'], value='[{ "text": "Not Indicated" }]', user=uid)

        elif actionType == u'reset_patient':
            event_id = actionData['value'] if actionData is not None and 'value' in actionData else None
            query.reset_patient(eid, uid, event_id)

        elif actionType == u'deactivate':
            query.deactivate(eid, uid, actionData['value'])

        elif actionType == u'set_deterioration_feedback':
            deterioration = {"value": actionData['value'], "other": actionData["other"]}
            query.set_deterioration_feedback(eid, deterioration, uid)

        else:
            msg = 'Invalid action type: ' + actionType
            logging.error(msg)
            return {'message': msg}

        return {'result': 'OK'}

    def update_criteria(self, criteria, data):


        SIRS = ['sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc']
        ORGAN_DYSFUNCTION = ["blood_pressure",
                             "mean_arterial_pressure",
                             "decrease_in_sbp",
                             "respiratory_failure",
                             "creatinine",
                             "bilirubin",
                             "platelet",
                             "inr",
                             "lactate"
                             ]


        HYPOTENSION = ["systolic_bp",
                       "hypotension_map",
                       "hypotension_dsbp"
                      ]

        HYPOPERFUSION = ['initial_lactate']

        ORDERS = [ "initial_lactate_order",
                   "blood_culture_order",
                   "antibiotics_order",
                   "crystalloid_fluid_order",
                   "repeat_lactate_order",
                   "vasopressors_order",
                   "focus_exam_order"
                 ]

        sirs_cnt = 0
        od_cnt = 0
        sirs_onsets = []
        od_onsets = []
        hp_cnt = 0
        shock_onsets_hypotension = []
        shock_onsets_hypoperfusion = []
        hpf_cnt = 0
        data['event_id'] = None
        # Update the event id.
        if criteria['event_id'] is not None and len(criteria['event_id'].values) > 0:
            event_id = str(criteria['event_id'].values[0])
            data['event_id'] = None if event_id == "nan" else event_id

        # TODO: set up the onset time
        criteria['override_epoch'] = pd.DatetimeIndex(criteria.override_time).astype(np.int64) // 10**9
        criteria['measurement_epoch'] = pd.DatetimeIndex(criteria.measurement_time).astype(np.int64) // 10**9
        for idx, row in criteria.iterrows():
            # update every criteria
            criterion = {
                "name": row['name'],
                "is_met": row['is_met'],
                "value": row['value'],
                "measurement_time": row['measurement_epoch'] if row['measurement_epoch'] > 0 else None,
                "override_time": row['override_epoch'] if row['override_epoch'] > 0 else None,
                "override_user": row['override_user'],
                "override_value": row['override_value'],
            }

            if criterion["name"] == 'suspicion_of_infection':
                data['severe_sepsis']['suspicion_of_infection'] = {
                    "name": "suspicion_of_infection",
                    "update_time": criterion['override_time'],
                    "update_user": criterion['override_user']
                }
                if criterion['override_value']:
                    text = criterion['override_value'][0]['text']
                    data['severe_sepsis']['suspicion_of_infection']["value"] = text
                    if 'other' in criterion['override_value'][0] and criterion['override_value'][0]['other']:
                        data['severe_sepsis']['suspicion_of_infection']["other"] = True
                else:
                    data['severe_sepsis']['suspicion_of_infection']["value"] = None

            if criterion["name"] == "sirs_temp" and criterion["override_value"]:
                if criterion["override_value"][0]["lower"]:
                    criterion["override_value"][0]["lower"] = temp_f_to_c(criterion["override_value"][0]["lower"])
                if criterion["override_value"][0]["upper"]:
                    criterion["override_value"][0]["upper"] = temp_f_to_c(criterion["override_value"][0]["upper"])

            if criterion["name"] in SIRS:
                sirs_idx = SIRS.index(criterion["name"])
                data['severe_sepsis']['sirs']['criteria'][sirs_idx] = criterion
                if criterion["is_met"]:
                    sirs_cnt += 1
                    if criterion['override_user']:
                        sirs_onsets.append(criterion['override_time'])
                    else:
                        sirs_onsets.append(criterion['measurement_time'])

            if criterion["name"] in ORGAN_DYSFUNCTION:
                od_idx = ORGAN_DYSFUNCTION.index(criterion["name"])
                data['severe_sepsis']['organ_dysfunction']['criteria'][od_idx] = criterion
                if criterion["is_met"]:
                    od_cnt += 1
                    if criterion['override_user']:
                        od_onsets.append(criterion['override_time'])
                    else:
                        od_onsets.append(criterion['measurement_time'])

            # septic shock


            if criterion["name"] in HYPOTENSION:
                hp_idx = HYPOTENSION.index(criterion["name"])
                data['septic_shock']['hypotension']['criteria'][hp_idx] = criterion
                if criterion["is_met"]:
                    hp_cnt += 1
                    if criterion['override_user']:
                        shock_onsets_hypotension.append(criterion['override_time'])
                    else:
                        shock_onsets_hypotension.append(criterion['measurement_time'])


            if criterion["name"] in HYPOPERFUSION:
                hpf_idx = HYPOPERFUSION.index(criterion["name"])
                data['septic_shock']['hypoperfusion']['criteria'][hpf_idx] = criterion
                if criterion["is_met"]:
                    hpf_cnt += 1
                    if criterion['override_user']:
                        shock_onsets_hypoperfusion.append(criterion['override_time'])
                    else:
                        shock_onsets_hypoperfusion.append(criterion['measurement_time'])

            if criterion["name"] == 'crystalloid_fluid':
                data['septic_shock']['crystalloid_fluid'] = criterion

            # update orders
            if criterion["name"] in ORDERS:
                value = criterion['value']
                if ('override_value' in criterion) and (criterion['override_value'] is not None) and ('text' in criterion['override_value'][0]):
                    value = criterion['override_value'][0]['text']

                valid_override_ts = 'override_time' in criterion and criterion['override_time'] is not None
                order_ts = criterion['override_time'] if valid_override_ts else criterion['measurement_time']

                data[criterion["name"]] = {
                    "name": criterion["name"],
                    "status": value,
                    "time": order_ts,
                    "user": criterion['override_user'],
                    "note": "note"
                }

        # update sirs
        data['severe_sepsis']['sirs']['is_met'] = sirs_cnt > 1
        data['severe_sepsis']['sirs']["num_met"] = sirs_cnt
        if sirs_cnt > 1:
            data['severe_sepsis']['sirs']['onset_time'] = sorted(sirs_onsets)[1]

        # update organ dysfunction
        data['severe_sepsis']['organ_dysfunction']['is_met'] = od_cnt > 0
        data['severe_sepsis']['organ_dysfunction']["num_met"] = od_cnt
        if od_cnt > 0:
            data['severe_sepsis']['organ_dysfunction']['onset_time'] = sorted(od_onsets)[0]

        # update severe_sepsis
        if data['severe_sepsis']['sirs']['is_met'] and \
            data['severe_sepsis']['organ_dysfunction']['is_met'] and\
            not ( data['severe_sepsis']['suspicion_of_infection']['value'] == 'No Infection' \
                or data['severe_sepsis']['suspicion_of_infection']['value'] is None):
            data['severe_sepsis']['is_met'] = 1
            data['severe_sepsis']['onset_time'] = sorted(
                    [
                        data['severe_sepsis']['sirs']['onset_time'] ,
                        data['severe_sepsis']['organ_dysfunction']['onset_time'] ,
                        data['severe_sepsis']['suspicion_of_infection']['update_time']
                        ]
                )[2]
            data['chart_data']['severe_sepsis_onset']['timestamp'] = data['severe_sepsis']['onset_time']
        else:
            data['severe_sepsis']['is_met'] = 0

        # update septic shock
        data['septic_shock']['hypotension']['is_met'] = hp_cnt > 0
        data['septic_shock']['hypotension']['num_met'] = hp_cnt
        data['septic_shock']['hypoperfusion']['is_met'] = hpf_cnt > 0
        data['septic_shock']['hypoperfusion']['num_met'] = hpf_cnt
        if data['severe_sepsis']['is_met']:
            # only when severs_sepsis is met
            if (data['septic_shock']['crystalloid_fluid']['is_met'] == 1 and hp_cnt > 0) or hpf_cnt > 0:
                data['septic_shock']['is_met'] = True
                # setup onset time
                if data['septic_shock']['crystalloid_fluid']['is_met'] == 1 and hp_cnt > 0:
                    max_fluid_time = max(data['septic_shock']['crystalloid_fluid']['override_time'], data['septic_shock']['crystalloid_fluid']['measurement_time'])
                    data['septic_shock']['onset_time'] = sorted(shock_onsets_hypotension + [max_fluid_time])[-1]
                    # print shock_onsets_hypotension
                    if hpf_cnt > 0:
                        data['septic_shock']['onset_time'] = sorted([data['septic_shock']['onset_time']] +shock_onsets_hypoperfusion)[0]
                else:
                    data['septic_shock']['onset_time'] = sorted(shock_onsets_hypoperfusion)[0]
        #logging.info(json.dumps(data['severe_sepsis'], indent=4))
        #logging.info(json.dumps(data['septic_shock'], indent=4))




    def update_response_json(self, data, eid):
        # update chart data
        data['pat_id'] = eid
        criteria = query.get_criteria(eid)

        deactivated = query.get_deactivated(eid)
        data['deactivated'] = deactivated

        deterioration_feedback = query.get_deterioration_feedback(eid)
        data['deterioration_feedback'] = deterioration_feedback

        auditlist = query.get_criteria_log(eid)
        data['auditlist'] = auditlist

        # update criteria from database query
        self.update_criteria(criteria, data)
        data['chart_data']['trewscore_threshold'] = THRESHOLD
        admittime = query.get_admittime(eid)
        data['chart_data']['patient_arrival']['timestamp'] =  admittime
        df = query.get_trews(eid)
        cdm = query.get_cdm(eid)
        epoch = pd.DatetimeIndex(df.tsp).astype(np.int64) // 10**9
        data['chart_data']['chart_values']['timestamp'] = epoch.values
        data['chart_data']['chart_values']['trewscore'] = [s.item() for s in df.trewscore.values]
        df_trews = df.drop(['enc_id','trewscore','tsp'],1)


        # for each row sort by column
        sorted_trews = [row.sort_values(ascending=False) for idx, row in df_trews.iterrows()]
        # for idx, row in df_trews.iterrows():
        #     sorted_row = row.sort_values(ascending=False)
        #     print sorted_row.index[0], sorted_row[0]
        data['chart_data']['chart_values']['tf_1_name'] = [row.index[0] for row in sorted_trews]
        vals = []
        for i, row in enumerate(sorted_trews):
            fid = row.index[0]
            if fid in cdm.iloc[i]:
                vals.append(cdm.iloc[i][fid])
            else:
                vals.append(None)
        data['chart_data']['chart_values']['tf_1_value'] = vals

        data['chart_data']['chart_values']['tf_2_name'] = [row.index[1] for row in sorted_trews]

        vals = []
        for i, row in enumerate(sorted_trews):
            fid = row.index[1]
            if fid in cdm.iloc[i]:
                vals.append(cdm.iloc[i][fid])
            else:
                vals.append(None)
        data['chart_data']['chart_values']['tf_2_value'] = vals

        data['chart_data']['chart_values']['tf_3_name'] = [row.index[2] for row in sorted_trews]

        vals = []
        for i, row in enumerate(sorted_trews):
            fid = row.index[2]
            if fid in cdm.iloc[i]:
                vals.append(cdm.iloc[i][fid])
            else:
                vals.append(None)
        data['chart_data']['chart_values']['tf_3_value'] = vals


        # df_rank = df_trews.rank(axis=1, method='max', ascending=False)
        # top1 = df_rank.as_matrix() < 1.5
        # top1_cols = [df_rank.columns.values[t][0] for t in top1]
        # data['chart_data']['chart_values']['tf_1_name'] \
        #     = [df_rank.columns.values[t][0] for t in top1]
        # data['chart_data']['chart_values']['tf_1_value'] = []
        # for i, row in cdm.iterrows():
        #     if top1_cols[i] in row:
        #         data['chart_data']['chart_values']['tf_1_value'].append(row[top1_cols[i]])
        #     else:
        #         data['chart_data']['chart_values']['tf_1_value'].append(0)
        # top2 = (df_rank.as_matrix() < 2.5) & (df_rank.as_matrix() > 1.5)
        # top2_cols = [df_rank.columns.values[t][0] for t in top2]
        # data['chart_data']['chart_values']['tf_2_name'] \
        #     = [df_rank.columns.values[t][0] for t in top2]
        # # data['chart_data']['chart_values']['tf_2_value'] \
        # #      = [row[top2_cols[i]] for i, row in cdm.iterrows()]
        # for i, row in cdm.iterrows():
        #     if top2_cols[i] in row:
        #         data['chart_data']['chart_values']['tf_2_value'].append(row[top2_cols[i]])
        #     else:
        #         data['chart_data']['chart_values']['tf_2_value'].append(0)
        # top3 = (df_rank.as_matrix() < 3.5) & (df_rank.as_matrix() > 2.5)
        # top3_cols = [df_rank.columns.values[t][0] for t in top3]
        # data['chart_data']['chart_values']['tf_3_name'] \
        #     = [df_rank.columns.values[t][0] for t in top3]
        # # data['chart_data']['chart_values']['tf_3_value'] \
        # #      = [row[top3_cols[i]] for i, row in cdm.iterrows()]
        # for i, row in cdm.iterrows():
        #     if top3_cols[i] in row:
        #         data['chart_data']['chart_values']['tf_3_value'].append(row[top3_cols[i]])
        #     else:
        #         data['chart_data']['chart_values']['tf_3_value'].append(0)

        # update_notifications
        data['notifications'] = query.get_notifications(eid)

    def on_post(self, req, resp):
        with trews_api_request_latency.labels("any").time():
            trews_api_request_counts.labels("any").inc()
            srvnow = datetime.datetime.utcnow().isoformat()
            try:
                raw_json = req.stream.read()
                logging.info('%(date)s %(reqdate)s %(remote_addr)s %(access_route)s %(protocol)s %(method)s %(host)s %(headers)s %(body)s'
                    % { 'date'         : srvnow,
                        'reqdate'      : str(req.date),
                        'remote_addr'  : req.remote_addr,
                        'access_route' : req.access_route,
                        'protocol'     : req.protocol,
                        'method'       : req.method,
                        'host'         : req.host,
                        'headers'      : str(req.headers),
                        'body'         : json.dumps(raw_json, indent=4) })

            except Exception as ex:
                # logger.info(json.dumps(ex, default=lambda o: o.__dict__))
                raise falcon.HTTPError(falcon.HTTP_400,
                    'Error',
                    ex.message)

            try:
                req_body = json.loads(raw_json)
            except ValueError as ex:
                # logger.info(json.dumps(ex, default=lambda o: o.__dict__))
                raise falcon.HTTPError(falcon.HTTP_400,
                    'Malformed JSON',
                    'Could not decode the request body. The '
                    'JSON was incorrect. request body = %s' % raw_json)

            eid = req_body['q']
            uid = req_body['u'] if 'u' in req_body and req_body['u'] is not None else 'user'
            resp.status = falcon.HTTP_202
            data = copy.deepcopy(data_example.patient_data_example)

            if eid:
                # if DECRYPTED:
                #     eid = self.decrypt(eid)
                #     print("unknown eid: " + eid)

                if query.eid_exist(eid):
                    print("query for eid:" + eid)

                    response_body = {}
                    if 'actionType' in req_body and 'action' in req_body:
                        actionType = req_body['actionType']
                        with trews_api_request_latency.labels(actionType).time():
                            trews_api_request_counts.labels(actionType).inc()
                            actionData = req_body['action']

                            if actionType is not None:
                                response_body = self.take_action(actionType, actionData, eid, uid)

                            if actionType != u'pollNotifications' and actionType != u'pollAuditlist':
                                self.update_response_json(data, eid)
                                response_body = {'trewsData': data}
                    else:
                        response_body = {'message': 'Invalid TREWS REST API request'}

                    resp.body = json.dumps(response_body, cls=NumpyEncoder)
                    resp.status = falcon.HTTP_200

                else:
                    resp.status = falcon.HTTP_400
                    resp.body = json.dumps({'message': 'No patient found'})
        push_to_gateway(prom_gateway_url, job=prom_job, registry=registry)


    def hash_password(key):
        """
        example in test_hash.py
        Note: not sure do we need to run this code in runtime
        """
        hash_object = hashlib.sha1(key.encode('utf-8'))
        dig = bytearray(hash_object.digest())
        hex_dig = hash_object.hexdigest()
        key_size = 128
        hashed_key_1 = bytearray('\x00' * 64)
        hashed_key_2 = bytearray('\x00' * 64)

        for i in range(64):
            if i < len(dig):
                hashed_key_1[i] = dig[i] ^ 0x36
            else:
                hashed_key_1[i] = 0x36

        for i in range(64):
            if i < len(dig):
                hashed_key_2[i] = dig[i] ^ 0x5c
            else:
                hashed_key_2[i] = 0x5c

        hash_object = hashlib.sha1(hashed_key_1)
        hashed_key_1 = bytearray(hash_object.digest())

        hash_object = hashlib.sha1(hashed_key_2)
        hashed_key_2 = bytearray(hash_object.digest())


        hashed_key = hashed_key_1 + hashed_key_2
        return binascii.hexlify(hashed_key[:16])