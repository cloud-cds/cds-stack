import falcon
from Crypto.Cipher import AES
from pkcs7 import PKCS7Encoder
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
THRESHOLD = 0.72
logging.basicConfig(format='%(levelname)s|%(message)s', level=logging.DEBUG)
#hashed_key = 'C8ED911A8907EFE4C1DE24CA67DF5FA2'
#hashed_key = '\xC8\xED\x91\x1A\x89\x07\xEF\xE4\xC1\xDE\x24\xCA\x67\xDF\x5F\xA2'
#hashed_key = 'e7cde81226f1d5e03c2681035692964d'
hashed_key = '\xe7\xcd\xe8\x12\x26\xf1\xd5\xe0\x3c\x26\x81\x03\x56\x92\x96\x4d'
IV = '\x00' * 16
MODE = AES.MODE_CBC

DECRYPTED = False

def tsp_to_unix_epoch(tsp):
    if tsp is None or pd.isnull(tsp):
        return None
    else:
        if type(tsp) != datetime.datetime:
            tsp = tsp.to_pydatetime()
        tsp = int(tsp.strftime("%s"))
    return tsp

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
        else:
            return super(MyEncoder, self).default(obj)

class TREWSAPI(object):
    def on_get(self, req, resp):
        """
        See example in test_decrpyt.py
        """
        encrypted_text = urllib.unquote(req.query_string)
        resp.content_type = 'text/html'
        resp.status = falcon.HTTP_200  # This is the default status

        body = "query_string: " + encrypted_text

        decodetext =  base64.b64decode(encrypted_text)
        aes = AES.new(hashed_key, MODE, IV)
        cipher = aes.decrypt(decodetext)
        encoder = PKCS7Encoder()
        pad_text = encoder.decode(cipher)

        body += "</br></br>plain_text: " + pad_text
        resp.body = (body)

    def decrypt(self, encrypted_text):
        encrypted_text = urllib.unquote(encrypted_text)
        decodetext =  base64.b64decode(encrypted_text)
        aes = AES.new(hashed_key, MODE, IV)
        cipher = aes.decrypt(decodetext)
        encoder = PKCS7Encoder()
        pad_text = encoder.decode(cipher)
        return pad_text

    # TODO: match and test the consistent API for overriding
    def take_action(self, actionType, actionData, eid):

        # Match pollNotifications first since this is the most common action.
        if actionType == u'pollNotifications':
            notifications = query.get_notifications(eid)
            return {'notifications': notifications}

        elif actionType == u'override':
            for action in actionData:
                action_is_clear = 'clear' in action
                # Note{andong} In current version, we only customize range not override value
                #action_is_met = action['is_met'] if 'is_met' in action else ('false' if action_is_clear else 'true')
                action_is_met = 'false' if action_is_clear else None
                if action_is_clear:
                    query.override_criteria(eid, action['actionName'], is_met=action_is_met, clear=True)
                else:
                    if 'value' in action:
                        as_min = action['range'] == 'min'
                        value = ('[{ "lower": %(val)s }]' if as_min else '[{"upper": %(val)s }]') % {'val': action['value']}
                        logging.debug('override_criteria value: %(name)s %(val)s' % {'name': action['actionName'], 'val': value})
                        query.override_criteria(eid, action['actionName'], value=value, is_met=action_is_met)
                    else:
                        value = '[{ "lower": %(l)s, "upper": %(u)s }]' % {'l': action['values'][0], 'u': action['values'][1] }
                        logging.debug('override_criteria values: %(name)s %(val)s' % {'name': action['actionName'], 'val': value})
                        query.override_criteria(eid, action['actionName'], value=value, is_met=action_is_met)
            #query.update_notifications()

        elif actionType == u'suspicion_of_infection':
            is_met = 'true'
            if actionData['actionName'] == u'sus-edit':
                if actionData['value'] == 'No Infection':
                    is_met = 'false'

            value = '{ "text": "%(val)s" }' % {'val': actionData['value']}
            query.override_criteria(eid, actionType, value=value, is_met=is_met)
            #query.update_notifications()

        elif actionType == u'notification':
            if 'id' in actionData and 'read' in actionData:
                query.toggle_notification_read(eid, actionData['id'], actionData['read'])
            else:
                logging.error('Invalid notification update action data' + json.dumps(actionData))

        elif actionType == u'place_order':
            query.override_criteria(eid, actionData['actionName'], value='{ "text": "Ordered" }')

        elif actionType == u'complete_order':
            query.override_criteria(eid, actionData['actionName'], value='{ "text": "Completed" }')

        elif actionType == u'order_not_indicated':
            query.override_criteria(eid, actionData['actionName'], value='{ "text": "Not Indicated" }')

        else:
            logging.error('Invalid action type: ' + actionType)

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
                       "mean_arterial_pressure",
                       "decrease_in_sbp"
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
        # TODO: set up the onset time
        for idx, row in criteria.iterrows():
            # update every criteria
            criterion = {
                "name": row['name'],
                "is_met": row['is_met'],
                "value": row['value'],
                "measurement_time": tsp_to_unix_epoch(row['measurement_time']),
                "override_time": tsp_to_unix_epoch(row['override_time']),
                "override_user": row['override_user'],
                "override_value": row['override_value'],
            }

            if criterion["name"] == 'suspicion_of_infection':
                value = criterion['value']
                if ('override_value' in criterion) and (criterion['override_value'] is not None) and ('text' in criterion['override_value']):
                    value = criterion['override_value']['text']

                data['severe_sepsis']['suspicion_of_infection'] = {
                    "name": "suspicion_of_infection",
                    "value": value,
                    "update_time": criterion['override_time'],
                    "update_user": criterion['override_user']
                }


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
                # data['septic_shock']['fluid_administered'] = criterion['is_met'] if 'is_met' in criterion else False
                # if criterion['override_user']:
                #     data['septic_shock']['fluid_administered_time'] = criterion['override_time']
                #     data['septic_shock']['fluid_override_user'] = criterion['override_user']
                # else:
                #     data['septic_shock']['fluid_administered_time'] = criterion['measurement_time']

            # update orders
            if criterion["name"] in ORDERS:
                order_id = re.sub('_order$', '', criterion["name"])
                value = criterion['value']
                if ('override_value' in criterion) and (criterion['override_value'] is not None) and ('text' in criterion['override_value']):
                    value = criterion['override_value']['text']

                data[order_id] = {
                    "name": order_id,
                    "status": value,
                    "time": criterion['override_time'] if 'override_time' in criterion else criterion['measurement_time'],
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
        if (data['septic_shock']['crystalloid_fluid']['is_met'] == 1 and hpf_cnt > 0) or hp_cnt > 0:
            data['septic_shock'] = True
            # setup onset time
            if data['septic_shock']['crystalloid_fluid']['is_met'] == 1 and hpf_cnt > 0:
                data['septic_shock']['onset_time'] = sorted(shock_onsets_hypotension)[0]
                if hp_cnt > 0:
                    data['septic_shock']['onset_time'] = sorted([data['septic_shock']['onset_time']] +shock_onsets_hypoperfusion)[0]
            else:
                data['septic_shock']['onset_time'] = sorted(shock_onsets_hypotension)[0]
        logging.debug(json.dumps(data['severe_sepsis'], indent=4))
        logging.debug(json.dumps(data['septic_shock'], indent=4))




    def update_response_json(self, data, eid):
        """
        TODO: update other part
        """
        # update chart data
        data['pat_id'] = eid
        criteria = query.get_criteria(eid)

        # update criteria from database query
        self.update_criteria(criteria, data)
        data['chart_data']['trewscore_threshold'] = THRESHOLD
        admittime = query.get_admittime(eid)
        data['chart_data']['patient_arrival']['timestamp'] =  admittime
        df = query.get_trews(eid)
        cdm = query.get_cdm(eid)

        data['chart_data']['chart_values']['timestamp'] = [tsp_to_unix_epoch(tsp) for tsp in df.tsp]
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
        # logger = logging.getLogger(__name__)
        # logger.addHandler(watchtower.CloudWatchLogHandler(log_group="opsdx-web-logs-prod", create_log_group=False))

        # logger.info(
        #     {
        #         'req': {
        #             'protocol': req.protocol,
        #             'method': req.method.
        #             'host': req.host,
        #             'subdomain': req.subdomain,
        #             'app': req.app,
        #             'access_route': req.access_route,
        #             'remote_addr': req.remote_addr
        #         }
        #     }
        #     )
        try:
            raw_json = req.stream.read()
            logging.debug(json.dumps(raw_json, indent=4))
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
        resp.status = falcon.HTTP_202
        data = copy.deepcopy(data_example.patient_data_example)

        if eid:
            if DECRYPTED:
                eid = self.decrypt(eid)
                print("unknown eid: " + eid)
            if query.eid_exist(eid):
                print("query for eid:" + eid)
                actionType = req_body['actionType']
                actionData = req_body['action']

                response_body = {}
                if actionType is not None:
                    response_body = self.take_action(actionType, actionData, eid)

                if actionType != u'pollNotifications':
                    self.update_response_json(data, eid)
                    response_body = {'trewsData': data}

                resp.body = json.dumps(response_body, cls=NumpyEncoder)
                resp.status = falcon.HTTP_200
                # logging.debug(json.dumps(data, indent=4 ))
            else:
                resp.status = falcon.HTTP_400
                resp.body = json.dumps({'message': 'no patient found'})



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