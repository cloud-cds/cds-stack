import os, sys, traceback
import functools
import logging
import json
import time, datetime, calendar
import pandas as pd
import numpy as np
import copy

import data_example
import dashan_query as query

from aiohttp import web
from aiohttp.web import Response, json_response
from monitoring import prometheus

logging.basicConfig(format='%(levelname)s|%(message)s', level=logging.INFO)

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
      print("encoder: %s %s" % (obj, type(obj)))
      return super(NumpyEncoder, self).default(obj)

class TREWSAPI(web.View):
  async def get(self):
    try:
      reponse = Response()
      response.content_type = 'text/html'
      response.body = "TREWS API"
      return response

    except Exception as ex:
      logging.warning(ex.message)
      traceback.print_exc()
      raise web.HTTPBadRequest(ex.message)


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
    data['pat_id'] = eid

    # update criteria from database query
    criteria               = query.get_criteria(eid)
    chart_values           = query.get_trews_contributors(eid)
    notifications, history = query.get_patient_events(eid)
    patient_scalars        = query.get_patient_profile(eid)

    self.update_criteria(criteria, data)

    # update chart data
    data['chart_data']['patient_arrival']['timestamp'] = patient_scalars['admit_time']
    data['chart_data']['trewscore_threshold']          = patient_scalars['trews_threshold']
    data['chart_data']['chart_values']                 = chart_values

    # update_notifications and history
    data['notifications'] = notifications
    data['auditlist']     = history

    # update profile components
    data['deactivated'] = patient_scalars['deactivated']

    data['deterioration_feedback'] = {
        "tsp"           : patient_scalars['detf_tsp'],
        "deterioration" : patient_scalars['deterioration'],
        "uid"           : patient_scalars['detf_uid']
    }


  async def post(self):
    try:
      with prometheus.trews_api_request_latency.labels(prometheus.prom_job, 'any').time():
        prometheus.trews_api_request_counts.labels(prometheus.prom_job, 'any').inc()

        try:
          srvnow = datetime.datetime.utcnow().isoformat()
          req_body = await self.request.json()

          # Make available to the CW log middleware
          self.request.app['body'] = req_body

          logging.info('%(date)s %(method)s %(host)s %(headers)s %(body)s'
              % { 'date'         : srvnow,
                  'method'       : self.request.method,
                  'host'         : self.request.host,
                  'headers'      : dict(self.request.headers.items()),
                  'body'         : json.dumps(req_body, indent=4) })

        except ValueError as ex:
          logging.warning(ex.message)
          traceback.print_exc()
          raise web.HTTPBadRequest('Invalid JSON body', ex.message)

        except Exception as ex:
          logging.warning(ex.message)
          traceback.print_exc()
          raise web.HTTPBadRequest('Error', ex.message)

        eid = req_body['q']
        uid = req_body['u'] if 'u' in req_body and req_body['u'] is not None else 'user'
        data = copy.deepcopy(data_example.patient_data_example)

        if eid:
          if query.eid_exist(eid):
            logging.info("query for eid: " + eid)

            response_body = {}
            if 'actionType' in req_body and 'action' in req_body:
              actionType = req_body['actionType']
              with prometheus.trews_api_request_latency.labels(prometheus.prom_job, actionType).time():
                prometheus.trews_api_request_counts.labels(prometheus.prom_job, actionType).inc()
                actionData = req_body['action']

                if actionType is not None:
                  response_body = self.take_action(actionType, actionData, eid, uid)

                if actionType != u'pollNotifications' and actionType != u'pollAuditlist':
                  self.update_response_json(data, eid)
                  response_body = {'trewsData': data}
            else:
              response_body = {'message': 'Invalid TREWS REST API request'}

            return json_response(response_body, dumps=functools.partial(json.dumps, cls=NumpyEncoder))

          else:
            raise web.HTTPBadRequest(body=json.dumps({'message': 'No patient found'}))

        else:
          raise web.HTTPBadRequest('No patient identifier supplied in request')

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest('Error', ex.message)
