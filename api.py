import os, sys, traceback
import time, datetime, calendar
import asyncio
import copy
import functools
import logging
import json
import pytz

import data_example
import dashan_query as query

from aiohttp import web
from aiohttp.web import Response, json_response
from aiocache import LRUMemoryCache
from aiocache.plugins import HitMissRatioPlugin

from monitoring import APIMonitor

import re
EID_REGEX = '^(E[0-9]{9}|[0-9]{,6})$' # including test ids

logging.basicConfig(format='%(levelname)s|%(asctime)s.%(msecs)03d|%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

##############################
# Constants.
use_trews_lmc          = os.environ['use_trews_lmc'] if 'use_trews_lmc' in os.environ else False
chart_sample_start_hrs = int(os.environ['chart_sample_start_hrs']) if 'chart_sample_start_hrs' in os.environ else 6
chart_sample_start_day = int(os.environ['chart_sample_start_day']) if 'chart_sample_start_day' in os.environ else 2
chart_sample_end_day   = int(os.environ['chart_sample_end_day']) if 'chart_sample_end_day' in os.environ else 7
chart_sample_mins      = int(os.environ['chart_sample_mins']) if 'chart_sample_mins' in os.environ else 30
chart_sample_hrs       = int(os.environ['chart_sample_hrs']) if 'chart_sample_hrs' in os.environ else 6


##############################
# Globals: cache and monitor.
pat_cache = LRUMemoryCache(plugins=[HitMissRatioPlugin()], max_size=5000)
api_monitor = APIMonitor()

# Register API metrics
if api_monitor.enabled:
  api_monitor.register_metric('CacheSize', 'None', [('API', api_monitor.monitor_target)])
  api_monitor.register_metric('CacheHits', 'Count', [('API', api_monitor.monitor_target)])
  api_monitor.register_metric('CacheMisses', 'Count', [('API', api_monitor.monitor_target)])
  api_monitor.register_metric('CacheRequests', 'Count', [('API', api_monitor.monitor_target)])


def temp_f_to_c(f):
    return (f - 32) * .5556

class TREWSAPI(web.View):

  async def get(self):
    try:
      reponse = Response()
      response.content_type = 'text/html'
      response.body = "TREWS API"
      return response

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))

  # match and test the consistent API for overriding
  async def take_action(self, db_pool, actionType, actionData, eid, uid):

    # Match pollNotifications first since this is the most common action.
    if actionType == u'pollNotifications':
      notifications = await query.get_notifications(db_pool, eid)
      return {'notifications': notifications}

    elif actionType == u'pollAuditlist':
      auditlist = await query.get_criteria_log(db_pool, eid)
      return {'auditlist': auditlist}

    elif actionType == u'getAntibiotics':
      antibiotics = await query.get_order_detail(db_pool, eid)
      return {'getAntibioticsResult': antibiotics}

    elif actionType == u'override':
      action_is_clear = 'clear' in actionData and actionData['clear']
      logging.info('override_criteria action %(clear)s: %(v)s' % {'v': json.dumps(actionData), 'clear': action_is_clear})
      if action_is_clear:
        await query.override_criteria(db_pool, eid, actionData['actionName'], clear=True, user=uid)
      else:
        logging.info('override_criteria value: %(name)s %(val)s' % {'name': actionData['actionName'], 'val': actionData['value']})
        await query.override_criteria(db_pool, eid, actionData['actionName'], value=actionData['value'], user=uid)

    elif actionType == u'suspicion_of_infection':
      if 'value' in actionData and actionData['value'] == 'Reset':
        await query.override_criteria(db_pool, eid, actionData['actionName'], clear=True, user=uid)
      else:
        if "other" in actionData:
          value = '[{ "text": "%(val)s", "other": true }]' % {'val': actionData['other']}
          await query.override_criteria(db_pool, eid, actionType, value=value, user=uid)
        else:
          value = '[{ "text": "%(val)s" }]' % {'val': actionData['value']}
          await query.override_criteria(db_pool, eid, actionType, value=value, user=uid)

    elif actionType == u'notification':
      if 'id' in actionData and 'read' in actionData:
        await query.toggle_notification_read(db_pool, eid, actionData['id'], actionData['read'])
      else:
        msg = 'Invalid notification update action data' + json.dumps(actionData)
        logging.error(msg)
        return {'message': msg}

    elif actionType == u'place_order':
      await query.override_criteria(db_pool, eid, actionData['actionName'], value='[{ "text": "Ordered" }]', user=uid)

    elif actionType == u'complete_order':
      await query.override_criteria(db_pool, eid, actionData['actionName'], value='[{ "text": "Completed" }]', user=uid)

    elif actionType == u'order_not_indicated':
      await query.override_criteria(db_pool, eid, actionData['actionName'], value='[{ "text": "Not Indicated" }]', user=uid)

    elif actionType == u'reset_patient':
      event_id = actionData['value'] if actionData is not None and 'value' in actionData else None
      await query.reset_patient(db_pool, eid, uid, event_id)

    elif actionType == u'deactivate':
      await query.deactivate(db_pool, eid, uid, actionData['value'])

    elif actionType == u'set_deterioration_feedback':
      deterioration = {"value": actionData['value'], "other": actionData["other"]}
      await query.set_deterioration_feedback(db_pool, eid, deterioration, uid)

    else:
      msg = 'Invalid action type: ' + actionType
      logging.error(msg)
      return {'message': msg}

    # All actions other than pollNotifications or pollAuditlist
    # reach this point, and thus we invalidate the cached patient data here.
    # These non-polling actions may change the patient or frontend state,
    # thus we must ensure we query the database again.
    logging.info("Invalidating cache for %s" % eid)
    await pat_cache.delete(eid)
    channel = os.environ['etl_channel']
    await query.notify_pat_update(db_pool, channel, eid)
    return {'result': 'OK'}


  def update_criteria(self, criteria_result_set, data):

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

    sirs_cnt    = 0
    od_cnt      = 0
    sirs_onsets = []
    od_onsets   = []
    hp_cnt      = 0
    hpf_cnt     = 0
    shock_onsets_hypotension   = []
    shock_onsets_hypoperfusion = []

    data['event_id'] = None

    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)

    for row in criteria_result_set:
      # Update the event id.
      if data['event_id'] is None and row['event_id'] is not None:
        event_id = str(row['event_id'])
        data['event_id'] = None if event_id == "nan" else event_id

      # update every criteria
      criterion = {
          "name"             : row['name'],
          "is_met"           : row['is_met'],
          "value"            : row['value'],
          "measurement_time" : (row['measurement_time'] - epoch).total_seconds() if row['measurement_time'] is not None else None,
          "override_time"    : (row['override_time'] - epoch).total_seconds() if row['override_time'] is not None else None,
          "override_user"    : row['override_user'],
          "override_value"   : json.loads(row['override_value']) if row['override_value'] is not None else None,
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
        criterion_ts = criterion['override_time'] if criterion['override_user'] else criterion['measurement_time']
        if criterion["is_met"] and criterion_ts is not None:
          sirs_cnt += 1
          sirs_onsets.append(criterion_ts)

      if criterion["name"] in ORGAN_DYSFUNCTION:
        od_idx = ORGAN_DYSFUNCTION.index(criterion["name"])
        data['severe_sepsis']['organ_dysfunction']['criteria'][od_idx] = criterion
        criterion_ts = criterion['override_time'] if criterion['override_user'] else criterion['measurement_time']
        if criterion["is_met"]:
          od_cnt += 1
          od_onsets.append(criterion_ts)


      # septic shock

      if criterion["name"] in HYPOTENSION:
        hp_idx = HYPOTENSION.index(criterion["name"])
        data['septic_shock']['hypotension']['criteria'][hp_idx] = criterion
        criterion_ts = criterion['override_time'] if criterion['override_user'] else criterion['measurement_time']
        if criterion["is_met"]:
          hp_cnt += 1
          shock_onsets_hypotension.append(criterion_ts)


      if criterion["name"] in HYPOPERFUSION:
        hpf_idx = HYPOPERFUSION.index(criterion["name"])
        data['septic_shock']['hypoperfusion']['criteria'][hpf_idx] = criterion
        criterion_ts = criterion['override_time'] if criterion['override_user'] else criterion['measurement_time']
        if criterion["is_met"]:
          hpf_cnt += 1
          shock_onsets_hypoperfusion.append(criterion_ts)


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


  async def update_response_json(self, db_pool, data, eid):
    global pat_cache

    data['pat_id'] = eid

    # cache lookup
    pat_values = await pat_cache.get(eid)

    if pat_values is None:
      api_monitor.add_metric('CacheMisses')

      # parallel query execution
      pat_values = await asyncio.gather(
                      query.get_criteria(db_pool, eid),
                      query.get_trews_contributors(db_pool, eid, use_trews_lmc=use_trews_lmc, start_hrs=chart_sample_start_hrs, start_day=chart_sample_start_day, end_day=chart_sample_end_day, sample_mins=chart_sample_mins, sample_hrs=chart_sample_hrs),
                      query.get_patient_events(db_pool, eid),
                      query.get_patient_profile(db_pool, eid, use_trews_lmc=use_trews_lmc)
                    )
      if pat_values[0] is None or len(pat_values[0]) == 0:
        # cannot find data for this eid
        data = None
        return
      await pat_cache.set(eid, pat_values, ttl=300)
    else:
      api_monitor.add_metric('CacheHits')

    sz = await pat_cache.raw('__len__')
    api_monitor.add_metric('CacheSize', value=sz)
    api_monitor.add_metric('CacheRequests')

    logging.info('*** Cache stats: s: %s h: %s t: %s' %
      ( sz, pat_cache.hit_miss_ratio["hits"], pat_cache.hit_miss_ratio["total"] ))

    criteria_result_set    = pat_values[0]
    chart_values           = pat_values[1]
    notifications, history = pat_values[2]
    patient_scalars        = pat_values[3]

    self.update_criteria(criteria_result_set, data)

    try:
      # update chart data
      data['chart_data']['patient_arrival']['timestamp'] = patient_scalars['admit_time']
      data['chart_data']['trewscore_threshold']          = patient_scalars['trews_threshold']
      data['chart_data']['patient_age']                  = patient_scalars['age']
      data['chart_data']['chart_values']                 = chart_values

      # update profile components
      data['deactivated'] = patient_scalars['deactivated']

      data['deterioration_feedback'] = {
          "tsp"           : patient_scalars['detf_tsp'],
          "deterioration" : patient_scalars['deterioration'],
          "uid"           : patient_scalars['detf_uid']
      }

      # update_notifications and history
      data['notifications'] = notifications
      data['auditlist']     = history

    except KeyError as ex:
      traceback.print_exc()
      logging.warning(str(patient_scalars))
      raise


  async def post(self):
    try:
      with api_monitor.time(self.request.path):
        api_monitor.request(self.request.path)

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
          logging.warning(str(ex))
          traceback.print_exc()
          raise web.HTTPBadRequest(body=json.dumps({'message': 'Invalid JSON body: %s' % str(ex)}))

        except Exception as ex:
          logging.warning(ex.message)
          traceback.print_exc()
          raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))

        db_pool = self.request.app['db_pool']

        eid = req_body['q']
        uid = req_body['u'] if 'u' in req_body and req_body['u'] is not None else 'user'
        data = copy.deepcopy(data_example.patient_data_example)

        if eid and re.match(EID_REGEX, eid):
          logging.info("query for eid: " + eid)

          response_body = {}
          if 'actionType' in req_body and 'action' in req_body:
            actionType = req_body['actionType']
            stats_name = '%s[%s]' % (self.request.path, actionType)

            with api_monitor.time(stats_name):
              api_monitor.request(stats_name)
              actionData = req_body['action']

              if actionType is not None:
                response_body = await self.take_action(db_pool, actionType, actionData, eid, uid)

              if not actionType in [u'pollNotifications', u'pollAuditlist', u'getAntibiotics']:
                await self.update_response_json(db_pool, data, eid)
                if data is not None:
                  response_body = {'trewsData': data}
                  logging.info('trewsData response {}'.format(str(data['severe_sepsis']['suspicion_of_infection'])))
                else:
                  raise web.HTTPBadRequest(body=json.dumps({'message': 'No patient found'}))
          else:
            response_body = {'message': 'Invalid TREWS REST API request'}
          return json_response(response_body)
        else:
          raise web.HTTPBadRequest(body=json.dumps({'message': 'No patient identifier supplied in request'}))

    except web.HTTPException:
      raise

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))
