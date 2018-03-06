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
from utils import explain

from aiohttp import web
from aiohttp.web import Response, json_response
from aiocache import LRUMemoryCache
from aiocache.plugins import HitMissRatioPlugin

from monitoring import APIMonitor

import re
EID_REGEX = '^((E|Z)[0-9]{9}|[0-9]{,6})$' # including test ids

logging.basicConfig(format='%(levelname)s|%(asctime)s.%(msecs)03d|%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

##############################
# Constants.
no_check_for_orders        = str(os.environ.get('no_check_for_orders', '')).lower() == 'true'
order_signing_timeout_secs = int(os.environ['order_signing_timeout_secs']) if 'order_signing_timeout_secs' in os.environ else 300
order_poll_rate_secs       = int(os.environ['order_poll_rate_secs']) if 'order_poll_rate_secs' in os.environ else 10
chart_sample_start_hrs     = int(os.environ['chart_sample_start_hrs']) if 'chart_sample_start_hrs' in os.environ else 6
chart_sample_start_day     = int(os.environ['chart_sample_start_day']) if 'chart_sample_start_day' in os.environ else 2
chart_sample_end_day       = int(os.environ['chart_sample_end_day']) if 'chart_sample_end_day' in os.environ else 7
chart_sample_mins          = int(os.environ['chart_sample_mins']) if 'chart_sample_mins' in os.environ else 30
chart_sample_hrs           = int(os.environ['chart_sample_hrs']) if 'chart_sample_hrs' in os.environ else 6

# User-based access control.
admin_whitelist = list(map(lambda u: u.lower(), os.environ['admin_whitelist'].split(','))) \
                    if 'admin_whitelist' in os.environ else None

user_whitelist = list(map(lambda u: u.lower(), os.environ['user_whitelist'].split(','))) \
                    if 'user_whitelist' in os.environ else None

# Deactivated sites.
disabled_msg = os.environ['disabled_msg'] if 'disabled_msg' in os.environ else None
location_blacklist = os.environ['location_blacklist'] if 'location_blacklist' in os.environ else None

# Location constants.
loc_prefixes = {
  '1101': 'JHH',
  '1102': 'BMC',
  '1103': 'HCGH',
  '1104': 'Sibley',
  '1105': 'Suburban',
  '1107': 'KKI',
}

locations = {
  'JHH' : 'Johns Hopkins Hospital',
  'BMC' : 'Bayview Medical Center',
  'HCGH' : 'Howard County General Hospital'
}


##############################
# Globals: cache and monitor.
pat_cache = LRUMemoryCache(plugins=[HitMissRatioPlugin()], max_size=5000)
api_monitor = query.api_monitor

# Data structures for order placement in TREWS.
# 1. pending_orders: a dict of pending orders by session.
#    Pending orders are represented as a list of order type and order placement time
#    as initiated by the user through the TREWS page.
#
# 2. order_processing_tasks: a dict of background order checking tasks by session.
#    Tasks are represented as a list of asyncio Tasks.
#
# Entries in both of these datastructures are deleted at the end of a session.
#
pending_orders = {}
order_processing_tasks = {}



# Register API metrics
if api_monitor.enabled:
  api_monitor.register_metric('CacheSize', 'None', [('API', api_monitor.monitor_target)])
  api_monitor.register_metric('CacheHits', 'Count', [('API', api_monitor.monitor_target)])
  api_monitor.register_metric('CacheMisses', 'Count', [('API', api_monitor.monitor_target)])
  api_monitor.register_metric('CacheRequests', 'Count', [('API', api_monitor.monitor_target)])


def temp_f_to_c(f):
    return (f - 32) * .5556

def get_readable_loc(loc):
  global loc_prefixes

  # Convert loc to a human-readable name.
  result = None
  for pfx, loc_name in loc_prefixes.items():
    if loc.startswith(pfx):
      result = loc_name

  return result


###################
# Background tasks

# Removes any order processing task entries every 3 hours,
# to ensure that long sessions don't accumulate memory, and if
# we do not see close_session messages.

gc_order_processing_task = None
do_gc_order_processing_task = True # Background task control variable.

async def run_gc_order_processing_tasks():
  sleep_period = 3 * 3600
  while do_gc_order_processing_task:
    await asyncio.sleep(sleep_period)
    t_gc = datetime.datetime.now() - datetime.timedelta(seconds=sleep_period)

    keys_to_delete = []
    for task_key in order_processing_tasks:
      t_task = order_processing_tasks[task_key]['start']
      if t_task < t_gc:
        logging.info('Found stale order processing task %s (%s < %s = %s)' % (task_key, t_task, t_gc, t_task < t_gc))
        keys_to_delete.append(task_key)
      else:
        logging.info('Skipping GC for order processing task %s (%s < %s = %s)' % (task_key, t_task, t_gc, t_task < t_gc))

    for k in keys_to_delete:
      tasks_to_cancel = order_processing_tasks.pop(k, [])
      cancelled = sum(map(lambda t: 1 if not t['task'].done() and t['task'].cancel() else 0, tasks_to_cancel))
      logging.info('GC session %s cancelling %s / %s tasks' % (k, cancelled, len(tasks_to_cancel)))

async def start_gc_order_processing_tasks(app):
  gc_order_processing_task = asyncio.ensure_future(run_gc_order_processing_tasks())

async def stop_gc_order_processing_tasks(app):
  do_gc_order_processing_task = False
  if gc_order_processing_task and not gc_order_processing_task.done():
    gc_order_processing_task.cancel()


###########################
# Handlers.

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

  async def invalidate_cache(self, db_pool, eid):
    logging.info("Invalidating cache after signing for %s" % eid)
    await pat_cache.delete(eid)
    await query.notify_pat_update(db_pool, os.environ['etl_channel'], eid)

  # an async order checking loop.
  async def check_order_signed(self, db_pool, task_key, eid, uid, timeout_secs, rate_secs):
    # Initialize timeout.
    epoch = datetime.datetime.utcfromtimestamp(0)
    timeout = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout_secs)

    while timeout > datetime.datetime.utcnow():
      # Copy all pending orders since the last sleep (or as initialization)
      orders_to_check = copy.deepcopy(pending_orders[task_key]) if task_key in pending_orders else []
      if orders_to_check:
        logging.info('Found %s pending orders for %s' % (len(orders_to_check), eid))
        ordered = await query.find_active_orders(db_pool = db_pool, eid = eid, orders = orders_to_check)

        if ordered:
          # Mark active orders as overrides.
          order_overrides = { o: None for (o,t) in ordered }
          for o in order_overrides:
            # Shift orders to ensure their measured timestamps (i.e., when actions took place in Epic),
            # are not dominated by our detection timestamps due to our polling process.
            #
            # TODO: use the signing time as the override timestamp for order interactions.
            await query.override_criteria(db_pool, eid, o, value='[{ "text": "Ordered" }]', user=uid, override_pre_offset_secs=30)

          # Immediately invalidate cache.
          await self.invalidate_cache(db_pool, eid)

          # Remove orders found from pending_orders.
          # Note we do this from pending_orders to include any orders added while
          # searching for active orders.
          logging.info('Pending orders and timeout before: %s / %s // %s' % (timeout, ordered, pending_orders[task_key]))
          pending_orders[task_key] = list(filter(lambda o: o not in ordered, pending_orders[task_key]))
          if pending_orders[task_key]:
            timeout = functools.reduce(lambda acc, o: max(acc, o[1]), pending_orders[task_key]) \
                        + datetime.timedelta(seconds=timeout_secs)
          logging.info('Pending orders and timeout after: %s / %s' % (timeout, pending_orders[task_key]))

        else:
          logging.info('No active orders from %s' % orders_to_check)

        # Sleep if we still have orders to process.
        if pending_orders[task_key]:
          await asyncio.sleep(rate_secs)
        else:
          timeout = epoch # Terminate the loop.

      else:
        logging.info('No pending orders for %s' % eid)
        timeout = epoch # Terminate the loop.

    if timeout != epoch and task_key in pending_orders and pending_orders[task_key]:
      orders_to_clear = pending_orders.pop(task_key, [])
      logging.info('Timing out orders %s' % orders_to_clear)
      order_overrides = { o: None for (o,t) in orders_to_clear }
      for o in order_overrides:
        await query.override_criteria(db_pool, eid, o, clear=True, user=uid)

      # Immediately invalidate cache.
      await self.invalidate_cache(db_pool, eid)

    elif task_key in pending_orders and not pending_orders[task_key]:
      # Domain maintenance for pending orders
      pending_orders.pop(task_key, None)

    logging.info('Order checking task completed for %s %s, pending_orders(%s)=%s' \
                    % (eid, task_key, task_key in pending_orders, len(pending_orders.get(task_key, []))))


  # match and test the consistent API for overriding
  async def take_action(self, db_pool, action_key, actionType, actionData, eid, uid):

    # Match poll_notifications first since this is the most common action.
    if actionType == u'poll_notifications':
      notifications = await query.get_notifications(db_pool, eid)
      return {'notifications': notifications}

    elif actionType == u'poll_auditlist':
      auditlist = await query.get_criteria_log(db_pool, eid)
      return {'auditlist': auditlist}

    elif actionType == u'get_antibiotics':
      antibiotics = await query.get_order_detail(db_pool, eid)
      return {'antibiotics_result': antibiotics}

    elif actionType == u'close_session':
      # Clean all session state.
      # - pending_orders
      # - order_processing_tasks
      #
      if action_key and len(action_key) > 1:
        order_action_key = action_key[0] + '-' + 'place_order' + '-' + action_key[1]

        still_ordering = pending_orders.pop(order_action_key, [])
        order_tasks = order_processing_tasks.pop(order_action_key, [])

        cancelled = sum(map(lambda t: 1 if not t['task'].done() and t['task'].cancel() else 0, order_tasks))
        logging.info('Closed session with %s in-flight orders, cancelling %s / %s tasks' \
                        % (len(still_ordering), cancelled, len(order_tasks)))

      else:
        # TODO: return error status here.
        logging.error('Invalid action key when closing a session: %s' % str(action_key))

      return {'result': 'OK'}

    elif actionType == u'override':
      action_is_met = actionData['is_met'] if 'is_met' in actionData else None
      action_is_clear = 'clear' in actionData and actionData['clear']
      logging.info('override_criteria action %(clear)s %(is_met)s: %(v)s' % {'v': json.dumps(actionData), 'clear': action_is_clear, 'is_met': action_is_met})
      if action_is_clear:
        await query.override_criteria(db_pool, eid, actionData['actionName'], clear=True, user=uid, is_met=action_is_met)
      else:
        logging.info('override_criteria value: %(name)s %(val)s' % {'name': actionData['actionName'], 'val': actionData['value']})
        await query.override_criteria(db_pool, eid, actionData['actionName'], value=actionData['value'], user=uid, is_met=action_is_met)

    elif actionType == u'override_many':
      for a in actionData:
        await self.take_action(db_pool, action_key, u'override', a, eid, uid)

    elif actionType == u'suspicion_of_infection':
      if 'value' in actionData and actionData['value'] == 'Reset':
        await query.override_criteria(db_pool, eid, actionData['actionName'], clear=True, user=uid)
      else:
        if "other" in actionData:
          soi = actionData['other']
          value = '[{ "text": "%(val)s", "other": true }]' % {'val': soi}
          await query.override_criteria(db_pool, eid, actionType, value=value, user=uid)
        else:
          soi = actionData['value']
          value = '[{ "text": "%(val)s" }]' % {'val': soi}
          await query.override_criteria(db_pool, eid, actionType, value=value, user=uid)

        visit_id = await query.eid_visit(db_pool, eid)
        if soi and visit_id:
          await query.load_epic_soi_for_bpa(eid, uid, visit_id, soi)
        else:
          logging.error('Failed to push SOI marker for BPA for pt:%s user:%s visit:%s soi:%s' % (eid, uid, visit_id, soi))

    elif actionType == u'notification':
      if 'id' in actionData and 'read' in actionData:
        await query.toggle_notification_read(db_pool, eid, actionData['id'], actionData['read'])
      else:
        msg = 'Invalid notification update action data' + json.dumps(actionData)
        logging.error(msg)
        return {'message': msg}

    elif actionType == u'update_nursing_eval':
        await query.update_nursing_eval(db_pool,eid,actionData,uid)

    elif actionType == u'place_order':

      order_type = actionData['actionName']
      order_time = datetime.datetime.now() - datetime.timedelta(seconds=5)

      if no_check_for_orders:
        # Skip the 'Ordering' step and directly mark as 'Ordered'.
        await query.override_criteria(db_pool, eid, order_type, value='[{ "text": "Ordered" }]', user=uid)
      else:
        # Mark that the user is ordering an item.
        # Fall through to cache invaldation at the end of this block to push the new Ordering status.
        await query.override_criteria(db_pool, eid, order_type, value='[{ "text": "Ordering" }]', user=uid)

        # Check if an order has been signed for (by default) 5 minutes, at 10 second intervals.
        order_task_timeout = order_signing_timeout_secs
        order_task_rate = order_poll_rate_secs

        # If we do not have a request key, the user will wait until the next ETL to see the Ordered status.
        if action_key and len(action_key) > 1:
          task_key = action_key[0] + '-' + actionType + '-' + action_key[1]

          # Add to pending orders.
          if task_key not in pending_orders:
            pending_orders[task_key] = []

          pending_orders[task_key].append((order_type, order_time))

          # Find any existing task.
          order_tasks = order_processing_tasks[task_key] if task_key in order_processing_tasks else []

          # Maintain order processing task list for this session.
          existing_tasks = []
          tasks_to_delete = []
          other_tasks = []
          for t in order_tasks:
            if t['type'] == 'check_orders':
              if t['task'].done():
                tasks_to_delete.append(t)
              else:
                existing_tasks.append(t)
            else:
              other_tasks.append(t)

          logging.info('Order processing tasks stats: existing:%s delete:%s other:%s' % (len(existing_tasks), len(tasks_to_delete), len(other_tasks)) )
          order_processing_tasks[task_key] = existing_tasks + other_tasks

          # Start a order checking task as needed.
          if existing_tasks:
            logging.info('Found existing order checking task for %s: ' % (eid))

          else:
            logging.info('Creating order checking task for %s: %s' % (eid, task_key))
            task = { 'type'  : 'check_orders',
                     'start' : datetime.datetime.now(),
                     'task'  : asyncio.ensure_future( \
                        self.check_order_signed(db_pool, task_key, eid, uid, order_task_timeout, order_task_rate))
                   }

            if task_key not in order_processing_tasks:
              order_processing_tasks[task_key] = []

            order_processing_tasks[task_key].append(task)

        else:
          logging.error('Invalid action key while placing order: key=%s order_type=%s' % (str(action_key), order_type))

    elif actionType == u'complete_order':
      await query.override_criteria(db_pool, eid, actionData['actionName'], value='[{ "text": "Completed" }]', user=uid)

    elif actionType == u'order_inappropriate':
      reason = (':' + actionData['reason']) if 'reason' in actionData and actionData['reason'] else ''
      value = json.dumps([{ 'text': 'Clinically Inappropriate{}'.format(reason)}])
      await query.override_criteria(db_pool, eid, actionData['actionName'], value=value, user=uid)

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

    # All actions other than read-only actions (poll notifications/activities, and get_antibiotics)
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

    TREWS_ORGAN_DYSFUNCTION = ["trews_sbpm",
                               "trews_map",
                               "trews_dsbp",
                               "trews_vent",
                               "trews_creatinine",
                               "trews_bilirubin",
                               "trews_platelet",
                               "trews_inr",
                               "trews_lactate",
                               "trews_gcs",
                               "trews_vasopressors"
                              ]

    TREWS_ALERT_CRITERIA = ['trews_subalert']

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

    UI = [ "ui_septic_shock",
           "ui_severe_sepsis",
           "ui_deactivate" ]

    sirs_cnt     = 0
    od_cnt       = 0
    trews_od_cnt = 0
    trews_subalert_cnt = 0

    sirs_ovr_cnt     = 0
    od_ovr_cnt       = 0
    trews_od_ovr_cnt = 0

    sirs_onsets     = []
    od_onsets       = []
    trews_od_onsets = []
    trews_subalert_onsets = []

    hp_cnt       = 0
    hpf_cnt      = 0
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
          "name"                     : row['name'],
          "is_met"                   : row['is_met'],
          "value"                    : row['value'],
          "measurement_time"         : (row['measurement_time'] - epoch).total_seconds() if row['measurement_time'] is not None else None,
          "override_time"            : (row['override_time'] - epoch).total_seconds() if row['override_time'] is not None else None,
          "override_user"            : row['override_user'],
          "override_value"           : json.loads(row['override_value']) if row['override_value'] is not None else None
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

        if criterion['override_user']:
          sirs_ovr_cnt += 1

      if criterion["name"] in ORGAN_DYSFUNCTION:
        od_idx = ORGAN_DYSFUNCTION.index(criterion["name"])
        data['severe_sepsis']['organ_dysfunction']['criteria'][od_idx] = criterion
        criterion_ts = criterion['override_time'] if criterion['override_user'] else criterion['measurement_time']
        if criterion["is_met"]:
          od_cnt += 1
          od_onsets.append(criterion_ts)

        if criterion['override_user']:
          od_ovr_cnt += 1

      if criterion["name"] in TREWS_ORGAN_DYSFUNCTION:
        trews_od_idx = TREWS_ORGAN_DYSFUNCTION.index(criterion["name"])
        data['severe_sepsis']['trews_organ_dysfunction']['criteria'][trews_od_idx] = criterion
        criterion_ts = criterion['override_time'] if criterion['override_user'] else criterion['measurement_time']
        if criterion["is_met"]:
          trews_od_cnt += 1
          trews_od_onsets.append(criterion_ts)

        if criterion['override_user']:
          trews_od_ovr_cnt += 1

      # update TREWS subalert component
      if criterion["name"] in TREWS_ALERT_CRITERIA:
        data['severe_sepsis'][criterion["name"]] = criterion
        criterion_ts = criterion['override_time'] if criterion['override_user'] else criterion['measurement_time']
        if criterion["is_met"]:
          trews_subalert_cnt += 1
          trews_subalert_onsets.append(criterion_ts)


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
        is_met = criterion['is_met']
        order_ts = criterion['measurement_time']

        valid_override_value = ('override_value' in criterion) and (criterion['override_value'] is not None) and ('text' in criterion['override_value'][0])
        valid_override_ts = 'override_time' in criterion and criterion['override_time'] is not None

        if valid_override_value and valid_override_ts:
            override_value = criterion['override_value'][0]['text']
            override_ts = criterion['override_time']

            # For orders placed through the webapp (i.e., as override_value in ['Ordering', 'Ordered']),
            # we use the value retrieved by ETL if it is more recent than the time of the override action.
            if not (override_value in ['Ordering', 'Ordered'] and order_ts is not None and order_ts > override_ts):
              value = override_value
              order_ts = override_ts

        data[criterion["name"]] = {
          "name": criterion["name"],
          "status": value,
          "time": order_ts,
          "user": criterion['override_user'],
          "note": "note",
          "is_met": is_met
        }

      # update UI components
      if criterion["name"] in UI:
        data['ui'][criterion["name"]] = criterion



    # update sirs
    data['severe_sepsis']['sirs']['is_met'] = sirs_cnt > 1
    data['severe_sepsis']['sirs']["num_met"] = sirs_cnt
    data['severe_sepsis']['sirs']["num_overridden"] = sirs_ovr_cnt
    if sirs_cnt > 1:
      data['severe_sepsis']['sirs']['onset_time'] = sorted(sirs_onsets)[1]

    # update organ dysfunction
    data['severe_sepsis']['organ_dysfunction']['is_met'] = od_cnt > 0
    data['severe_sepsis']['organ_dysfunction']["num_met"] = od_cnt
    data['severe_sepsis']['organ_dysfunction']["num_overridden"] = od_ovr_cnt
    if od_cnt > 0:
      data['severe_sepsis']['organ_dysfunction']['onset_time'] = sorted(od_onsets)[0]

    # update TREWS organ dysfunction
    data['severe_sepsis']['trews_organ_dysfunction']['is_met'] = trews_od_cnt > 0
    data['severe_sepsis']['trews_organ_dysfunction']["num_met"] = trews_od_cnt
    data['severe_sepsis']['trews_organ_dysfunction']["num_overridden"] = trews_od_ovr_cnt
    if trews_od_cnt > 0:
      data['severe_sepsis']['trews_organ_dysfunction']['onset_time'] = sorted(trews_od_onsets)[0]

    if trews_subalert_cnt > 0:
      data['severe_sepsis']['trews_subalert']['onset_time'] = sorted(trews_subalert_onsets)[0]

    # update severe_sepsis
    cms_met = data['severe_sepsis']['sirs']['is_met'] and \
              data['severe_sepsis']['organ_dysfunction']['is_met'] and \
              not ( data['severe_sepsis']['suspicion_of_infection']['value'] == 'No Infection' \
                  or data['severe_sepsis']['suspicion_of_infection']['value'] is None)

    trews_met = (data['severe_sepsis']['trews_subalert']['is_met'] if 'trews_subalert' in data['severe_sepsis'] else False) and \
                not ( data['severe_sepsis']['suspicion_of_infection']['value'] == 'No Infection' \
                    or data['severe_sepsis']['suspicion_of_infection']['value'] is None)


    data['severe_sepsis']['is_met'] = trews_met or cms_met or data['ui']['ui_severe_sepsis']['is_met'] or data['ui']['ui_septic_shock']['is_met']
    data['severe_sepsis']['is_trews'] = trews_met
    data['severe_sepsis']['is_cms'] = cms_met

    if trews_met:
      data['severe_sepsis']['trews_onset_time'] = sorted(
            [ data['severe_sepsis']['trews_subalert']['onset_time'] ,
              data['severe_sepsis']['suspicion_of_infection']['update_time']
            ]
        )[1]

    if cms_met:
      data['severe_sepsis']['cms_onset_time'] = sorted(
            [ data['severe_sepsis']['sirs']['onset_time'] ,
              data['severe_sepsis']['organ_dysfunction']['onset_time'] ,
              data['severe_sepsis']['suspicion_of_infection']['update_time']
            ]
        )[2]

    if data['ui']['ui_severe_sepsis']['is_met'] or data['ui']['ui_septic_shock']['is_met']:
      # Pick the earlier of ui override times, handling if either is none.
      data['severe_sepsis']['onset_time'] = data['ui']['ui_severe_sepsis']['override_time']

      if data['severe_sepsis']['onset_time'] is None:
        data['severe_sepsis']['onset_time'] = data['ui']['ui_septic_shock']['override_time']

      elif data['ui']['ui_septic_shock']['override_time'] is not None:
        data['severe_sepsis']['onset_time'] = min(data['severe_sepsis']['onset_time'], data['ui']['ui_severe_sepsis']['override_time'])

      data['chart_data']['severe_sepsis_onset']['timestamp'] = data['severe_sepsis']['onset_time']

    elif trews_met and cms_met:
      # Pick the earlier onset time if both TREWS and CMS are met.
      data['severe_sepsis']['onset_time'] = min(data['severe_sepsis']['trews_onset_time'], data['severe_sepsis']['cms_onset_time'])
      data['chart_data']['severe_sepsis_onset']['timestamp'] = data['severe_sepsis']['onset_time']

    elif trews_met:
      data['severe_sepsis']['onset_time'] = data['severe_sepsis']['trews_onset_time']
      data['chart_data']['severe_sepsis_onset']['timestamp'] = data['severe_sepsis']['onset_time']

    elif cms_met:
      data['severe_sepsis']['onset_time'] = data['severe_sepsis']['cms_onset_time']
      data['chart_data']['severe_sepsis_onset']['timestamp'] = data['severe_sepsis']['onset_time']


    # update septic shock
    data['septic_shock']['hypotension']['is_met'] = hp_cnt > 0
    data['septic_shock']['hypotension']['num_met'] = hp_cnt
    data['septic_shock']['hypoperfusion']['is_met'] = hpf_cnt > 0
    data['septic_shock']['hypoperfusion']['num_met'] = hpf_cnt

    # update only when severe_sepsis is met
    if data['severe_sepsis']['is_met']:
      if data['ui']['ui_septic_shock']['is_met']:
        data['septic_shock']['is_met'] = True
        data['septic_shock']['onset_time'] = data['ui']['ui_septic_shock']['override_time']
        data['chart_data']['septic_shock_onset']['timestamp'] = data['septic_shock']['onset_time']

      elif (data['septic_shock']['crystalloid_fluid']['is_met'] == 1 and hp_cnt > 0) or hpf_cnt > 0:
        data['septic_shock']['is_met'] = True
        # setup onset time
        if data['septic_shock']['crystalloid_fluid']['is_met'] == 1 and hp_cnt > 0:

          fluid_criterion = data['septic_shock']['crystalloid_fluid']
          max_fluid_time = fluid_criterion['measurement_time']
          if max_fluid_time is None:
            max_fluid_time = fluid_criterion['override_time']
          elif fluid_criterion['override_time'] and fluid_criterion['override_time'] > max_fluid_time:
            max_fluid_time = fluid_criterion['override_time']

          data['septic_shock']['onset_time'] = sorted(shock_onsets_hypotension + [max_fluid_time])[-1]
          if hpf_cnt > 0:
            data['septic_shock']['onset_time'] = sorted([data['septic_shock']['onset_time']] +shock_onsets_hypoperfusion)[0]
        else:
          data['septic_shock']['onset_time'] = sorted(shock_onsets_hypoperfusion)[0]

        data['chart_data']['septic_shock_onset']['timestamp'] = data['septic_shock']['onset_time']


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
                      query.get_patient_events(db_pool, eid),
                      query.get_patient_profile(db_pool, eid),
                      query.get_trews_intervals(db_pool, eid),
                      query.get_feature_mapping(db_pool),
                      query.get_explanations(db_pool, eid),
                      query.get_nursing_eval(db_pool, eid),
                      #query.get_trews_jit_score(db_pool, eid, start_hrs=chart_sample_start_hrs, start_day=chart_sample_start_day, end_day=chart_sample_end_day, sample_mins=chart_sample_mins, sample_hrs=chart_sample_hrs)
                    )

      if pat_values[0] is None or len(pat_values[0]) == 0:
        # cannot find data for this eid
        data = None
        return data
      await pat_cache.set(eid, pat_values, ttl=300)

    else:
      api_monitor.add_metric('CacheHits')

    sz = await pat_cache.raw('__len__')
    api_monitor.add_metric('CacheSize', value=sz)
    api_monitor.add_metric('CacheRequests')

    logging.info('*** Cache stats: s: %s h: %s t: %s' %
      ( sz, pat_cache.hit_miss_ratio["hits"], pat_cache.hit_miss_ratio["total"] ))

    criteria_result_set    = pat_values[0]
    notifications, history = pat_values[1]
    patient_scalars        = pat_values[2]
    trews_intervals        = pat_values[3]
    mapping                = pat_values[4]
    explanations           = pat_values[5]
    nurse_eval             = pat_values[6]
    feature_relevances = explanations['feature_relevance'] if 'feature_relevance' in explanations else None
    measurements       = explanations['twf_raw_values'] if 'twf_raw_values' in explanations else None
    static_features    = explanations['s_raw_values'] if 's_raw_values' in explanations else None
    orgdfs             = explanations['orgdfs'] if 'orgdfs' in explanations else None

    self.update_criteria(criteria_result_set, data)

    try:
      # update chart data
      #data['chart_data']['trewscore_threshold']          = patient_scalars['trews_threshold']
      #data['chart_data']['chart_values']                 = chart_values

      # update consolidated profile
      profile = {
        'age'                  : patient_scalars['age'],
        'admit_time'           : patient_scalars['admit_time'],
        'deactivated'          : patient_scalars['deactivated'],
        'refresh_time'         : patient_scalars['refresh_time'],
        'excluded_units'       : patient_scalars['excluded_units']
      }

      data['profile']              = profile
      data['orgdf_baselines']      = patient_scalars['orgdf_baselines']

      data['deterioration_feedback'] = {
          "tsp"           : patient_scalars['detf_tsp'],
          "deterioration" : patient_scalars['deterioration'],
          "uid"           : patient_scalars['detf_uid']
      }

      # update_notifications and history
      data['notifications'] = notifications
      data['auditlist']     = history

      # update trews intervals
      data['trews_intervals'] = trews_intervals
      if 'sbpm' in measurements and 'dbpm' in measurements:
        measurements['bp'] = {'value': str(measurements['sbpm']['value']) + '/' + str(measurements['dbpm']['value']),
                                        'tsp': measurements['sbpm']['tsp']}
      data['measurements'] = measurements

      if 'gender' in static_features:
        static_features['gender'] = 'Male' if static_features['gender'] == 1 else 'Female'
      data['static_features'] = static_features

      data['feature_relevances'] = explain.thresholdImportances(explain.getMappedImportances(feature_relevances,mapping)) if feature_relevances else None
      # Mark orgdfs as important if present
      if orgdfs is not None:
        for orgdf, value in orgdfs.items():
          if value == 1 and orgdf in mapping:
            data['feature_relevances'][mapping[orgdf]] = 1
        
      data['nursing_eval'] = nurse_eval

      return data

    except KeyError as ex:
      traceback.print_exc()
      logging.warning(str(patient_scalars))
      raise


  ###########################
  # Handler for POST /api
  #
  async def post(self):
    try:
      request_key = None
      action_key = None

      with api_monitor.time(self.request.path):
        api_monitor.request(self.request.path)

        try:
          srvnow = datetime.datetime.utcnow().isoformat()
          req_body = await self.request.json()

          # Make available to the CW log middleware
          if 's' in req_body and req_body['s'] is not None and 'actionType' in req_body:
            request_key_action = 'load' if req_body['actionType'] is None else req_body['actionType']
            request_key = self.request.path + '-' + request_key_action + '-' + req_body['s']
            action_key = (self.request.path, req_body['s'])

          if request_key:
            self.request.app['body'][request_key] = req_body

          logging.info('%(date)s %(method)s %(host)s HDR %(headers)s BODY %(body)s'
              % { 'date'         : srvnow,
                  'method'       : self.request.method,
                  'host'         : self.request.host,
                  'headers'      : dict(self.request.headers.items()),
                  'body'         : req_body })

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
        loc = req_body['loc'] if 'loc' in req_body and req_body['loc'] is not None else ''

        # Whitelisted users access control.
        if user_whitelist and uid and not uid.lower() in user_whitelist:
          # Prioritize disabled message if one exists
          if disabled_msg is not None:
            logging.info("DISABLED")
            raise web.HTTPBadRequest(body=json.dumps({'message': disabled_msg, 'standalone': True}))

          else:
            msg = '<b>Unauthorized access:</b> You are not registered to use TREWS.<br>' + \
                  'Please contact trews-helpdesk@opsdx.io for more information'

            logging.info('Unauthorized access by %s' % uid)
            raise web.HTTPBadRequest(body=json.dumps({'message': msg, 'standalone': True}))

        # Full-site disabling.
        if not user_whitelist and disabled_msg is not None:
          logging.info("DISABLED")
          raise web.HTTPBadRequest(body=json.dumps({'message': disabled_msg, 'standalone': True}))

        # Server-side location-based access control.
        readable_loc = get_readable_loc(loc)
        loc_matched = readable_loc is not None

        deactivated_locs = location_blacklist.split(',') if location_blacklist else []
        deactivated_loc_matched = readable_loc in deactivated_locs

        if (not loc_matched) or deactivated_loc_matched:
          admin_matched = admin_whitelist and uid and uid in admin_whitelist
          if not admin_matched:
            active_locs = [locations[k] for k in locations if k not in deactivated_locs]
            msg = 'TREWS is in beta testing, and is only available at {}.<br/>'.format(', '.join(active_locs)) \
                  + 'Please contact trews-helpdesk@opsdx.io for more information on availability at your location.'
            raise web.HTTPBadRequest(body=json.dumps({'message': msg, 'standalone': True}))

        # Start of request handling.
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
                response_body = await self.take_action(db_pool, action_key, actionType, actionData, eid, uid)

              if not actionType in [u'poll_notifications', u'poll_auditlist', u'get_antibiotics', u'close_session']:
                data = await self.update_response_json(db_pool, data, eid)
                if data is not None:
                  response_body = {'trewsData': data}

                  # Track summary object for user interaction logs.
                  if request_key:
                    self.request.app['render_data'][request_key] = {
                      'notifications'           : data['notifications'],
                      'deactivated'             : data['profile']['deactivated'],
                      'refresh_time'            : data['profile']['refresh_time'],
                      'severe_sepsis'           : { 'is_met'                  : data['severe_sepsis']['is_met'],
                                                    'suspicion_of_infection'  : data['severe_sepsis']['suspicion_of_infection'],
                                                    'sirs'                    : { 'is_met': data['severe_sepsis']['sirs']['is_met'] },
                                                    'organ_dysfunction'       : { 'is_met': data['severe_sepsis']['organ_dysfunction']['is_met'],
                                                                                  'num_met': data['severe_sepsis']['organ_dysfunction']['num_met'],
                                                                                  'overridden': [i['name'] for i in data['severe_sepsis']['organ_dysfunction']['criteria'] if i['override_time'] is not None]
                                                                                },
                                                    'trews_subalert'          : data['severe_sepsis']['trews_subalert'],
                                                    'trews_organ_dysfunction' : { 'is_met': data['severe_sepsis']['trews_organ_dysfunction']['is_met'],
                                                                                  'num_met': data['severe_sepsis']['trews_organ_dysfunction']['num_met'],
                                                                                  'overridden': [i['name'] for i in data['severe_sepsis']['trews_organ_dysfunction']['criteria'] if i['override_time'] is not None]
                                                                                },
                                                  },
                      'septic_shock'            : { 'is_met'            : data['septic_shock']['is_met'],
                                                    'crystalloid_fluid' : { 'is_met': data['septic_shock']['crystalloid_fluid']['is_met'] },
                                                    'hypoperfusion'     : { 'is_met': data['septic_shock']['hypoperfusion']['is_met'] },
                                                    'hypotension'       : { 'is_met': data['septic_shock']['hypotension']['is_met'] }
                                                  },
                      'initial_lactate_order'   : { k: data['initial_lactate_order'][k]   for k in ['status', 'time', 'user'] },
                      'antibiotics_order'       : { k: data['antibiotics_order'][k]       for k in ['status', 'time', 'user'] },
                      'blood_culture_order'     : { k: data['blood_culture_order'][k]     for k in ['status', 'time', 'user'] },
                      'crystalloid_fluid_order' : { k: data['crystalloid_fluid_order'][k] for k in ['status', 'time', 'user'] },
                      'repeat_lactate_order'    : { k: data['repeat_lactate_order'][k]    for k in ['status', 'time', 'user'] },
                      'vasopressors_order'      : { k: data['vasopressors_order'][k]      for k in ['status', 'time', 'user'] },
                      'ui'                      : { 'ui_severe_sepsis': { 'is_met': data['ui']['ui_severe_sepsis']['is_met'] },
                                                    'ui_septic_shock': { 'is_met': data['ui']['ui_septic_shock']['is_met'] }
                                                  },
                      'explanations'            : { 'feature_relevance': data['feature_relevances'],
                                                    'static_features'  : data['static_features']}
                    }

                else:
                  msg = 'This patient recently entered Epic and is not yet available in TREWS. Please check back soon.'
                  raise web.HTTPBadRequest(body=json.dumps({'message': msg, 'standalone': True}))

              # Track summary object for user interaction logs.
              elif actionType == u'poll_notifications' and request_key:
                self.request.app['render_data'][request_key] = { 'notifications': response_body['notifications'] }

          else:
            response_body = {'message': 'Invalid TREWS REST API request'}

          return json_response(response_body)

        else:
          raise web.HTTPBadRequest(body=json.dumps({'message': 'No patient identifier supplied in TREWS request'}))

    except web.HTTPException:
      raise

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))
