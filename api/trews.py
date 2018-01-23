import gevent.monkey
gevent.monkey.patch_all()

import os, sys, traceback
import binascii
import copy
import logging
import datetime
import functools
import json
import boto3

import asyncio
import asyncpg

from aiohttp import web
from aiohttp.web import Response, json_response

from jinja2 import Environment, FileSystemLoader
from monitoring import TREWSPrometheusMetrics, cloudwatch_logger_middleware, cwlog_enabled, start_bg_log_flush, stop_bg_log_flush

import urllib.parse

import api, dashan_query
from constants import bmc_jhh_antibiotics, bmc_jhh_ed_antibiotics, departments_by_hospital, order_key_urls
from api import pat_cache, api_monitor, start_gc_order_processing_tasks, stop_gc_order_processing_tasks

from encrypt import encrypt, decrypt, encrypted_query
import time

#################################
# Constants

STATIC_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(STATIC_DIR, 'static')

URL = '/'
URL_STATIC = URL
URL_API = URL + "api"
URL_LOG = URL + "log"
URL_FEEDBACK = URL + "feedback"
URL_HEALTHCHECK = URL + "healthcheck"
URL_PROMETHEUS_METRICS = URL + "metrics"
INDEX_FILENAME = 'index.html'
ORDER_FILENAME = 'orders.html'

# default keys for JHH-BMC
KEYS = {
  'lactate'       : '2',
  'blood_culture' : '4',
  'fluid'         : '1',
  'antibiotics'   : '12',
  'vasopressors'  : '13'
}

release = os.environ['release'] if 'release' in os.environ else 'development'

user = os.environ['db_user']
host = os.environ['db_host']
db   = os.environ['db_name']
port = os.environ['db_port']
pw   = os.environ['db_password']

etl_channel = os.environ['etl_channel'] if 'etl_channel' in os.environ else None

# Security configuration.
# Also, see 'querystring_key' in encrypt.py
trews_app_key = os.environ['trews_app_key'] if 'trews_app_key' in os.environ else None
trews_admin_key = os.environ['trews_admin_key'] if 'trews_admin_key' in os.environ else None
trews_open_access = os.environ['trews_open_access'] if 'trews_open_access' in os.environ else None

log_decryption = os.environ['log_decryption'].lower() == 'true' if 'log_decryption' in os.environ else False
log_user_latency = os.environ['log_user_latency'].lower() == 'true' if 'log_user_latency' in os.environ else False

ie_mode = os.environ['ie_mode'] if 'ie_mode' in os.environ else '8'
order_link_mode = os.environ['order_link_mode'] if 'order_link_mode' in os.environ else 'frame'

force_server_loc = os.environ['force_server_loc'] if 'force_server_loc' in os.environ else None
force_server_dep = os.environ['force_server_dep'] if 'force_server_dep' in os.environ else None
force_pat_ids = os.environ['force_pat_ids'].split(',') if 'force_pat_ids' in os.environ else None

model_in_use = os.environ['model_in_use'] if 'model_in_use' in os.environ else None

logging.info('''TREWS Configuration::
  release: %s
  epic_server: %s
  v1_flowsheets: %s
  soi_flowsheet: %s
  encrypted query: %s
  trews_app_key: %s
  trews_admin_key: %s
  trews_open_access: %s
  log_decryption: %s
  log_user_latency: %s
  ie_mode: %s
  order_link_mode: %s
  order_signing_timeout_secs: %s
  order_poll_rate_secs: %s
  force_server_loc: %s
  force_server_dep: %s
  force_pat_ids: %s
  model_in_use: %s
  ''' % (release,
         dashan_query.EPIC_SERVER, \
         dashan_query.v1_flowsheets, \
         dashan_query.soi_flowsheet, \
         'on' if encrypted_query else 'off', \
         'on' if trews_app_key else 'off', \
         'on' if trews_admin_key else 'off', \
         'on' if trews_open_access and trews_open_access.lower() == 'true' else 'off',
         'on' if log_decryption else 'off',
         'on' if log_user_latency else 'off',
         ie_mode, order_link_mode,
         api.order_signing_timeout_secs, api.order_poll_rate_secs,
         force_server_loc, force_server_dep,
         ','.join(force_pat_ids) if force_pat_ids else '',
         model_in_use)
  )

# global
epic_sync_tasks = {}

###################################
# Handlers

## Static files.
class TREWSStaticResource(web.View):

  def get_index_body(self, parameters):
    global ie_mode, order_link_mode

    if 'LOC' not in parameters:
      logging.warning("No LOC in query string. Using JHH as default hospital.")
      loc = 'JHH'
    else:
      loc = parameters['LOC']
      if len(loc) == 6:
        loc_prefixes = {
          '1101': 'JHH',
          '1102': 'BMC',
          '1103': 'HCGH',
          '1104': 'Sibley',
          '1105': 'Suburban',
          '1107': 'KKI',
        }

        for pfx, loc_name in loc_prefixes.items():
          if loc.startswith(pfx):
            loc = loc_name

        if loc not in loc_prefixes.values():
          logging.error('Invalid LOC: {}. Using JHH as default hospital.'.format(str(loc)))

      else:
        logging.error('LOC parsing error: {}. Using JHH as default hospital.'.format(str(loc)))
        loc = 'JHH'

    if 'DEP' not in parameters:
      logging.warning("No DEP in query string. Using ICU as default department.")
      dep = 'ICU'
    else:
      dep = parameters['DEP']
      if loc in departments_by_hospital:
        if dep in departments_by_hospital[loc]:
          dep = departments_by_hospital[loc][dep]
        else:
          dep = 'Non-ICU'
      else:
        dep = 'Non-ICU'


    if force_server_loc:
      loc = force_server_loc

    if force_server_dep:
      dep = force_server_dep


    # Customize orders.
    custom_antibiotics = None
    if loc in ['JHH', 'BMC']:
      custom_antibiotics = bmc_jhh_ed_antibiotics if dep == 'ED' else bmc_jhh_antibiotics

    elif loc == 'HCGH':
      KEYS['lactate'] = '6' if dep == 'ED' else '2'
      KEYS['antibiotics'] = '14' if dep == 'ED' else '3'
      KEYS['vasopressors'] = '7'

    # TODO: order customizations for SH, SMH, KKI

    logging.info("Index request for loc: {}, dep: {}".format(loc, dep))

    j2_env = Environment(loader=FileSystemLoader(STATIC_DIR), trim_blocks=True)
    return j2_env.get_template(INDEX_FILENAME) \
                 .render(release=release, loc=loc, ie_mode=ie_mode, order_link_mode=order_link_mode, \
                         keys=KEYS, custom_antibiotics=custom_antibiotics, order_key_urls=order_key_urls)


  async def get(self):
    global URL_STATIC, STATIC_DIR, INDEX_FILENAME

    abspath = self.request.path
    if abspath == '' or abspath == '/':
      abspath = INDEX_FILENAME
      logging.info("index request uri:" + self.request.path_qs)

    elif abspath.startswith('/'):
      abspath = abspath[1:]

    filename = os.path.join(STATIC_DIR, abspath)
    f_binary = False

    if filename.endswith('.css'):
      r_content_type = 'text/css'
    elif filename.endswith('json'):
      r_content_type = 'application/json'
    elif filename.endswith('js'):
      r_content_type = 'application/javascript'
    elif filename.endswith('.html'):
      r_content_type = 'text/html'
    else:
      f_binary = True
      r_content_type = 'application/octet-stream'

    if filename.endswith(INDEX_FILENAME):
      parameters = self.request.query

      if encrypted_query and 'token' in parameters:
        if log_decryption:
          logging.info('Found encrypted query: %s' % parameters['token'])

        param_bytes = decrypt(parameters['token'])
        param_str = param_bytes.decode() if param_bytes else None
        params = dict(urllib.parse.parse_qsl(param_str)) if param_str else None

        if log_decryption:
          logging.info('Decrypted %s' % str(params))

        if params is not None and ('USERID' in params or release != 'production') and 'PATID' in params:

          if trews_app_key:
            params['trewsapp'] = encrypt(trews_app_key)

          if 'USERID' not in params:
            params['USERID'] = 'UNKNOWN'

          # Add a session id
          if 'TSESSID' not in params:
            params['TSESSID'] = binascii.hexlify(os.urandom(24))

          new_qs = urllib.parse.urlencode(params)

          # Redirect to the index page, with unencrypted query variables.
          # This is necessary becuase our Javascript code retrieves
          # variables from the query string.
          # We add a private token to the query string during the redirect
          # to ensure that consider subsequent accesses verified.
          return web.HTTPFound(URL+INDEX_FILENAME+'?'+new_qs)

        else:
          param_keys = list(params.keys()) if params is not None else []
          self.bad_request('Failed to decrypt query parameters (found keys: %s)' % ', '.join(param_keys))

      else:
        validated = trews_open_access.lower() == 'true' if trews_open_access else False

        if 'trewsapp' in parameters:
          query_app_key_bytes = decrypt(parameters['trewsapp'])
          query_app_key = query_app_key_bytes.decode() if query_app_key_bytes else None
          validated = query_app_key == trews_app_key

        elif 'adminkey' in parameters:
          validated = trews_admin_key == parameters['adminkey']

        if validated:
          if 'TSESSID' not in parameters or 'USERID' not in parameters:
            new_params = dict(parameters.items())

            if 'TSESSID' not in new_params:
              new_params['TSESSID'] = binascii.hexlify(os.urandom(24))

            if 'USERID' not in new_params:
              new_params['USERID'] = 'UNKNOWN'

            new_qs = urllib.parse.urlencode(new_params)
            return web.HTTPFound(URL+INDEX_FILENAME+'?'+new_qs)

          elif force_pat_ids and 'PATID' in parameters and parameters['PATID'] in force_pat_ids:
            r_body = self.get_index_body(parameters)

          elif force_pat_ids:
            new_params = dict(parameters.items())
            new_params['PATID'] = force_pat_ids[0]
            new_qs = urllib.parse.urlencode(new_params)
            return web.HTTPFound(URL+INDEX_FILENAME+'?'+new_qs)

          else:
            r_body = self.get_index_body(parameters)

        else:
          self.bad_request('Unauthorized access')

    elif filename.endswith(ORDER_FILENAME):
      parameters = self.request.query
      if 'key' in parameters:
        j2_env = Environment(loader=FileSystemLoader(STATIC_DIR), trim_blocks=True)
        r_body = j2_env.get_template(ORDER_FILENAME).render(key=parameters['key'])

      else:
        self.bad_request('Invalid order key')

    else:
      if os.path.exists(filename):
        try:
          with open(filename, 'rb' if f_binary else 'r') as f:
              r_body = f.read()
        except:
          raise web.HTTPBadRequest(body=json.dumps({'message': 'Failed to open static file: %s' % filename}))
      else:
        raise web.HTTPNotFound(body=json.dumps({'message': 'Invalid static  file: %s' % filename}))

    return Response(content_type=r_content_type, body=r_body)


  def bad_request(self, error_msg):
    logging.error(error_msg)
    raise web.HTTPBadRequest(body=json.dumps({'message': error_msg}))



class TREWSLog(web.View):
  async def post(self):
    try:
      srvnow = datetime.datetime.utcnow().isoformat()
      log_entry = await self.request.json()
      if 'buffer' in log_entry:
        # Handle frontend stats
        # TODO: separate latency by endpoint and userid (as dimensions per point).
        for req in log_entry['buffer']:
          duration_ms = req['end_time'] - req['start_time']
          if log_user_latency:
            logging.info('UserLatency {} {}'.format(str(duration_ms), json.dumps(req)))
          api_monitor.append_metric('UserLatency', value=duration_ms)

      elif 'acc' in log_entry:
        # Handle frontend error logs.
        api_monitor.add_metric('UserErrors')
        logging.error(json.dumps(log_entry, indent=4))

      else:
        # Generic printing
        logging.warning(json.dumps(log_entry, indent=4))

      return Response()

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))


class TREWSFeedback(web.View):
  async def post(self):
    try:
      result_json = await self.request.json()

      await dashan_query.save_feedback(
          db_pool = self.request.app['db_pool'],
          doc_id = str(result_json['u']),
          pat_id = str(result_json['q']),
          dep_id = str(result_json['depid']),
          feedback = str(result_json['feedback'])
      )

      if result_json['u'] is not None:
        subject = 'Feedback - {}'.format(str(result_json['u']))
      else:
        subject = 'Feedback'

      html_text = [
          ("Physician", str(result_json['u'])),
          ("Current patient in view", str(result_json['q'])),
          ("Department", str(result_json['depid'])),
          ("Feedback", str(result_json['feedback'])),
      ]
      body = "".join(["<h4>{}</h4><p>{}</p>".format(x, y) for x,y in html_text])
      client = boto3.client('ses')
      client.send_email(
          Source      = 'trews-jhu@opsdx.io',
          Destination = {
              'ToAddresses': [ 'trews-jhu@opsdx.io' ],
          },
          Message     = {
              'Subject': { 'Data': subject, },
              'Body': {
                  'Html': { 'Data': body, },
              },
          }
      )

      return json_response(result_json)

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': 'Error sending email: %s' % str(ex)}))


class TREWSEchoHealthcheck(web.View):
  async def post(self):
    try:
      hc_json = await self.request.json()
      return json_response(result_json)

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': 'Error echoing healthcheck: %s' % str(ex)}))


############################
# DB Pool init and cleanup.

listener_conn = None

def etl_channel_recv(conn, proc_id, channel, payload):
  logging.info("etl_channel_recv: payload: {}".format(payload))
  if payload is None or len(payload) == 0:
    # the original mode without suppression alert
    invalidate_cache(conn, proc_id, channel, payload)
  else:
    # the simple payload format is <header>:<body>:<model>
    if payload.count(':') == 2:
      if payload.startswith('invalidate_cache:'):
        header, body, model = payload.split(":")
        if model_in_use == model:
          invalidate_cache(conn, proc_id, channel, body.split(","))
      elif payload.startswith('invalidate_cache_batch:'):
        header, serial_id, model = payload.split(":")
        if model_in_use == model:
          global pat_cache
          asyncio.ensure_future(dashan_query.invalidate_cache_batch(app['db_pool'], proc_id, channel, serial_id, pat_cache))
      else:
        logging.error("ETL Channel Error: Unknown payload {}".format(payload))
    elif payload.startswith('future_epic_sync:'):
      header, body = payload.split(":")
      add_future_epic_sync(conn, proc_id, channel, body)
    else:
      logging.error("invalidate_cache payload error: {}".format(payload))


def invalidate_cache(conn, pid, channel, pat_ids):
  global pat_cache
  logging.info('Invalidating patient cache... (via channel %s)' % channel)
  if pat_ids is None or len(pat_ids) == 0:
    asyncio.ensure_future(pat_cache.clear())
  else:
    for pat_id in pat_ids:
      logging.info("Invalidating cache for %s" % pat_id)
      asyncio.ensure_future(pat_cache.delete(pat_id))
      if 'db_pool' in app:
        asyncio.ensure_future(\
          dashan_query.push_notifications_to_epic(app['db_pool'], pat_id,
            notify_future_notification=False))




def add_future_epic_sync(conn, proc_id, channel, body):
  global epic_sync_tasks

  def run_future_epic_sync(pat_id, tsp):
    if 'db_pool' in app:
      asyncio.ensure_future(\
        dashan_query.push_notifications_to_epic(app['db_pool'], pat_id, notify_future_notification=False))
      for i in range(len(epic_sync_tasks.get(pat_id, []))):
        if epic_sync_tasks[pat_id][i]:
          if tsp == epic_sync_tasks[pat_id][i][0]:
            del epic_sync_tasks[pat_id][i]
            return
    else:
      logging.error("DB POOL does not exist ERROR!")

  event_loop = asyncio.get_event_loop()
  delay = 1
  logging.info("add_future_epic_sync: {}".format(body))
  for pat_tsp in body.split('|'):
    pat = pat_tsp.split(",")
    pat_id = pat[0]
    tsps = pat[1:]
    for task in epic_sync_tasks.get(pat_id, []):
      task[1].cancel()
      logging.info("cancel future_epic_sync {},{}".format(pat_id, task[0]))
    epic_sync_tasks[pat_id] = []
    for tsp in tsps:
      later = int(tsp) - time.time() + delay
      new_task = event_loop.call_later(later, run_future_epic_sync, pat_id, tsp)
      epic_sync_tasks[pat_id].append((tsp, new_task))
      logging.info("add future_epic_sync {},{}".format(pat_id, tsp))


async def init_epic_sync_loop(app):
  event_loop = asyncio.get_event_loop()
  if not event_loop.is_running():
    try:
        print('entering event loop')
        event_loop.run_forever()
    finally:
        print('closing event loop')
        event_loop.close()

async def init_db_pool(app):
  global listener_conn
  app['db_pool'] = await asyncpg.create_pool(database=db, user=user, password=pw, host=host, port=port)
  # Set up the ETL listener to invalidate the patient cache.
  if etl_channel is not None:
    listener_conn = await app['db_pool'].acquire()
    logging.info('Added listener on %s' % etl_channel)
    await listener_conn.add_listener(etl_channel, etl_channel_recv)

async def cleanup_db_pool(app):
  global listener_conn
  if 'pool' in app:
    # Remove the ETL listener.
    if etl_channel is not None and listener_conn is not None:
      logging.info('Removing listener on %s' % etl_channel)
      await listener_conn.remove_listener(etl_channel, etl_channel_recv)

    await app['db_pool'].close()


###################
# Routes.

# Configure Falcon middleware.
mware = []
if cwlog_enabled:
  mware = [cloudwatch_logger_middleware]

app = web.Application(middlewares=mware)

app.on_startup.append(init_db_pool)
app.on_cleanup.append(cleanup_db_pool)

epic_notifications = os.environ['epic_notifications']

app.on_startup.append(init_epic_sync_loop)

# Background tasks.
if api_monitor.enabled:
  app.on_startup.append(api_monitor.start_monitor)
  app.on_cleanup.append(api_monitor.stop_monitor)

app.on_startup.append(start_gc_order_processing_tasks)
app.on_cleanup.append(stop_gc_order_processing_tasks)

app.on_startup.append(start_bg_log_flush)
app.on_cleanup.append(stop_bg_log_flush)

app.router.add_route('POST', URL_API, api.TREWSAPI)
app.router.add_route('POST', URL_LOG, TREWSLog)
app.router.add_route('POST', URL_FEEDBACK, TREWSFeedback)

if 'api_with_healthcheck' in os.environ and int(os.environ['api_with_healthcheck']):
  app.router.add_route('POST', URL_HEALTHCHECK, TREWSEchoHealthcheck)

if api_monitor.use_prometheus:
  app.router.add_route('GET', URL_PROMETHEUS_METRICS, TREWSPrometheusMetrics)

app.router.add_route('GET', '/{tail:.*}', TREWSStaticResource)

# Register additional TREWS metrics.
if api_monitor.enabled:
  api_monitor.register_metric('UserLatency', 'Milliseconds', [('Browser' , api_monitor.monitor_target)])
  api_monitor.register_metric('UserErrors', 'Count', [('Browser' , api_monitor.monitor_target)])
