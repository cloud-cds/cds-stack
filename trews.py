import gevent.monkey
gevent.monkey.patch_all()

import os, sys, traceback
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
from monitoring import TREWSPrometheusMetrics, cloudwatch_logger_middleware, cwlog_enabled

import urllib.parse

import api, dashan_query
from constants import bmc_jhh_antibiotics, bmc_jhh_ed_antibiotics, departments_by_hospital
from api import pat_cache, api_monitor
from encrypt import encrypt, decrypt, encrypted_query


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

# default keys for JHH
KEYS = {
  'lactate'       : '2',
  'blood_culture' : '4',
  'fluid'         : '1',
  'antibiotics'   : '14',
  'vasopressors'  : '13'
}


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

logging.info('''TREWS Configuration::
  encrypted query: %s
  trews_app_key: %s
  trews_admin_key: %s
  trews_open_access: %s
  log_decryption: %s
  log_user_latency: %s
  ie_mode: %s
  ''' % ('on' if encrypted_query else 'off', \
         'on' if trews_app_key else 'off', \
         'on' if trews_admin_key else 'off', \
         'on' if trews_open_access and trews_open_access.lower() == 'true' else 'off',
         'on' if log_decryption else 'off',
         'on' if log_user_latency else 'off',
         ie_mode)
  )

###################################
# Handlers

## Static files.
class TREWSStaticResource(web.View):

  def get_index_body(self, parameters):

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

    # Customize orders.
    custom_antibiotics = None
    if loc in ['JHH', 'BMC']:
      custom_antibiotics = bmc_jhh_ed_antibiotics if dep == 'ED' else bmc_jhh_antibiotics

    elif loc == 'HCGH':
      KEYS['antibiotics'] = '3'
      KEYS['vasopressors'] = '7'

    # TODO: order customizations for SH, SMH, KKI

    logging.info("Index request for loc: {}, dep: {}".format(loc, dep))

    j2_env = Environment(loader=FileSystemLoader(STATIC_DIR), trim_blocks=True)
    return j2_env.get_template(INDEX_FILENAME).render(ie_mode=ie_mode, keys=KEYS, custom_antibiotics=custom_antibiotics)

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

        if params is not None and 'USERID' in params and 'PATID' in params:

          if trews_app_key:
            params['trewsapp'] = encrypt(trews_app_key)

          new_qs = urllib.parse.urlencode(params)

          # Redirect to the index page, with unencrypted query variables.
          # This is necessary becuase our Javascript code retrieves
          # variables from the query string.
          # We add a private token to the query string during the redirect
          # to ensure that consider subsequent accesses verified.
          return web.HTTPFound(URL+INDEX_FILENAME+'?'+new_qs)

        else:
          self.bad_request('Failed to decrypt query parameters')

      else:
        validated = trews_open_access.lower() == 'true' if trews_open_access else False

        if 'trewsapp' in parameters:
          query_app_key_bytes = decrypt(parameters['trewsapp'])
          query_app_key = query_app_key_bytes.decode() if query_app_key_bytes else None
          validated = query_app_key == trews_app_key

        elif 'adminkey' in parameters:
          validated = trews_admin_key == parameters['adminkey']

        if validated:
          r_body = self.get_index_body(parameters)

        else:
          self.bad_request('Unauthorized access')

    else:
      if os.path.exists(filename):
        with open(filename, 'rb' if f_binary else 'r') as f:
            r_body = f.read()
      else:
        raise web.HTTPNotFound(body=json.dumps({'message': 'Invalid file: %s' % filename}))

    return Response(content_type=r_content_type, body=r_body)


  def bad_request(self, error_msg):
    logging.error(error_msg)
    raise web.HTTPBadRequest(body=json.dumps({'message': error_msg}))



class TREWSLog(web.View):
  async def post(self):
    try:
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

      subject = 'Feedback - {}'.format(str(result_json['u']))
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

def invalidate_cache(conn, pid, channel, payload):
  global pat_cache
  logging.info('Invalidating patient cache... (via channel %s)' % channel)
  asyncio.ensure_future(pat_cache.clear())

async def init_db_pool(app):
  global listener_conn
  app['db_pool'] = await asyncpg.create_pool(database=db, user=user, password=pw, host=host, port=port)
  # Set up the ETL listener to invalidate the patient cache.
  if etl_channel is not None:
    listener_conn = await app['db_pool'].acquire()
    logging.info('Added listener on %s' % etl_channel)
    await listener_conn.add_listener(etl_channel, invalidate_cache)

async def cleanup_db_pool(app):
  global listener_conn
  if 'pool' in app:
    # Remove the ETL listener.
    if etl_channel is not None and listener_conn is not None:
      logging.info('Removing listener on %s' % etl_channel)
      await listener_conn.remove_listener(etl_channel, invalidate_cache)

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

# Background tasks.
if api_monitor.enabled:
  app.on_startup.append(api_monitor.start_monitor)
  app.on_cleanup.append(api_monitor.stop_monitor)

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
