import gevent.monkey
gevent.monkey.patch_all()

import os, sys, traceback
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

import api, dashan_query
from api import pat_cache, api_monitor


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
  'lactate': '2',
  'blood_culture': '4',
  'antibiotics': '5',
  'fluid': '1',
  "vasopressors": '7'
}

user = os.environ['db_user']
host = os.environ['db_host']
db   = os.environ['db_name']
port = os.environ['db_port']
pw   = os.environ['db_password']
client_id = os.environ['jhapi_client_id'],
client_secret = os.environ['jhapi_client_secret']
etl_channel = os.environ['etl_channel'] if 'etl_channel' in os.environ else None


###################################
# Handlers

## Static files.
class TREWSStaticResource(web.View):
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
      # TODO: customize order keys based on LOC
      parameters = self.request.query
      hospital = 'JHH'
      if 'LOC' in parameters:
        loc = parameters['LOC']
        if len(loc) == 6:
          if loc.startswith("1101"):
            loc = 'JHH'
          elif loc.startswith("1102"):
            loc = 'BMC'
            KEYS['antibiotics'] = '6'
            KEYS['vasopressors'] = '13'
          elif loc.startswith("1103"):
            loc = 'HCGH'
            KEYS['antibiotics'] = '3'
          elif loc.startswith("1104"):
            loc = 'Sibley'
          elif loc.startswith("1105"):
            loc = 'Suburban'
          elif loc.startswith("1107"):
            loc = 'KKI'
        else:
          logging.error("LOC parsing error:" + loc)
      else:
        logging.warning("No LOC in query string. Use JHH as default hospital")

      j2_env = Environment(loader=FileSystemLoader(STATIC_DIR), trim_blocks=True)
      r_body = j2_env.get_template(INDEX_FILENAME).render(keys=KEYS)
      logging.info("Static file request on index.html")

    else:
      if os.path.exists(filename):
        with open(filename, 'rb' if f_binary else 'r') as f:
            r_body = f.read()
      else:
        raise web.HTTPNotFound(body=json.dumps({'message': 'Invalid file: %s' % filename}))

    return Response(content_type=r_content_type, body=r_body)


class TREWSLog(web.View):
  async def post(self):
    try:
      log_entry = await self.request.json()
      if 'buffer' in log_entry:
        # Handle frontend stats
        # TODO: separate latency by endpoint and userid (as dimensions per point).
        for req in log_entry['buffer']:
          duration_ms = req['end_time'] - req['start_time']
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
