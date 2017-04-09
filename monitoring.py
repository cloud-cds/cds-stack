import os, sys, traceback
import datetime
import json
import logging

from aiohttp import web
from aiohttp.web import Response

from cw_log import CloudWatchLogHandler
from prometheus_client import Histogram, Counter, CollectorRegistry, CONTENT_TYPE_LATEST
from prometheus_client import multiprocess, generate_latest

# Prometheus client.
#
# ENVIRONMENT VARIABLES:
#   prometheus
#   db_name
#
class PrometheusMonitor:
  def __init__(self):
    self.enabled = False
    if 'prometheus' in os.environ and int(os.environ['prometheus']) == 1:
        self.enabled = True

    self.prom_job = os.environ['db_name'].replace("_", "-") if 'db_name' in os.environ else 'trews;'
    self.trews_api_request_latency = Histogram('trews_api_request_latency', 'Time spent processing API request', ['deployment', 'actionType'])
    self.trews_api_request_counts = Counter('trews_api_request_counts', 'Number of requests per seconds', ['deployment', 'actionType'])

# Global instance of the prometheus monitor.
prometheus = PrometheusMonitor()

# Prometheus Metrics HTTP endpoint.
class TREWSPrometheusMetrics(web.View):
  async def get(self):
    try:
      registry = CollectorRegistry()
      multiprocess.MultiProcessCollector(registry)
      response = Response()
      response.body = generate_latest(registry)
      response.content_type = CONTENT_TYPE_LATEST
      return response

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))


# Cloudwatch Logger.
#
# ENVIRONMENT VARIABLES:
#   logging
#   cloudwatch_log_group
#

cwlog_enabled = False
if 'logging' in os.environ and int(os.environ['logging']) == 1 and 'cloudwatch_log_group' in os.environ:
  cwlog_enabled = True
  cwlog = logging.getLogger(__name__)
  cwlog.propagate = False
  cwlog.addHandler(CloudWatchLogHandler(log_group=os.environ['cloudwatch_log_group']))
  cwlog.setLevel(logging.INFO)

def log_object(request):
  srvnow = datetime.datetime.utcnow().isoformat()
  return {
    'date'         : srvnow,
    'method'       : request.method,
    'url'          : request.path_qs,
    'headers'      : dict(request.headers.items())
  }

async def cloudwatch_logger_middleware(app, handler):
  async def middleware_handler(request):

    # Pre-logging
    if cwlog_enabled:
      log_entry = log_object(request)
      cwlog.info(json.dumps({ 'req': log_entry }))

    response = await handler(request)

    # Post-logging
    if cwlog_enabled:
      srvnow = datetime.datetime.utcnow().isoformat()

      actionType = None
      if 'body' in request.app and 'actionType' in request.app['body']:
        actionType = request.app['body']['actionType']

      log_entry = log_object(request)
      log_entry['actionType'] = actionType
      log_entry['status'] = response.status
      cwlog.info(json.dumps({ 'resp': log_entry }))

    return response

  return middleware_handler