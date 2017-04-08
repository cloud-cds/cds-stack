import os, sys, traceback
import datetime
import falcon
import json
import logging
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
class TREWSPrometheusMetrics(object):
  def on_get(self, req, resp):
    try:
      registry = CollectorRegistry()
      multiprocess.MultiProcessCollector(registry)
      resp.status = falcon.HTTP_200
      resp.body = generate_latest(registry)
      resp.content_type = CONTENT_TYPE_LATEST

    except Exception as ex:
      logging.warning(ex.message)
      traceback.print_exc()
      raise falcon.HTTPError(falcon.HTTP_400, 'Error on metrics', ex.message)


# Cloudwatch Logger.
#
# ENVIRONMENT VARIABLES:
#   logging
#   cloudwatch_log_group
#
class CloudwatchLoggerMiddleware(object):
  def __init__(self):
    if 'logging' in os.environ and int(os.environ['logging']) == 1 and 'cloudwatch_log_group' in os.environ:
      self.enabled = True
      self.cwLogger = logging.getLogger(__name__)
      #self.cwLogger.addHandler(watchtower.CloudWatchLogHandler(log_group=os.environ['cloudwatch_log_group'], create_log_group=False))
      self.cwLogger.addHandler(CloudWatchLogHandler(log_group=os.environ['cloudwatch_log_group']))
      self.cwLogger.setLevel(logging.INFO)
    else:
      self.enabled = False

  def process_request(self, req, resp):
    if self.enabled:
      srvnow = datetime.datetime.utcnow().isoformat()
      self.cwLogger.info(json.dumps({
        'req': {
          'date'         : srvnow,
          'reqdate'      : req.date,
          'method'       : req.method,
          'url'          : req.relative_uri,
          'remote_addr'  : req.remote_addr,
          'access_route' : req.access_route,
          'headers'      : req.headers
        }
      }))

  def process_resource(self, req, resp, resource, params):
    if self.enabled:
      srvnow = datetime.datetime.utcnow().isoformat()
      self.cwLogger.info(json.dumps({
        'res': {
          'date'         : srvnow,
          'reqdate'      : req.date,
          'method'       : req.method,
          'url'          : req.relative_uri,
          'remote_addr'  : req.remote_addr,
          'access_route' : req.access_route,
          'headers'      : req.headers,
          'params'       : params
        }
      }))

  def process_response(self, req, resp, resource, req_succeeded):
    if self.enabled:
      srvnow = datetime.datetime.utcnow().isoformat()
      actionType = None
      if 'body' in req.context and 'actionType' in req.context['body']:
        actionType = req.context['body']['actionType']
      self.cwLogger.info(json.dumps({
        'resp': {
          'actionType'   : actionType,
          'date'         : srvnow,
          'reqdate'      : req.date,
          'method'       : req.method,
          'url'          : req.relative_uri,
          'status'       : resp.status[:3],
          'headers'      : req.headers
        }
      }))

