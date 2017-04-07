import os

import watchtower
from prometheus_client import Histogram, Counter
from prometheus_client import CollectorRegistry

# Prometheus client.
#
# ENVIRONMENT VARIABLES:
#   prometheus
#   prometheus_timeout
#   db_name
#   prometheus_push_gateway
#
class PrometheusMonitor:
  def __init__(self):
    self.enabled = False
    self.prometheus_timeout = None
    if 'prometheus' in os.environ and int(os.environ['prometheus']) == 1:
        self.enabled = True
        if 'prometheus_timeout' in os.environ:
            self.prometheus_timeout = int(os.environ['prometheus_timeout'])

    self.prom_job = None
    if 'db_name' in os.environ:
      self.prom_job = os.environ['db_name'].replace("_", "-")
    else:
      self.enabled = False

    self.prom_gateway_url = None
    if 'prometheus_push_gateway' in os.environ:
      self.prom_gateway_url = os.environ['prometheus_push_gateway']
    else:
      self.enabled = False

    self.registry = CollectorRegistry()
    self.trews_api_request_latency = Histogram('trews_api_request_latency', 'Time spent processing API request', ['actionType'], registry=self.registry)
    self.trews_api_request_counts = Counter('trews_api_request_counts', 'Number of requests per seconds', ['actionType'], registry=self.registry)


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
        self.cwLogger.addHandler(watchtower.CloudWatchLogHandler(log_group=os.environ['cloudwatch_log_group'], create_log_group=False))
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
        self.cwLogger.info(json.dumps({
            'resp': {
                'date'         : srvnow,
                'reqdate'      : req.date,
                'method'       : req.method,
                'url'          : req.relative_uri,
                'status'       : resp.status[:3],
                'headers'      : req.headers
            }
        }))

