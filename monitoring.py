import os, sys, traceback
import datetime, time
import json
import logging
import functools
from numbers import Number

import asyncio
from aiohttp import web
from aiohttp.web import Response

#import watchtower
from cw_log import CloudWatchLogHandler
from fluentmetrics import FluentMetric

from prometheus_client import Histogram, Counter, CollectorRegistry, CONTENT_TYPE_LATEST
from prometheus_client import multiprocess, generate_latest

# Prometheus client.
class PrometheusMonitor:
  def __init__(self):
    self.trews_api_request_latency = Histogram('trews_api_request_latency', 'Time spent processing API request', ['deployment', 'actionType'])
    self.trews_api_request_counts = Counter('trews_api_request_counts', 'Number of requests per seconds', ['deployment', 'actionType'])

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


# API Monitor Wrapper, dispatching to either CW/Prometheus
#
# ENVIRONMENT VARIABLES:
#   api_monitor
#   api_monitor_prometheus
#   api_monitor_cw_push_period
#   db_name
#
class APIMonitor:
  def __init__(self):
    self.enabled = False
    if 'api_monitor' in os.environ and int(os.environ['api_monitor']) == 1:
      self.enabled = True

    self.use_prometheus = False
    if 'api_monitor_prometheus' in os.environ and int(os.environ['api_monitor_prometheus']) == 1:
      self.use_prometheus = True

    if self.enabled:
      self.monitor_target = os.environ['db_name'].replace("_", "-") \
                              if 'db_name' in os.environ else 'trews;'

      if self.use_prometheus:
        self.prometheus = PrometheusMonitor()
      else:
        self._push_period_secs = int(os.environ['api_monitor_cw_push_period']) \
                                    if 'api_monitor_cw_push_period' in os.environ else 60

        # k8s pods have their pod name set as the hostname.
        stream_id = os.environ['HOSTNAME'] if 'HOSTNAME' in os.environ else 'api-testing'
        self.cw_metrics = FluentMetric().with_namespace('OpsDX').with_stream_id(stream_id)

        # Latencies and request counters.
        self._counters = {}
        self._latencies = {}

        # General-purpose metrics.
        self._metrics = {}
        self._metric_specs = {}


  # Asynchronous stats uploads.
  async def start_monitor(self, app):
    if self.enabled and not self.use_prometheus:
      loop = asyncio.get_event_loop()
      self._push_handle = loop.call_later(self._push_period_secs, self._cw_flush, loop)
    return True

  async def stop_monitor(self, app):
    if self._push_handle:
      self._push_handle.cancel()
    return True

  # Returns a context manager for timing in a 'with' block.
  def time(self, name):
    if self.enabled:
      if self.use_prometheus:
        return self.prometheus.trews_api_request_latency.labels(self.monitor_target, name).time()
      else:
        return _CloudwatchTimer(self, name)
    else:
      return _NullContextManager()

  # Track a request served/processed. Internally increments a counter.
  def request(self, name, value=1):
    if self.enabled:
      if self.use_prometheus:
        self.prometheus.trews_api_request_counts.labels(self.monitor_target, name).inc()
      else:
        self._request(name, value)

  # Helpers.
  def _latency(self, name):
    if self.enabled:
      timer = self.cw_metrics.get_timer(name)
      duration_ms = timer.elapsed_in_ms()
      if name in self._latencies:
        self._latencies[name].append(duration_ms)
      else:
        self._latencies[name] = [duration_ms]

  def _request(self, name, value):
    if self.enabled:
      if name in self._counters:
        self._counters[name] += value
      else:
        self._counters[name] = value

  # Metrics.
  # TODO: Prometheus implementation
  def register_metric(self, metric_name, unit, dimensions):
    if self.enabled and not self.use_prometheus:
      self._metric_specs[metric_name] = {
        'unit': unit,
        'dimensions': dimensions
      }

  def add_metric(self, name, value=1):
    if self.enabled and not self.use_prometheus:
      if name in self._metrics:
        self._metrics[name] += value
      else:
        self._metrics[name] = value

  def append_metric(self, name, value=1):
    if self.enabled and not self.use_prometheus:
      if name in self._metrics:
        self._metrics[name].append(value)
      else:
        self._metrics[name] = [value]

  # Metrics upload.
  def _cw_flush(self, loop):
    if self.enabled:
      try:
        logging.info('Flushing CW metrics... %s %s' % (len(self._latencies), len(self._counters)))

        self.cw_metrics.with_dimension('API', self.monitor_target)

        for k,v in self._counters.items():
          logging.info('Requests %s %s' % (k, str(v)))
          self.cw_metrics.with_dimension('Route', k)
          self.cw_metrics.count(MetricName='Requests', Count=v)

        for k,v in self._latencies.items():
          self.cw_metrics.with_dimension('Route', k)
          l_cnt = float(len(v))
          l_sum = float(functools.reduce(lambda acc, x: acc+x, v))
          l_avg = l_sum/l_cnt if l_cnt > 0 else 0.0

          logging.info('Latency %s %s %s %s' % (k, l_cnt, l_sum, l_avg))
          self.cw_metrics.count(MetricName='LatencyCount', Count=l_cnt) \
                         .log(MetricName='LatencySum', Value=l_sum, Unit='Milliseconds') \
                         .log(MetricName='LatencyAvg', Value=l_avg, Unit='Milliseconds')

        self.cw_metrics.without_dimension('Route')
        self.cw_metrics.without_dimension('API')

        for k,v in self._metrics.items():
          unit = self._metric_specs.get(k, {}).get('unit', 'None')
          dimensions = self._metric_specs.get(k, {}).get('dimensions', [])

          self.cw_metrics.push_dimensions()
          for dn, dv in dimensions:
            self.cw_metrics.with_dimension(dn, dv)

          if isinstance(v, Number):
            logging.info('NMetric %s %s' % (k, v))
            self.cw_metrics.log(MetricName=k, Value=v, Unit=unit)

          elif isinstance(v, list):
            v_cnt = float(len(v))
            v_sum = float(functools.reduce(lambda acc, x: acc+x, v))
            v_avg = v_sum/v_cnt if v_cnt > 0 else 0.0

            logging.info('LMetric %s %s %s %s' % (k, v_cnt, v_sum, v_avg))
            self.cw_metrics.count(MetricName='%sCount' % k, Count=v_cnt) \
                           .log(MetricName='%sSum' % k, Value=v_sum, Unit=unit) \
                           .log(MetricName='%sAvg' % k, Value=v_avg, Unit=unit)

          self.cw_metrics.pop_dimensions()

        self._metrics = {}
        self._counters = {}
        self._latencies = {}

        # Schedule the next flush.
        self._push_handle = loop.call_later(self._push_period_secs, self._cw_flush, loop)

      except Exception as e:
        logging.error(str(e))
        traceback.print_exc()

        # TODO: exponential backoff on flush failures.


# Context Managers
class _NullContextManager(object):
  def __enter__(self):
    pass

  def __exit__(self, typ, value, traceback):
    pass

class _CloudwatchTimer(object):
  def __init__(self, monitor, job_name):
    self.monitor = monitor
    self.job_name = job_name

  def __enter__(self):
    self.monitor.cw_metrics.with_timer(self.job_name)

  def __exit__(self, typ, value, traceback):
    self.monitor._latency(self.job_name)


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
  cwlog_handler = CloudWatchLogHandler(log_group=os.environ['cloudwatch_log_group'])
  # cwlog_handler = watchtower.CloudWatchLogHandler(log_group=os.environ['cloudwatch_log_group'], create_log_group=False)
  cwlog.addHandler(cwlog_handler)
  cwlog.setLevel(logging.INFO)

def pre_log_object(request):
  srvnow = datetime.datetime.utcnow().isoformat()
  return {
    'date'         : srvnow,
    'method'       : request.method,
    'url'          : request.path_qs,
    'headers'      : dict(request.headers.items())
  }

def post_log_object(request, response):
  srvnow = datetime.datetime.utcnow().isoformat()
  result = {
    'date'         : srvnow,
    'method'       : request.method,
    'url'          : request.path_qs,
    'headers'      : dict(request.headers.items())
  }

  result['status'] = response.status
  for i in ['body', 'render_data']:
    result[i] = request.app[i] if i in request.app else None

  return result


last_log_flush = datetime.datetime.utcnow()
try:
  log_period = int(os.environ['logging_period']) if 'logging_period' in os.environ else 30
except ValueError:
  log_period = 30

async def cloudwatch_logger_middleware(app, handler):
  async def middleware_handler(request):
    global last_log_flush, log_period

    # Pre-logging
    if cwlog_enabled:
      cwlog.info(json.dumps({ 'req': pre_log_object(request) }))

    request.app['body'] = None
    request.app['render_data'] = None
    response = await handler(request)

    # Post-logging
    if cwlog_enabled:
      cwlog.info(json.dumps({ 'resp': post_log_object(request, response) }))

      # Time-based manual flush
      # TODO: exponential backoff on flush failures.
      srvnow = datetime.datetime.utcnow()
      do_flush = (srvnow - last_log_flush).total_seconds() > log_period
      if do_flush:
        cwlog_handler.flush()
        last_log_flush = srvnow

    return response

  return middleware_handler