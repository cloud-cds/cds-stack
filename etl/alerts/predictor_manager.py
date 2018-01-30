import asyncio
import datetime as dt
import etl.io_config.server_protocol as protocol
from etl.io_config.cloudwatch import Cloudwatch
import logging
import functools
import humanize
import os

STATUS_DICT = {
  'IDLE': 1,
  'BUSY': 2,
  'CATCHUP': 3,
  'DEAD': 4,
}

def predictor_str(partition_index, model_type, is_active):
  return "Predictor {}-{}-{}".format(partition_index,
                                     model_type,
                                     'active' if is_active else 'backup')

class Predictor:
  def __init__(self, reader, writer, status, node_index, partition_index,
               model_type, is_active, ip_address):
    self.id = (partition_index, model_type, is_active)
    self.reader = reader
    self.writer = writer
    self.shutdown = False

    # Predictor information
    self.model_type = model_type             # Long or short
    self.node_index = node_index             # The k8s node index
    self.partition_index = partition_index   # The patient partition index
    self.is_active = is_active               # active or backup (boolean)
    self.last_updated = dt.datetime.now()    # Time of last update message
    self.ip_address = ip_address             # IP address of the node the predictor is running on
    self.set_status(status)                  # The last status message from the predictor

    # LMC information
    self.avg_total_time = 0
    self.avg_optimization_time = 0
    self.total_time = 0
    self.optimization_time = 0
    self.avg_datainterface_time = 0
    self.datainterface_time = 0

  def __str__(self):
    return predictor_str(self.partition_index, self.model_type, self.is_active)

  def __repr__(self):
    return str(self)

  def display(self):
    return '{} {} (updated {})'.format(
      str(self),
      self.status,
      humanize.naturaltime(dt.datetime.now() - self.last_updated)
    )


  def set_status(self, new_status):
    cur_status = self.status if hasattr(self, 'status') else 'None'
    if cur_status == new_status:
      return
    accepted_statuses = list(STATUS_DICT.keys())
    if not new_status in accepted_statuses:
      logging.error("{} not in {}".format(new_status, accepted_statuses))
      raise ValueError("Predictor status must be in {}".format(accepted_statuses))
    logging.info("{} status change: {} -> {}".format(self, cur_status, new_status))
    self.status = new_status


  def stop(self):
    self.shutdown = True

  async def listen(self, queue):
    ''' Listen for messages from worker - waits until message then processes it '''
    self.shutdown = False
    heartbeats = 0
    while self.shutdown == False:
      try:
        message = await protocol.read_message(self.reader, self.writer)
        logging.info("recv msg: {}".format(message))
      except Exception as e:
        print(e)
        return

      if message == protocol.CONNECTION_CLOSED:
        logging.error('Connection to {} closed'.format(self))
        self.set_status('DEAD')
        return

      self.set_status(message.get('status'))
      self.last_updated = dt.datetime.now()

      if message.get('type') == 'HEARTBEAT':
        heartbeats += 1
        if heartbeats % 30 == 0:
          logging.info(self.display())
          heartbeats = 0
        pass

      elif message.get('type') == 'FIN':
        # NOTE (andong): we also handle catchup fin message here
        logging.info("{} - received FIN: {}".format(self, message))
        num_pats = len(message['predicted_enc_ids'])
        self.total_time = message['total_time']
        self.optimization_time = message['optimization_time']
        self.datainterface_time = message['datainterface_time']
        self.avg_total_time = message['total_time'] / (num_pats if num_pats > 0 else 1)
        self.avg_optimization_time = message['optimization_time'] / (num_pats if num_pats > 0 else 1)
        self.avg_datainterface_time = message['datainterface_time'] / (num_pats if num_pats > 0 else 1)
        logging.info("avg_total_time: {}, avg_optimization_time: {}".format(self.avg_total_time, self.avg_optimization_time))
        await queue.put({
          'type': 'FIN',
          'time': message['time'],
          'hosp': message['hosp'],
          'enc_ids': message['enc_ids'],
          'job_id': message['job_id'],
          'predicted_enc_ids': message['predicted_enc_ids']
        })

      else:
        logging.error("Can't process this message")


  async def start_predictor(self, hosp, time, job_id, active_encids):
    ''' Start the predictor '''
    logging.info("Starting {}".format(self))
    return await protocol.write_message(self.writer, {
      'type': 'ETL',
      'hosp': hosp,
      'time': time,
      'job_id': job_id,
      'active_encids': active_encids
    })




class PredictorManager:
  def __init__(self, alert_message_queue, event_loop):
    self.predictors = {}
    self.alert_message_queue = alert_message_queue
    self.predict_task_futures = {}
    self.loop = event_loop
    self.cloudwatch_logger = Cloudwatch()
    self.model = os.getenv('suppression_model', 'trews')
    # Start monitoring task
    self.loop.create_task(self.monitor_predictors())



  async def monitor_predictors(self):
    ''' Monitor predictors and log current status in cloudwatch every 30 seconds '''

    while True:
      # Build list of tuples of cloudwatch info (name, value, unit)
      metric_tuples = []

      # Get overall predictor info
      metric_tuples.append(('push_num_predictors', int(len(self.predictors)), 'Count'))

      # Send individual predictor info to cloudwatch
      for pred_id, pred in self.predictors.items():
        metric_tuples += [
          ('push_predictor_{}_{}_{}_status'.format(*pred_id), STATUS_DICT[pred.status], 'None'),
        ]
        if pred.avg_total_time > 0:
          metric_tuples += [
            ('push_avg_total_time_{}'.format(pred.model_type), pred.avg_total_time, 'Seconds'),
            ('push_avg_optimization_time_{}'.format(pred.model_type), pred.avg_optimization_time, 'Seconds'),
            ('push_avg_datainterface_time_{}'.format(pred.model_type), pred.avg_datainterface_time, 'Seconds'),
            ('push_total_time_{}'.format(pred.model_type), pred.total_time, 'Seconds'),
            ('push_optimization_time_{}'.format(pred.model_type), pred.optimization_time, 'Seconds'),
            ('push_datainterface_time_{}'.format(pred.model_type), pred.datainterface_time, 'Seconds'),
          ]
          pred.avg_total_time = 0
      logging.info("cloudwatch metrics: {}".format(metric_tuples))
      # Send all info to cloudwatch
      self.cloudwatch_logger.push_many(
        dimension_name = 'LMCPredictors',
        metric_names   = [metric[0] for metric in metric_tuples],
        metric_values  = [metric[1] for metric in metric_tuples],
        metric_units   = [metric[2] for metric in metric_tuples]
      )

      await asyncio.sleep(30)



  async def register(self, reader, writer, msg):
    ''' Register connection from a predictor '''

    # Create predictor object
    pred = Predictor(reader, writer, msg['status'], msg['node_index'],
                     msg['partition_index'], msg['model_type'],
                     msg['is_active'], msg['ip_address'])

    # Cancel any existing predictor with same id
    if pred.id in self.predictors:
      self.predictors[pred.id].shutdown = True

    # Save predictor in data structure
    self.predictors[pred.id] = pred
    logging.info("Registered {}".format(pred))

    # Start listener loop
    return await pred.listen(self.alert_message_queue)


  def get_partition_ids(self):
    return set([p.partition_index for p in self.predictors.values()])

  def get_model_types(self):
    return set([p.model_type for p in self.predictors.values()])


  def cancel_predict_tasks(self, job_id):
    ''' Cancel the existing tasks for previous ETL '''
    logging.info("cancel the existing tasks for previous ETL")
    for future in self.predict_task_futures.get(job_id, []):
      future.cancel()
      logging.info("{} cancelled".format(future))


  def create_predict_tasks(self, hosp, time, job_id, active_encids=None):
    ''' Start all predictors '''
    logging.info("Starting all predictors for ETL {} {} {} {}".format(hosp, time, job_id, active_encids))
    self.predict_task_futures[job_id] = []
    for pid in self.get_partition_ids():
      for model in self.get_model_types():
        future = asyncio.ensure_future(self.run_predict(pid, model, hosp, time, job_id, active_encids),
                                       loop=self.loop)
        self.predict_task_futures[job_id].append(future)
    logging.info("Started {} predictors".format(len(self.predict_task_futures[job_id])))




  async def run_predict(self, partition_id, model_type, hosp, time, job_id, active_encids, active=True):
    ''' Start a predictor for a given partition id and model '''
    backoff = 1

    # Start the predictor
    logging.info("start the predictors for {} at {}: partition_id {} model_type {}".format(hosp, time, partition_id, model_type))
    while True:
      pred = self.predictors.get((partition_id, model_type, active))
      if pred and pred.status != 'DEAD':
        try:
          predictor_started = await pred.start_predictor(hosp, time, job_id, active_encids)
          break
        except (ConnectionRefusedError) as e:
          err = e
      else:
        err = '{} dead'.format(predictor_str(partition_id, model_type, active))

      if self.model == 'trews-jit':
        return
      else:
        # Log reason for error
        logging.error("{} -- trying {} predictor {}".format(
          err, 'backup' if active else 'active',
          humanize.naturaltime(dt.datetime.now() + dt.timedelta(seconds=backoff))
        ))

        # Switch to backup if active (and vice versa)
        active = not active
        await asyncio.sleep(backoff)
        backoff = backoff * 2 if backoff < 64 else backoff

    # Check status
    if predictor_started == False:
      return None

    # Monitor predictor
    logging.info("start monitoring the predictors for {} at {}: partition_id {} model_type {}".format(hosp, time, partition_id, model_type))

    # start_time = dt.datetime.now()
    timeout = dt.timedelta(seconds = 10)
    while True:
      if pred.last_updated - dt.datetime.now() > timeout:
        logging.error("{} not getting updated - timeout - restart run_predict".format(pred))
        pred.stop()
        self.loop.create_task(self.run_predict(partition_id, model_type, hosp,
                                               time, job_id, active_encids, not active))
        return

      # BUSY - all ok, keep monitoring
      elif pred.status == 'BUSY':
        logging.info("{} is busy now".format(pred))

      # DEAD - predictor failed, restart run_predict
      elif pred.status == 'DEAD':
        pred.stop()
        if self.model == 'trews-jit':
          logging.error("{} died.".format(pred))
        else:
          logging.error("{} died, trying again".format(pred))
          self.loop.create_task(self.run_predict(partition_id, model_type, hosp,
                                               time, job_id, active))
        return

      # FIN - return
      elif pred.status == 'FIN':
        logging.info("{} finished task, returning".format(pred))
        return

      await asyncio.sleep(1)
