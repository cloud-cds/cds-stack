import asyncio
import datetime as dt
import etl.io_config.server_protocol as protocol
import logging
import functools
import humanize

def predictor_str(partition_index, model_type, is_active):
  return "Predictor {}-{}-{}".format(partition_index,
                                     model_type,
                                     'active' if is_active else 'backup')

class Predictor:
  def __init__(self, reader, writer, status, node_index, partition_index,
               model_type, is_active):
    self.id = (partition_index, model_type, is_active)
    self.reader = reader
    self.writer = writer
    self.status = status # The last status message from the predictor
    self.model_type = model_type # Long or short
    self.node_index = node_index # The k8s node index
    self.partition_index = partition_index # The partition index
    self.is_active = is_active  # active or backup (boolean)
    self.last_updated = dt.datetime.now()
    self.shutdown = False

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

  def stop(self):
    self.shutdown = True

  async def listen(self, queue):
    ''' Listen for messages from worker - waits until message then processes it '''
    self.shutdown = False
    while self.shutdown == False:
      try:
        message = await protocol.read_message(self.reader, self.writer)
      except Exception as e:
        print(e)
        return

      if message == protocol.CONNECTION_CLOSED:
        logging.error('Connection to {} closed'.format(self))
        self.status = 'DEAD'
        return

      self.status = message.get('status')
      self.last_updated = dt.datetime.now()

      if message.get('type') == 'HEARTBEAT':
        logging.info(self.display())
        pass

      elif message.get('type') == 'FIN':
        queue.put({
          'type': 'FIN',
          'time': message['time'],
          'hosp': message['hosp'],
          'enc_ids': message['enc_ids'],
        })

      else:
        logging.error("Can't process this message")


  async def start_predictor(self, hosp, time):
    ''' Start the predictor '''
    logging.info("Starting {}".format(self))
    return await protocol.write_message(self.writer, {
      'type': 'ETL',
      'hosp': hosp,
      'time': time
    })




class PredictorManager:
  def __init__(self, alert_message_queue, event_loop):
    self.predictors = {}
    self.alert_message_queue = alert_message_queue
    self.predict_task_futures = {}
    self.loop = event_loop


  async def register(self, reader, writer, msg):
    ''' Register connection from a predictor '''

    # Create predictor object
    pred = Predictor(reader, writer, msg['status'], msg['node_index'],
                     msg['partition_index'], msg['model_type'], msg['is_active'])

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


  def cancel_predict_tasks(self, hosp):
    ''' Cancel the existing tasks for previous ETL '''
    logging.info("cancel the existing tasks for previous ETL")
    for future in self.predict_task_futures.get(hosp, []):
      future.cancel()
      logging.info("{} cancelled".format(future))


  def create_predict_tasks(self, hosp, time):
    ''' Start all predictors '''
    logging.info("Starting all predictors for ETL {} {}".format(hosp, time))
    self.predict_task_futures[hosp] = []
    for pid in self.get_partition_ids():
      for model in self.get_model_types():
        future = asyncio.ensure_future(self.run_predict(pid, model, hosp, time),
                                       loop=self.loop)
        self.predict_task_futures[hosp].append(future)
    logging.info("Started {} predictors".format(len(self.predict_task_futures[hosp])))




  async def run_predict(self, partition_id, model_type, hosp, time, active=True):
    ''' Start a predictor for a given partition id and model '''
    backoff = 1

    # Start the predictor
    while True:
      pred = self.predictors.get((partition_id, model_type, active))
      if pred and pred.status != 'DEAD':
        try:
          predictor_started = await pred.start_predictor(hosp, time)
          break
        except (ConnectionRefusedError) as e:
          err = e
      else:
        err = '{} dead'.format(predictor_str(partition_id, model_type, active))

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
    start_time = dt.datetime.now()
    timeout = dt.timedelta(seconds = 10)
    while True:
      # TIMEOUT - predictor not getting updated
      if pred.last_updated - start_time > timeout:
        logging.error("{} not getting updated - timeout".format(pred))
        self.loop.create_task(self.run_predict(partition_id, model_type, hosp,
                                               time, not active))
        pred.stop()
        return

      # IDLE - restart run_predict on timeout
      elif pred.status == 'IDLE' and (dt.datetime.now() - start_time) > timeout:
        logging.error("{} is in IDLE state after 10 seconds - timeout".format(pred))
        self.loop.create_task(self.run_predict(partition_id, model_type, hosp,
                                               time, not active))
        pred.stop()
        return

      # BUSY - all ok, keep monitoring
      elif pred.status == 'BUSY':
        pass

      # DEAD - predictor failed, restart run_predict
      elif pred.status == 'DEAD':
        logging.error("{} died, trying again".format(pred))
        self.loop.create_task(self.run_predict(partition_id, model_type, hosp,
                                               time, not active))
        pred.stop()
        return

      # FIN - return
      elif pred.status == 'FIN':
        logging.info("{} finished task, returning".format(pred))
        return

      await asyncio.sleep(1)
