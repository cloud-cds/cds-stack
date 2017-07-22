import asyncio
import datetime as dt
import etl.io_config.server_protocol as protocol
import logging
import functools

class Predictor:
  def __init__(self, reader, writer, status, index, is_active):
    self.id = "predictor {}-{}".format(index, 'active' if is_active else 'backup')
    self.reader = reader
    self.writer = writer
    self.status = status        # The last status message from the predictor
    self.index = index          # The partition index
    self.is_active = is_active  # active or backup (boolean)
    self.last_updated = dt.datetime.now()

  def send(self):
    pass

  def recv(self):
    pass

  def print(self):
    logging.info('''
        Predictor:
            id: {}
            index: {}
            status: {}
            is_active: {}
            last_updated: {}
        '''.format(self.id, self.index, self.status, self.is_active, self.last_updated)
        )


class PredictorManager:
  def __init__(self, alert_message_queue):
    self.predictors = {}
    self.alert_message_queue = alert_message_queue
    self.predict_task_futures = {}


  async def register(self, reader, writer, msg):
    ''' Register connection from a predictor '''

    # Create predictor object
    pred = Predictor(reader, writer, msg['status'], msg['index'], msg['is_active'])

    # Save predictor in data structure
    self.predictors[pred.id] = pred

    # Start listener loop
    return await self.listen(pred)


  async def listen(self, pred):
    ''' Listen for messages from predictor '''
    while True:
      try:
        message = await protocol.read_message(pred.reader, pred.writer)
      except Exception as e:
        print(e)
        return

      if message == protocol.CONNECTION_CLOSED:
        logging.error('Connection to {} closed'.format(pred.id))
        # TODO: change status in data structure
        return

      print(message)
      pred.status = message.get('status')
      pred.last_updated = dt.datetime.now()
      pred.print()

      if message.get('status') == 'IDLE':
        pass

      elif message.get('status') == 'BUSY':
        pass

      elif message.get('status') == 'DEAD':
        pass

      elif message.get('status') == 'FIN':
        pass

      else:
        logging.error("Can't process this message")

  def get_partition_ids(self):
    partition_ids = set()
    for pred_id in self.predictors:
      pred = self.predictors[pred_id]
      partition_ids.add(pred.index)
    return partition_ids

  def cancel_predict_tasks(self, loop, hosp):
    ''' TODO cancel the existing tasks for previous ETL '''
    logging.info("cancel the existing tasks for previous ETL")
    for future in self.predict_task_futures.get(hosp, []):
      future.cancel()
      logging.info("{} cancelled".format(future))

  def create_predict_tasks(self, loop, hosp):
    self.predict_task_futures[hosp] = []
    for partition_id in self.get_partition_ids():
      future = asyncio.ensure_future(self.run_predict(partition_id=partition_id, hosp=hosp), loop=loop)
      logging.info("create new predict task {} {}".format(partition_id, hosp))
      self.predict_task_futures[hosp].append(future)


  async def run_predict(self, partition_id, hosp):
    # TODO: implement predict logic
    logging.info("start run_predict {} {}".format(partition_id, hosp))
    asyncio.sleep(10)
    # TODO: decide to run active or backup node
    # TODO: monitor both short and long models
    # TODO: communicate the lmc clients if something failed
    logging.info("end run_predict {}".format(partition_id, hosp))
  # def send_to_predictors(self):
  #     for predictor_list in self.predictors:
  #         for predictor in predictor_list:
