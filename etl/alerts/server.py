import pandas as pd
pd.set_option('display.width', 200)
import asyncio, asyncpg
import concurrent.futures
import logging
import json
import etl.io_config.server_protocol as protocol
from etl.io_config.database import Database
from etl.alerts.predictor_manager import PredictorManager
import datetime as dt
import pytz
import socket
import random, string
import functools
import os

def randomword(length):
   return ''.join(random.choice(string.ascii_uppercase) for i in range(length))


start_timeout = 15 #seconds

HB_TIMEOUT = 5

SRV_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=SRV_LOG_FMT)


class AlertServer:
  def __init__(self, event_loop, alert_server_port=31000,
               alert_dns='0.0.0.0'):
    self.db                   = Database()
    self.loop                 = event_loop
    self.alert_message_queue  = asyncio.Queue(loop=event_loop)
    self.predictor_manager    = PredictorManager(self.alert_message_queue, self.loop)
    self.alert_server_port    = alert_server_port
    self.alert_dns            = alert_dns
    self.channel              = os.getenv('etl_channel', 'on_opsdx_dev_etl')
    self.suppression_tasks    = {}
    self.model                = os.getenv('suppression_model', 'trews')
    self.notify               = os.getenv('notify_web', 0)


  async def async_init(self):
    self.db_pool = await self.db.get_connection_pool()



  async def convert_enc_ids_to_pat_ids(self, enc_ids):
    ''' Return a list of pat_ids from their corresponding enc_ids '''
    async with self.db_pool.acquire() as conn:
      sql = '''
      SELECT distinct pat_id FROM pat_enc where enc_id
      in ({})
      '''.format(','.join([str(i) for i in enc_ids]))
      pat_ids = await conn.fetch(sql)
      return pat_ids



  async def suppression(self, pat_id, tsp):
    ''' Alert suppression task for a single patient
        and notify frontend that the patient has updated'''

    async def criteria_ready(conn, pat_id, tsp):
      '''
      criteria is ready when
      1. criteria is updated after tsp
      2. no new data in criteria_meas within lookbackhours (ETL will not update criteria)
      '''
      sql = '''
      SELECT count(*) > 0
        or (select count(*) = 0 from criteria_meas m
            where m.pat_id = '{pat_id}' and now() - tsp < (select value::interval from parameters where name = 'lookbackhours')) ready
       FROM criteria where pat_id = '{pat_id}'
      and update_date > '{tsp}'::timestamptz
      '''.format(pat_id=pat_id, tsp=tsp)
      cnt = await conn.fetch(sql)

      return cnt[0]['ready']

    async with self.db_pool.acquire() as conn:
      n = 0
      N = 60

      logging.info("enter suppression task for {} - {}".format(pat_id, tsp))
      while not await criteria_ready(conn, pat_id, tsp):
        await asyncio.sleep(10)
        n += 1
        logging.info("retry criteria_ready {} times for {}".format(n, pat_id))
        if n >= 60:
          break
      if n < 60:
        logging.info("criteria is ready for {}".format(pat_id))
        sql = '''
        select update_suppression_alert('{pat_id}', '{channel}', '{model}', '{notify}');
        '''.format(pat_id=pat_id, channel=self.channel, model=self.model, notify=self.is_notify)
        logging.info("suppression sql: {}".format(sql))
        await conn.fetch(sql)
        logging.info("generate suppression alert for {}".format(pat_id))
      else:
        logging.info("criteria is not ready for {}".format(pat_id))



  def garbage_collect_suppression_tasks(self, hosp):
    for task in self.suppression_tasks.get(hosp, []):
      task.cancel()
    self.suppression_tasks[hosp] = []



  async def alert_queue_consumer(self):
    '''
    Check message queue and process messages
    '''
    logging.info("alert_queue_consumer started")
    while True:
      msg = await self.alert_message_queue.get()
      logging.info("alert_message_queue recv msg: {}".format(msg))
      # Predictor finished
      if msg.get('type') == 'FIN':
        # Wait for Advance Criteria Snapshot to finish and then start generating notifications
        pat_ids = await self.convert_enc_ids_to_pat_ids(msg['enc_ids'])
        logging.info("received FIN for pat_ids: {}".format(pat_ids))
        for pat_id in pat_ids:
          suppression_future = asyncio.ensure_future(self.suppression(pat_id['pat_id'], msg['time']), loop=self.loop)
          self.suppression_tasks[msg['hosp']].append(suppression_future)
          logging.info("created lmc suppression task for {}".format(pat_id['pat_id']))
    logging.info("alert_queue_consumer quit")

  async def run_trews_suppression(self, hospital):
    async with self.db_pool.acquire() as conn:
      sql = '''
      select update_suppression_alert(pat_id, '{channel}', '{model}', '{notify}') from
      (select distinct m.pat_id from criteria_meas m
      inner join pat_hosp() h on h.pat_id = m.pat_id
      where now() - tsp < (select value::interval from parameters where name = 'lookbackhours') and h.hospital = '{hospital}');
        '''.format(channel=self.channel, model=self.model, notify=self.is_notify, hospital=hospital)
      logging.info("suppression sql: {}".format(sql))
      await conn.fetch(sql)
      logging.info("generate trews suppression alert for {}".format(hospital))

  async def connection_handler(self, reader, writer):
    ''' Alert server connection handler '''
    addr = writer.transport.get_extra_info('peername')
    sock = writer.transport.get_extra_info('socket')

    if not addr:
      logging.error('Connection made without a valid remote address, (Timeout %s)' % str(sock.gettimeout()))
      return
    else:
      logging.debug('Connection from %s (Timeout %s)' % (str(addr), str(sock.gettimeout())))

    # Get the message that started this callback function
    message = await protocol.read_message(reader, writer)
    logging.info("connection_handler: recv msg from {} type {}".format(message.get('from'), message.get('type')))
    if message.get('from') == 'predictor':
      return await self.predictor_manager.register(reader, writer, message)

    elif message.get('type') == 'ETL':
      if self.model == 'lmc':
        self.garbage_collect_suppression_tasks(message['hosp'])
        self.predictor_manager.cancel_predict_tasks(hosp=message['hosp'])
        self.predictor_manager.create_predict_tasks(hosp=message['hosp'],
                                                    time=message['time'])
      elif self.model == 'trews':
        await self.run_trews_suppression(message['hosp'])
      else:
        logging.error("Unknown suppression model {}".format(self.model))

    else:
      logging.error("Don't know how to process this message")



  def start(self):
    ''' Start the alert server and queue consumer '''
    self.loop.run_until_complete(self.async_init())
    consumer_future = asyncio.ensure_future(self.alert_queue_consumer())
    server_future = self.loop.run_until_complete(asyncio.start_server(
      self.connection_handler, self.alert_dns, self.alert_server_port, loop=self.loop
    ))
    logging.info('Serving on {}'.format(server_future.sockets[0].getsockname()))
    # Run server until Ctrl+C is pressed
    try:
      self.loop.run_forever()
    except KeyboardInterrupt:
      print("Exiting")
      consumer_future.cancel()
      # Close the server
      logging.info('received stop signal, cancelling tasks...')
      for task in asyncio.Task.all_tasks():
        task.cancel()
      logging.info('bye, exiting in a minute...')
      server_future.close()
      self.loop.run_until_complete(server_future.wait_closed())
      self.loop.stop()
    finally:
      self.loop.close()


def main():
  loop = asyncio.get_event_loop()
  server = AlertServer(loop)
  server.start()

if __name__ == '__main__':
  main()
