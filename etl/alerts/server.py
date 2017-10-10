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
from dateutil import parser
import pytz
import socket
import random, string
import functools
import os
from etl.io_config.cloudwatch import Cloudwatch

def randomword(length):
   return ''.join(random.choice(string.ascii_uppercase) for i in range(length))


start_timeout = 15 #seconds

HB_TIMEOUT = 5

SRV_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=SRV_LOG_FMT)


class AlertServer:
  def __init__(self, event_loop, alert_server_port=31000,
               alert_dns='0.0.0.0'):
    self.db                     = Database()
    self.loop                   = event_loop
    self.alert_message_queue    = asyncio.Queue(loop  =event_loop)
    self.predictor_manager      = PredictorManager(self.alert_message_queue, self.loop)
    self.alert_server_port      = alert_server_port
    self.alert_dns              = alert_dns
    self.channel                = os.getenv('etl_channel', 'on_opsdx_dev_etl')
    self.suppression_tasks      = {}
    self.model                  = os.getenv('suppression_model', 'trews')
    self.TREWS_ETL_SUPPRESSION  = int(os.getenv('TREWS_ETL_SUPPRESSION', 0))
    self.notify_web             = int(os.getenv('notify_web', 0))
    self.lookbackhours          = int(os.getenv('TREWS_ETL_HOURS', 24))
    self.nprocs                 = int(os.getenv('nprocs', 2))
    self.hospital_to_predict    = os.getenv('hospital_to_predict', 'HCGH')
    self.cloudwatch_logger      = Cloudwatch()
    self.job_status = {}

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
        '''.format(pat_id=pat_id, channel=self.channel, model=self.model, notify=self.notify_web)
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
        if self.model == 'lmc' or self.model == 'trews-jit':
          if self.TREWS_ETL_SUPPRESSION == 1:
            suppression_future = asyncio.ensure_future(self.run_suppression(msg), loop=self.loop)
          elif self.TREWS_ETL_SUPPRESSION == 2:
            suppression_future = asyncio.ensure_future(self.run_suppression_mode_2(msg), loop=self.loop)
        else:
          logging.error("Unknown model: {}".format(self.model))
        # self.suppression_tasks[msg['hosp']].append(suppression_future)
        # logging.info("create {model} suppression task for {}".format(self.model,msg['hosp']))
    logging.info("alert_queue_consumer quit")

  async def suppression(self, pat_id, tsp):
    ''' Alert suppression task for a single patient
        and notify frontend that the patient has updated'''

  async def run_suppression_mode_2(self, msg):
    t_fin = dt.datetime.now()
    logging.info("start to run suppression mode 2 for msg {}".format(msg))
    tsp = msg['time']
    enc_id_str = ','.join([str(i) for i in msg['enc_ids'] if i])
    hospital = msg['hosp']
    logging.info("received FIN for enc_ids: {}".format(enc_id_str))
    # calculate criteria here
    async with self.db_pool.acquire() as conn:
      await self.calculate_criteria_enc(conn, enc_id_str)
      if self.notify_web:
        # sql = '''
        # with pats as (
        #   select p.enc_id, p.pat_id from pat_enc p
        #   where p.enc_id in ({enc_id_str})
        # ),
        # refreshed as (
        #   insert into refreshed_pats (refreshed_tsp, pats)
        #   select now(), jsonb_agg(pat_id) from pats
        #   returning id
        # )
        # select pg_notify('{channel}', 'invalidate_cache_batch:' || id || ':' || '{model}') from refreshed;
        # '''.format(channel=self.channel, model=self.model, enc_id_str=enc_id_str)
        # NOTE: I don't turst the enc_ids from FIN msg
        sql = '''
        with pats as (
          select p.enc_id, p.pat_id from pat_enc p
          inner join get_latest_enc_ids('{hosp}')
        ),
        refreshed as (
          insert into refreshed_pats (refreshed_tsp, pats)
          select now(), jsonb_agg(pat_id) from pats
          returning id
        )
        select pg_notify('{channel}', 'invalidate_cache_batch:' || id || ':' || '{model}') from refreshed;
        '''.format(channel=self.channel, model=self.model, enc_id_str=enc_id_str, hosp=msg['hosp'])
        logging.info("trews alert sql: {}".format(sql))
        await conn.fetch(sql)
        logging.info("generated trews alert for {}".format(hospital))
    logging.info("complete to run suppression mode 2 for msg {}".format(msg))
    t_end = dt.datetime.now()
    if msg['hosp']+msg['time'] in self.job_status:
      self.cloudwatch_logger.push_many(
        dimension_name = 'Alert Server',
        metric_names   = ['e2e_time_{}'.format(msg['hosp']),
                          'criteria_time_{}'.format(msg['hosp']),
                          'prediction_time_{}'.format(msg['hosp']),
                          'prediction_enc_cnt_{}'.format(msg['hosp'])],
        metric_values  = [(t_end - parser.parse(msg['time'])).total_seconds(),
                          (t_end - t_fin).total_seconds(),
                          (t_end - self.job_status[msg['hosp']+msg['time']]['t_start']).total_seconds(),
                          len(msg['enc_ids'])],
        metric_units   = ['Seconds','Seconds','Seconds', 'Count']
      )
    self.job_status.pop(msg['hosp']+msg['time'],None)


  async def run_suppression(self, msg):
    # Wait for Advance Criteria Snapshot to finish and then start generating notifications
    logging.info("start to run suppression for msg {}".format(msg))
    tsp = msg['time']
    pat_ids = await self.convert_enc_ids_to_pat_ids(msg['enc_ids'])
    pats_str = ','.join([str(i) for i in pat_ids])
    hospital = msg['hosp']
    logging.info("received FIN for enc_ids: {}".format(pats_str))

    async def criteria_ready(conn, enc_ids, tsp):
      '''
      criteria is ready when
      1. criteria is updated after tsp
      2. no new data in criteria_meas within lookbackhours (ETL will not update criteria)
      '''
      sql = '''
      with pats as (
        select distinct enc_id from criteria where enc_id in ({enc_ids})
      ),
      updated_pats as (
        select distinct enc_id from criteria where enc_id in ({enc_ids}) and update_date >= '{tsp}'::timestamptz
      )
      SELECT * from pats except select * from updated_pats
      '''.format(enc_ids=enc_ids, tsp=tsp)
      cnt = await conn.fetch(sql)
      if cnt is None or len(cnt) == 0:
        logging.info("criteria is ready")
        return True
      else:
        logging.info("criteria is not ready ({})".format(len(cnt)))
        return False

    async with self.db_pool.acquire() as conn:
      n = 0
      N = 60

      logging.info("enter suppression task for {}".format(msg))
      while not await criteria_ready(conn, pats_str, tsp):
        await asyncio.sleep(10)
        n += 1
        logging.info("retry criteria_ready {} times for {}".format(n, pats_str))
        if n >= 60:
          break
      if n < 60:
        if self.notify_web:
          sql = '''
          with pats as (
            select enc_id, pat_id from pat_enc where enc_id in ({pats})
          ),
          alerts as (
            select update_suppression_alert(enc_id, '{channel}', '{model}', 'false') from pats),
          refreshed as (
            insert into refreshed_pats (refreshed_tsp, pats)
            select now(), jsonb_agg(pat_id) from pats
            returning id
          )
          select pg_notify('{channel}', 'invalidate_cache_batch:' || id || ':' || '{model}') from refreshed;
          '''.format(channel=self.channel, model=self.model, pats=pats_str)
        else:
          sql = '''
          with pats as (
            select enc_id from pat_enc where enc_id in ({pats})
          )
          select update_suppression_alert(enc_id, '{channel}', '{model}', 'false') from pats)
          '''.format(channel=self.channel, model=self.model, pats=pats_str)
          logging.info("lmc suppression sql: {}".format(sql))
          await conn.fetch(sql)
          logging.info("generate suppression alert for {}".format(hospital))
      else:
        logging.info("criteria is not ready for {}".format(pats_str))


  async def calculate_criteria(self, conn, job_id):
    server = 'dev_db' if 'dev' in self.channel else 'prod_db'
    sql = 'select garbage_collection();'
    logging.info("calculate_criteria sql: {}".format(sql))
    await conn.fetch(sql)
    sql = '''
    select distribute_advance_criteria_snapshot_for_job('{server}', {hours}, '{job_id}', {nprocs});
    '''.format(server=server,hours=self.lookbackhours,job_id=job_id,nprocs=self.nprocs)
    logging.info("calculate_criteria sql: {}".format(sql))
    await conn.fetch(sql)

  async def calculate_criteria_enc(self, conn, enc_id_str):
    server = 'dev_db' if 'dev' in self.channel else 'prod_db'
    sql = 'select garbage_collection();'
    logging.info("calculate_criteria sql: {}".format(sql))
    await conn.fetch(sql)
    sql = '''
    select distribute_advance_criteria_snapshot_for_enc('{server}', {hours}, '{enc_id_str}', {nprocs});
    '''.format(server=server,hours=self.lookbackhours,enc_id_str=enc_id_str,nprocs=self.nprocs)
    logging.info("calculate_criteria sql: {}".format(sql))
    await conn.fetch(sql)
    logging.info("complete calculate_criteria_enc")

  async def run_trews_alert(self, job_id, hospital):
    async with self.db_pool.acquire() as conn:
      if self.TREWS_ETL_SUPPRESSION == 2:
        # calculate criteria here
        await self.calculate_criteria(conn, job_id)
        if self.notify_web:
          sql = '''
          with pats as (
            select e.enc_id, p.pat_id from get_latest_enc_ids('{hospital}') e inner join pat_enc p on e.enc_id = p.enc_id
          ),
          refreshed as (
            insert into refreshed_pats (refreshed_tsp, pats)
            select now(), jsonb_agg(pat_id) from pats
            returning id
          )
          select pg_notify('{channel}', 'invalidate_cache_batch:' || id || ':' || '{model}') from refreshed;
          '''.format(channel=self.channel, model=self.model, hospital=hospital)
          logging.info("trews alert sql: {}".format(sql))
          await conn.fetch(sql)
          logging.info("generated trews alert for {}".format(hospital))
      elif self.TREWS_ETL_SUPPRESSION == 1:
        if self.notify_web:
          sql = '''
          with pats as (
            select e.enc_id, p.pat_id from get_latest_enc_ids('{hospital}') e inner join pat_enc p on e.enc_id = p.enc_id
          ),
          alerts as (
            select update_suppression_alert(enc_id, '{channel}', '{model}', 'false') from pats),
          refreshed as (
            insert into refreshed_pats (refreshed_tsp, pats)
            select now(), jsonb_agg(pat_id) from pats
            returning id
          )
          select pg_notify('{channel}', 'invalidate_cache_batch:' || id || ':' || '{model}') from refreshed;
            '''.format(channel=self.channel, model=self.model, hospital=hospital)
        else:
          sql = '''
          select update_suppression_alert(enc_id, '{channel}', '{model}', 'false') from
          (select distinct t.enc_id from cdm_t t
          inner join get_latest_enc_ids('{hospital}') h on h.enc_id = t.enc_id
          where now() - tsp < (select value::interval from parameters where name = 'lookbackhours')) sub;
            '''.format(channel=self.channel, model=self.model, hospital=hospital)
        logging.info("trews suppression sql: {}".format(sql))
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
      if self.model == 'lmc' or self.model == 'trews-jit':
        if message.get('hosp') in self.hospital_to_predict:
          if self.model == 'lmc':
            self.garbage_collect_suppression_tasks(message['hosp'])
          self.predictor_manager.cancel_predict_tasks(hosp=message['hosp'])
          self.predictor_manager.create_predict_tasks(hosp=message['hosp'],
                                                    time=message['time'])
        else:
          logging.info("skip prediction for msg: {}".format(message))
          await self.run_trews_alert(message['job_id'],message['hosp'])
      elif self.model == 'trews':
        await self.run_trews_alert(message['job_id'],message['hosp'])
      else:
        logging.error("Unknown suppression model {}".format(self.model))
      self.job_status[message['hosp'] + message['time']] = {
        'msg': message, 't_start': dt.datetime.now()
      }
      self.cloudwatch_logger.push_many(
        dimension_name = 'Alert Server',
        metric_names = ['etl_done_{}'.format(message['hosp'])],
        metric_values = [1],
        metric_units = ['Count']
      )

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
