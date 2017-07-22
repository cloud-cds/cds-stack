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

def randomword(length):
   return ''.join(random.choice(string.ascii_uppercase) for i in range(length))


start_timeout = 15 #seconds

HB_TIMEOUT = 5

SRV_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=SRV_LOG_FMT)


class AlertServer:
  def __init__(self, event_loop, alert_server_port=31000,
               alert_dns='0.0.0.0',
               predictor_ports=[8181, 8182]):
    self.db                   = Database()
    self.loop                 = event_loop
    self.alert_message_queue  = asyncio.Queue(loop=event_loop)
    self.predictor_manager    = PredictorManager(self.alert_message_queue)
    self.alert_server_port    = alert_server_port
    self.alert_dns            = alert_dns
    self.predictor_ports      = predictor_ports
    self.last_etl_msg         = {}
    self.active_connections   = []
    self.suppression_tasks     = {}
    self.start_predictor_tasks = {}


  async def async_init(self):
    self.db_pool = await self.db.get_connection_pool()



  def create_new_task(self, type, unique_id, task_dict, coro):
    ''' Schedule a new task to run '''
    logging.info("Starting {} task: {}".format(type, unize_id))
    new_task = self.loop.create_task(coro)
    task_dict[unique_id] = new_task



  def cancel_task(self, type, unique_id, task_dict):
    ''' Cancel any currently running tasks in task_dict '''
    if unique_id in task_dict:
      logging.info("Cancelling {} task: {}".format(type, unique_id))
      task_dict[unique_id].cancel()



  async def get_predictors(self):
    ''' Get predictors from database '''
    async with self.db_pool.acquire() as conn:
      results = await conn.fetch('SELECT * FROM lmc_predictors;')
    return pd.DataFrame(
      data  = [dict(r.items()) for r in results],
      index = [r['partition_id'] for r in results]
    )



  async def add_predictor_to_queue(self, predictor_number, predictor_type):
    ''' Add predictor to the queue and return dns '''
    logging.debug("Adding {} to queue".format(predictor_number))
    qry = '''
      UPDATE lmc_predictors SET (queue_machine, queue_tsp) = ({0}_dns, '{1}')
      WHERE partition_id = {2}
      RETURNING {0}_dns;
    '''.format(predictor_type, dt.datetime.utcnow(), predictor_number)
    async with self.db_pool.acquire() as conn:
      return await conn.fetchval(qry)



  async def pop_predictor_from_queue(self, predictor_number):
    ''' Remove a predictor from the queue '''
    logging.debug("Popping {} from queue".format(predictor_number))
    qry = '''
      UPDATE lmc_predictors SET (queue_machine, queue_tsp) = (NULL, NULL)
      WHERE partition_id = {};
    '''.format(predictor_number)
    async with self.db_pool.acquire() as conn:
      await conn.execute(qry)



  async def start_predictor(self, id, message, port, predictor_type='active'):
    '''
    Puts dns on the queue and then sends a message to that dns.
    Cycles between active and backup until it succeeds
    '''
    backoff = 2
    while True:
      try:
        # Add to queue
        dns_on_queue = await self.add_predictor_to_queue(id, predictor_type)
        # Forward ETL message
        reader, writer = await asyncio.open_connection(dns_on_queue, port)
        protocol.write_message(writer, message)
        writer.write_eof()
        writer.close()
        # Wait to be popped off the queue
        logging.info("start queue_watcher {} {}".format(id, port))
        response = await self.queue_watcher(id, predictor_type, message)
        if response == "SUCCESS":
          return
      except (socket.gaierror, ConnectionRefusedError):
        logging.error("Error connecting to predictor {} ({}) at '{}:{}'".format(
          id, predictor_type, dns_on_queue, port
        ))
        await self.pop_predictor_from_queue(id)
        backoff = backoff * 2 if backoff < 30 else 30
        predictor_type = 'active' if predictor_type == 'backup' else 'backup'
        await asyncio.sleep(backoff)




  async def alert_server(self, reader, writer):
    ''' Alert server callback/message handler '''
    addr = writer.transport.get_extra_info('peername')
    sock = writer.transport.get_extra_info('socket')

    if not addr:
      logging.error('Connection made without a valid remote address, (Timeout %s)' % str(sock.gettimeout()))
      return
    else:
      logging.debug('Connection from %s (Timeout %s)' % (str(addr), str(sock.gettimeout())))

    # Connection loop
    while True:

      # Get the message that started this callback function
      message = await protocol.read_message(reader, writer)

      # Message = Connection ended
      if message == protocol.CONNECTION_CLOSED:
        break

      # Message = ETL Done (from ETL)
      # sender should periodically push ETL done messages as soft state.
      elif message.get('type') == 'ETL':
        hosp = message['hosp']
        ts = message['time']
        logging.info("{} ETL is finished".format(hosp))

        # Cancel existing suppression task
        self.cancel_task('suppression', hosp, self.suppression_tasks)

        # Forward message to all predictors if this is a new ETL job.
        # Since 'ETL Done' messages are soft state, we will consider this a new message on crash recovery.
        if (hosp not in self.last_etl_msg) or (hosp in self.last_etl_msg and self.last_etl_msg[hosp]['time'] < ts):
          self.last_etl_msg[hosp] = message

          predictors = await self.get_predictors()
          for idx, row in predictors.iterrows():
            for port in self.predictor_ports:
              self.create_new_task(
                type      = 'start predictor',
                unique_id = "{}_{}_{}".format(hosp, row['partition_id'], port),
                task_dict = self.start_predictor_tasks,
                coro      = self.start_predictor(row['partition_id'], message, port)
              )

      # Message = START (from predictor)
      elif message.get('type') == 'START':
        # Get info from message
        hosp = message['hosp']
        msg_time = message['time']
        predictor_id = int(message['predictor_id'])
        predictor_type = message['predictor_type']
        predictor_model = message['predictor_model']
        predictor_str = "Predictor {} ({} {})".format(predictor_id, predictor_type, predictor_model)
        logging.info("{} said they are starting".format(predictor_str))

        # Put connection in active connections list
        conn_id = "{}{}".format(hosp, msg_time)
        self.active_connections.append(conn_id)

        # Cancel queue watcher - pop from queue
        logging.info("Popping from queue and cancelling queue watcher")
        await self.pop_predictor_from_queue(predictor_id)

        # Keep connection open and wait for next message
        logging.info("{} waiting for message".format(predictor_str))
        predictor_status = 'pending'
        while predictor_status == 'pending':

          # Wait for message (with a timeout)
          try:
            listener = protocol.read_message(reader, writer)
            message2 = await asyncio.wait_for(listener, timeout=HB_TIMEOUT)
            logging.info("{} got message {}".format(predictor_str, message2))
          except asyncio.TimeoutError:
            print("Predictor {} {} Timeout".format(predictor_id, predictor_model))
            predictor_status = 'timeout'
            break

          # Got a connection closed message
          if message2 == protocol.CONNECTION_CLOSED:
            logging.error("Connection closed, remove from active_connections and " +\
              "wait for a new connection (tcp error) or a timeout (predictor error)")
            self.active_connections.remove(conn_id)
            start_time = dt.datetime.now()
            while True:
              await asyncio.sleep(0.2)
              if conn_id in self.active_connections:
                logging.info("Reconnected in another thread, exiting")
                predictor_status = 'reconnected'
                break
              elif (dt.datetime.now() - start_time) > dt.timedelta(seconds=HB_TIMEOUT):
                logging.info("Predictor {} {} Timeout".format(predictor_id, predictor_model))
                predictor_status = 'timeout'
                break
            break

          # Got a heartbeat message
          elif message2.get('type') == 'HB':
            logging.info("Predictor {} {} heart beats".format(predictor_id, predictor_model))
            predictor_status = 'pending'

          # Got the FIN
          elif message2.get('type') == 'FIN':
            logging.info("{} said they are finished: {}".format(predictor_str, message2))
            # Wait for Advance Criteria Snapshot to finish and then start generating notifications
            pat_ids = await self.convert_enc_ids_to_pat_ids(message2['enc_ids'])
            for pat_id in pat_ids:
              suppression_task = self.loop.create_task(self.suppression(pat_id['pat_id'], message2['time']))
              self.existing_suppression_tasks.append(suppression_task)
            predictor_status = 'fin'

          # Got an unknown message
          else:
            logging.error("UNKNOWN MESSAGE TYPE - Looking for FIN or Connection closed")
            predictor_status = 'unknown msg'

        # Predictor timed out (assume a crash)
        if predictor_status == 'timeout':
          # Resend ETL-done to backup
          new_type = 'backup' if predictor_type == 'active' else 'active'
          logging.info("{} connection broken, trying {}".format(predictor_str, new_type))
          if hosp in self.last_etl_msg:
            port = self.predictor_ports[0 if predictor_model == 'short' else 1]
            unique_identifier = "{}_{}_{}".format(hosp, predictor_id, port)

            # Cancel old task
            self.cancel_task('start predictor', unique_identifier, self.start_predictor_tasks)

            # Start new task
            self.create_new_task(
              type      = 'start predictor',
              unique_id = unique_identifier,
              task_dict = self.start_predictor_tasks,
              coro      = self.start_predictor(predictor_id, self.last_etl_msg[hosp], port, new_type)
            )

          else:
            logging.warn("{} has no existing ETL message for {}".format(predictor_str, hosp))

          predictor_status = 'closed'

        logging.info("Close predictor {} {}: {}".format(predictor_id, predictor_model, predictor_status))
        break

      # Message = Unknown
      else:
        logging.error("Don't know how to handle this message: {}".format(message))

    writer.close()



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
    ''' Alert suppression task '''
    async def criteria_ready(pat_id, tsp):
      async with self.db_pool.acquire() as conn:
        sql = '''
        SELECT count(*) FROM criteria where pat_id = '{}'
        and update_date > '{}'::timestamptz
        '''.format(pat_id, tsp)
        cnt = await conn.fetch(sql)
        return cnt[0]['count'] > 0
    n = 0
    N = 60
    while not await criteria_ready(pat_id, tsp):
      await asyncio.sleep(10)
      n += 1
      logging.info("retry criteria_ready {} times for {}".format(n, pat_id))
      if n >= 60:
        break
    if n < 60:
      logging.info("criteria is ready for {}".format(pat_id))
      async with self.db_pool.acquire() as conn:
        sql = '''select suppression_alert('{}')'''.format(pat_id)
        await conn.fetch(sql)



  async def queue_watcher(self, partition_id, predictor_type, message):
    ''' Watches the predictor queue to generate timeouts '''
    wid = 'queue_watcher_' + randomword(6)
    logging.info("{}: Starting queue watcher for {}".format(wid, partition_id))
    while True:
      await asyncio.sleep(2)
      logging.info("{} Checking {}'s queue".format(wid, partition_id))
      predictor_df = await self.get_predictors()
      row = predictor_df.sort_index().iloc[partition_id]
      if row['queue_machine'] is None:
        logging.error("{}: Predictior {} ({}) removed from queue. Exiting queue watcher".format(
          wid, partition_id, predictor_type
        ))
        return "SUCCESS"
      time_diff = dt.datetime.now(pytz.utc) - row['queue_tsp'].to_pydatetime()
      if time_diff > dt.timedelta(seconds=start_timeout):
        logging.error("{}: Predictor {} ({}) timeout. Exiting queue watcher".format(
          wid, partition_id, predictor_type
        ))
        return "TIMEOUT"


  async def alert_queue_consumer(self):
    while True:
      # Check message queue
      logging.info("Checking message queue")
      await asyncio.sleep(5)
      pass


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

    if message.get('type') == 'predictor':
      return await self.predictor_manager.register(reader, writer, message)

    elif message.get('type') == 'ETL':
      # TODO: add task to work queue to forward to all predictor managers
      return await self.handle_etl_done(message)
    else:
      logging.error("Don't know how to process this message")

  async def handle_etl_done(self, message):
    hosp = message['hosp']
    tsp = message['time']
    # TODO: remove running tasks for last etl round
    self.predictor_manager.cancel_predict_tasks(self.loop, hosp)
    # TODO: enqueue new tasks for this etl round
    self.predictor_manager.create_predict_tasks(self.loop, hosp)

def main():
  # Create alert server class
  loop = asyncio.get_event_loop()
  server = AlertServer(loop)
  loop.run_until_complete(server.async_init())
  consumer_future = asyncio.ensure_future(server.alert_queue_consumer())

  server_future = loop.run_until_complete((asyncio.start_server(
    server.connection_handler, server.alert_dns, server.alert_server_port
  )))
  # Run server until Ctrl+C is pressed
  try:
    loop.run_forever()
  except KeyboardInterrupt:
    print("Exiting")
    consumer_future.cancel()
    # Close the server
    logging.info('received stop signal, cancelling tasks...')
    for task in asyncio.Task.all_tasks():
      task.cancel()
    logging.info('bye, exiting in a minute...')
    server_future.close()
    loop.run_until_complete(server_future.wait_closed())
    loop.stop()

  # Close loop
  loop.close()

if __name__ == '__main__':
  main()
