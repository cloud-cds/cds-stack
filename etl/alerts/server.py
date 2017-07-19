import pandas as pd
pd.set_option('display.width', 200)
import asyncio, asyncpg
import concurrent.futures
import logging
import json
import etl.io_config.server_protocol as protocol
from etl.io_config.database import Database
import datetime as dt
import pytz
import socket
import random, string

def randomword(length):
   return ''.join(random.choice(string.ascii_uppercase) for i in range(length))


start_timeout = 15 #seconds

SRV_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=SRV_LOG_FMT)


class AlertServer:
  def __init__(self, event_loop, alert_server_port=31000,
               alert_dns='0.0.0.0',
               predictor_ports=[8181, 8182]):
    self.db                   = Database()
    self.loop                 = event_loop
    self.alert_server_port    = alert_server_port
    self.alert_dns            = alert_dns
    self.predictor_ports      = predictor_ports
    self.predictor_start_task = {}
    self.last_etl_msg         = {}
    self.existing_supression_tasks = []


  async def async_init(self):
    self.db_pool = await self.db.get_connection_pool()



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
    logging.info("Adding {} to queue".format(predictor_number))
    qry = '''
      UPDATE lmc_predictors SET (queue_machine, queue_tsp) = ({0}_dns, '{1}')
      WHERE partition_id = {2}
      RETURNING {0}_dns;
    '''.format(predictor_type, dt.datetime.utcnow(), predictor_number)
    async with self.db_pool.acquire() as conn:
      return await conn.fetchval(qry)



  async def pop_predictor_from_queue(self, predictor_number):
    ''' Remove a predictor from the queue '''
    logging.info("Popping {} from queue".format(predictor_number))
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

    while True:
      message = await protocol.read_message(reader, writer)

      if message == protocol.CONNECTION_CLOSED:
        # Connection ended
        break

      elif message.get('type') == 'ETL':
        # Received ETL Done from ETL
        # sender should periodically push ETL done messages as soft state.
        # remove existing supression tasks
        for task in self.existing_supression_tasks:
          task.cancel()
        self.existing_supression_tasks = []
        hosp = message['hosp']
        ts = message['time']
        logging.info("{} ETL is finished".format(hosp))


        # Forward message to all predictors if this is a new ETL job.
        # Since 'ETL Done' messages are soft state, we will consider this a new message on crash recovery.
        if (hosp not in self.last_etl_msg) or (hosp in self.last_etl_msg and self.last_etl_msg[hosp]['time'] < ts):
          self.last_etl_msg[hosp] = message

          predictors = await self.get_predictors()
          for idx, row in predictors.iterrows():
            self.loop.create_task(self.start_predictor(row['partition_id'], message, self.predictor_ports[0]))
            self.loop.create_task(self.start_predictor(row['partition_id'], message, self.predictor_ports[1]))

      elif message.get('type') == 'START':
        # Received START from predictor
        hosp = message['hosp']
        predictor_id = message['predictor_id']
        predictor_type = message['predictor_type']
        predictor_model = message['predictor_model']
        predictor_str = "Predictor {} ({} {})".format(predictor_id, predictor_type, predictor_model)
        logging.info("{} said they are starting".format(predictor_str))

        # Cancel queue watcher - pop from queue
        logging.info("Popping from queue and cancelling queue watcher")
        await self.pop_predictor_from_queue(predictor_id)

        # Keep connection open and wait for next message
        logging.info("{} waiting for message".format(predictor_str))
        message2 = await protocol.read_message(reader, writer)
        logging.info("{} got message".format(predictor_str))

        if message2 == protocol.CONNECTION_CLOSED:
          # Received connection closed
          new_type = 'backup' if predictor_type == 'active' else 'active'
          logging.info("{} connection broken, trying {}".format(predictor_str, new_type))
          if hosp in self.last_etl_msg:
            if predictor_id in self.predictor_start_task:
              self.predictor_start_task[predictor_id].cancel()
              logging.info("{} cancelling existing predictor_start_task".format(predictor_str))

            task_msg = self.last_etl_msg[hosp]
            if predictor_model == 'short':
              port = self.predictor_ports[0]
            elif predictor_model == 'long':
              port = self.predictor_ports[1]
            else:
              logging.error("UNKNOWN MODEL {}".format(predictor_model))
            task = self.loop.create_task(self.start_predictor(predictor_id, task_msg, port, new_type))
            self.predictor_start_task[predictor_id] = task
          else:
            logging.warn("{} has no existing ETL message for {}".format(predictor_str, hosp))
            # TODO: wait for the ETL message to be resent.

        elif message2.get('type') == 'FIN':
          # Received FIN
          logging.info("{} said they are finished: {}".format(predictor_str, message2))
          # Wait for Advance Criteria Snapshot to finish and then start generating notifications
          pat_ids = await self.convert_enc_ids_to_pat_ids(message2['enc_ids'])
          for pat_id in pat_ids:
            supression_task = self.loop.create_task(self.supression(pat_id['pat_id'], message2['time']))
            self.existing_supression_tasks.append(supression_task)
        else:
          logging.error("UNKNOWN MESSAGE TYPE - Looking for FIN or Connection closed")

        break

    writer.close()

  async def convert_enc_ids_to_pat_ids(self, enc_ids):
    async with self.db_pool.acquire() as conn:
      sql = '''
      SELECT distinct pat_id FROM pat_enc where enc_id
      in ({})
      '''.format(','.join([str(i) for i in enc_ids]))
      pat_ids = await conn.fetch(sql)
      return pat_ids

  async def supression(self, pat_id, tsp):
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
        sql = '''select supression_alert('{}')'''.format(pat_id)
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




def main():
  # Create alert server class
  loop = asyncio.get_event_loop()
  server = AlertServer(loop)
  loop.run_until_complete(server.async_init())

  # Start listening server
  server_future = loop.run_until_complete(asyncio.start_server(
    server.alert_server, server.alert_dns, server.alert_server_port, loop=loop
  ))
  logging.info('Serving on {}'.format(server_future.sockets[0].getsockname()))

  # Run server until Ctrl+C is pressed
  try:
    loop.run_forever()
  except KeyboardInterrupt:
    pass

  # Close everything
  server_future.close()
  loop.run_until_complete(server_future.wait_closed())
  loop.close()



if __name__ == '__main__':
  main()
