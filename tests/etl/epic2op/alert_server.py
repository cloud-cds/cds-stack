import pandas as pd
import asyncio, asyncpg
import concurrent.futures
import logging
import json
import etl.io_config.server_protocol as protocol
from etl.io_config.database import Database
import datetime as dt
import socket

alert_dns = '127.0.0.1'
predictor_dns = '0.0.0.0'
alert_server_port = 31000
predictor_port = 31001

SRV_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=SRV_LOG_FMT)


class AlertServer:
  def __init__(self):
    self.db = Database()

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



  async def add_to_queue(self, active_or_backup, predictor_number=None):
    ''' Add all predictors to the queue and return dns '''
    if active_or_backup not in ['active', 'backup']:
      raise ValueError('active_or_backup must be "active" or "backup"')
    where_clause = "WHERE partition_id = {}".format(predictor_number) if predictor_number else ""
    qry = '''
      UPDATE lmc_predictors SET (queue_machine, queue_tsp) = ({pred_type}_dns, '{tsp}')
      {where_clause};
    '''.format(pred_type=active_or_backup, tsp=dt.datetime.utcnow(), where_clause=where_clause)
    async with self.db_pool.acquire() as conn:
      results = await conn.fetch(qry)
    return [tuple(x) for x in results]



  async def pop_from_queue(self, predictor_number, tsp):
    ''' Remove a predictor from the queue '''
    pass



  async def recv_etl(self, message):
    '''
    Received ETL Done:
      1) Add all predictors to queue and get dns
      2) Start all predictors (or backups on failure)
    '''

    logging.info('Putting predictors on queue.')
    active_predictors = await self.add_to_queue('active')

    logging.info('Forwarding message to all predictors')
    for partition_id, predictor_dns in active_predictors.items():
      try: # Try active
        reader, writer = await asyncio.open_connection(predictor_dns, predictor_port)
        protocol.write_message(writer, message)
        writer.write_eof()
        writer.close()
      except socket.gaierror:
        logging.error("Error starting predictor {} at '{}'".format(partition_id, predictor_dns))




  async def recv_start(self, message):
    ''' Received a START message from a predictor '''
    # TODO
    pass



  async def recv_fin(self, message):
    ''' Received a FIN message from a predictor '''
    # TODO
    pass



  async def alert_server(self, reader, writer):
    ''' Alert server callback '''
    addr = writer.transport.get_extra_info('peername')
    sock = writer.transport.get_extra_info('socket')

    if not addr:
      logging.error('Connection made without a valid remote address, (Timeout %s)' % str(sock.gettimeout()))
      return
    else:
      logging.info('Connection from %s (Timeout %s)' % (str(addr), str(sock.gettimeout())))

    while True:
      message = await protocol.read_message(reader, writer)
      if message == protocol.CONNECTION_CLOSED:
        break
      elif message.get('type') == 'ETL':
        await self.recv_etl(message)
      elif message.get('type') == 'START':
        await self.recv_start(message)
      elif message.get('type') == 'FIN':
        await self.recv_fin(message)

    logging.info("Closing the client socket")
    writer.close()



  async def queue_watcher(self):
    ''' Watches the predictor queue to generate timeouts '''
    while True:
      await asyncio.sleep(5)
      logging.info("Checking queue")
      results = await self.get_predictors()
      print(results)
      # TODO: Check queue for timeout
      # TODO: If timeout, forward to backup



def main():
  # Create alert server class
  loop = asyncio.get_event_loop()
  server = AlertServer()
  loop.run_until_complete(server.async_init())

  # Start queue watcher
  queue_watcher_future = asyncio.run_coroutine_threadsafe(
    server.queue_watcher(), loop=loop
  )

  # Start listening server
  server_future = loop.run_until_complete(asyncio.start_server(
    server.alert_server, alert_dns, alert_server_port, loop=loop
  ))
  logging.info('Serving on {}'.format(server_future.sockets[0].getsockname()))

  # Run server until Ctrl+C is pressed
  try:
    loop.run_forever()
  except KeyboardInterrupt:
    pass

  # Close everything
  queue_watcher_future.cancel()
  server_future.close()
  loop.run_until_complete(server_future.wait_closed())
  loop.close()



if __name__ == '__main__':
  main()
