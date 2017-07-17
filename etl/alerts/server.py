import asyncio, asyncpg
import logging
import json

alert_dns = 'alerts.default.svc.cluster.local'
alert_port = 31000
predictor_port = 31001

SRV_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=SRV_LOG_FMT)


async def get_predictors(conn):
  ''' Get dns of all predictors and backups from the database '''
  predictors = await conn.execute('SELECT * FROM lmc_predictors;')
  active_predictors = {x[0]: x[1] for x in predictors}
  backup_predictors = {x[0]: x[2] for x in predictors}
  return active_predictors, backup_predictors


async def send_start_message(dns):
  ''' Send start message to single predictor '''
  _, writer = await asyncio.open_connection(dns, predictor_port, loop=loop)
  logging.info('Forwarding message to {} on port {}'.format(dns, predictor_port))
  writer.write(message_data)
  writer.write_eof()
  writer.close()


async def start_predictors(message_data):
  ''' Send start message to all predictors '''

  # Get predictor dns from db
  db_config = Config()
  db_pool = await asyncpg.create_pool(
    database = db_config['db_name'],
    user     = db_config['db_user'],
    password = db_config['db_pass'],
    host     = db_config['db_host'],
    port     = db_config['db_port']
  )
  async with db_pool.acquire() as conn:
    actives, backups = await get_predictors(conn)

  # Try forwarding start message to predictor - try backups on failure
  for active_id, active_dns in actives.items():
    try:
      await send_start_message(active_dns)
    except Exception:
      await send_start_message(backups[active_id])


async def keep_predictor_alive(message):
  '''
  Keep connection open until:
    1) FIN - prediction finished, wait for advance_criteria_snapshot, then generate notifications
    2) connection closed - try backup predictor
  '''
  try:
    while True:
      asyncio.sleep(1)
      logging.info("Connection open")
  except:



async def handle_message(data, addr):
  ''' Message handler on the alert server '''
  message = json.loads(data.decode())
  logging.info("Received %r from %r" % (message, addr))

  # ETL has finished
  if message.get('type') == 'ETL':
    await start_predictors(data)

  elif message.get('type') == 'START':
    await keep_predictor_alive(message)

async def alert_server(reader, writer):
  ''' Main server loop '''
  addr = writer.transport.get_extra_info('peername')
  sock = writer.transport.get_extra_info('socket')

  if not addr:
    logging.error('Connection made without a valid remote address, (Timeout %s)' % str(sock.gettimeout()))
    finished()
    return
  else:
    logging.info('Connection from %s (Timeout %s)' % (str(addr), str(sock.gettimeout())))

  # Loop until eof
  while not reader.at_eof():
    data = await reader.read()

    if data == b'':
      break

    addr = writer.get_extra_info('peername')
    await handle_message(data, addr)

  logging.info("Closing the client socket")
  writer.close()



def main():
  # Start the alert server
  loop = asyncio.get_event_loop()
  coro = asyncio.start_server(alert_server, alert_dns, alert_port, loop=loop)
  server = loop.run_until_complete(coro)

  # Serve requests until Ctrl+C is pressed
  logging.info('Serving on {}'.format(server.sockets[0].getsockname()))
  try:
    loop.run_forever()
  except KeyboardInterrupt:
    pass

  # Close the server
  server.close()
  loop.run_until_complete(server.wait_closed())
  loop.close()


if __name__ == '__main__':
  main()
