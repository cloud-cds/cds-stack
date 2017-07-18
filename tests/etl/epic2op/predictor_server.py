import asyncio
import logging
import os
import json
import etl.io_config.server_protocol as protocol

alert_dns = '127.0.0.1'
predictor_dns = '0.0.0.0'
SRV_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=SRV_LOG_FMT)



async def start_predicting(writer, job_tsp):
  logging.info("Connecting to server")
  reader, writer = await asyncio.open_connection(alert_dns, 31000)

  protocol.write_message(writer, {
    'type': 'START',
    'time': job_tsp,
    'hosp': 'HCGH',
    'dns':  predictor_dns,
    'predictor_id': 0,
    'predictor_type': 'active',
  })

  logging.info("Predicting on patients")
  await asyncio.sleep(6)

  protocol.write_message(writer,
    {'type': 'FIN', 'time': job_tsp, 'hosp': 'HCGH', 'predictor_id': 0}
  )
  writer.close()



async def notification_loop(reader, writer):
  # main workflow
  addr = writer.transport.get_extra_info('peername')
  sock = writer.transport.get_extra_info('socket')

  if not addr:
    logging.error('Connection made without a valid remote address, (Timeout %s)' % str(sock.gettimeout()))
    finished()
    return
  else:
    logging.info('Connection from %s (Timeout %s)' % (str(addr), str(sock.gettimeout())))

  while not reader.at_eof():
    message = await protocol.read_message(reader, writer)

    if message == protocol.CONNECTION_CLOSED:
      break
    elif message.get('type') == 'ETL':
      await start_predicting(writer, message['time'])

  logging.info("Closing the client socket")
  writer.close()

loop = asyncio.get_event_loop()
coro = asyncio.start_server(notification_loop, predictor_dns, 31001, loop=loop)
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
