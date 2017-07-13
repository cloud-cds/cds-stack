import uvloop
import asyncio
import logging
import traceback
import json

alert_dns = '127.0.0.1'

SRV_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=SRV_LOG_FMT)

async def start_predictor(message_data):
  # TODO: Get predictor dns from db
  predictor_dns = '0.0.0.0'

  reader, writer = await asyncio.open_connection(predictor_dns, 31001, loop=loop)
  print('Forwarding message')
  writer.write(message_data)
  writer.write_eof()
  writer.close()
  return

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
    data = await reader.read()

    if data == b'':
      break

    message = json.loads(data.decode())
    addr = writer.get_extra_info('peername')
    print("Received %r from %r" % (message, addr))

    if 'type' in message and message['type'] == 'ETL':
      await start_predictor(data)

  print("Closing the client socket")
  writer.close()

loop = asyncio.get_event_loop()
coro = asyncio.start_server(notification_loop, alert_dns, 31000, loop=loop)
server = loop.run_until_complete(coro)

# Serve requests until Ctrl+C is pressed
print('Serving on {}'.format(server.sockets[0].getsockname()))
try:
  loop.run_forever()
except KeyboardInterrupt:
  pass

# Close the server
server.close()
loop.run_until_complete(server.wait_closed())
loop.close()
