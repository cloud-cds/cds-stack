import asyncio
import json
import logging
import socket, errno
from etl.io_config.core import get_environment_var

SRV_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=SRV_LOG_FMT)

MAGIC_NUMBER = b'trews_magic_number'
CONNECTION_CLOSED = 'Connection Closed'

ALERT_SERVER_IP = get_environment_var('ALERT_SERVER_IP',
                                      'alerts.default.svc.cluster.local')
ALERT_SERVER_PORT = 31000

async def read_message(reader, writer):
  try:
    data = await reader.readuntil(MAGIC_NUMBER)
  except asyncio.streams.IncompleteReadError:
    return CONNECTION_CLOSED

  # Decode and return message
  EOM = -1 * len(MAGIC_NUMBER)
  data = data[:EOM]
  logging.debug('Receiving from {}:  {}'.format(writer.get_extra_info('peername'), data))
  return json.loads(data.decode())



async def write_message(writer, message):
  logging.debug('Sending to {}:  {}'.format(writer.get_extra_info('sockname'), message))
  if type(message) != dict:
    raise ValueError('write_message takes a dictionary as the second argument')
  try:
    writer.write(json.dumps(message).encode() + MAGIC_NUMBER)
    await writer.drain()
    return True
  except (socket.error, IOError) as e:
    if e.errno == errno.EPIPE:
      logging.error(e)
    else:
      logging.error("Other error: {}".format(e))
  writer.close()
  return False
