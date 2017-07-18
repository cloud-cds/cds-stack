import asyncio
import json
import logging

SRV_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=SRV_LOG_FMT)

MAGIC_NUMBER = b'trews_magic_number'
CONNECTION_CLOSED = 'Connection Closed'

ALERT_SERVER_IP = '127.0.0.1'
ALERT_SERVER_PORT = 30000

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



def write_message(writer, message):
  logging.debug('Sending to {}:  {}'.format(writer.get_extra_info('sockname'), message))
  if type(message) != dict:
    raise ValueError('write_message takes a dictionary as the second argument')
  writer.write(json.dumps(message).encode() + MAGIC_NUMBER)
