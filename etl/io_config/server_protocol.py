import asyncio
import json
import logging

SRV_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=SRV_LOG_FMT)

MAGIC_NUMBER = b'trews_magic_number'
CONNECTION_CLOSED = 'Connection Closed'


async def read_message(reader, writer):
  try:
    data = await reader.readuntil(MAGIC_NUMBER)
  except asyncio.streams.IncompleteReadError:
    logging.info(CONNECTION_CLOSED)
    return CONNECTION_CLOSED

  # Decode and return message
  EOM = -1 * len(MAGIC_NUMBER)
  data = data[:EOM]
  addr = writer.get_extra_info('peername')
  logging.info('Received message from {}:  {}'.format(addr, data))
  return json.loads(data.decode())



def write_message(writer, message):
  logging.info('Sending {}'.format(message))
  if type(message) != dict:
    raise ValueError('write_message takes a dictionary as the second argument')
  writer.write(json.dumps(message).encode() + MAGIC_NUMBER)
