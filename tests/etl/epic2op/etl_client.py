import asyncio
import json
import datetime as dt
import logging
import etl.io_config.server_protocol as protocol

alert_dns = '127.0.0.1'
alert_server_port = protocol.ALERT_SERVER_PORT

async def etl_client(message, loop):
  reader, writer = await asyncio.open_connection(alert_dns, alert_server_port, loop=loop)
  await protocol.write_message(writer, message)
  logging.info('Closing the socket')
  writer.close()

message = {
  'type': 'ETL',
  'time': str(dt.datetime.utcnow()),
  'hosp': 'HCGH'
}
loop = asyncio.get_event_loop()
loop.run_until_complete(etl_client(message, loop))
loop.close()
