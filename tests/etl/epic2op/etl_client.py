import asyncio
import json
import datetime as dt

alert_dns = '127.0.0.1'

async def etl_client(message, loop):
  reader, writer = await asyncio.open_connection(alert_dns, 31000, loop=loop)

  print('Send: %r' % message)
  writer.write(message.encode())
  writer.write_eof()

  data = await reader.read()
  print('Received: %r' % data.decode())

  print('Closing the socket')
  writer.close()

message = json.dumps({
  'type': 'ETL',
  'time': str(dt.datetime.now()),
  'hosp': 'HCGH'
})

loop = asyncio.get_event_loop()
loop.run_until_complete(etl_client(message, loop))
loop.close()
