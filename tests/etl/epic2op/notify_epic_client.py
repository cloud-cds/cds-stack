import asyncio
import aiomas

async def test_client(message, loop):
  reader, writer = await asyncio.open_connection('alerts.default.svc.cluster.local', 31000, loop=loop)

  print('Send: %r' % message)
  writer.write(message.encode())

  data = await reader.read(100)
  print('Received: %r' % data.decode())

  print('Close the socket')
  writer.close()

message = 'Hello\n World!\n'
loop = asyncio.get_event_loop()
loop.run_until_complete(test_client(message, loop))
loop.close()

async def client():
  channel = await aiomas.channel.open_connection(('alerts.default.svc.cluster.local', 31000))
  rep = await channel.send('ohai')
  print(rep)
  await channel.close()

aiomas.run(client())
