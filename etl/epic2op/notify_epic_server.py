import uvloop
import asyncio
import logging
import traceback
SRV_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=SRV_LOG_FMT)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

enc_ids_end_block = '\n'.encode()# b'\x1c\x0d'


async def notification_loop(reader, writer):
  # main workflow
  addr = writer.transport.get_extra_info('peername')
  sock = writer.transport.get_extra_info('socket')

  def abort():
    reader.feed_eof()
    writer.transport.abort()

  if not addr:
    logging.error('Connection made without a valid remote address, (Timeout %s)' % str(sock.gettimeout()))
    finished()
    return
  else:
    logging.info('Connection from %s (Timeout %s)' % (str(addr), str(sock.gettimeout())))

  while True:
    data = await reader.read(100)
    message = data.decode()
    addr = writer.get_extra_info('peername')
    print("Received %r from %r" % (message, addr))

    print("Send: %r" % message)
    writer.write(data)
    await writer.drain()

    # print("Close the client socket")
    # writer.close()

  # while not reader.at_eof() and writer.can_write_eof():
  #   try:
  #     data = await reader.readuntil(separator=enc_ids_end_block)
  #     msg = data.decode()
  #     logging.debug('Received data, len: %s' % str(len(msg)))
  #     logging.debug(msg)

  #   except (asyncio.IncompleteReadError, asyncio.LimitOverrunError) as ex:
  #     logging.error('Connection failed to read data, error: %s' % str(ex))
  #     traceback.print_exc()
  #     abort()
  #     break

  #   except (RuntimeError, ConnectionResetError) as ex:
  #     logging.error('Connection closed for %s: %s' % (str(addr), str(ex)))
  #     traceback.print_exc()
  #     abort()
  #     break



loop = asyncio.get_event_loop()
coro = asyncio.start_server(notification_loop, '0.0.0.0', 31000, loop=loop)
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
