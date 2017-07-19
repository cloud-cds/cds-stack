import asyncio, logging
from etl.alerts.server import AlertServer

alert_dns = '127.0.0.1'
predictor_dns = '0.0.0.0'
alert_server_port = 31000
predictor_ports = [8181, 8182]

def main():
  # Create alert server class
  loop = asyncio.get_event_loop()
  server = AlertServer(loop, alert_server_port, alert_dns, predictor_ports)
  loop.run_until_complete(server.async_init())

  # Start listening server
  server_future = loop.run_until_complete(asyncio.start_server(
    server.alert_server, server.alert_dns, server.alert_server_port, loop=loop
  ))
  logging.info('Serving on {}'.format(server_future.sockets[0].getsockname()))

  # Run server until Ctrl+C is pressed
  try:
    loop.run_forever()
  except KeyboardInterrupt:
    pass

  # Close everything
  server_future.close()
  loop.run_until_complete(server_future.wait_closed())
  loop.close()



if __name__ == '__main__':
  main()
