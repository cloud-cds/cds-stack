import asyncio, logging
from etl.alerts.server import AlertServer

alert_dns = '127.0.0.1'
predictor_dns = '0.0.0.0'
alert_server_port = 31000
predictor_ports = [8181, 8182]

def main():
  # Create alert server class
  loop = asyncio.get_event_loop()
  server = AlertServer(loop)
  loop.run_until_complete(server.async_init())
  consumer_future = asyncio.ensure_future(server.alert_queue_consumer())

  server_future = loop.run_until_complete((asyncio.start_server(
    server.connection_handler, server.alert_dns, server.alert_server_port
  )))
  # Run server until Ctrl+C is pressed
  try:
    loop.run_forever()
  except KeyboardInterrupt:
    print("Exiting")
    consumer_future.cancel()
    # Close the server
    logging.info('received stop signal, cancelling tasks...')
    for task in asyncio.Task.all_tasks():
      task.cancel()
    logging.info('bye, exiting in a minute...')
    server_future.close()
    loop.run_until_complete(server_future.wait_closed())
    loop.stop()

  # Close loop
  loop.close()



if __name__ == '__main__':
  main()
