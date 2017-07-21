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

  # Start coroutines
  server_coro = asyncio.start_server(
    server.connection_handler, server.alert_dns, server.alert_server_port, loop=loop
  )
  consumer_coro = server.alert_queue_consumer()
  gathered_tasks = asyncio.gather(server_coro, consumer_coro, loop=loop)

  # Run server until Ctrl+C is pressed
  try:
    loop.run_until_complete(gathered_tasks)
  except KeyboardInterrupt:
    pass

  # Close everything
  loop.close()



if __name__ == '__main__':
  main()
