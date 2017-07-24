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
  server.start()

if __name__ == '__main__':
  main()
