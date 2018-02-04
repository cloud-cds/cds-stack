import os
import sys
import json
import asyncio
import asyncpg
import logging
import traceback
import aiobotocore
import botocore.exceptions

import etllib
import event


QUEUE_NAME = os.environ['queue_name']
log_consumer = os.environ['log_consumer'] == 'true' if 'log_consumer' in os.environ else False
terminated = False

# db parameters
user = os.environ['db_user']
host = os.environ['db_host']
db   = os.environ['db_name']
port = os.environ['db_port']
pw   = os.environ['db_password']
MAX_DB_CONN = os.environ['max_db_conn'] if 'max_db_conn' in os.environ else 50

class SQSHandler():
  def __init__(self, app):
    self.delegate = event.EventHandler(app)

  async def process(self, sqs_msg):
    try:
      msg = json.loads(sqs_msg['Body'])
      await self.delegate.process(msg)

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()

class App():
  def __init__(self):
    self.logger = logging.getLogger('app')
    self.db_pool = None
    self.etl = None
    self.event_handler = None
    self.web_req_buf = None
    self.sqs_client = None
    self.queue_url = None

app = App()

async def init_queue(app):
  loop = asyncio.get_event_loop()
  session = aiobotocore.get_session(loop=loop)
  app.sqs_client = session.create_client('sqs')
  try:
    response = await app.sqs_client.get_queue_url(QueueName=QUEUE_NAME)
  except botocore.exceptions.ClientError as err:
    if err.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
      logging.error("Queue {0} does not exist".format(QUEUE_NAME))
      await app.sqs_client.close()
      app.sqs_client = None
    else:
      raise

  app.queue_url = response['QueueUrl']

async def init_db_pool(app):
  app.db_pool = await asyncpg.create_pool(database=db, user=user, password=pw, host=host, port=port, max_size=MAX_DB_CONN)

async def cleanup_db_pool(app):
  if 'pool' in app:
    await app.db_pool.close()

async def init_etl(app):
  app.etl = etllib.ETL(app)

async def init_event_processor(app):
  app.web_req_buf = event.WebRequestBuffer(app)
  app.event_handler = SQSHandler(app)
  event.run_epic_web_requests(app)

async def go(loop):
  await init_db_pool(app)
  await init_etl(app)
  await init_event_processor(app)
  await init_queue(app)

  if app.sqs_client is None:
    return

  app.logger.info('Starting SQS event loop')

  while not terminated:
    try:
      # This loop wont spin really fast as there is
      # essentially a sleep in the receieve_message call
      response = await app.sqs_client.receive_message(
          QueueUrl=app.queue_url,
          WaitTimeSeconds=2,
      )

      if 'Messages' in response:
        for msg in response['Messages']:
          if log_consumer:
            app.logger.info('Got msg "{0}"'.format(msg['Body']))

          # Process message
          await app.event_handler.process(msg)

          # Need to remove msg from queue or else it'll reappear
          await app.sqs_client.delete_message(
              QueueUrl=app.queue_url,
              ReceiptHandle=msg['ReceiptHandle']
          )
      else:
        app.logger.info('No messages in SQS')

    except KeyboardInterrupt:
      break

  app.logger.info('Finished')
  await cleanup_db_pool(app)
  await app.sqs_client.close()


def main():
  try:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(go(loop))
  except KeyboardInterrupt:
    pass

if __name__ == '__main__':
    main()