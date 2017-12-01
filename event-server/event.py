import os, sys, traceback
import asyncio
import logging
from aiohttp import web
from aiohttp.web import Response, json_response
from aiocache import LRUMemoryCache
from aiocache.plugins import HitMissRatioPlugin

ETL_INTERVAL_SECS = int(os.environ['ETL_INTERVAL_SECS']) if 'ETL_INTERVAL_SECS' in os.environ else 10

def init_etl(loop):
  '''
  TODO: initialize attributes
  '''
  # start etl perioidically
  print('start etl perioidically (every {} seconds)'.format(ETL_INTERVAL_SECS))
  if loop is None:
    loop = asyncio.get_event_loop()
  loop.call_later(ETL_INTERVAL_SECS, self.run_etl)



###########################
# Handlers.

class Epic(web.View):
  async def run_etl(self):
    '''
    Consume data buffer and run ETL
    '''
    logging.info("etl started")
    if self.buffer_is_ready():
        # TODO: start ETL for current buffer
        logging.info("buffer is ready for etl")
    else:
      logging.info("buffer is not ready, so skip etl.")
    # TODO: start the ETL_scheduler 5 minutes later
    self.loop.call_later(ETL_INTERVAL_SECS, self.run_etl)
    logging.info("etl end")


  async def post(self):
    try:
        message = await self.request.json()
        event = self.parse_epic_event(message)
        requests = self.get_web_requests(event)
        future = asyncio.ensure_future(self.epic_request(requests), loop=self.loop)
    except web.HTTPException:
      raise

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))

    def parse_epic_event(message):
        '''
        TODO: parse epic soap message to event -- include event_name and key attributes
        '''
        return event

    def get_web_requests(self, event):
        '''
        TODO: generate the required web request functions
        based on the event type and attributes
        '''
        pass

    def epic_request(self, requests):
        for req in requests:
            '''
            TODO: run request
            '''
            #res = extract...
            self.add_to_buf(res)

    def add_to_buf(self, result):
        # TODO: add data to buffer
        if self.buffer_is_full():
            asyncio.ensure_future(self.run_etl(), loop=self.loop)

    def buffer_is_full(self):
        '''
        return True if the buffer size is larger than a threshold
        '''

    def buffer_is_ready(self):
        '''
        buffer is ready to run etl, e.g., buffer need to have enough data to start etl
        '''