from aiohttp import web
from aiohttp.web import Response, json_response
import logging
import asyncio
import traceback
import json
from lxml import etree
import os

async def init_epic_sync_loop(app):
  event_loop = asyncio.get_event_loop()
  if not event_loop.is_running():
    try:
      logging.info('entering event loop')
      event_loop.run_forever()
    finally:
      logging.info('closing event loop')
      event_loop.close()

class EventResource(web.View):
  async def get(self):
    try:
      response = Response()
      response.content_type = 'text/html'
      response.body = "epic event notifications"
      return response

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))

  async def post(self):
    try:
      req_body = await self.request.text()
      root = etree.fromstring(req_body)
      logging.info(etree.tostring(root))
      logging.info(root.tag)
      for child in root:
        logging.info(child.tag)
      return json_response({'message': 'success'})
    except Exception as ex:
      logging.warning(ex.message)
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))


app = web.Application()
app.on_startup.append(init_epic_sync_loop)
epic_env = os.environ['epic_env'] if 'epic_env' in os.environ else 'poc'
app.router.add_route('POST', '/', EventResource)
app.router.add_route('GET', '/', EventResource)