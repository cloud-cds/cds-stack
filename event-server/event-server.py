import event
import asyncio, asyncpg
import asyncpg
from aiohttp import web
from aiohttp.web import Response, json_response
from aiohttp import web
import logging
import etl

URL = '/'
URL_EPIC_EVENT = URL + 'epic'

async def init_db_pool(app):
  app['db_pool'] = await asyncpg.create_pool(database=db, user=user, password=pw, host=host, port=port)

async def cleanup_db_pool(app):
  if 'pool' in app:
    await app['db_pool'].close()

async def init_event_loop(app):
  event_loop = asyncio.get_event_loop()
  if not event_loop.is_running():
    try:
        app.logger.info('entering event loop')
        event_loop.run_forever()
    finally:
        app.logger.info('closing event loop')
        event_loop.close()
  else:
    app.logger.info('event loop is already running')

async def init_etl(app):
  app.etl = etl.ETL(app)

logging.info("start event server")
app = web.Application()
app.on_startup.append(init_db_pool)
app.on_cleanup.append(cleanup_db_pool)
app.on_startup.append(init_event_loop)
app.on_startup.append(init_etl)
app.router.add_route('POST', URL_EPIC_EVENT, event.Epic)