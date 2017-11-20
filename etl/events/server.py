import falcon
import asyncio


async def init_epic_sync_loop(app):
  event_loop = asyncio.get_event_loop()
  if not event_loop.is_running():
    try:
        print('entering event loop')
        event_loop.run_forever()
    finally:
        print('closing event loop')
        event_loop.close()

class EventResource(web.View):
    def post(self):
        try:
            req_body = await self.request.xml()
            print(req_body)
        except Exception as ex:
          logging.warning(ex.message)
          traceback.print_exc()
          raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))


api = falcon.API()
app.on_startup.append(init_epic_sync_loop)
api.add_route('POST', '/event', EventResource)