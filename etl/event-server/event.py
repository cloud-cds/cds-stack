import os, sys, traceback
import asyncio
import logging
from aiohttp import web
from aiohttp.web import Response, json_response
from aiocache import LRUMemoryCache
from aiocache.plugins import HitMissRatioPlugin
from etllib import extractor
from etl.mappings.flowsheet_ids import flowsheet_ids
import etl.io_config.core as core
import json

EPIC_WEB_REQUEST_INTERVAL_SECS = core.get_environment_var('EPIC_WEB_REQUEST_INTERVAL_SECS', 5)

full_extraction = []

order_extraction = [
  extractor.extract_active_procedures,
  extractor.extract_lab_orders,
  extractor.extract_lab_results,
  extractor.extract_med_orders,
  extractor.extract_med_admin
]

med_extraction = [
  extractor.extract_med_orders,
  extractor.extract_med_admin
]

EpicEvents = {
  'Flowsheet - Add': [extractor.extract_flowsheets],
  'Flowsheet - Update': [extractor.extract_flowsheets],
  'Flowsheet - Update': [extractor.extract_flowsheets],
  'Admission Notification': [extractor.extract_flowsheets],
  'Discharge Notification': [extractor.extract_flowsheets],
  'Preadmit': full_extraction,
  'Admit': full_extraction,
  'L&D Arrival': full_extraction,
  'Discharge': full_extraction,
  'Undo Admit': full_extraction,
  'Undo Discharge': full_extraction,
  'Undo Preadmit': full_extraction,
  'Transfer': full_extraction,
  'Undo Transfer': full_extraction,
  'ADT Update': full_extraction,
  'Undo Update': full_extraction,
  'Patient Location Update': full_extraction,
  'ADT - ED Arrival': full_extraction,
  'ADT - ED Dismiss': full_extraction,
  'ADT - ED Depart': full_extraction,
  'ADT - ED Department Change': full_extraction,
  'ADT - ED Encounter Creation': full_extraction,
  'ADT - ED Undo Arrival': full_extraction,
  'ADT - ED Undo Dismiss': full_extraction,
  'Sign Order': order_extraction,
  'Cancel Order': order_extraction,
  'Med Admin Notification - Given': med_extraction,
  'Med Admin Notification - Cancel': med_extraction,
  'Med Admin Notification - New Bag': med_extraction,
  'Med Admin Notification - Restarted': med_extraction,
  'Med Admin Notification - Stopped': med_extraction,
  'Med Admin Notification - Rate Change': med_extraction,
  'Med Admin Notification - Bolus': med_extraction,
  'Med Admin Notification - Push': med_extraction,
  'Med Admin Notification - Paused': med_extraction,
  'UCN Note Updated': [extractor.extract_notes, extractor.extract_note_texts],
}


def run_epic_web_requests(app, later=EPIC_WEB_REQUEST_INTERVAL_SECS):
  '''
  run Epic web requests
  '''
  app.logger.info("start to run web requests")
  try:
    if not app.web_req_buf.is_empty():
        # TODO: start ETL for current buffer
        app.logger.info("web_req_buf is ready for extraction")
    else:
      app.logger.info("web_req_buf is not ready, so skip extraction.")
  except Exception as ex:
    app.logger.warning(str(ex))
    traceback.print_exc()
    raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))

  finally:
    loop = asyncio.get_event_loop()
    loop.call_later(later, run_epic_web_requests, app)
  app.logger.info("complete running web requests")

class WebRequestBuffer():
  def __init__(self):
    self.buf = {}

  def is_empty(self):
    return len(self.buf) == 0

  def add_requests(self, requests):
    for eid in requests:
      if eid in self.buf:
        self.buf[eid].union(requests[eid])
      else:
        self.buf[eid] = requests[eid]


###########################
# Handlers.

class Epic(web.View):

  async def post(self):
    try:
        message = await self.request.json()
        event = self.parse_epic_event(message)
        if event:
          requests = self.get_web_requests(event)
          if requests and len(requests) > 0:
            self.request.app.web_req_buf.add_requests(requests)
    except web.HTTPException:
      raise

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))

    finally:
      # TODO: define fault tolerant resones
      return json_response({})

  def parse_epic_event(self, message):
    '''
    parse epic soap message to event
    include
      - event_type
      - ids
      - zid
    '''
    try:
      event_type = message['eventInfo']['Type']['$value']
      ids = [entity['ID']['$value'] for entity in message['eventInfo']['OtherEntities'][0]['Entity']]
      zid = message['eventInfo']['PrimaryEntity']['ID']['$value']
      return {'event_type': event_type, 'zid': zid, 'ids': ids}
    except Exception as ex:
      logging.warning(str(ex))
      logging.info(message)
      traceback.print_exc()

  def get_web_requests(self, event):
    '''
    TODO: generate the required web request functions
    based on the event type and attributes
    TODO: map event to web request; if not valid, return None
    TODO: convert ZID to EID
    '''
    requests = self.get_epic_web_requests(event['event_type'], event['ids'])
    logging.info(requests)
    eid = self.request.app.etl.convert_zid_to_eid(event['zid'])
    return {'eid': eid, 'requests': requests}

  def get_epic_web_requests(self, event_type, ids):
    if event_type in EpicEvents:
      if event_type.startswith('Flowsheet') and not self.contain_valid_flo_id(ids):
        return None
      # TODO: lab results/orders
      return EpicEvents[event_type]
    else:
      return None

  def contain_valid_flo_id(self, ids):
    for id in ids:
      for item in flowsheet_ids:
        if id in item[1]:
          return True
    return False

  def epic_request(self, requests):
    for req in requests:
      '''
      TODO: run request
      '''
      #res = extract...
      self.add_to_buf(res)
