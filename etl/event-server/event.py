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
import pandas as pd
from etl.io_config.cloudwatch import Cloudwatch


EPIC_WEB_REQUEST_INTERVAL_SECS = int(core.get_environment_var('EPIC_WEB_REQUEST_INTERVAL_SECS', 10))
SWITCH_WEB_REQUEST = int(core.get_environment_var('SWITCH_WEB_REQUEST', 1))
CLOUDWATCH_ON = int(core.get_environment_var('CLOUDWATCH_ON', 0))
cloudwatch_logger = Cloudwatch()

order_extraction = {
  extractor.extract_active_procedures,
  extractor.extract_lab_orders,
  extractor.extract_med_orders
}

med_order_extraction = {
  extractor.extract_med_orders
}

med_admin_extraction = {
  extractor.extract_med_admin
}

note_extraction = {extractor.extract_notes, extractor.extract_note_texts}

full_extraction = {extractor.extract_flowsheets, extractor.extract_chiefcomplaint, extractor.extract_loc_history, extractor.extract_treatmentteam}.union(order_extraction).union(med_admin_extraction).union(note_extraction)

PT_MAP_INVALID_EVENTS = ['Admission Notification',
                         'Admit',
                         'L&D Arrival',
                         'Undo Admit',
                         'Undo Discharge',
                         'Undo Preadmit',
                         'Transfer',
                         'Undo Transfer',
                         'ADT Update',
                         'ADT - ED Arrival',
                         ]

EpicEvents = {
  'Flowsheet - Add': {extractor.extract_flowsheets},
  'Flowsheet - Update': {extractor.extract_flowsheets},
  'Flowsheet - Delete': {extractor.extract_flowsheets},
  'Admission Notification': {extractor.extract_flowsheets},
  'Discharge Notification': {extractor.extract_discharge},
  'Preadmit': full_extraction,
  'Admit': full_extraction,
  'L&D Arrival': full_extraction,
  'Discharge': {extractor.extract_discharge},
  'Undo Admit': full_extraction,
  'Undo Discharge': full_extraction,
  'Undo Preadmit': full_extraction,
  'Transfer': full_extraction,
  'Undo Transfer': full_extraction,
  'ADT Update': full_extraction,
  'Undo Update': full_extraction,
  'Patient Location Update': {extractor.extract_loc_history},
  'ADT - ED Arrival': full_extraction,
  'ADT - ED Dismiss': full_extraction,
  'ADT - ED Depart': full_extraction,
  'ADT - ED Department Change': full_extraction,
  'ADT - ED Encounter Creation': full_extraction,
  'ADT - ED Undo Arrival': full_extraction,
  'ADT - ED Undo Dismiss': full_extraction,
  'ADT - Discharge': {extractor.extract_discharge},
  'Sign Order': order_extraction,
  'Cancel Order': order_extraction,
  'Med Admin Notification - Given': med_admin_extraction,
  'Med Admin Notification - Cancel': med_admin_extraction,
  'Med Admin Notification - New Bag': med_admin_extraction,
  'Med Admin Notification - Restarted': med_admin_extraction,
  'Med Admin Notification - Stopped': med_admin_extraction,
  'Med Admin Notification - Rate Change': med_admin_extraction,
  'Med Admin Notification - Bolus': med_admin_extraction,
  'Med Admin Notification - Push': med_admin_extraction,
  'Med Admin Notification - Paused': med_admin_extraction,
  'UCN Note Updated': note_extraction,
  'Result Updated': {extractor.extract_lab_results}
}


def run_epic_web_requests(app, later=EPIC_WEB_REQUEST_INTERVAL_SECS):
  '''
  run Epic web requests
  '''
  app.logger.info("start to run web requests")
  try:
    if app.web_req_buf.is_ready():
        # start extraction for current buffer
        app.logger.info("web_req_buf is ready for extraction")
        app.web_req_buf.running = True
        asyncio.ensure_future(app.etl.run_requests(app.web_req_buf.get_buf()))
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
  def __init__(self, app):
    self.buf = {}
    self.app = app
    self.running = False

  def is_empty(self):
    return len(self.buf) == 0

  def is_ready(self):
    return not self.running and not self.is_empty()

  def add_requests(self, requests):
    for zid in requests:
      if zid in self.buf:
        funcs = requests[zid]['funcs']
        args = requests[zid]['args']
        for func in funcs:
          if func == extractor.extract_flowsheets:
            if 'flowsheet_ids' in args:
              if extractor.extract_flowsheets in self.buf[zid]['funcs']:
                if 'flowsheet_ids' in self.buf[zid]['args']:
                  self.buf[zid]['args']['flowsheet_ids'].union(args['flowsheet_ids'])
              else:
                self.buf[zid]['args']['flowsheet_ids'] = args['flowsheet_ids']
            else:
              self.buf[zid]['args'].pop('flowsheet_ids', None)

          elif func == extractor.extract_med_admin:
            if 'med_order_ids' in args:
              if extractor.extract_med_admin in self.buf[zid]['funcs']:
                if 'med_order_ids' in self.buf[zid]['args']:
                  self.buf[zid]['args']['med_order_ids'].union(args['med_order_ids'])
              else:
                self.buf[zid]['args']['med_order_ids'] = args['med_order_ids']
            else:
              self.buf[zid]['args'].pop('med_order_ids', None)
          self.buf[zid]['funcs'].add(func)
      else:
        self.buf[zid] = requests[zid]
    # logging.info("WebRequestBuffer: updated zid {} buffer {}".format(zid, self.buf[zid]))

  def get_buf(self):
    buf = self.buf
    self.buf = {}
    return buf


###########################
# Handlers.

class EventHandler():
  def __init__(self, app):
    self.app = app

  async def process(self, msg):
    event = self.parse_epic_event(msg)
    if event and SWITCH_WEB_REQUEST:
      logging.info('parsed event: {} {} {}'.format(event['zid'], event['event_type'], event['ids']))
      if event['event_type'] in PT_MAP_INVALID_EVENTS:
        self.app.etl.invalidate_pt_map(event['zid'])
      requests = await self.get_web_requests(event)
      if requests and len(requests) > 0:
        self.app.web_req_buf.add_requests(requests)

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
      label = event_type.replace('-','_').replace(' ','')
      if CLOUDWATCH_ON:
        cloudwatch_logger.push_many(
          dimension_name  = 'ETL',
          metric_names    = ['EventCount', 'EventCount_{}'.format(label)],
          metric_values   = [1,1],
          metric_units    = ['Count','Count']
        )
      ids = None
      if 'OtherEntities' in message['eventInfo']:
        entity = message['eventInfo']['OtherEntities'][0]['Entity']
        if isinstance(entity, list):
          ids = {e['ID']['$value'] for e in entity}
        else:
          ids = {entity['ID']['$value']}
      zid = message['eventInfo']['PrimaryEntity']['ID']['$value']
      if zid.startswith('Z'):
        return {'event_type': event_type, 'zid': zid, 'ids': ids}
      else:
        return None

    except Exception as ex:
      logging.warning(str(ex))
      logging.info(message)
      traceback.print_exc()

  async def get_web_requests(self, event):
    '''
    generate the required web request functions
    based on the event type and attributes
    map event to web request; if not valid, return None
    '''
    event_type = event['event_type']
    if event_type in EpicEvents:
      args = {}
      if event_type.startswith('Flowsheet'):
        valid_ids = self.contain_valid_flo_id(event['ids'])
        if valid_ids:
          args['flowsheet_ids'] = valid_ids
        else:
          logging.debug("Discard event with invalid flowsheet id: {}".format(event['ids']))
          return None
      elif event_type.startswith('Med Admin Notification'):
        args['med_order_ids'] = event['ids']
      funcs = EpicEvents[event_type]
      return {event['zid']: {'funcs': funcs, 'args': args}}
    else:
      return None

  def contain_valid_flo_id(self, ids):
    valid_ids = set()
    for id in ids:
      for item in flowsheet_ids:
        if id in item[1]:
          if item[0] in ['urine_output', 'fluids_intake', 'map']:
            # make sure extract all urine_output/fluids_intake at the same time so we can sum them up
            valid_ids.union(item[1])
          else:
            valid_ids.add(id)
    return valid_ids


class Epic(web.View):

  async def post(self):
    try:
      message = await self.request.json()
      await self.request.app.event_handler.process(message)

    except Exception as ex:
      logging.warning(str(ex))
      traceback.print_exc()
      raise web.HTTPBadRequest(body=json.dumps({'message': str(ex)}))

    finally:
      # TODO: define fault tolerant responses
      return json_response({})
