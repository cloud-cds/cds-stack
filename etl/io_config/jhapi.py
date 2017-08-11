from etl.mappings.api_servers import servers
from etl.mappings.flowsheet_ids import flowsheet_ids
from etl.mappings.component_ids import component_ids
from etl.mappings.lab_procedures import procedure_ids

import sys
import asyncio
import etl.io_config.core as core
from aiohttp import ClientSession
from aiohttp import client_exceptions
from time import sleep
import pandas as pd
import datetime as dt
import itertools
import logging
import pytz
import random

class JHAPIConfig:
  def __init__(self, hospital, lookback_hours, jhapi_server, jhapi_id,
               jhapi_secret, lookback_days=None):
    if jhapi_server not in servers:
      raise ValueError("Incorrect server provided")
    if int(lookback_hours) > 72:
      raise ValueError("Lookback hours must be less than 72 hours")
    self.server = servers[jhapi_server]
    self.hospital = hospital
    self.lookback_hours = int(lookback_hours)
    self.lookback_days = int(lookback_days) if lookback_days else int(int(lookback_hours)/24.0 + 1)
    self.from_date = (dt.datetime.now() + dt.timedelta(days=1)).strftime('%Y-%m-%d')
    tomorrow = dt.datetime.now() + dt.timedelta(days=1)
    self.dateFrom = (tomorrow - dt.timedelta(days=self.lookback_days)).strftime('%Y-%m-%d')
    self.dateTo = tomorrow.strftime('%Y-%m-%d')
    self.headers = {
      'client_id': jhapi_id,
      'client_secret': jhapi_secret,
      'User-Agent': ''
    }

  def make_requests(self, ctxt, endpoint, payloads, http_method='GET'):
    if type(payloads) != list:
      raise TypeError("Must pass in a list of payloads")

    url = "{}{}".format(self.server, endpoint)
    request_settings = self.generate_request_settings(http_method, url, payloads)

    async def fetch(session, sem, setting):
      backoff = 2
      base = 2
      max_backoff = 60

      request_attempts = 10
      for i in range(request_attempts):
        try:
          async with sem:
            async with session.request(**setting) as response:
              if response.status != 200:
                body = await response.text()
                logging.error("  Status={}\tMessage={}".format(response.status, body))
                return None
              return await response.json()
        except Exception as e:
          if i < request_attempts - 1 and str(e) != 'Session is closed':
            logging.error("Request Error Caught for URL {}, retrying... {} times".format(url,i+1))
            logging.exception(e)
            random_secs = random.uniform(0, 1)
            wait_time = min(((base**attempts) + random_secs), max_backoff)
            sleep(wait_time)
          else:
            raise Exception("Fail to request URL {}".format(url))

    async def run(request_settings, loop):
      tasks = []
      sem = asyncio.Semaphore(200)
      async with ClientSession(headers=self.headers, loop=loop) as session:
        for setting in request_settings:
          task = asyncio.ensure_future(fetch(session, sem, setting), loop=loop)
          tasks.append(task)
        return await asyncio.gather(*tasks)

    backoff = 2
    base = 2
    max_backoff = 60

    attempts = 100
    for attempt in range(attempts):
      try:
        future = asyncio.ensure_future(run(request_settings, ctxt.loop), loop=ctxt.loop)
        ctxt.loop.run_until_complete(future)
        return future.result()
      except Exception as e:
        # retrying
        if attempt < attempts - 1:
          logging.error("Session Error Caught for URL {}, retrying... {} times".format(url, attempt+1))
          logging.exception(e)
          random_secs = random.uniform(0, 1)
          wait_time = min(((base**attempts) + random_secs), max_backoff)
          sleep(wait_time)
        else:
          raise Exception("Session failed for URL {}".format(url))

  def generate_request_settings(self, http_method, url, payloads=None):
    request_settings = []
    for payload in payloads:
      setting = {
        'method': http_method,
        'url': url
      }
      if payload is not None:
        key = 'params' if http_method == 'GET' else 'json'
        setting[key] = payload
      request_settings.append(setting)

    return request_settings


  def extract_bedded_patients(self, ctxt, hospital, limit=None):
    resource = '/facilities/hospital/' + self.hospital + '/beddedpatients'
    responses = self.make_requests(ctxt, resource, [None], 'GET')
    if limit:
      logging.info("max_num_pats = {}".format(limit))
    df = pd.DataFrame(responses[0]).head(limit) if limit else pd.DataFrame(responses[0])
    return df.assign(hospital = hospital)


  def combine(self, response_list, to_merge):
    if type(response_list) != list:
      raise TypeError("First argument must be a list of responses")
    dfs = pd.DataFrame()
    for idx, df in enumerate(response_list):
      if not df.empty:
        dfs = pd.concat([dfs, df.assign(index_col=idx)])
    if dfs.empty:
      return dfs
    return pd.merge(dfs, to_merge, how='inner', left_on='index_col',
            right_index=True, sort=False).drop('index_col', axis=1)


  def extract_flowsheets(self, ctxt, bedded_patients):
    resource = '/patients/flowsheetrows'
    flowsheet_row_ids = []
    for fid, internal_id_list in flowsheet_ids:
      for internal_id in internal_id_list:
        flowsheet_row_ids.append({'ID': str(internal_id),
                      'Type': 'Internal'})
    payloads = [{
      'ContactID':        pat['visit_id'],
      'ContactIDType':    'CSN',
      'FlowsheetRowIDs':  flowsheet_row_ids,
      'LookbackHours':    self.lookback_hours,
      'PatientID':        pat['pat_id'],
      'PatientIDType':    'EMRN'
    } for _, pat in bedded_patients.iterrows()]
    responses = self.make_requests(ctxt, resource, payloads, 'POST')
    dfs = [pd.DataFrame(r) for r in responses]
    return self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])


  def extract_active_procedures(self, ctxt, bedded_patients):
    resource = '/facilities/hospital/' + self.hospital + '/orders/activeprocedures'
    payloads = [{'csn': pat['visit_id']} for _, pat in bedded_patients.iterrows()]
    responses = self.make_requests(ctxt, resource, payloads, 'GET')
    dfs = [pd.DataFrame(r) for r in responses]
    return self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])


  def extract_lab_orders(self, ctxt, bedded_patients):
    resource = '/patients/labs/procedure'
    procedure_types = []
    for _, ids in procedure_ids:
      procedure_types += ({'Type': 'INTERNAL', 'ID': str(x)} for x in ids)
    payloads = [{
      'Id':                   pat['pat_id'],
      'IdType':               'patient',
      'FromDate':             self.from_date,
      'MaxNumberOfResults':   200,
      'NumberDaysToLookBack': self.lookback_days,
      'ProcedureTypes':       procedure_types
    } for _, pat in bedded_patients.iterrows()]
    responses = self.make_requests(ctxt, resource, payloads, 'POST')
    dfs = [pd.DataFrame(r['ProcedureResults'] if r else None) for r in responses]
    return self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])


  def extract_lab_results(self, ctxt, bedded_patients):
    resource = '/patients/labs/component'
    component_types = []
    for _, cidl in component_ids:
      component_types += ({'Type': 'INTERNAL', 'Value': str(x)} for x in cidl)
    payloads = [{
      'Id':                   pat['pat_id'],
      'IdType':               'patient',
      'FromDate':             self.from_date,
      'MaxNumberOfResults':   200,
      'NumberDaysToLookBack': self.lookback_days,
      'ComponentTypes':       component_types
    } for _, pat in bedded_patients.iterrows()]
    responses = self.make_requests(ctxt, resource, payloads, 'POST')
    dfs = [pd.DataFrame(r['ResultComponents'] if r else None) for r in responses]
    return self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])


  def extract_loc_history(self, ctxt, bedded_patients):
    resource = '/patients/adtlocationhistory'
    payloads = [{'id': pat['visit_id'], 'type': 'CSN'} for _, pat in bedded_patients.iterrows()]
    responses = self.make_requests(ctxt, resource, payloads, 'GET')
    dfs = [pd.DataFrame(r) for r in responses]
    return self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])


  def extract_med_orders(self, ctxt, bedded_patients):
    resource = '/patients/medications'
    payloads = [{
      'id':           pat['pat_id'],
      'searchtype':   'IP',
      'dayslookback': str(self.lookback_days)
    } for _, pat in bedded_patients.iterrows()]
    responses = self.make_requests(ctxt, resource, payloads, 'GET')
    dfs = [pd.DataFrame(r) for r in responses]
    return self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])


  def extract_med_admin(self, ctxt, med_orders):
    resource = '/patients/medicationadministrationhistory'
    payloads = [{
      'ContactID':        order['visit_id'],
      'ContactIDType':    'CSN',
      'OrderIDs':         list(itertools.chain.from_iterable(order['ids'])),
      'PatientID':        order['pat_id']
    } for _, order in med_orders.iterrows()]
    responses = self.make_requests(ctxt, resource, payloads, 'POST')
    dfs = [pd.DataFrame(r) for r in responses]
    return self.combine(dfs, med_orders[['pat_id', 'visit_id']])


  def extract_notes(self, ctxt, bedded_patients):
    resource = '/patients/documents/list'
    payloads = [{
      'id'       : pat['pat_id'],
      'dateFrom' : self.dateFrom,
      'dateTo'   : self.dateTo
    } for _, pat in bedded_patients.iterrows()]
    responses = self.make_requests(ctxt, resource, payloads, 'GET')
    logging.info('#NOTES PAYLOADS: %s' % len(payloads))
    logging.info('#NOTES RESPONSES: %s' % len(responses))
    dfs = [pd.DataFrame(r['DocumentListData'] if r else None) for r in responses]
    return self.combine(dfs, bedded_patients[['pat_id']])


  def extract_note_texts(self, ctxt, notes):
    if not notes.empty:
      resource = '/patients/documents/text'
      payloads = [{ 'key' : note['Key'] } for _, note in notes.iterrows()]
      responses = self.make_requests(ctxt, resource, payloads, 'GET')
      logging.info('#NOTE TEXTS PAYLOADS: %s' % len(payloads))
      logging.info('#NOTE TEXTS RESPONSES: %s' % len(responses))
      dfs = [pd.DataFrame([{'DocumentText': r['DocumentText']}] if r else None) for r in responses]
      return self.combine(dfs, notes[['Key']])
    return pd.DataFrame()


  def extract_contacts(self, ctxt, pat_id_list):
    if not pat_id_list:
      return pd.DataFrame()
    resource = '/patients/contacts'
    pat_id_df = pd.DataFrame(pat_id_list)
    pat_id_df = pat_id_df[pat_id_df['pat_id'].str.contains('E.*')] # Gets rid of fake patients
    payloads = [{
      'id'       : pat['visit_id'],
      'idtype'   : 'csn',
      'dateFrom' : self.dateFrom, #(dt.datetime.now() - dt.timedelta(days=1000)).strftime('%Y-%m-%d'),
      'dateTo'   : self.dateTo,
    } for _, pat in pat_id_df.iterrows()]
    responses = self.make_requests(ctxt, resource, payloads, 'GET')
    response_dfs = [pd.DataFrame(r['Contacts'] if r else None) for r in responses]
    dfs = pd.concat(response_dfs)
    return pd.merge(pat_id_df, dfs, left_on='visit_id', right_on='CSN')


  def push_notifications(self, ctxt, notifications):
    notify_epic = int(core.get_environment_var('TREWS_ETL_EPIC_NOTIFICATIONS', 0))
    logging.info("pushing notifications to epic ({})".format(notify_epic))
    if notify_epic:
      resource = '/patients/addflowsheetvalue'
      load_tz='US/Eastern'
      t_utc = dt.datetime.utcnow().replace(tzinfo=pytz.utc)
      current_time = str(t_utc.astimezone(pytz.timezone(load_tz)))
      payloads = [{
        'PatientID':            n['pat_id'],
        'ContactID':            n['visit_id'],
        'UserID':               'WSEPSIS',
        'FlowsheetID':          '9490',
        'Value':                n['count'],
        'InstantValueTaken':    current_time,
        'FlowsheetTemplateID':  '304700006',
      } for n in notifications]
      for payload in payloads:
        logging.info('%s NOTIFY %s %s %s' % (payload['InstantValueTaken'], payload['PatientID'], payload['ContactID'], payload['Value']))
      self.make_requests(ctxt, resource, payloads, 'POST')
    logging.info("pushed notifications to epic")
