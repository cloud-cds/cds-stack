from etl.mappings.api_servers import servers
from etl.mappings.flowsheet_ids import flowsheet_ids
from etl.mappings.component_ids import component_ids
from etl.mappings.lab_procedures import lab_procedure_ids

from etl.core.environment import Environment
from etl.io_config.cloudwatch import Cloudwatch

import sys
import asyncio
from aiohttp import ClientSession
from aiohttp import client_exceptions
from time import sleep
import pandas as pd
import datetime as dt
import itertools
import logging
import pytz
import random

from dateutil.parser import parse
from datetime import date

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
    self.cloudwatch_logger = Cloudwatch()



  def make_requests(self, ctxt, endpoint, payloads, http_method='GET', url_type=None):
    # Define variables
    url = "{}{}".format(self.server, endpoint)
    request_settings = self.generate_request_settings(http_method, url, payloads, url_type)
    semaphore = asyncio.Semaphore(ctxt.flags.JHAPI_SEMAPHORE, loop=ctxt.loop)
    base = ctxt.flags.JHAPI_BACKOFF_BASE
    max_backoff = ctxt.flags.JHAPI_BACKOFF_MAX
    session_attempts = ctxt.flags.JHAPI_ATTEMPTS_SESSION
    request_attempts = ctxt.flags.JHAPI_ATTEMPTS_REQUEST
    # Asyncronous task to make a request
    async def fetch(session, sem, setting):
      success = 0
      error = 0
      for i in range(request_attempts):
        try:
          async with sem:
            async with session.request(**setting) as response:
              if response.status != 200:
                body = await response.text()
                logging.error("Status={}\tMessage={}\tRequest={}".format(response.status, body, setting))
                response = None
                error += 1
              else:
                response = await response.json()
                success += 1
              break
        except IOError as e:
          if i < request_attempts - 1 and not e.errno in (104): # Connection reset by peer
            logging.error(e)
            wait_time = min(((base**i) + random.uniform(0, 1)), max_backoff)
            sleep(wait_time)
          else:
            raise Exception("Fail to request URL {}".format(url))
        except Exception as e:
          if i < request_attempts - 1 and str(e) != 'Session is closed':
            logging.error(e)
            wait_time = min(((base**i) + random.uniform(0, 1)), max_backoff)
            sleep(wait_time)
          else:
            raise Exception("Fail to request URL {}".format(url))
      return response, i+1, success, error


    # Get the client session and create a task for each request
    async def run(request_settings, semaphore, loop):
      async with ClientSession(headers=self.headers, loop=ctxt.loop) as session:
        tasks = [asyncio.ensure_future(fetch(session, semaphore, setting),
                                       loop=loop) for setting in request_settings]
        return await asyncio.gather(*tasks)

    # Start the run task to make all requests
    for attempt in range(session_attempts):
      try:
        task = run(request_settings, semaphore, ctxt.loop)
        future = asyncio.ensure_future(task, loop=ctxt.loop)
        ctxt.loop.run_until_complete(future)
        break
      except Exception as e:
        if attempt < session_attempts - 1:
          logging.error("Session Error Caught for URL {}, retrying... {} times".format(url, attempt+1))
          logging.exception(e)
          wait_time = min(((base**attempt) + random.uniform(0, 1)), max_backoff)
          sleep(wait_time)
        else:
          raise Exception("Session failed for URL {}".format(url))

    # Push number of requests to cloudwatch
    logging.info("Made {} requests".format(sum(x[1] for x in future.result())))
    self.cloudwatch_logger.push(
      dimension_name = 'ETL',
      metric_name    = 'requests_made',
      value          = sum(x[1] for x in future.result()),
      unit           = 'Count'
    )
    label = self.hospital + '_' + endpoint.replace('/', '_') + '_' + http_method
    self.cloudwatch_logger.push_many(
      dimension_name  = 'ETL',
      metric_names    = ['{}_success'.format(label), '{}_error'.format(label), 'jh_api_request_success', 'jh_api_request_error'],
      metric_values   = [sum(x[2] for x in future.result()), sum(x[3] for x in future.result()), sum(x[2] for x in future.result()), sum(x[3] for x in future.result())],
      metric_units    = ['Count','Count','Count','Count']
    )
    # Return responses
    return [x[0] for x in future.result()]



  def generate_request_settings(self, http_method, url, payloads=None, url_type=None):
    request_settings = []
    if url_type == 'rest' and http_method == 'GET':
      for payload in payloads:
        u = url + payload
        request_settings.append({'method': http_method,'url': u})
    else:
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

  def extract_ed_patients(self, ctxt, hospital, limit=None):
    resource = '/facilities/hospital/' + self.hospital + '/edptntlist?eddept=ADULT'
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

  def extract_ed_patients_mrn(self, ctxt, ed_patients):
    resource = '/patients/mrn/'
    payloads = [row['pat_id'] for i, row in ed_patients.iterrows()]
    responses = self.make_requests(ctxt, resource, payloads, 'GET', url_type='rest')
    def calculate_age(born):
      today = date.today()
      return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

    for r in responses:
      pat_id = [pid["ID"] for pid in r[0]['IDs'] if pid['Type'] == 'EMRN'][0]
      sex = r[0]['Sex']
      gender = 0 if sex == 'Female' else 1
      dob = parse(r[0]["DateOfBirth"])
      age = calculate_age(dob)
      ed_patients.loc[ed_patients.pat_id == pat_id,'age'] = age
      ed_patients.loc[ed_patients.pat_id == pat_id,'gender'] = gender
    return ed_patients



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
    for _, ids in lab_procedure_ids:
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
    payloads = [{
      'id': pat['visit_id'],
      'type': 'CSN'
    } for _, pat in bedded_patients.iterrows()]
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
      dfs = [
        pd.DataFrame([{'DocumentText': r['DocumentText']}] if r else None)
        for r in responses
      ]
      return self.combine(dfs, notes[['Key']])
    return pd.DataFrame()


  def extract_contacts(self, ctxt, pat_id_list):
    if not pat_id_list:
      return pd.DataFrame()
    resource = '/patients/contacts'
    pat_id_df = pd.DataFrame(pat_id_list)
    # Get rid of fake patients by filtering out incorrect pat_ids
    pat_id_df = pat_id_df[pat_id_df['pat_id'].str.contains('E.*')]
    payloads = [{
      'id'       : pat['visit_id'],
      'idtype'   : 'csn',
      'dateFrom' : self.dateFrom,
      'dateTo'   : self.dateTo,
    } for _, pat in pat_id_df.iterrows()]
    responses = self.make_requests(ctxt, resource, payloads, 'GET')
    response_dfs = [pd.DataFrame(r['Contacts'] if r else None) for r in responses]
    dfs = pd.concat(response_dfs)
    return pd.merge(pat_id_df, dfs, left_on='visit_id', right_on='CSN')


  def push_notifications(self, ctxt, notifications):
    if ctxt.flags.TREWS_ETL_EPIC_NOTIFICATIONS:
      logging.info("pushing notifications to epic")
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
        logging.info('%s NOTIFY %s %s %s' % (payload['InstantValueTaken'],
                                             payload['PatientID'],
                                             payload['ContactID'],
                                             payload['Value']))
      self.make_requests(ctxt, resource, payloads, 'POST')
      logging.info("pushed notifications to epic")
    else:
      logging.info("not pushing notifications to epic")
