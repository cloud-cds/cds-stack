from etl.mappings.api_servers import servers
from etl.mappings.flowsheet_ids import flowsheet_ids
from etl.mappings.component_ids import component_ids
from etl.mappings.lab_procedures import lab_procedure_ids
from etl.transforms.pipelines import epic2op_transform as jhapi_transform_lists
from etl.core.environment import Environment
from etl.io_config.cloudwatch import Cloudwatch
import json
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
import uvloop
from dateutil.parser import parse
from datetime import date
import traceback
import etl.io_config.core as core
import pdb

EPIC_ENV = core.get_environment_var('EPIC_ENV', '')
ALL_FLO_IDS_DICT = {}
for fid, internal_id_list in flowsheet_ids:
  for internal_id in internal_id_list:
    ALL_FLO_IDS_DICT[internal_id] = {'ID': str(internal_id), 'Type': 'Internal'}
ALL_FLO_IDS_LIST = list(ALL_FLO_IDS_DICT.values())


class EpicAPIConfig:
  def __init__(self, lookback_hours, jhapi_server, jhapi_id,
               jhapi_secret, lookback_days=None, op_lookback_days=None):
    if jhapi_server not in servers:
      raise ValueError("Incorrect server provided")
    if int(lookback_hours) > 72:
      raise ValueError("Lookback hours must be less than 72 hours")
    self.jhapi_server = jhapi_server
    self.server = servers[jhapi_server]
    self.lookback_hours = int(lookback_hours)
    self.lookback_days = int(lookback_days) if lookback_days else int(int(lookback_hours)/24.0 + 1)
    self.op_lookback_days = op_lookback_days
    self.from_date = (dt.datetime.now() + dt.timedelta(days=1)).strftime('%Y-%m-%d')
    tomorrow = dt.datetime.now() + dt.timedelta(days=1)
    self.dateFrom = (tomorrow - dt.timedelta(days=self.lookback_days)).strftime('%Y-%m-%d')
    self.dateFromOneYear = (tomorrow - dt.timedelta(days=365)).strftime('%Y-%m-%d')
    self.dateFromOneMonth = (tomorrow - dt.timedelta(days=30)).strftime('%Y-%m-%d')
    self.dateTo = tomorrow.strftime('%Y-%m-%d')
    self.headers = {
      'client_id': jhapi_id,
      'client_secret': jhapi_secret,
      'User-Agent': ''
    }
    self.cloudwatch_logger = Cloudwatch()

  def generate_request_settings(self, http_method, url, payloads=None, url_type=None):
    request_settings = []
    if isinstance(url, list):
      if url_type == 'rest' and http_method == 'GET':
        for u, payload in zip(url, payloads):
          u = u + payload
          if 'api-test' in u and EPIC_ENV:
            u += ('&' if '&' in u else '?') + 'env=' + EPIC_ENV
          request_settings.append({'method': http_method,'url': u})
      else:
        if 'api-test' in url and EPIC_ENV:
          url += ('&' if '&' in url else '?') + 'env=' + EPIC_ENV
        for u, payload in zip(url, payloads):
          setting = {
            'method': http_method,
            'url': u + ('&' if '&' in u else '?') + 'env=' + EPIC_ENV if 'api-test' in u and EPIC_ENV else u
          }
          if payload is not None:
            key = 'params' if http_method == 'GET' else 'json'
            setting[key] = payload
          request_settings.append(setting)
    else:
      if url_type == 'rest' and http_method == 'GET':
        for payload in payloads:
          url = url + payload
          if 'api-test' in url and EPIC_ENV:
            url += ('&' if '&' in url else '?') + 'env=' + EPIC_ENV
          request_settings.append({'method': http_method,'url': url})
      else:
        if 'api-test' in url and EPIC_ENV:
          url += ('&' if '&' in url else '?') + 'env=' + EPIC_ENV
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

  async def make_requests(self, ctxt, endpoint, payloads, http_method='GET', url_type=None, server_type='internal'):
    # Define variables
    server = self.server if server_type == 'internal' else servers['{}-{}'.format(self.jhapi_server, server_type)]
    if isinstance(endpoint, list):
      url = ["{}{}".format(server, e) for e in endpoint]
    else:
      url = "{}{}".format(server, endpoint)
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
          if i < request_attempts - 1 and e.errno in [104]: # Connection reset by peer
            logging.error(e)
            logging.error(setting)
            traceback.print_exc()
            wait_time = min(((base**i) + random.uniform(0, 1)), max_backoff)
            error += 1
            sleep(wait_time)
          else:
            raise Exception("Fail to request URL {}".format(url))
        except Exception as e:
          if i < request_attempts - 1 and str(e) != 'Session is closed':
            logging.error(e)
            logging.error(setting)
            traceback.print_exc()
            wait_time = min(((base**i) + random.uniform(0, 1)), max_backoff)
            error += 1
            sleep(wait_time)
          else:
            raise Exception("Fail to request URL {}".format(url))
      return response, i+1, success, error


    # Get the client session and create a task for each request
    async def run(request_settings, semaphore, loop):
      async with ClientSession(headers=self.headers, loop=loop) as session:
        tasks = [asyncio.ensure_future(fetch(session, semaphore, setting),
                                       loop=loop) for setting in request_settings]
        return await asyncio.gather(*tasks)

    # Start the run task to make all requests
    for attempt in range(session_attempts):
      try:
        result = await run(request_settings, semaphore, ctxt.loop)
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
    logging.info("Made {} requests".format(sum(x[1] for x in result)))
    self.cloudwatch_logger.push(
      dimension_name = 'ETL',
      metric_name    = 'requests_made_push',
      value          = sum(x[1] for x in result),
      unit           = 'Count'
    )
    if isinstance(endpoint, list):
      labels = ['push_' + e.replace('/', '_') + '_' + http_method for e in endpoint]
      for x, label in zip(result, labels):
        self.cloudwatch_logger.push_many(
          dimension_name  = 'ETL',
          metric_names    = ['{}_success_push'.format(label), '{}_error_push'.format(label), 'jh_api_request_success_push', 'jh_api_request_error_push'],
          metric_values   = [x[2], x[3], x[2], x[3]],
          metric_units    = ['Count','Count','Count','Count']
        )
    else:
      label = 'push_' + endpoint.replace('/', '_') + '_' + http_method
      self.cloudwatch_logger.push_many(
        dimension_name  = 'ETL',
        metric_names    = ['{}_success_push'.format(label), '{}_error_push'.format(label), 'jh_api_request_success_push', 'jh_api_request_error_push'],
        metric_values   = [sum(x[2] for x in result), sum(x[3] for x in result), sum(x[2] for x in result), sum(x[3] for x in result)],
        metric_units    = ['Count','Count','Count','Count']
      )
    # Return responses
    return [x[0] for x in result]

  async def extract_mrn_by_zid(self, ctxt, zid):
    resource = '/patients/mrn/'
    payloads = [zid]
    responses = await self.make_requests(ctxt, resource, payloads, 'GET', url_type='rest')
    def calculate_age(born):
      today = date.today()
      return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

    p = {'zid': zid}
    r = responses[0]
    pat_id = [pid["ID"] for pid in r[0]['IDs'] if pid['Type'] == 'EMRN'][0]
    sex = r[0]['Sex']
    gender = 0 if sex == 'Female' else 1
    dob = parse(r[0]["DateOfBirth"])
    age = calculate_age(dob)
    p['pat_id'] = pat_id
    p['age'] = age
    p['gender'] = gender
    return p

  async def extract_ed_patients_mrn(self, ctxt, ed_patients):
    resource = '/patients/mrn/'
    payloads = [row['pat_id'] for i, row in ed_patients.iterrows()]
    responses = await self.make_requests(ctxt, resource, payloads, 'GET', url_type='rest')
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

  async def extract_active_procedures(self, ctxt, bedded_patients, args):
    bp_hospital_null = bedded_patients[bedded_patients.hospital.isnull()]
    if not bp_hospital_null.empty:
      logging.warn('extract_active_procedures: empty hospital: {}'.format(bp_hospital_null))
    bp = bedded_patients[~bedded_patients.hospital.isnull()]
    resource = ['/facilities/hospital/{}/orders/activeprocedures'.format(pat['hospital']) for _, pat in bp.iterrows()]
    payloads = [{'csn': pat['visit_id']} for _, pat in bp.iterrows()]
    responses = await self.make_requests(ctxt, resource, payloads, 'GET')
    dfs = [pd.DataFrame(r) for r in responses]
    df_raw = self.combine(dfs, bp[['pat_id', 'visit_id']])
    return {'active_procedures_transformed': self.transform(ctxt, df_raw, 'active_procedures_transforms')}

  async def extract_chiefcomplaint(self, ctxt, beddedpatients, args):
    resource = '/patients/getdata/chiefcomplaint'
    payloads = [{
      "ContactID": {
        "ID": pat['visit_id'],
        "Type": "CSN"
      },
      "DataFormat": None,
      "Items": [
        {
          "ItemNumber": "18100",
          "LineRange": {
            "From": 1,
            "To": 10
          }
        }
      ],
      "RecordID": {
        "ID": pat['pat_id'],
        "Type":"EMRN"
      }
    } for _, pat in beddedpatients.iterrows()]
    responses = await self.make_requests(ctxt, resource, payloads, 'POST', server_type='epic')
    for r in responses:
      if r:
        raw_items = r['Items'][0]
        new_items = '[' + ','.join(["{{\"reason\" : \"{}\"}}".format(reason) for reason in [item['Value'] for item in raw_items['Lines'] if item['LineNumber'] > 0]]) + ']'
        r['Items'] = new_items
        r['RecordIDs'] = None
        r['ContactIDs'] = [id for id in r['ContactIDs'] if id['Type'] == 'CSN']
    dfs = [pd.DataFrame(r) for r in responses]
    df_raw = self.combine(dfs, beddedpatients[['pat_id', 'visit_id']])
    return {'chiefcomplaint_transformed': self.transform(ctxt, df_raw, 'chiefcomplaint_transforms')}

  async def extract_lab_orders(self, ctxt, bedded_patients, args):
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
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    dfs = [pd.DataFrame(r['ProcedureResults'] if r else None) for r in responses]
    df_raw = self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])
    return {'lab_orders_transformed': self.transform(ctxt, df_raw, 'lab_orders_transforms')}

  async def extract_loc_history(self, ctxt, bedded_patients, args):
    resource = '/patients/adtlocationhistory'
    payloads = [{
      'id': pat['visit_id'],
      'type': 'CSN'
    } for _, pat in bedded_patients.iterrows()]
    responses = await self.make_requests(ctxt, resource, payloads, 'GET')
    dfs = [pd.DataFrame(r) for r in responses]
    df_raw = self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])
    return {'location_history_transformed': self.transform(ctxt, df_raw, 'loc_history_transforms')}


  async def extract_lab_results(self, ctxt, bedded_patients, args):
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
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    dfs = [pd.DataFrame(r['ResultComponents'] if r else None) for r in responses]
    df_raw = self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])
    return {'lab_results_transformed': self.transform(ctxt, df_raw, 'lab_results_transforms')}

  async def extract_med_admin(self, ctxt, beddedpatients, args, results):
    def build_med_admin_request_data(ctxt, pats, med_orders_df, args):
      if med_orders_df is None or med_orders_df.empty:
        pats['ids'] = pats.apply(lambda x: [], axis=1)
      else:
        med_orders = med_orders_df[['pat_id', 'visit_id', 'ids']]\
          .groupby(['pat_id', 'visit_id'])['ids']\
          .apply(list)\
          .reset_index()
        pats = pd.merge(pats, med_orders, left_on=['pat_id','visit_id'], right_on=['pat_id', 'visit_id'], how='left')
      for i, pt in pats.iterrows():
        if (isinstance(pt['ids'], float) or len(pt['ids']) == 0) and ('med_order_ids' in args[i]):
          pats.set_value(i, 'ids', [[{'ID': id, 'Type': 'Internal'}] for id in args[i]['med_order_ids']])
      return pats[(pats.astype(str)['ids'] != '[]') & pats.ids.notnull()]

    logging.debug("extracting med admin")
    med_orders_df = None
    for result in results:
      for name in result:
        if name == 'med_orders_transformed':
          med_orders_df = result[name]
          med_orders_df['ids'] = med_orders_df['ids'].astype(list)
          med_orders_df = med_orders_df[med_orders_df.order_mode == 'Inpatient']
    if med_orders_df is None or med_orders_df.empty:
      logging.debug("No med_orders for MAR")
      return {'med_admin_transformed': None}
    med_orders_df.loc[:, "ids"] = med_orders_df.ids.apply(lambda x: eval(x))
    med_orders = build_med_admin_request_data(ctxt, beddedpatients, med_orders_df, args)
    if med_orders is None or med_orders.empty:
      logging.debug("No med_orders for MAR")
      return {'med_admin_transformed': None}
    else:
      med_orders = med_orders.reset_index(drop=True)
      resource = '/patients/medicationadministrationhistory'
      payloads = [{
        'ContactID':        order['visit_id'],
        'ContactIDType':    'CSN',
        'OrderIDs':         list(itertools.chain.from_iterable(order['ids'])),
        'PatientID':        order['pat_id']
      } for _, order in med_orders.iterrows()]
      logging.debug('med_orders: {}'.format(med_orders))
      responses = await self.make_requests(ctxt, resource, payloads, 'POST')
      dfs = [pd.DataFrame(r) for r in responses]
      logging.debug('dfs: {}'.format(dfs))
      df_raw = self.combine(dfs, med_orders[['pat_id', 'visit_id']])
      logging.debug('df_raw: {}'.format(df_raw))
      df_tran = self.transform(ctxt, df_raw, 'med_admin_transforms')
      logging.debug(df_tran)
      if df_tran is not None:
        return {'med_admin_transformed': self.tz_hack(ctxt, df_tran)}


  async def extract_med_orders(self, ctxt, bedded_patients, args):
    resource = '/patients/medications'
    payloads = [{
      'id':           pat['pat_id'],
      'dayslookback': str(self.lookback_days),
      'searchtype':   'IP'
    } for _, pat in bedded_patients.iterrows()] + \
    [{
      'id':           pat['pat_id'],
      'dayslookback': str(self.op_lookback_days),
      'searchtype':   'OP'
    } for _, pat in bedded_patients.iterrows()]
    self.log.debug("med_order payloads: {}".format(payloads))
    responses = await self.make_requests(ctxt, resource, payloads, 'GET')
    dfs = [pd.DataFrame(r) for r in responses]
    half = len(dfs)//2
    med_ip = self.combine(dfs[:half], bedded_patients[['pat_id', 'visit_id']])
    med_op = self.combine(dfs[half:], bedded_patients[['pat_id', 'visit_id']])
    df_raw = pd.concat([med_ip, med_op]).reset_index(drop=True)
    # self.log.debug("med_order df_raw: {}".format(df_raw))
    if not df_raw.empty:
      # self.log.debug('med_order df_raw.med-order: {}'.format(df_raw.MedicationOrders))
      df_tran = self.transform(ctxt, df_raw, 'med_orders_transforms')
      df_tran['ids'] = df_tran['ids'].astype(str)
      # self.log.debug("med_order df_tran: {}".format(df_tran))
    else:
      self.log.debug("empty raw med_orders")
      df_tran = None
    return {'med_orders_transformed': df_tran}

  def tz_hack(self, ctxt, df):
    if not df.empty:
      df['tsp'] = df['tsp'].str.replace('-04:00', '+00:00')
      df['tsp'] = df['tsp'].str.replace('-05:00', '+00:00')
    return df

  async def extract_notes(self, ctxt, bedded_patients, args):
    resource = '/patients/documents/list'
    payloads = [{
      'id'       : pat['pat_id'],
      'dateFrom' : self.dateFrom,
      'dateTo'   : self.dateTo
    } for _, pat in bedded_patients.iterrows()]
    responses = await self.make_requests(ctxt, resource, payloads, 'GET')
    logging.debug('#NOTES PAYLOADS: %s' % len(payloads))
    logging.debug('#NOTES RESPONSES: %s' % len(responses))
    dfs = [pd.DataFrame(r['DocumentListData'] if r else None) for r in responses]
    df = self.combine(dfs, bedded_patients[['pat_id']])
    if not df.empty:
      not_empty_idx = df.Key.str.len() > 0
      df = df[not_empty_idx].reset_index()
    return {'notes_transformed': self.transform(ctxt, df, 'notes_transforms')}

  async def extract_note_texts(self, ctxt, beddedpatients, args, results):
    notes = None
    for name in results:
      if name == 'notes_transformed':
        notes = results[name]
    if notes is not None and not notes.empty:
      resource = '/patients/documents/text'
      payloads = [{ 'key' : note['Key'] } for _, note in notes.iterrows()]
      responses = await self.make_requests(ctxt, resource, payloads, 'GET')
      logging.debug('#NOTE TEXTS PAYLOADS: %s' % len(payloads))
      logging.debug('#NOTE TEXTS RESPONSES: %s' % len(responses))
      dfs = [
        pd.DataFrame([{'DocumentText': r['DocumentText']}] if r else None)
        for r in responses
      ]
      df_raw = self.combine(dfs, notes[['Key']])
      return {'note_texts_transformed': self.transform(ctxt, df, 'note_texts_transforms')}
    return None

  async def extract_flowsheets(self, ctxt, pts, args):
    resource = '/patients/flowsheetrows'
    payloads = [{
      'ContactID':        pat['visit_id'],
      'ContactIDType':    'CSN',
      'FlowsheetRowIDs':  [ALL_FLO_IDS_DICT[id] for id in args[i]['flowsheet_ids']] if 'flowsheet_ids' in args[i] else ALL_FLO_IDS_LIST,
      'LookbackHours':    self.lookback_hours,
      'PatientID':        pat['pat_id'],
      'PatientIDType':    'EMRN'
    } for i, pat in pts.iterrows()]
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    dfs = [pd.DataFrame(r) for r in responses]
    df_raw = self.combine(dfs, pts[['pat_id', 'visit_id']])
    if df_raw is None or df_raw.empty:
      return {'flowsheets_transformed': None}
    else:
      df_tran = self.transform(ctxt, df_raw, 'flowsheet_transforms')
      if not df_tran.empty:
        return {'flowsheets_transformed': self.tz_hack(ctxt, df_tran)}
      else:
        return {'flowsheets_transformed': None}

  async def extract_treatmentteam(self, ctxt, bedded_patients, args):
    resource = '/patients/treatmentteam'
    payloads = [{
      'id': pat['visit_id'],
      'idtype': 'csn'
    } for _, pat in bedded_patients.iterrows()]
    responses = await self.make_requests(ctxt, resource, payloads, 'GET')
    dfs = [pd.DataFrame(r['TreatmentTeam'] if r else None) for r in responses]
    df_raw = self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])
    if df_raw is None or df_raw.empty:
      return {'treatmentteam_transformed': None}
    else:
      df_tran = self.transform(ctxt, df_raw, 'treatmentteam_transforms')
      if df_tran.empty:
        return {'treatmentteam_transformed': None}
      else:
        return {'treatmentteam_transformed': df_tran}

  async def extract_contacts(self, ctxt, pat_id_list, args, idtype='csn', dateFromOneYear=False):
    def get_hospital(row):
      dept = row['DepartmentName']
      if dept is not None and len(dept) > 0:
        if 'HC' in dept:
          return 'HCGH'
        elif 'JH' in dept or 'KKI' in dept:
          return 'JHH'
        elif 'BMC' in dept or 'BV' in dept:
          return 'BMC'
        elif 'SM' in dept:
          return 'SMH'
        elif 'SH' in dept:
          return 'SH'
    if not pat_id_list:
      return None
    resource = '/patients/contacts'
    pat_id_df = pd.DataFrame(pat_id_list)
    dfs = None
    if idtype == 'csn':
      # Get rid of fake patients by filtering out incorrect pat_ids
      pat_id_df = pat_id_df[pat_id_df['pat_id'].str.contains('E.*')]
      payloads = [{
        'id'       : pat['visit_id'],
        'idtype'   : 'csn',
        'dateFrom' : self.dateFromOneYear if dateFromOneYear else self.dateFromOneMonth,
        'dateTo'   : self.dateTo,
      } for _, pat in pat_id_df.iterrows()]
      responses = await self.make_requests(ctxt, resource, payloads, 'GET')
      response_dfs = [pd.DataFrame(r['Contacts'] if r else None) for r in responses]
      dfs = pd.concat(response_dfs)
    elif idtype == 'patient':
      payloads = [{
        'id'       : pat['pat_id'],
        'idtype'   : 'patient',
        'dateFrom' : self.dateFromOneYear if dateFromOneYear else self.dateFromOneMonth,
        'dateTo'   : self.dateTo,
      } for _, pat in pat_id_df.iterrows()]
      responses = await self.make_requests(ctxt, resource, payloads, 'GET')
      response_dfs = []
      logging.debug(responses)
      for r in responses:
        if r and r['Contacts']:
          for contact in r['Contacts']:
            if contact['EncounterType'] == 'Hospital Encounter':
              if 'Outpatient' in contact['PatientClass']:
                return None # ignore outpatient
              else:
                rec = {'CSN': contact['CSN'], 'DepartmentName': contact['DepartmentName'], 'patient_class': contact['PatientClass']}
                for item in r['PatientIDs']:
                  if item['IDType'] == 'EMRN':
                    rec['pat_id'] = item['ID']
                    logging.debug(rec)
                    response_dfs.append(pd.DataFrame([rec]))
                    dfs = pd.concat(response_dfs)
                    dfs['hospital'] = dfs.apply(get_hospital, axis=1)
                    return pd.merge(pat_id_df, dfs, left_on='pat_id', right_on='pat_id')
        else:
          logging.warn("No Contacts INFO for {}".format(payloads))
    return None

  async def extract_discharge(self, ctxt, pts, args):
    if pts is None or pts.empty:
      return {'discharged': None}
    resource = '/patients/contacts'
    # Get rid of fake patients by filtering out incorrect pat_ids
    payloads = [{
      'id'       : pat['visit_id'],
      'idtype'   : 'csn',
      'dateFrom' : self.dateFrom,
      'dateTo'   : self.dateTo,
    } for _, pat in pts.iterrows()]
    responses = await self.make_requests(ctxt, resource, payloads, 'GET')
    response_dfs = [pd.DataFrame(r['Contacts'] if r else None) for r in responses]
    dfs = pd.concat(response_dfs)
    if dfs.empty:
      return {'discharged': None}
    else:
      contacts = pd.merge(pts, dfs, left_on='visit_id', right_on='CSN')
      discharged = await self.create_discharge_times(ctxt, contacts)
      return {'discharged': discharged}

  async def create_discharge_times(self, ctxt, contacts_df):
    if contacts_df.empty:
      return
    discharged_df = contacts_df[contacts_df['DischargeDate'] != '']
    if discharged_df.empty:
      return None
    def build_value(row):
      value      = json.dumps({
        'disposition':  row['DischargeDisposition'],
        'department': row['DepartmentName']
      })
      return value
    discharged_df['confidence'] = 1
    discharged_df['fid'] = 'discharge'
    discharged_df['tsp'] = discharged_df['DischargeDate']
    discharged_df['value'] = discharged_df.apply(build_value, axis=1)
    return discharged_df

  def skip_none(self, df, transform_function):
    if df is None or df.empty:
      return None
    try:
      start = dt.datetime.now()
      df = transform_function(df)
      logging.debug("function time: {}".format(dt.datetime.now() - start))
      return df
    except Exception as e:
      logging.error("== EXCEPTION CAUGHT ==")
      logging.error("error location:   " + e.func_name)
      logging.error("reason for error: " + e.reason)
      logging.error(e.context)
      traceback.print_exc()

  def transform(self, ctxt, df, transform_list_name):
    if df is None:
      return None
    if type(df) == list:
      df = pd.concat(df)
    for transform_fn in getattr(jhapi_transform_lists, transform_list_name):
      df = self.skip_none(df, transform_fn)
    return df