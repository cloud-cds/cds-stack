from etl.mappings.api_servers import servers
from etl.mappings.flowsheet_ids import flowsheet_ids
from etl.mappings.component_ids import component_ids
from etl.mappings.lab_procedures import procedure_ids

import sys
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

from aiohttp import ClientSession
import pandas as pd
import datetime as dt
import itertools
import logging

class Extractor:
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
        self.headers = {
            'client_id': jhapi_id,
            'client_secret': jhapi_secret,
            'User-Agent': ''
        }

    def make_requests(self, endpoint, payloads, http_method='GET'):
        if type(payloads) != list:
            raise TypeError("Must pass in a list of payloads")

        url = "{}{}".format(self.server, endpoint)
        request_settings = self.generate_request_settings(http_method, url, payloads)

        async def fetch(session, sem, setting):
            async with sem:
                async with session.request(**setting) as response:
                    if response.status != 200:
                        logging.error("  Status={}\tMessage={}".format(
                            response.status, response.text
                        ))
                        return None
                    return await response.json()

        async def run(request_settings, loop):
            tasks = []
            sem = asyncio.Semaphore(200)
            async with ClientSession(headers=self.headers, loop=loop) as session:
                for setting in request_settings:
                    task = asyncio.ensure_future(fetch(session, sem, setting))
                    tasks.append(task)
                return await asyncio.gather(*tasks)

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run(request_settings, loop))
        loop.run_until_complete(future)
        return future.result()


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


    def extract_bedded_patients(self, limit):
        resource = '/facilities/hospital/' + self.hospital + '/beddedpatients'
        responses = self.make_requests(resource, [None], 'GET')
        if limit:
            logging.info("max_num_pats = {}".format(limit))
        return pd.DataFrame(responses[0]).head(limit) if limit else pd.DataFrame(responses[0])


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


    def extract_flowsheets(self, bedded_patients):
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
        responses = self.make_requests(resource, payloads, 'POST')
        dfs = [pd.DataFrame(r) for r in responses]
        return self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])


    def extract_active_procedures(self, bedded_patients):
        resource = '/facilities/hospital/' + self.hospital + '/orders/activeprocedures'
        payloads = [{'csn': pat['visit_id']} for _, pat in bedded_patients.iterrows()]
        responses = self.make_requests(resource, payloads, 'GET')
        dfs = [pd.DataFrame(r) for r in responses]
        return self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])


    def extract_lab_orders(self, bedded_patients):
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
        responses = self.make_requests(resource, payloads, 'POST')
        dfs = [pd.DataFrame(r['ProcedureResults'] if r else None) for r in responses]
        return self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])


    def extract_lab_results(self, bedded_patients):
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
        responses = self.make_requests(resource, payloads, 'POST')
        dfs = [pd.DataFrame(r['ResultComponents'] if r else None) for r in responses]
        return self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])


    def extract_loc_history(self, bedded_patients):
        resource = '/patients/adtlocationhistory'
        payloads = [{'id': pat['visit_id'], 'type': 'CSN'} for _, pat in bedded_patients.iterrows()]
        responses = self.make_requests(resource, payloads, 'GET')
        dfs = [pd.DataFrame(r) for r in responses]
        return self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])


    def extract_med_orders(self, bedded_patients):
        resource = '/patients/medications'
        payloads = [{
            'id':           pat['pat_id'],
            'searchtype':   'IP',
            'dayslookback': str(self.lookback_days)
        } for _, pat in bedded_patients.iterrows()]
        responses = self.make_requests(resource, payloads, 'GET')
        dfs = [pd.DataFrame(r) for r in responses]
        return self.combine(dfs, bedded_patients[['pat_id', 'visit_id']])


    def extract_med_admin(self, med_orders):
        resource = '/patients/medicationadministrationhistory'
        payloads = [{
            'ContactID':        order['visit_id'],
            'ContactIDType':    'CSN',
            'OrderIDs':         list(itertools.chain.from_iterable(order['ids'])),
            'PatientID':        order['pat_id']
        } for _, order in med_orders.iterrows()]
        responses = self.make_requests(resource, payloads, 'POST')
        dfs = [pd.DataFrame(r) for r in responses]
        return self.combine(dfs, med_orders[['pat_id', 'visit_id']])


    def extract_notes(self, bedded_patients):
        resource = '/patients/documents/list'
        tomorrow = dt.datetime.now() + dt.timedelta(days=1)
        dateFrom = (tomorrow - dt.timedelta(days=self.lookback_days)).strftime('%Y-%m-%d')
        dateTo = tomorrow.strftime('%Y-%m-%d')
        payloads = [{
            'id'       : pat['pat_id'],
            'dateFrom' : dateFrom,
            'dateTo'   : dateTo
        } for _, pat in bedded_patients.iterrows()]
        responses = self.make_requests(resource, payloads, 'GET')
        dfs = [pd.DataFrame(r['DocumentListData'] if r else None) for r in responses]
        return self.combine(dfs, bedded_patients[['pat_id']])


    def extract_note_texts(self, notes):
        resource = '/patients/documents/text'
        payloads = [{ 'key' : note['Key'] } for _, note in notes.iterrows()]
        responses = self.make_requests(resource, payloads, 'GET')
        dfs = [pd.DataFrame([{'DocumentText': r['DocumentText']}]) for r in responses]
        return self.combine(dfs, notes[['Key']])


    def push_notifications(self, notifications):
        resource = '/patients/addflowsheetvalue'
        payloads = [{
            'PatientID':            n['pat_id'],
            'ContactID':            n['visit_id'],
            'UserID':               'WSEPSIS',
            'FlowsheetID':          '9490',
            'Value':                n['count'],
            'InstantValueTaken':    str(dt.datetime.utcnow()),
            'FlowsheetTemplateID':  '304700006',
        } for n in notifications]
        for n in notifications:
            logging.info('%s NOTIFY %s %s %s' % (n['InstantValueTaken'], n['PatientID'], n['ContactID'], n['Value']))
        self.make_requests(resource, payloads, 'POST')
        logging.info("pushed notifications to epic")
