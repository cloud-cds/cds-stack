from etl.mappings.api_servers import servers
from etl.mappings.flowsheet_ids import flowsheet_ids
from etl.mappings.component_ids import component_ids
from etl.mappings.procedure_ids import procedure_ids

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
        self.lookback_days = int(lookback_days) if lookback_days else int(lookback_hours)/24 + 1
        self.headers = {
            'client_id': jhapi_id,
            'client_secret': jhapi_secret
        }

    def make_requests(self, endpoint, payloads, http_method='GET'):
        if type(payloads) != list:
            raise TypeError("Must pass in a list of payloads")

        url = "{}{}".format(self.server, endpoint)
        request_settings = self.generate_request_settings(http_method, url, payloads)

        async def fetch(session, sem, setting):
            async with sem:
                async with session.request(**setting) as response:
                    return await response.json()

        async def run(request_settings, loop):
            tasks = []
            sem = asyncio.Semaphore(1000)
            async with ClientSession(headers=self.headers, loop=loop) as session:
                for setting in request_settings:
                    task = asyncio.ensure_future(fetch(session, sem, setting))
                    tasks.append(task)
                return await asyncio.gather(*tasks)

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(run(request_settings, loop))
        loop.run_until_complete(future)
        return [pd.DataFrame(r) for r in future.result()]


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


    def extract_bedded_patients(self):
        resource = '/facilities/hospital/' + self.hospital + '/beddedpatients'
        responses = self.make_requests(resource, [None], 'GET')
        return responses[0]


    def combine(self, response_list, to_merge):
        if type(response_list) != list:
            raise TypeError("First argument must be a list of responses")
        dfs = pd.DataFrame()
        for idx, df in enumerate(response_list):
            dfs = pd.concat([dfs, df.assign(index_col=idx)])
        return pd.merge(dfs, to_merge, how='outer', left_on='index_col',
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
        } for idx, pat in bedded_patients.iterrows()]
        responses = self.make_requests(resource, payloads, 'POST')
        return self.combine(responses, bedded_patients[['pat_id', 'visit_id']])


    def extract_lab_results(self, bedded_patients):
        resource = '/patients/labs/component'
        component_types = []
        for fid, component_id_list in component_ids:
            for cid in component_id_list:
                component_types.append({'Type': 'INTERNAL', 'Value': str(cid)})
        payloads = [{
            'Id':                   pat['pat_id'],
            'IdType':               'patient',
            'FromDate':      (dt.datetime.now() + dt.timedelta(days=1)).strftime('%Y-%m-%d'),
            'MaxNumberOfResults':   200,
            'NumberDaysToLookBack': self.lookback_days,
            'ComponentTypes':       component_types
        } for idx, pat in bedded_patients.iterrows()]
        return self.make_requests(resource, payloads, 'POST')
