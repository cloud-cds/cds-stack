# -*- coding: utf-8 -*-
# @Author: Andong Zhan
# @Date:   2018-06-05 17:01:53
# @Last Modified by:   Andong Zhan
# @Last Modified time: 2018-06-06 17:41:32
import asyncio
from aiohttp import ClientSession
from aiohttp import client_exceptions
from aiohttp import BasicAuth
import logging
import random
from time import sleep
import traceback
import pandas as pd

class EpicAppOrchardAPIConfig:
  def __init__(self, settings):
    self.baseurl = settings.baseurl
    self.auth = BasicAuth(login=settings.username, password=settings.password)
    self.lookback_hours = settings.lookback_hours
    # self.lookback_days = int(lookback_days) if lookback_days else int(int(lookback_hours)/24.0 + 1)
    # self.op_lookback_days = op_lookback_days
    # self.from_date = (dt.datetime.now() + dt.timedelta(days=1)).strftime('%Y-%m-%d')
    # tomorrow = dt.datetime.now() + dt.timedelta(days=1)
    # self.dateFrom = (tomorrow - dt.timedelta(days=self.lookback_days)).strftime('%Y-%m-%d')
    # self.dateTo = tomorrow.strftime('%Y-%m-%d')
    # self.headers = {
    #   'client_id': jhapi_id,
    #   'client_secret': jhapi_secret,
    #   'User-Agent': ''
    # }
    # self.cloudwatch_logger = Cloudwatch()



  def generate_request_settings(self, http_method, url, payloads=None, url_type=None):
    request_settings = []
    if isinstance(url, list):
      for u, payload in zip(url, payloads):
        setting = {
          'method': http_method,
          'url': u
        }
        if payload is not None:
          key = 'params' if http_method == 'GET' else 'json'
          setting[key] = payload
        request_settings.append(setting)
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

  async def make_requests(self, ctxt, endpoint, payloads, http_method='GET', url_type=None, server_type='internal'):
    # Define variables
    server = self.baseurl
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
      async with ClientSession(auth=self.auth, loop=loop) as session:
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

    # # Push number of requests to cloudwatch
    # logging.info("Made {} requests".format(sum(x[1] for x in result)))
    # self.cloudwatch_logger.push(
    #   dimension_name = 'ETL',
    #   metric_name    = 'requests_made_push',
    #   value          = sum(x[1] for x in result),
    #   unit           = 'Count'
    # )
    # if isinstance(endpoint, list):
    #   labels = ['push_' + e.replace('/', '_') + '_' + http_method for e in endpoint]
    #   for x, label in zip(result, labels):
    #     self.cloudwatch_logger.push_many(
    #       dimension_name  = 'ETL',
    #       metric_names    = ['{}_success_push'.format(label), '{}_error_push'.format(label), 'jh_api_request_success_push', 'jh_api_request_error_push'],
    #       metric_values   = [x[2], x[3], x[2], x[3]],
    #       metric_units    = ['Count','Count','Count','Count']
    #     )
    # else:
    #   label = 'push_' + endpoint.replace('/', '_') + '_' + http_method
    #   self.cloudwatch_logger.push_many(
    #     dimension_name  = 'ETL',
    #     metric_names    = ['{}_success_push'.format(label), '{}_error_push'.format(label), 'jh_api_request_success_push', 'jh_api_request_error_push'],
    #     metric_values   = [sum(x[2] for x in result), sum(x[3] for x in result), sum(x[2] for x in result), sum(x[3] for x in result)],
    #     metric_units    = ['Count','Count','Count','Count']
    #   )
    # Return responses
    return [x[0] for x in result]

  async def get_patient_location(self, ctxt, pat_usr_df):
    resource = '/api/epic/2012/access/Patient/GETPATIENTLOCATION/Location'
    payloads = pat_usr_df.apply(lambda row:
    {
      "PatientID": row["PatientID"],
      "PatientIDType": row["PatientIDType"],
      "UserID": row["UserID"],
      "UserIDType": row["UserIDType"]
    }, axis=1).tolist()
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    logging.debug("get_patient_location results: {}".format(responses))
    return responses
    # dfs = [pd.DataFrame(r) for r in responses]
    # print(dfs)
    # return dfs

  async def get_active_problems(self, ctxt, pat_usr_df, ExcludeNonHospitalProblems='false'):
    resource = '/api/epic/2011/Clinical/Patient/GETACTIVEPROBLEMS/activeProblems?ExcludeNonHospitalProblems={}'.format(ExcludeNonHospitalProblems)
    payloads = pat_usr_df.apply(lambda row:
    {
      "PatientID"     : row["PatientID"],
      "UserID"        : row["UserID"],
      "ContactID"     : row["ContactID"],
    }, axis=1).tolist()
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    logging.debug("get_active_problems results: {}".format(responses))
    return responses

  async def get_medical_history(self, ctxt, pat_usr_df):
    resources = pat_usr_df.apply(lambda row:
      '/api/epic/2010/Clinical/Patient/GETMEDICALHISTORY/MedicalHistory?PatientID={PatientID}&PatientIDType={PatientIDType}'.format(PatientID=row["PatientID"], PatientIDType=row["PatientIDType"]), axis=1).tolist()
    responses = await self.make_requests(ctxt, resources, [], 'GET')
    logging.debug("get_medical_history results: {}".format(responses))
    return responses

  async def get_patient_list(self, ctxt, pat_usr_df):
    # TODO: found out the right URL
    resource = '/api/epic/2017/Clinical/Patient/GETPATIENTLIST/PatientList'
    payloads = pat_usr_df.apply(lambda row:
    {
      "UserID": row["UserID"],
      "UserIDType": row["UserIDType"]
    }, axis=1).tolist()
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    logging.debug("get_patient_list results: {}".format(responses))
    return responses

  async def get_patient_demographics(self, ctxt, pat_usr_df):
    resource = '/api/epic/2017/Common/Patient/GetPatientDemographics/Patient/Demographics'
    payloads = pat_usr_df.apply(lambda row:
    {
      "PatientID": row["PatientID"],
      "PatientIDType": row["PatientIDType"],
      "UserID": row["UserID"],
      "UserIDType": row["UserIDType"]
    }, axis=1).tolist()
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    logging.debug("get_patient_demographics results: {}".format(responses))
    return responses

  async def get_flowsheet_rows(self, ctxt, input):
    resource = '/api/epic/2014/Clinical/Patient/GETFLOWSHEETROWS/FlowsheetRows'
    payloads = input.apply(lambda row:
    {
      "PatientID"       : row["PatientID"],
      "PatientIDType"   : row["PatientIDType"],
      "ContactID"       : row["ContactID"],
      "ContactIDType"   : row["ContactIDType"],
      "LookbackHours"   : row["LookbackHours"] if "LookbackHours" in row else self.lookback_hours,
      "FlowsheetRowIDs" : row["FlowsheetRowIDs"]
    }, axis=1).tolist()
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    logging.debug("get_flowsheet_rows results: {}".format(responses))
    return responses

  async def get_medication_administration_history(self, ctxt, input):
    resource = '/api/epic/2014/Clinical/Patient/GETMEDICATIONADMINISTRATIONHISTORY/MedicationAdministration'
    payloads = input.apply(lambda row:
    {
      "PatientID"       : row["PatientID"],
      "PatientIDType"   : row["PatientIDType"],
      "ContactID"       : row["ContactID"],
      "ContactIDType"   : row["ContactIDType"],
      "UserID": row["UserID"],
      "UserIDType": row["UserIDType"],
      "OrderIDs" : row["OrderIDs"]
    }, axis=1).tolist()
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    logging.debug("get_flowsheet_rows results: {}".format(responses))
    return responses


  async def get_current_medications(self, ctxt, input):
    resource = '/api/epic/2013/Clinical/Utility/GetCurrentMedications/CurrentMedications'
    payloads = input.apply(lambda row:
    {
      "PatientID"       : row["PatientID"],
      "PatientIDType"   : row["PatientIDType"],
      "ProfileView"     : row["ProfileView"],
      "NumberDaysToIncludeDiscontinuedAndEndedOrders" : row["NumberDaysToIncludeDiscontinuedAndEndedOrders"]
    }, axis=1).tolist()
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    logging.debug("get_current_medications results: {}".format(responses))
    return responses

  async def get_test_results(self, ctxt, pat_usr_df):
    resource = '/api/epic/2015/PatientAccess/Patient/GetTestResults/GetTestResults'
    payloads = pat_usr_df.apply(lambda row:
    {
      "PatientID"       : row["PatientID"],
      "PatientIDType"   : row["PatientIDType"],
    }, axis=1).tolist()
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    logging.debug("get_test_results results: {}".format(responses))
    return responses

  async def get_test_result_details(self, ctxt, pat_usr_df):
    resource = '/api/epic/2014/PatientAccess/Patient/GetTestResultDetails/TestResult/Details'
    payloads = pat_usr_df.apply(lambda row:
    {
      "PatientID"           : row["PatientID"],
      "PatientIDType"       : row["PatientIDType"],
      "MyChartAccountID"    : row["MyChartAccountID"],
      "MyChartAccountIDType": row["MyChartAccountIDType"],
      "TestResultID"        : row["TestResultID"],
      "TestResultIDType"    : row["TestResultIDType"]
    }, axis=1).tolist()
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    logging.debug("get_test_result_details results: {}".format(responses))
    return responses

  async def get_patient_result_components(self, ctxt, input):
    resource = '/urn:Epic-com:Results.2014.Services.Utility.GetPatientResultComponents'
    payloads = input.apply(lambda row:
    {
      "PatientID"       : row["PatientID"],
      "PatientIDType"   : row["PatientIDType"],
      "UserID"          : row["UserID"],
      "UserIDType"      : row["UserIDType"],
      "MaxNumberOfResults"   : row["MaxNumberOfResults"],
      "NumberDaysToLookBack"   : row["NumberDaysToLookBack"] if "NumberDaysToLookBack" in row else self.lookback_hours,
      "ComponentTypes"  : row["ComponentTypes"]
    }, axis=1).tolist()
    responses = await self.make_requests(ctxt, resource, payloads, 'POST')
    logging.debug("get_patient_result_components results: {}".format(responses))
    return responses