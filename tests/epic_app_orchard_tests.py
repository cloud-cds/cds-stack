# -*- coding: utf-8 -*-
# @Author: Andong Zhan
# @Date:   2018-06-05 17:02:47
# @Last Modified by:   Andong Zhan
# @Last Modified time: 2018-06-06 16:45:52
import etl.io_config.core as core
from etl.core.task import Task
from etl.core.plan import Plan
from etl.core.engine import Engine
from etl.io_config.epic_app_orchard import EpicAppOrchardAPIConfig
from collections import namedtuple
import pandas as pd
import logging
import asyncio
import asyncpg, aiohttp


PatientInput = pd.DataFrame.from_dict({
    "PatientID"     :["E3232"],
    "PatientIDType" :["EPI"],
    "UserID"        :["1"],
    "UserIDType"    :["External"]
  })

ProblemInput = pd.DataFrame.from_dict({
    "PatientID"     : [{"ID":"E2734","Type":"EPI"}],
    "UserID"        : [{"ID":"1","Type":"External"}],
    "ContactID"     : [{"ID":"1855","Type":"CSN"}]
  })

MHInput = pd.DataFrame.from_dict({
    "PatientID"     : ["Z4574"],
    "PatientIDType" : ["External"],
  })

FLTInput = pd.DataFrame.from_dict({
    "PatientID"       : ["E2731"],
    "PatientIDType"   : ["EPI"],
    "ContactID"       : ["58706"],
    "ContactIDType"   : ["DAT"],
    "LookbackHours"   : 72,
    "FlowsheetRowIDs" : [[{"ID": "1400000000", "Type": "EXTERNAL"},
                          {"ID": "1400000032", "Type": "EXTERNAL"}]]
  })

LabResultInput = pd.DataFrame.from_dict({
    "PatientID"     :["202500"],
    "PatientIDType" :["EHSMRN"]
  })

LabResultDetailsInput = pd.DataFrame.from_dict({
    "PatientID"            : ["E2734"],
    "PatientIDType"        : ["EPI"],
    "MyChartAccountID"     : ["19"],
    "MyChartAccountIDType" : ["Internal"],
    "TestResultID"         : ["795518"],
    "TestResultIDType"     : ["Internal"]
  })

LabResultComponentsInput = pd.DataFrame.from_dict({
    "PatientID"            : ["Z5336"],
    "PatientIDType"        : ["EXTERNAL"],
    "UserID"               : ["1"],
    "UserIDType"           : ["External"],
    "NumberDaysToLookBack" : ["2000"],
    "MaxNumberOfResults"   : ["200"],
    "ComponentTypes"       : [[{"Value": "1802008"}]]
  })

MARInput = pd.DataFrame.from_dict({
    "PatientID"       : ["E3385"],
    "PatientIDType"   : ["EPI"],
    "ContactID"       : ["59254"],
    "ContactIDType"   : ["DAT"],
    "UserID"          : ["1"],
    "UserIDType"      : ["External"],
    "OrderIDs"        : [[{"ID": "887752", "Type": "EXTERNAL"}]]
  })

MedInput = pd.DataFrame.from_dict({
    "PatientID"       : ["E2734"],
    "PatientIDType"   : ["EPI"],
    "UserID"          : ["1"],
    "UserIDType"      : ["External"],
    "ProfileView"     : ["3"], # 1 is for outpatient medication (including suspended medications). 2 is for inpatient medication. 3 is for both outpatient and inpatient medications (including suspended medications).
    "NumberDaysToIncludeDiscontinuedAndEndedOrders"        : 365
  })

def get_extraction_tasks(extractor):
  return [
    {
      'name': 'GetPatientLocation',
      'coro': extractor.get_patient_location,
      'args': [PatientInput],
    },
    {
      'name': 'GetPatientList',
      'coro': extractor.get_patient_list,
      'args': [PatientInput],
    },
    {
      'name': 'GetPatientDemographics',
      'coro': extractor.get_patient_demographics,
      'args': [PatientInput],
    },
    {
      'name': 'GetFlowsheetRows',
      'coro': extractor.get_flowsheet_rows,
      'args': [FLTInput],
    },
    {
      'name': 'GetTestResults',
      'coro': extractor.get_test_results,
      'args': [LabResultInput],
    },
    {
      'name': 'GetTestResultDetails',
      'coro': extractor.get_test_result_details,
      'args': [LabResultDetailsInput],
    },
    {
      'name': 'GetPatientResultComponents',
      'coro': extractor.get_patient_result_components,
      'args': [LabResultComponentsInput],
    },
    {
      'name': 'GetMedicationAdministrationHistory',
      'coro': extractor.get_medication_administration_history,
      'args': [MARInput],
    },
    {
      'name': 'GetCurrentMedications',
      'coro': extractor.get_current_medications,
      'args': [MedInput],
    },
    {
      'name': 'GetActiveProblemList',
      'coro': extractor.get_active_problems,
      'args': [ProblemInput, 'false'],
    },
    {
      'name': 'GetMedicalHistory',
      'coro': extractor.get_medical_history,
      'args': [MHInput],
    },
    ]


def main():
  logging.getLogger().setLevel(0)
  # Configuration
  config_dict = {
    'db_name': core.get_environment_var('db_name'),
    'db_user': core.get_environment_var('db_user'),
    'db_pass': core.get_environment_var('db_password'),
    'db_host': core.get_environment_var('db_host'),
    'db_port': core.get_environment_var('db_port'),
  }

  # Create jhapi_extractor
  Settings = namedtuple('Settings', ['baseurl',
                                     'username',
                                     'password',
                                     'lookback_hours'])
  settings = Settings(core.get_environment_var('epic_app_orchard_baseurl'),
                      core.get_environment_var('epic_app_orchard_username'),
                      core.get_environment_var('epic_app_orchard_password'),
                      int(core.get_environment_var('epic_app_orchard_lookback_hours', '24'))
  )
  extractor = EpicAppOrchardAPIConfig(settings)

  ########################
  # Build plan
  all_tasks = []
  all_tasks += get_extraction_tasks(extractor)
  ########################
  # Run plan
  plan = Plan(name="epic2op_plan", config=config_dict)
  for task_def in all_tasks:
    plan.add(Task(**task_def))

  engine = Engine(
    plan     = plan,
    name     = "epic2op_engine",
    nprocs   = 1,
    loglevel = logging.DEBUG,
    with_gc  = True,
    with_graph = True
  )
  loop = asyncio.new_event_loop()
  loop.run_until_complete(engine.run())
  loop.close()


if __name__ == '__main__':
  main()