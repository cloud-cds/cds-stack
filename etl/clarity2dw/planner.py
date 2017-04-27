import asyncio
import logging
import time
from etl.core.engine import Engine
import uvloop
import os
import json
from etl.clarity2dw.new_extractor import Extractor

CONF = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf')

# a complete job definition
JOB = {
  'reset_dataset': {
    'remove_pat_enc': True,
    'remove_data': True,
    'start_enc_id': 1
  },
  'transform': {
    'populate_patients': True,
    'populate_measured_features': {
      'plan': False,
      # 'fid': ['fluids_intake'],
    },
  },
  'fillin': {
    'recalculate_popmean': False,
  },
  'derive': {
    'fid': None,
  },
  'offline_criteria_processing': {
    'load_cdm_to_criteria_meas':True,
    'calculate_historical_criteria':False
  },
  'engine': {
    'name': 'engine-c2dw',
    'nprocs': int(os.environ['nprocs']),
  },
  'planner': {
    'name': 'planner-c2dw',
    'loglevel': logging.DEBUG,
  },
  'extractor': {
    'name': 'extractor-c2dw',
    'dataset_id': int(os.environ['dataset_id']),
    'loglevel': logging.DEBUG,
    'db_name': 'test_c2dw_parallel', #'opsdx_dev_dw',
    'db_host': 'db.dev.opsdx.io',
    'conf': CONF,
  },
}
PLANNER_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'

class Planner():
  def __init__(self, job):
    self.job = job
    self.extractor = Extractor(**self.job['extractor'])
    # Configure planner logging.
    self.name = self.job['planner']['name']
    self.log = logging.getLogger(self.name)
    self.log.setLevel(self.job['planner'].get('loglevel', logging.INFO))

    sh = logging.StreamHandler()
    formatter = logging.Formatter(PLANNER_LOG_FMT)
    sh.setFormatter(formatter)
    self.log.addHandler(sh)
    self.log.propagate = False

  def generate_plan(self):
    self.log.info("planning now")
    self.plan = {
      'clean_dataset': ([], self.extractor.example),
      'populate_patients': (['clean_dataset'], self.extractor.example)
    }
    self.log.info("plan is ready")
    self.log.info("current plan is {}".format(self.plan))
    return self.plan

  def start_engine(self):
    self.log.info("start job in the engine")
    self.job['engine']['tasks'] = self.plan
    self.log.info('engine conf: {}'.format(self.job['engine']))
    self.engine = Engine(**self.job['engine'])
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(self.engine.run())
    loop.close()
    self.log.info("job completed")


if __name__ == '__main__':
  planner = Planner(JOB)
  planner.generate_plan()
  planner.start_engine()