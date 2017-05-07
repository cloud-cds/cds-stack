import asyncio
import logging
import time
import uvloop
import os
import json
import functools

from etl.core.engine import Engine
from etl.core.task import Task
from etl.core.plan import Plan
from etl.clarity2dw.extractor import Extractor

CONF = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf')

# a complete job definition
job_config = {
  'plan': False,
  'reset_dataset': {
    'remove_pat_enc': True,
    'remove_data': True,
    'start_enc_id': 1
  },
  'transform': {
    'populate_patients': {
      'limit': None
    },
    'populate_measured_features': {
      'fid': None,
      'nprocs': int(os.environ['nprocs']) if 'nprocs' in os.environ else 1,
    },
    'min_tsp': os.environ['min_tsp'] if 'min_tsp' in os.environ else None
  },
  'fillin': {
    'recalculate_popmean': False,
  },
  'derive': {
    'parallel': False,
    'fid': None,
    'mode': None,
  },
  'offline_criteria_processing': {
    'load_cdm_to_criteria_meas':True,
    'calculate_historical_criteria':False
  },
  'engine': {
    'name': 'engine-c2dw',
    'nprocs': int(os.environ['nprocs']) if 'nprocs' in os.environ else 1,
    'loglevel': logging.DEBUG
  },
  'planner': {
    'name': 'planner-c2dw',
    'loglevel': logging.DEBUG,
  },
  'extractor': {
    'name': 'extractor-c2dw',
    'dataset_id': int(os.environ['dataset_id']),
    'loglevel': logging.DEBUG,
    'conf': CONF,
  },
}

PLANNER_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'

db_config = {
  'db_name': os.environ['db_name'],
  'db_user': os.environ['db_user'],
  'db_pass': os.environ['db_password'],
  'db_host': os.environ['db_host'],
  'db_port': os.environ['db_port']
}


class Planner():
  def __init__(self, job):
    self.job = job
    self.extractor = Extractor(self.job)
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
    self.init_plan()
    self.gen_transform_plan()
    self.gen_fillin_plan()
    self.gen_derive_plan()
    self.log.info("plan is ready")
    return self.plan

  def init_plan(self):
    self.plan = Plan('plan-c2dw', db_config)
    self.plan.add(Task('extract_init', deps=[], coro=self.extractor.extract_init))

  def gen_transform_plan(self):
    self.plan.add(Task('populate_patients', deps=['extract_init'], coro=self.extractor.populate_patients))
    self.plan.add(Task('transform_init', deps=['populate_patients'], coro=self.extractor.transform_init))

    for i, transform_task in enumerate(self.extractor.get_transform_tasks()):
      self.plan.add(Task('transform_task_{}'.format(i), \
        deps=['transform_init'], coro=self.extractor.run_transform_task, args=[transform_task]))

  def gen_fillin_plan(self):
    transform_tasks = [task for task in self.plan if task.startswith('transform_task_')]
    self.plan.add(Task('fillin', deps=transform_tasks, coro=self.extractor.run_fillin))

  def gen_derive_plan(self):
    parallel = self.job.get('derive').get('parallel')
    if parallel:
      for task in self.extractor.get_derive_tasks(db_config):
        self.plan.add(Task(task['name'], deps=task['dependencies'], coro=self.extractor.run_derive, args=task['fid']))
    else:
      self.plan.add(Task('derive', deps=['fillin'], coro=self.extractor.run_derive))


  def start_engine(self):
    self.log.info("start job in the engine")
    self.engine = Engine(self.plan, **self.job['engine'])
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(self.engine.run())
    loop.close()
    self.log.info("job completed")


if __name__ == '__main__':
  planner = Planner(job_config)
  planner.generate_plan()
  planner.start_engine()
