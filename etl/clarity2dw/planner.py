import asyncio
import logging
import time
from etl.core.engine import Engine
import uvloop
import os
import json
from etl.clarity2dw.extractor import Extractor
import functools

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
  # 'derive': {
  #   'fid': None,
  # },
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
    self.log.info("plan is ready")
    return self.plan

  def init_plan(self):
    self.plan = {
      'extract_init': ([],
        {'config': db_config,
         'coro': self.extractor.extract_init}),
    }

  def gen_transform_plan(self):
    self.plan.update({
                                'populate_patients': (['extract_init'],
                                  {'config': db_config,
                                   'coro': self.extractor.populate_patients}),
                                'transform_init': (['populate_patients'],
                                  {
                                  'config': db_config,
                                  'coro': self.extractor.transform_init
                                  })})
    for i, transform_task in enumerate(self.extractor.get_transform_tasks()):
      self.plan.update({'transform_task_{}'.format(i): (['transform_init'],
                                  {
                                  'config': db_config,
                                  'coro': self.extractor.run_transform_task,
                                  'args': [transform_task]
                                  })})

  def gen_fillin_plan(self):
    transform_tasks = [task for task in self.plan if task.startswith('transform_task_')]
    self.plan.update({'fillin': (transform_tasks,
                                {
                                  'config': db_config,
                                  'coro': self.extractor.run_fillin,
                                })})

  def get_derive_plan(self):
    self.plan.update(
        {'derive': (['fillin'],
          {
            'config': db_config,
            'coro': self.extractor.run_derive,
          })}
      )

  def start_engine(self):
    self.log.info("start job in the engine")
    self.job['engine']['tasks'] = self.plan
    self.engine = Engine(**self.job['engine'])
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