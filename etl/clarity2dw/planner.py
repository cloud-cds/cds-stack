# import asyncio
import asyncio
import logging
import time
import os
import json
import functools
import asyncpg

from etl.core.engine import Engine
from etl.core.task import Task
from etl.core.plan import Plan
from etl.clarity2dw.extractor import Extractor
from etl.load.pipelines.derive_main import get_derive_seq

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
      'nprocs': int(os.environ['nprocs']) if 'nprocs' in os.environ else 2,
    },
    'min_tsp': os.environ['min_tsp'] if 'min_tsp' in os.environ else None
  },
  'fillin': {
    'recalculate_popmean': False,
    'vacuum': True,
  },
  'derive': {
    'parallel': True,
    'fid': None,
    'mode': None,
    'num_derive_groups': int(os.environ['num_derive_groups']) if 'num_derive_groups' in os.environ else 2,
    'vacuum_temp_table': bool(os.environ['vacuum_temp_table']) if 'vacuum_temp_table' in os.environ else False
  },
  'offline_criteria_processing': {
    'load_cdm_to_criteria_meas':True,
    'calculate_historical_criteria':False
  },
  'engine': {
    'name': 'engine-c2dw',
    'nprocs': int(os.environ['nprocs']) if 'nprocs' in os.environ else 2,
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
  def main(self):
    self.generate_plan()
    self.start_engine()

  def set_db_config(self, config):
    if 'db_name' in config:
      db_config['db_name'] = config['db_name']
    if 'db_host' in config:
      db_config['db_host'] = config['db_host']

  def __init__(self, job, config=None):
    self.job = job
    if config:
      self.set_db_config(config)
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
    self.gen_offline_criteria_plan()
    self.log.info("plan is ready")
    self.print_plan()
    return self.plan

  def print_plan(self):
    self.log.info("TASK TODO:\n" + str(self.plan))

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
    transform_tasks = [task.name for task in self.plan.tasks if task.name.startswith('transform_task_')]
    self.plan.add(Task('fillin', deps=transform_tasks, coro=self.extractor.run_fillin))
    self.plan.add(Task('vacuum', deps=['fillin'], coro=self.extractor.vacuum_analyze_dataset))


  def gen_derive_plan(self):
    num_derive_groups = self.job.get('derive').get('num_derive_groups', 0)
    parallel = self.job.get('derive').get('parallel')
    vacuum_temp_table = self.job.get('derive').get('vacuum_temp_table', False)
    self.extractor.derive_feature_addr = get_derive_feature_addr(db_config, self.extractor.dataset_id, num_derive_groups)
    self.log.info("derive_feature_addr: {}".format(self.extractor.derive_feature_addr))
    if num_derive_groups:
      self.plan.add(Task('derive_init', deps=['vacuum'], coro=self.extractor.derive_init))
    if parallel:
      for task in get_derive_tasks(db_config, self.extractor.dataset_id, num_derive_groups > 0):
        self.plan.add(Task(task['name'], deps=task['dependencies'], coro=self.extractor.run_derive, args=[task['fid']]))
    else:
      self.plan.add(Task('derive', deps=['vacuum'], coro=self.extractor.run_derive))
    if num_derive_groups:
      if vacuum_temp_table:
        self.gen_vacuum_temp_table_plan()
        vacuum_temp_table_tasks = [task.name for task in self.plan.tasks if task.name.startswith('vacuum_temp_table')]
        self.plan.add(Task('derive_join', deps=vacuum_temp_table_tasks, coro=self.extractor.derive_join))
      else:
        derive_tasks = [task.name for task in self.plan.tasks if task.name.startswith('derive')]
        self.plan.add(Task('derive_join', deps=derive_tasks, coro=self.extractor.derive_join))

  def gen_vacuum_temp_table_plan(self):
    temp_table_feature_mapping = {}
    for fid in self.extractor.derive_feature_addr:
      category = self.extractor.derive_feature_addr[fid]['category']
      temp_table = self.extractor.derive_feature_addr[fid]['twf_table_temp']
      if category == 'TWF':
        if temp_table in temp_table_feature_mapping:
          temp_table_feature_mapping[temp_table].append(fid)
        else:
          temp_table_feature_mapping[temp_table] = [fid]
    for temp_table in temp_table_feature_mapping:
      self.plan.add(Task('vacuum_temp_table-{}'.format(temp_table), deps=['derive_{}'.format(fid) for fid in temp_table_feature_mapping[temp_table]], coro=self.extractor.run_vacuum, args=[temp_table]))

  def gen_offline_criteria_plan(self):
    num_derive_groups = self.job.get('derive').get('num_derive_groups', 0)
    if num_derive_groups:
      deps = ['derive_join']
    else:
      deps = ['derive']
    self.plan.add(
      Task('offline_criteria', deps=deps, coro=self.extractor.offline_criteria_processing)
      )

  def start_engine(self):
    self.log.info("start job in the engine")
    self.engine = Engine(self.plan, **self.job['engine'])
    loop = asyncio.new_event_loop()
    loop.run_until_complete(self.engine.run())
    self.engine.shutdown()
    loop.close()
    self.log.info("job completed")


def get_derive_tasks(config, dataset_id, is_grouped):
  async def _run_get_derive_tasks(config):
    conn = await asyncpg.connect(database=config['db_name'], \
                                 user=config['db_user'], \
                                 password=config['db_pass'], \
                                 host=config['db_host'], \
                                 port=config['db_port'])
    derive_features = await conn.fetch('''
        SELECT fid, derive_func_input from cdm_feature
        where not is_measured and not is_deprecated %s
    ''' % ('and dataset_id = {}'.format(dataset_id) if dataset_id is not None else ''))
    sql = "select * from cdm_feature %s" % ('where dataset_id = {}'.format(dataset_id) \
            if dataset_id is not None else '')
    cdm_feature = await conn.fetch(sql)
    cdm_feature_dict = {f['fid']:f for f in cdm_feature}
    await conn.close()
    return [derive_features, cdm_feature_dict]

  loop = asyncio.new_event_loop()
  derive_features, cdm_feature_dict = loop.run_until_complete(_run_get_derive_tasks(config))
  loop.close()

  derive_tasks = []
  for feature in derive_features:
    fid = feature['fid']
    inputs =[fid.strip() for fid in feature['derive_func_input'].split(',')]
    dependencies = ['derive_{}'.format(fid) for fid in inputs if not cdm_feature_dict[fid]['is_measured']]
    if len(dependencies) == 0:
      dependencies.append('fillin' if not is_grouped else 'derive_init')
    name = 'derive_{}'.format(fid)
    derive_tasks.append(
      {
        'name': name,
        'dependencies': dependencies,
        'fid': fid
      }
    )
  return derive_tasks

def get_derive_feature_addr(config, dataset_id, num_derive_groups, twf_table='cdm_twf'):
  async def _get_derive_features(config):
    conn = await asyncpg.connect(database=config['db_name'], \
                                 user=config['db_user'],     \
                                 password=config['db_pass'], \
                                 host=config['db_host'],     \
                                 port=config['db_port'])
    sql = "select * from cdm_feature where not is_measured and not is_deprecated %s" %\
              ('and dataset_id = {}'.format(dataset_id) if dataset_id is not None else '')
    derive_features = await conn.fetch(sql)
    await conn.close()
    return derive_features

  def partition(lst, n):
    division = len(lst) / n
    return [lst[round(division) * i:round(division) * (i + 1)] for i in range(n)]

  loop = asyncio.new_event_loop()
  derive_features = loop.run_until_complete(_get_derive_features(config))
  loop.close()
  # get derive_features order based on dependencies
  derive_feature_dict = {
   feature['fid']:feature for feature in derive_features
  }
  derive_feature_order = get_derive_seq(derive_feature_dict)
  derive_features = [derive_feature_dict[fid] for fid in derive_feature_order]
  if num_derive_groups > 1:
    derive_feature_groups = partition(derive_features, num_derive_groups)
  else:
    derive_feature_groups = [derive_features]
  print(derive_features)
  print(derive_feature_groups)
  derive_feature_addr = {}
  for i, group in enumerate(derive_feature_groups):
    for feature in group:
      fid = feature['fid']
      if num_derive_groups:
        twf_table_temp = "{}_temp_{}".format(twf_table, i) if feature['category'] == 'TWF' else None
      else:
        twf_table_temp = twf_table if feature['category'] == 'TWF' else None
      derive_feature_addr[fid] = {
        'category'      : feature['category'],
        'twf_table'     : twf_table if feature['category'] == 'TWF' else None,
        'twf_table_temp': twf_table_temp,
      }
  return derive_feature_addr


if __name__ == '__main__':
  planner = Planner(job_config)
  planner.generate_plan()
  planner.start_engine()
