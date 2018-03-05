import asyncio
import logging
import time
import os
import json
import functools
import asyncpg
import sys

from etl.core.engine import Engine
from etl.core.task import Task
from etl.core.plan import Plan
from etl.clarity2dw.extractor import Extractor
from etl.load.pipelines.derive_main import get_derive_seq, get_dependent_features

CONF = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf')

# a complete job definition
job_config = {
  'plan': False,
  'incremental': os.environ['incremental'] == 'True'\
                    if 'incremental' in os.environ else False,
  'clarity_workspace': os.environ['clarity_workspace'] \
        if 'clarity_workspace' in os.environ else 'public',
  'extract_init': False if 'extract_init' in os.environ \
    and os.environ['extract_init'] == 'False' else \
  {
    'remove_pat_enc': os.environ['remove_pat_enc'] == 'True' \
      if 'remove_pat_enc' in os.environ else True,
    'remove_data': os.environ['remove_data'] == 'True' \
      if 'remove_data' in os.environ else True,
    'start_enc_id': os.environ['start_enc_id'] \
      if 'start_enc_id' in os.environ else 1
  } ,
  'transform': False if 'transform' in os.environ \
    and os.environ['transform'] == 'False' else \
  {
    'populate_patients': False if 'populate_patients' in os.environ \
      and os.environ['populate_patients'] == 'False' else {
      'limit': None
    },
    'populate_measured_features': {
      'fid': os.environ['transform_fids'].split(';') \
        if 'transform_fids' in os.environ else None,
      'nprocs': int(os.environ['nprocs']) if 'nprocs' in os.environ else 2,
      'shuffle': True if 'transform_shuffle' in os.environ and os.environ['transform_shuffle'] == 'True' else None,
    },
    'min_tsp': os.environ['min_tsp'] if 'min_tsp' in os.environ else None,
    'feature_mapping': os.environ['feature_mapping'] if 'feature_mapping' in os.environ else 'feature_mapping.csv',
    'transform_mode': os.environ['transform_mode'] if 'transform_mode' in os.environ else 'async'
  },
  'fillin': False if 'fillin' in os.environ \
    and os.environ['fillin'] == 'False' else \
  {
    'recalculate_popmean': os.environ['recalculate_popmean'] == 'True'\
                            if 'recalculate_popmean' in os.environ else False,
    'vacuum_after_fillin': os.environ['vacuum_after_fillin'] == 'True'\
                            if 'vacuum_after_fillin' in os.environ else True,
  },
  'derive': False if 'derive' in os.environ \
    and os.environ['derive'] == 'False' else \
  {
    'parallel': True,
    # NOTE: not ready to specifies derive_fids
    'fid': os.environ['derive_fids'].split(';') \
      if 'derive_fids' in os.environ else None,
    'mode': os.environ['derive_mode'] if 'derive_mode' in os.environ else None,
    'num_derive_groups': int(os.environ['num_derive_groups']) \
        if 'num_derive_groups' in os.environ else 2,
    'vacuum_temp_table': os.environ['vacuum_temp_table'] == 'True' \
        if 'vacuum_temp_table' in os.environ else False,
    'partition_mode': int(os.environ['partition_mode']) \
        if 'partition_mode' in os.environ else 1,
  },
  'offline_criteria_processing': False if 'offline_criteria_processing' \
  in os.environ and os.environ['offline_criteria_processing'] == 'False' else \
  {
    'gen_label_and_report': True if 'gen_label_and_report' in os.environ and \
      os.environ['gen_label_and_report'] else False
  },
  'engine': {
    'name': 'engine-c2dw',
    'nprocs': int(os.environ['nprocs']) if 'nprocs' in os.environ else 2,
    'loglevel': logging.DEBUG,
    'with_graph': True
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

PLANNER_LOG_FMT = \
'%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'

class Planner():
  def __init__(self, job, config=None):
    self.job = job

    self.db_config = {
      'db_name': os.environ['db_name'],
      'db_user': os.environ['db_user'],
      'db_pass': os.environ['db_password'],
      'db_host': os.environ['db_host'],
      'db_port': os.environ['db_port']
    }

    if config:
      if 'db_name' in config:
        self.db_config['db_name'] = config['db_name']
      if 'db_host' in config:
        self.db_config['db_host'] = config['db_host']

    self.extractor = Extractor(self.job)
    self.name = self.job['planner']['name']

    # Configure planner logging.
    self.log = logging.getLogger(self.name)
    self.log.setLevel(self.job['planner'].get('loglevel', logging.INFO))
    sh = logging.StreamHandler()
    formatter = logging.Formatter(PLANNER_LOG_FMT)
    sh.setFormatter(formatter)
    self.log.addHandler(sh)
    self.log.propagate = False

  def get_db_config(self):
    return self.db_config

  def main(self):
    self.generate_plan()
    self.start_engine()

  def generate_plan(self):
    self.log.info("planning now")
    self.init_plan()
    self.gen_transform_plan()
    self.gen_fillin_plan()
    self.gen_derive_plan()
    self.gen_offline_criteria_plan()
    self.gen_postprocessing_plan()
    self.log.info("plan is ready")
    self.print_plan()
    return self.plan

  def print_plan(self):
    self.log.info("TASK TODO:\n" + str(self.plan))

  def init_plan(self):
    self.plan = Plan('plan-c2dw', self.db_config)
    if self.job.get('extract_init', False):
      self.plan.add(Task('extract_init', deps=[],
                         coro=self.extractor.extract_init))

  def gen_transform_plan(self):
    if self.job.get('transform', False):
      if self.job.get('transform').get('populate_patients', False):
        all_tasks = self.plan.get_all_task_names()
        self.plan.add(Task('populate_patients',
                           deps=['extract_init'] if 'extract_init' \
                            in all_tasks else all_tasks,
                           coro=self.extractor.populate_patients))
      if self.job.get('transform').get('populate_measured_features', False):
        all_tasks = self.plan.get_all_task_names()
        self.plan.add(Task('transform_init', deps=['populate_patients']\
                            if 'populate_patients' in all_tasks else all_tasks,
                           coro=self.extractor.transform_init))

        for i, transform_task in enumerate(self.extractor.get_transform_tasks()):
          self.plan.add(Task('transform_task_{}'.format(i), \
            deps=['transform_init'], coro=self.extractor.run_transform_task,
            args=[transform_task]))

  def gen_fillin_plan(self):
    if self.job.get('fillin', False):
      all_tasks = self.plan.get_all_task_names()
      transform_tasks = [name for name in all_tasks \
          if name.startswith('transform_task_')]
      self.plan.add(Task('fillin', deps=transform_tasks, \
            coro=self.extractor.run_fillin))
      self.plan.add(Task('vacuum', deps=['fillin'], \
            coro=self.extractor.vacuum_analyze_dataset))


  def gen_derive_plan(self):
    if self.job.get('derive', False):
      num_derive_groups = self.job.get('derive').get('num_derive_groups', 0)
      partition_mode = self.job.get('derive').get('partition_mode', 1)
      parallel = self.job.get('derive').get('parallel')
      vacuum_temp_table = self.job.get('derive').get('vacuum_temp_table', False)
      derive_features, cdm_feature_dict = get_derive_features(self.db_config, self.extractor.dataset_id, self.job)
      self.extractor.derive_feature_addr = get_derive_feature_addr(\
        self.db_config, self.extractor.dataset_id, num_derive_groups,
        partition_mode, 'cdm_twf', self.job, derive_features, cdm_feature_dict)
      self.log.info("derive_feature_addr: {}".format(\
          self.extractor.derive_feature_addr))
      all_tasks = self.plan.get_all_task_names()
      if num_derive_groups:
        self.plan.add(Task('derive_init', \
          deps=['vacuum'] if 'vacuum' in all_tasks else all_tasks, \
          coro=self.extractor.derive_init))
      if parallel:
        for task in get_derive_tasks(self.db_config, self.extractor.dataset_id, num_derive_groups > 0, derive_features, cdm_feature_dict):
          self.plan.add(Task(task['name'], deps=task['dependencies'], \
            coro=self.extractor.run_derive, args=[task['fid']]))
      else:
        self.plan.add(Task('derive', \
          deps=['vacuum'] if 'vacuum' in all_tasks else all_tasks,\
          coro=self.extractor.run_derive))
      if num_derive_groups:
        if vacuum_temp_table:
          self.gen_vacuum_temp_table_plan()
          vacuum_temp_table_tasks = [task.name for task in self.plan.tasks \
              if task.name.startswith('vacuum_temp_table')]
          self.plan.add(Task('derive_join', deps=vacuum_temp_table_tasks, \
            coro=self.extractor.derive_join))
        else:
          derive_tasks = [task.name for task in self.plan.tasks \
              if task.name.startswith('derive')]
          self.plan.add(Task('derive_join', deps=derive_tasks, \
              coro=self.extractor.derive_join))

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
      self.plan.add(Task('vacuum_temp_table-{}'.format(temp_table), \
        deps=['derive_{}'.format(fid) for fid in \
          temp_table_feature_mapping[temp_table]], \
          coro=self.extractor.run_vacuum, args=[temp_table]))

  def gen_offline_criteria_plan(self):
    all_dep_tasks = [t.name for t in self.plan.tasks if t.name.startswith('derive') or t.name.startswith('transform')]
    if self.job.get('offline_criteria_processing', False):
      self.plan.add(
        Task('offline_criteria', deps=all_dep_tasks, \
          coro=self.extractor.offline_criteria_processing)
        )

  def gen_postprocessing_plan(self):
    all_tasks = [t.name for t in self.plan.tasks]
    self.plan.add(
      Task('postprocessing', deps=['offline_criteria'] \
        if 'offline_criteria' in all_tasks else all_tasks, \
        coro=self.extractor.postprocessing))

  def start_engine(self):
    try:
      self.log.info("start job in the engine")
      self.engine = Engine(self.plan, **self.job['engine'])
      loop = asyncio.new_event_loop()
      loop.run_until_complete(self.engine.run())
      self.log.info("job completed")
    except Exception as e:
      self.log.exception(e)
      self.log.info("job failed")
    except KeyboardInterrupt:
      # quit
      self.log.error("keyboard interrupted!")
      self.engine.shutdown()
      loop.close()
      sys.exit()
    finally:
      self.engine.shutdown()
      loop.close()


def get_derive_tasks(config, dataset_id, is_grouped, derive_features, cdm_feature_dict):
  derive_tasks = []
  derive_feature_dict = {f['fid']:f for f in derive_features}
  for feature in derive_features:
    fid = feature['fid']
    inputs =[fid.strip() for fid in feature['derive_func_input'].split(',')]
    dependencies = ['derive_{}'.format(fid) for fid in inputs \
      if not cdm_feature_dict[fid]['is_measured'] and fid in derive_feature_dict]
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


def get_derive_features(config, dataset_id, job):
  async def _get_derive_features(config):
    conn = await asyncpg.connect(database=config['db_name'], \
                                 user=config['db_user'],     \
                                 password=config['db_pass'], \
                                 host=config['db_host'],     \
                                 port=config['db_port'])
    sql = """select * from cdm_feature
    where not is_measured and not is_deprecated %s""" %\
      ('and dataset_id = {}'.format(dataset_id) \
        if dataset_id is not None else '')
    derive_features = await conn.fetch(sql)
    sql = "select * from cdm_feature %s" % \
      ('where dataset_id = {}'.format(dataset_id) \
            if dataset_id is not None else '')
    cdm_feature = await conn.fetch(sql)
    cdm_feature_dict = {f['fid']:f for f in cdm_feature}
    await conn.close()
    return derive_features, cdm_feature_dict
  loop = asyncio.new_event_loop()
  derive_features, cdm_feature_dict = loop.run_until_complete(_get_derive_features(config))
  loop.close()
  # get derive_features order based on dependencies
  derive_feature_dict = {
   feature['fid']:feature for feature in derive_features
  }
  # specifies a subset of derive features if derive_fids exists
  derive_fids = job.get('derive').get('fid', None)
  if derive_fids:
    mode = job.get('derive').get('mode', None)
    if mode is None:
      derive_feature_dict = {f:derive_feature_dict[f] for f in derive_feature_dict if f in derive_fids}
      derive_feature_order = get_derive_seq(derive_feature_dict)
    if mode == 'dependent':
      derive_feature_order = get_dependent_features(derive_fids, cdm_feature_dict)
      for fid in derive_fids:
        if not cdm_feature_dict[fid]['is_measured']:
          derive_feature_order = [fid] + derive_feature_order
  else:
    derive_feature_order = get_derive_seq(derive_feature_dict)
  derive_features = [derive_feature_dict[fid] for fid in derive_feature_order]
  print("derive_features: {}".format(derive_features))
  return derive_features, cdm_feature_dict

def get_derive_feature_addr(config, dataset_id, num_derive_groups,
                            partition_mode, twf_table, job,
                            derive_features, cdm_feature_dict):

  def partition(lst, n, partition_mode):
    if partition_mode == 1:
      if n > len(lst):
        n = len(lst)
      division = round(len(lst) / n)
      res = [lst[i:i + division] for i in range(0, len(lst), division)]
      return [l for l in res if l]
    elif partition_mode == 2: # NOTE this is not efficient
      return [lst[i::n] for i in range(n)]
    else:
      raise Exception('Unknown partition mode {}'.format(partition_mode))

  workspace = job.get('clarity_workspace')
  if num_derive_groups > 1:
    derive_feature_groups = partition(derive_features, num_derive_groups,
                                      partition_mode)
  else:
    derive_feature_groups = [derive_features]
  print("derive_feature_groups: {}".format(derive_feature_groups))
  derive_feature_addr = {}
  for i, group in enumerate(derive_feature_groups):
    for feature in group:
      fid = feature['fid']
      if num_derive_groups:
        twf_table_temp = "{}.{}_temp_{}".format(workspace, twf_table, i) \
          if feature['category'] == 'TWF' else None
      else:
        twf_table_temp = twf_table if feature['category'] == 'TWF' else None
      derive_feature_addr[fid] = {
        'category'      : feature['category'],
        'twf_table'     : twf_table if feature['category'] == 'TWF' else None,
        'twf_table_temp': twf_table_temp,
      }
  # for feature in derive_features:
  #   func_inputs = feature['derive_func_input']
  #   for fid_input in [fi.strip() for fi in func_inputs.split(',')]:
  #     if not fid_input in derive_feature_addr:
  #       derive_feature_addr[fid_input] = {
  #         'category'        : cdm_feature_dict[fid_input]['category'],
  #         'twf_table'       : twf_table if feature['category'] == 'TWF' else None,
  #         'twf_table_temp'  : None
  #       }
  return derive_feature_addr


if __name__ == '__main__':
  planner = Planner(job_config)
  planner.generate_plan()
  planner.start_engine()
