from etl.clarity2dw.planner import Planner as PlannerC2DW
from etl.clarity2dw.planner import CONF as CONF
from etl.epic2op.engine import Epic2Op as EngineEpic2op
import os
import asyncio
import asyncpg
from compare_cdm import TableComparator
from cdm_feature import cdm_twf_field as cdm_twf
import datetime as dt
import copy
import subprocess
from collections import OrderedDict
import pandas as pd
import sys
import logging

class Restore():
  def __init__(self, db_name, file):
    self.db_name = db_name
    self.file = file

  def main(self):
    print("restore test database")
    db_host = os.environ["db_host"]
    db_user = os.environ["db_user"]
    cmd = ["pg_restore", "--clean", "-h", db_host, "-U", db_user, "-d", self.db_name, "-p", "5432", "-v", self.file]
    os.system(" ".join(cmd))
    # print(subprocess.check_output(cmd, stderr=subprocess.STDOUT))

  def get_db_config(self):
    return {
      'db_name': os.environ['db_name'],
      'db_user': os.environ['db_user'],
      'db_pass': os.environ['db_password'],
      'db_host': os.environ['db_host'],
      'db_port': os.environ['db_port']
    }


#########################################################
# compare online ETL (epic2op) with offline ETL (c2dw)
#########################################################
job_c2dw_1 = {
  'reset_dataset': {
    'remove_pat_enc': False,
    'remove_data': True,
    'start_enc_id': '(select max(enc_id) from pat_enc)'
  },
  'transform': {
    'populate_patients': True,
    'populate_measured_features': {
      'plan': False,
      # 'fid': 'age',
    },
  },
  'fillin': {
    'recalculate_popmean': False,
  },
  'derive':
  {
    'fid': None
  },
  'offline_criteria_processing': {
    'load_cdm_to_criteria_meas': True,
    # 'calculate_historical_criteria':False
  },
}
# ome/ubuntu/clarity-db-staging/epic2op
epic2op_vs_c2dw = [
  {
    'name': 'test_epic2op',
    'engine': EngineEpic2op(db_name='test_epic2op'),
    # 'engine': Restore(db_name='test_epic2op',file='/home/ubuntu/clarity-db-staging/epic2op/2017-04-06.sql'),
    'pipeline': {
      'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
      'populate_db': True,
    },
  },
  {
    'name': 'test_c2dw',
    'engine': PlannerC2DW,
    'job': job_c2dw_1,
    'pipeline': {
      # 'load_clarity': {'folder': '~/clarity-db-staging/2017-04-12/'},
      'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
      'copy_pat_enc': True,
      # 'populate_db': True,
    },
    'db_compare': {
      'srcdid': None,
      'srcmid': None,
      'dstdid': 1,
      'dstmid': 1,
      'cmp_remote_server': 'test_epic2op',
      'counts': False,
      'date': '2017-04-10',
      'dst_tsp_shift': '4 hours',
      'feature_set': 'online',
    }
  }
]

############################################################
## daily compare: request latest data sources and run ETLs to compare
## TODO: enable clarity ETL automatically
############################################################

job_c2dw_daily = {
  'reset_dataset': {
    'remove_pat_enc': False,
    'remove_data': True,
    'start_enc_id': '(select max(enc_id) from pat_enc)'
  },
  'transform': {
    'populate_patients': True,
    'populate_measured_features': {
      'plan': False,
    },
  },
  'fillin': {
    'recalculate_popmean': False,
  },
  'derive':
  {
    'fid': None
  },
}

daily_compare = [
  {
    'name': 'daily_test_epic2op',
    'engine': EngineEpic2op(db_name='daily_test_epic2op'),
    'pipeline': {
      'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
      # 'populate_db': True,
    },
  },
  {
    'name': 'daily_test_c2dw',
    'engine': PlannerC2DW,
    'job': job_c2dw_daily,
    'pipeline': {
      # TODO: load the latest clarity db staging files
      # 'load_clarity': {'folder': '~/clarity-db-staging/2017-04-06/'},
      'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
      'copy_pat_enc': True,
      'populate_db': True,
    },
    'db_compare': {
      'srcdid': None,
      'srcmid': None,
      'dstdid': 1,
      'dstmid': 1,
      'cmp_remote_server': 'daily_test_epic2op',
      'counts': False,
      'dst_tsp_shift': '4 hours',
      'feature_set': 'online',
    }
  }
]

# ---------------------------------------------------------------------------------------------------
# daily_compare_light:
# daily_test_epic2op and daily_test_c2dw databases have to be created first
# the CI will run this when a new docker image is created
daily_c2dw_light_config = {
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
      'nprocs': 2,
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
    'num_derive_groups': 2,
  },
  'offline_criteria_processing': {
    'load_cdm_to_criteria_meas':True,
    'calculate_historical_criteria':False
  },
  'engine': {
    'name': 'engine-c2dw',
    'nprocs': 2,
    'loglevel': logging.DEBUG
  },
  'planner': {
    'name': 'planner-c2dw',
    'loglevel': logging.DEBUG,
  },
  'extractor': {
    'name': 'extractor-c2dw',
    'dataset_id': 1,
    'loglevel': logging.DEBUG,
    'conf': CONF,
  },
}

daily_compare_light = [
  {
    'name': 'daily_epic2op_light',
    'engine': EngineEpic2op(db_name='daily_epic2op_light', max_num_pats=20),
    'pipeline': {
      # 'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
      'populate_db': True,
    },
  },
  {
    'name': 'daily_c2dw_light',
    'engine': PlannerC2DW(daily_c2dw_light_config, {'db_name': 'daily_c2dw_light'}),
    'pipeline': {
      # 'load_clarity': {'folder': '/data/opsdx/clarity-db-staging/2017-04-06/'},
      # 'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
      # 'copy_pat_enc': True,
      'populate_db': True,
    },
    # 'db_compare': {
    #   'srcdid': None,
    #   'srcmid': None,
    #   'dstdid': 1,
    #   'dstmid': 1,
    #   'cmp_remote_server': 'daily_epic2op_light',
    #   'counts': False,
    #   'dst_tsp_shift': '4 hours',
    #   'feature_set': 'online',
    # }
  }
]
# ---------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------
# op2dw_compare:
# compare data between opsdx_dev vs opsdx_dw after op2dw ETL
# the CI will run this when a new docker image is created
job_opsdx_dev_dw = {
  # 'reset_dataset': {
  #   'remove_pat_enc': False,
  #   'remove_data': True,
  #   'start_enc_id': '(select max(enc_id) from pat_enc)'
  # },
  # 'transform': {
  #   'populate_patients': {
  #     'max_num_pats': 20,
  #   },
  #   'populate_measured_features': {
  #     'plan': False,
  #   },
  # },
  # 'fillin': {
  #   'recalculate_popmean': False,
  # },
  # 'derive':
  # {
  #   'fid': None
  # },
}

op2dw_compare = [
  {
    'name': 'opsdx_dev',
    'engine': EngineEpic2op(db_name='opsdx_dev'),
    # 'pipeline': {
    #   'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
    #   'populate_db': True,
    # },
  },
  {
    'name': 'opsdx_dev_dw',
    'engine': PlannerC2DW,
    'job': job_opsdx_dev_dw,
    # 'pipeline': {
    #   # 'load_clarity': {'folder': '~/clarity-db-staging/2017-04-06/'},
    #   'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
    #   'copy_pat_enc': True,
    #   'populate_db': True,
    # },
    'db_compare': {
      'srcdid': None,
      'srcmid': None,
      'dstdid': 1,
      'dstmid': 1,
      'cmp_remote_server': 'opsdx_dev_srv',
      'counts': False,
      # 'dst_tsp_shift': '4 hours',
      'feature_set': 'online',
      'max_num_pats': 500,
    }
  }
]
# ---------------------------------------------------------------------------------------------------


############################################################
## archive compare: load archived data sources and run ETL to compare
############################################################
job_c2dw_archive = {
  'reset_dataset': {
    'remove_pat_enc': False,
    'remove_data': True,
    'start_enc_id': '(select max(enc_id) from pat_enc)'
  },
  'transform': {
    'populate_patients': True,
    'populate_measured_features': {
      'plan': False,
    },
  },
  'fillin': {
    'recalculate_popmean': False,
  },
  'derive':
  {
    'fid': None
  },
  'offline_criteria_processing': {
    'load_cdm_to_criteria_meas': True,
    'calculate_historical_criteria': False
  },
}

archive_compare = [
  {
    'name': 'archive_epic2op',
    'engine': Restore(db_name='archive_epic2op', file='~/clarity-db-staging/epic2op/2017-04-06.sql'),
    'pipeline': {
      'populate_db': True
    }
  },
  {
    'name': 'archive_c2dw',
    'engine': PlannerC2DW,
    'job': job_c2dw_archive,
    'pipeline': {
      'load_clarity': {'folder': '~/clarity-db-staging/2017-04-06/'},
      'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
      'copy_pat_enc': True,
      'populate_db': True,
    },
    'db_compare': {
      'srcdid': None,
      'srcmid': None,
      'dstdid': 1,
      'dstmid': 1,
      'cmp_remote_server': 'archive_epic2op',
      'counts': False,
      'date': '2017-04-04',
      'dst_tsp_shift': '4 hours',
      'feature_set': 'online',
    }
  }
]

##########################################################
# regression test for offline ETL (c2dw)
# compare c2dw with c2dw_a (archived version)
##########################################################

# c2dw_a_vs_c2dw = [
#   {
#     'name': 'test_c2dw_a',
#     'engine': Restore(db_name='test_c2dw_a', file='~/clarity-db-staging/c2dw_a/2017-04-05.sql'),
#     'pipeline': {
#       # 'populate_db': True,
#     }
#   },
#   # {
#   #   'name': 'test_c2dw_a',
#   #   'engine': PlannerC2DW,
#   #   'job': job_c2dw_a,
#   #   'pipeline': {
#   #     # 'load_clarity': {'folder': '~/clarity-db-staging/2017-04-05/'},
#   #     # 'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
#   #     # 'populate_db': True,
#   #   },
#   # },
#   {
#     'name': 'test_c2dw',
#     'engine': PlannerC2DW,
#     'job': job_c2dw_2,
#     'pipeline': {
#       'load_clarity': {'folder': '~/clarity-db-staging/2017-04-05/'},
#       'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
#       'copy_pat_enc': True,
#       'populate_db': True,
#     },
#     'db_compare': {
#       'srcdid': None,
#       'srcmid': None,
#       'dstdid': 1,
#       'dstmid': 1,
#       'cmp_remote_server': 'test_c2dw_a',
#       'counts': False,
#       'date': '2017-04-03',
#       'feature_set': 'online'
#     }
#   }
# ]
##########################################################



class DBCompareTest():
  def __init__(self, db_pair):
    self.db_pair = db_pair
    self.passed = False
    self.db_pool = None

  def run(self):
    self.db_setup(self.db_pair[0])
    self.db_setup(self.db_pair[1])

  async def get_db_pool(self, loop, engine):
    if self.db_pool:
      return self.db_pool

    db_config = engine.get_db_config()
    self.db_pool = await asyncpg.create_pool( \
                      database = db_config['db_name'], \
                      user     = db_config['db_user'], \
                      password = db_config['db_pass'], \
                      host     = db_config['db_host'], \
                      port     = db_config['db_port'], \
                      loop     = loop)

    return self.db_pool


  def db_setup(self, db_test_config):
    if db_test_config.get('pipeline', False):
      pipeline = db_test_config['pipeline']
      if pipeline.get('load_clarity', False):
        self.load_clarity(pipeline['load_clarity'], db_test_config['name'])
      if pipeline.get('clean_db', False):
        self.clean_db(db_test_config)
      print(pipeline.get('copy_pat_enc', False))
      if pipeline.get('copy_pat_enc', False):
        print(pipeline.get('copy_pat_enc', False))
        self.copy_pat_enc(db_test_config)
      if pipeline.get('populate_db', False):
        self.populate_db(db_test_config)

    if db_test_config.get('test_override', False):
      self.passed = db_test_config['test_override']
    elif db_test_config.get('db_compare', False):
      self.db_compare(db_test_config)

  def load_clarity(self, settings, db_name):
    with open('load_clarity_template.sql', 'r') as f:
      sql = f.read()
    sql_str = sql.format(folder=settings['folder'])
    with open('load_clarity.sql', 'w') as f:
      f.write(sql_str)
    from subprocess import call
    db_host = os.environ['db_host']
    db_user = os.environ['db_user']
    call(["psql", "-h", db_host, "-U", db_user, "-d", db_name, "-p", "5432", "-f", 'load_clarity.sql'])

  def clean_db(self, db_test_config):
    print("clean_db: %s" % db_test_config['name'])
    loop = asyncio.new_event_loop()
    loop.run_until_complete(self.run_clean_db(loop, db_test_config))
    loop.close()

  async def run_clean_db(self, loop, db_test_config):
    if 'job' in db_test_config:
      job = db_test_config['job']
      # job['db_name'] = 'test_c2dw_a'
      engine = db_test_config['engine'](job)
    else:
      engine = db_test_config['engine']

    db_pool = await self.get_db_pool(loop, engine)

    rm_data = [
      "delete from trews;",
      "delete from cdm_s;",
      "delete from cdm_t;",
      "delete from cdm_twf;",
      "delete from criteria_meas;",
      "select drop_tables_pattern('workspace', 'job_etl_');",
    ]
    rm_pats = "delete from pat_enc;"
    reset_seq = "select setval('pat_enc_enc_id_seq', 1);"
    async with db_pool.acquire() as conn:
      if 'rm_data' in db_test_config['pipeline']['clean_db']:
        for query in rm_data:
          pglog = await conn.execute(query)
          print(pglog)
      if 'rm_pats' in db_test_config['pipeline']['clean_db']:
        pglog = await conn.execute(rm_pats)
        print(pglog)
      if 'reset_seq' in db_test_config['pipeline']['clean_db']:
        pglog = await conn.execute(reset_seq)
        print(pglog)

  def populate_db(self, db_test_config):
    print("populate_db: %s" % db_test_config['name'])
    if 'job' in db_test_config:
      job = db_test_config['job']
      engine = db_test_config['engine'](job)
    else:
      engine = db_test_config['engine']
    engine.main()

  def copy_pat_enc(self, db_test_config):
    print("copy_pat_enc")
    loop = asyncio.new_event_loop()
    loop.run_until_complete(self.run_copy_pat_enc(loop, db_test_config))
    loop.close()

  async def run_copy_pat_enc(self, loop, db_test_config):
    engine = db_test_config['engine'](db_test_config['job']) \
                if 'job' in db_test_config else db_test_config['engine']

    db_pool = await self.get_db_pool(loop, engine)
    sql = '''
    insert into pat_enc (dataset_id, enc_id, pat_id, visit_id)
    (
      select * from dblink('%s', $OPDB$
            select 1, enc_id, pat_id, visit_id from pat_enc
          $OPDB$) as pe (dataset_id int, enc_id int, pat_id text, visit_id text)
    );
    ''' % self.db_pair[0]['name']
    print(sql)
    async with db_pool.acquire() as conn:
      result = await conn.execute(sql)
      print(result)

  def db_compare(self, db_test_config):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(self.run_db_compare(loop, db_test_config))
    loop.close()

  async def check_tsp_range(self, tsp_range, src_server, db_pool):
    sql = '''
    select (t1.min, t1.max) OVERLAPS (t2.min, t2.max) from (select min(tsp), max(tsp) from cdm_twf) t1 cross join
        dblink('%s', $OPDB$ select min(tsp), max(tsp) from cdm_twf $OPDB$) as t2 (min timestamptz
    , max timestamptz)
    ''' % src_server
    async with db_pool.acquire() as conn:
      overlapping = await conn.fetch(sql)
      print(overlapping)
    return bool(overlapping[0][0])

  async def run_db_compare(self, loop, db_test_config):
    print("db_compare %s vs %s" % (self.db_pair[0]['name'], self.db_pair[1]['name']))
    engine = db_test_config['engine'](db_test_config['job']) \
                if 'job' in db_test_config else db_test_config['engine']
    db_pool = await self.get_db_pool(loop, engine)

    args = db_test_config['db_compare']
    src_dataset_id = args['srcdid']
    src_model_id   = args['srcmid']
    dst_dataset_id = args['dstdid']
    dst_model_id   = args['dstmid']
    src_server = args['cmp_remote_server']
    counts = args['counts']
    pat_limit = 'limit {}'.format(args['max_num_pats']) if 'max_num_pats' in args else ''
    dst_tsp_shift = args['dst_tsp_shift'] if 'dst_tsp_shift' in args else None
    online = True if 'feature_set' in args else False
    if 'date' in args:
      date = args['date']
    else:
      async with db_pool.acquire() as conn:
        date = await conn.fetchval("select max(tsp) from cdm_twf")
        if date is None:
          date = (dt.datetime.now() - dt.timedelta(days=2)).strftime('%Y-%m-%d')
        else:
          date = (date - dt.timedelta(days=2)).strftime('%Y-%m-%d')

    tsp_range = " tsp > '%(date)s 10:00:00 utc'::timestamptz and tsp < '%(date)s 20:00:00 utc'::timestamptz" % {'date': date}

    if not await self.check_tsp_range(tsp_range, src_server, db_pool):
      print("two databases are not overlapping in time range")
      self.passed = True
      return self.passed
    select_enc_ids_to_compare = '''
    SELECT distinct pat_enc.enc_id from pat_enc inner join cdm_s on cdm_s.enc_id = pat_enc.enc_id
          inner join dblink('%s', $OPDB$ select distinct enc_id from cdm_s where cdm_s.fid = 'age' $OPDB$) as remote (enc_id int) on remote.enc_id = pat_enc.enc_id
          where cdm_s.fid = 'age'
          order by pat_enc.enc_id
          %s
    ''' % (src_server, pat_limit)
    print(select_enc_ids_to_compare)
    async with db_pool.acquire() as conn:
      enc_ids = await conn.fetch(select_enc_ids_to_compare)
    enc_id_range = 'enc_id in (%s)' % ','.join([str(e['enc_id']) for e in enc_ids])
    # enc_id_range = 'enc_id < 31'

    select_pat_ids_to_compare = '''
    select pat_enc.pat_id from pat_enc inner join cdm_s on cdm_s.enc_id = pat_enc.enc_id
          inner join dblink('%s', $OPDB$ select enc_id from cdm_s where cdm_s.fid = 'age' $OPDB$) as remote (enc_id int) on remote.enc_id = pat_enc.enc_id
          where cdm_s.fid = 'age'
          order by pat_enc.pat_id
          limit 50
    ''' % src_server
    print(select_pat_ids_to_compare)
    async with db_pool.acquire() as conn:
      pat_ids = await conn.fetch(select_pat_ids_to_compare)
    if pat_ids is None or len(pat_ids) == 0:
      print("No common enc_id found in two databases")
      self.passed = False
      return self.passed
    pat_id_range = 'pat_id in (%s)' % ','.join([ '\'' + str(e['pat_id'])+ '\'' for e in pat_ids])
    print(pat_id_range)
    # enc_id_range = 'enc_id < 31'

    cdm_s_online_features = ['age','gender']
    cdm_t_online_features = []
    cdm_twf_online_features = []

    pat_enc_fields = [
      ['enc_id'                             ,     'integer'     ],
      ['pat_id'                                ,       'varchar(50)' ],
      ['visit_id'           , 'text',        ],
    ]
    after_admission_constraint = " tsp >= coalesce((select min(ct.tsp) from cdm_t ct where ct.enc_id = {cdm}.enc_id and ct.fid = 'care_unit'), tsp) "

    pat_enc_query = (pat_enc_fields, enc_id_range, 'enc_id', None)
    if online:
      cdm_s_range = 'fid ~ \'%s\'' % '|'.join(cdm_s_online_features)
      cdm_t_range = 'fid ~ \'%s\'' % '|'.join(cdm_t_online_features)
      cdm_t_range += ' and ' + tsp_range + ' and ' + after_admission_constraint.format(cdm='cdm_t')
      cdm_twf_fields = [row for row in cdm_twf if (row[0][:-2] if row[0].endswith('_c') else row[0]) in cdm_twf_online_features]
    else:
      cdm_s_range = None
      cdm_t_range = None
      cdm_twf_fields = cdm_twf
    cdm_dependent_expr_map = {
      }

    cdm_s_dependent_expr_map = {
      'admit_weight': ['(round(value::numeric, 1))', '='],
      }

    cdm_t_dependent_expr_map = {
      'any_pressor': ['value::boolean::text', '='],
      'any_inotrope': ['value::boolean::text', '='],
      'catheter': ['value::boolean::text', '='],
      'suspicion_of_infection': ['value::boolean::text', '='],
      'fluids_intake': ['(round(value::numeric, 2))', '='],
      }

    cdm_t_dose_dependent_expr_map = {
      '_dose': ['(round(((value::json)#>>\'{dose}\')::numeric, 2))', '~'],
      }

    cdm_t_dose_dependent_fields = {
      'dose': ('fid', cdm_t_dose_dependent_expr_map)
    }

    cdm_t_dependent_fields = {
      'value': ('fid', cdm_t_dependent_expr_map)
    }

    cdm_s_dependent_fields = {
      'value': ('fid', cdm_s_dependent_expr_map)
    }

    cdm_twf_dependent_expr_map = {
      # 'shock_idx': ['(round(value::numeric, 4))', '='],
      # 'weight': ['(round(weight::numeric, 4))', '='],
      # 'nbp_mean': ['(round(value::numeric, 4))', '='],
      # 'mapm': ['(round(value::numeric, 4))', '='],
      # 'pao2_to_fio2': ['(round(value, 4))', '='],
      'temperature': ['(round(temperature::numeric, 0)) as temperature', '='],
    }
    for field in cdm_twf_fields:
      if field[0] in cdm_twf_dependent_expr_map:
        field[0] = cdm_twf_dependent_expr_map[field[0]][0]


    cdm_twf_dependent_fields = {
      'value': ('fid', cdm_twf_dependent_expr_map)
    }


    cdm_twf_field_index = [
      ['enc_id'                             , 'enc_id',     'integer'     ],
      ['tsp'                                , 'tsp',        'timestamptz' ],
    ]

    confidence_range = '%s < 8'

    cdm_twf_queries = [(cdm_twf_field_index + [cdm_twf_fields[2*i]], enc_id_range + ' and ' + tsp_range + ' and ' + (confidence_range % cdm_twf_fields[2*i+1][0]) + ' and ' + after_admission_constraint.format(cdm='cdm_twf'), 'enc_id, tsp', cdm_twf_dependent_fields) for i in range(len(cdm_twf_fields)//2)]

    tables_to_compare = {  # touple 1, extra field, dataset_id, model_id, both, touple 2 customize comparison s
      # 'datalink'                 : ('dataset', []),
      # 'cdm_function'             : ('dataset',   []),
      # 'cdm_feature'              : ('dataset',   []),
      # 'datalink_feature_mapping' : ('dataset', []),
      'pat_enc'                  : ('dataset',   [pat_enc_query]),
      # 'cdm_g'                    : ('dataset'   ,   []),
      # 'cdm_s'                    : ('dataset', [cdm_s_query1]),
      # 'cdm_m'                    : ('dataset', []),
      # 'cdm_t'                    : ('dataset', [cdm_t_query1, cdm_t_query2, cdm_t_query3, cdm_t_query4]),
      # 'notifications'            : ('dataset', []),
      # 'parameters'               : ('dataset', []),
      # 'trews_scaler'             : ('model'  , []),
      # 'trews_feature_weights'    : ('model'  , []),
      # 'trews_parameters'         : ('model'  , []),
      'cdm_twf'                  : ('dataset', cdm_twf_queries),
      # 'trews'                    : ('dataset', []),
      # 'pat_status'               : ('dataset', []),
      # 'deterioration_feedback'   : ('dataset', []),
      # 'feedback_log'             : ('dataset', []),
    }



    results = []
    for tbl, version_type_and_queries in tables_to_compare.items():
      version_type = version_type_and_queries[0]
      queries = version_type_and_queries[1]
      print(tbl)
      print(version_type_and_queries)
      if queries:
        for field_map, predicate, sort_field, dependent_fields in queries:
          c = TableComparator(src_server,
                              src_dataset_id, src_model_id,
                              dst_dataset_id, dst_model_id,
                              tbl, src_pred=predicate,
                              field_map=field_map, dependent_fields=dependent_fields,
                              version_extension=version_type,
                              as_count_result=counts, sort_field=sort_field, dst_tsp_shift=dst_tsp_shift)
          records = await c.run(db_pool)
          results.append(records)
      else: #which is the remote defined here, can all be none
        c = TableComparator(src_server,
                            src_dataset_id, src_model_id,
                            dst_dataset_id, dst_model_id,
                            tbl, version_extension=version_type, as_count_result=counts, dst_tsp_shift=dst_tsp_shift)
        records = await c.run(db_pool)
        results.append(records)
    print("======== result ========")
    self.passed = True
    groups = {}
    for result in results:
      if 'rows' in result and len(result['rows']) > 0:
        for row in result['rows']:
          groups.setdefault(row['fid'] if 'fid' in row else list(OrderedDict(row).keys())[-1], []).append(dict(row))
          # print(OrderedDict(row))

    for fid in groups:
      print('------------- {fid} ---------------'.format(fid=fid))
      for row in groups[fid]:
        print(row)
      print("")
      self.passed = False


    #==================================
    # Summarize Differences
    #==================================

    print("FID's with differences")

    for fid in groups:
      print(fid)
    print("\n")


    print("Difference Summary")
    sum_dict_list = [];
    for fid in groups:
      this_group = groups[fid]
      mr = [row['missing_remotely'] for row in this_group]
      this_dict = {'fid':fid,'NumDiff':len(this_group),'NumMissRemote':sum(mr)}
      sum_dict_list.append(this_dict)

    sum_df = pd.DataFrame(sum_dict_list)
    print(sum_df)
    print("\n")

    return self.passed



if __name__ == '__main__':
  if len(sys.argv) == 2:
    db_pair_name = sys.argv[1]
    if db_pair_name == 'daily_compare':
      db_pair = daily_compare
    elif db_pair_name == 'daily_compare_light':
      db_pair = daily_compare_light
    elif db_pair_name == 'archive_compare':
      db_pair = archive_compare
    elif db_pair_name == 'epic2op_vs_c2dw':
      db_pair = epic2op_vs_c2dw
    elif db_pair_name == 'c2dw_a_vs_c2dw':
      db_pair = c2dw_a_vs_c2dw
    elif db_pair_name == 'op2dw_compare':
      db_pair = op2dw_compare
    else:
      print('unknown db_pair: {}'.format(db_pair_name))
      exit(0)
  else:
    print('please input db_pair_name')
    exit(0)
  test = DBCompareTest(db_pair)
  test.run()
  if test.passed:
    print("test succeed")
  else:
    print("test failed")
  exit(not test.passed)
