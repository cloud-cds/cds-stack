from etl.clarity2dw.engine import Engine as EngineC2dw
from etl.epic2op.engine import Engine as EngineEpic2op
import os
from etl.clarity2dw.engine import job_test_c2dw, CONF
import asyncio
import asyncpg
from compare_cdm import TableComparator
from cdm_feature import cdm_twf_field as cdm_twf
import datetime as dt
import copy
import subprocess
from collections import OrderedDict
import sys

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
  'config': {
    'dataset_id': 1,
    'debug': True,
    'db_name': 'test_c2dw',
    # 'db_host': 'dev.opsdx.io',
    'conf': CONF,
  },
}

epic2op_vs_c2dw = [
  {
    'name': 'test_epic2op',
    'engine': EngineEpic2op(db_name='test_epic2op'),
    'pipeline': {
      # 'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
      # 'populate_db': True,
    },
  },
  {
    'name': 'test_c2dw',
    'engine': EngineC2dw,
    'job': job_c2dw_1,
    'pipeline': {
      # # 'load_clarity': {'folder': '~/clarity-db-staging/2017-04-06/'},
      # 'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
      # 'copy_pat_enc': True,
      # 'populate_db': True,
    },
    'db_compare': {
      'srcdid': None,
      'srcmid': None,
      'dstdid': 1,
      'dstmid': 1,
      'cmp_remote_server': 'test_epic2op',
      'counts': False,
      'date': '2017-04-04',
      'dst_tsp_shift': '4 hours',
      'feature_set': 'online',
    }
  }
]

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
  'config': {
    'dataset_id': 1,
    'debug': True,
    'db_name': 'daily_test_c2dw',
    'conf': CONF,
  },
}

daily_compare = [
  {
    'name': 'daily_test_epic2op',
    'engine': EngineEpic2op(db_name='daily_test_epic2op'),
    'pipeline': {
      'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
      'populate_db': True,
    },
  },
  {
    'name': 'daily_test_c2dw',
    'engine': EngineC2dw,
    'job': job_c2dw_daily,
    'pipeline': {
      # TODO: load the latest clarity db staging files
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
      'cmp_remote_server': 'daily_test_epic2op',
      'counts': False,
      'dst_tsp_shift': '4 hours',
      'feature_set': 'online',
    }
  }
]

##########################################################
# regression test for offline ETL (c2dw)
# compare c2dw with c2dw_a (archived version)
##########################################################

job_c2dw_a = copy.deepcopy(job_test_c2dw)
job_c2dw_a['config']['db_name'] = 'test_c2dw_a'
job_c2dw_2 = copy.deepcopy(job_test_c2dw)
job_c2dw_2['config']['db_name'] = 'test_c2dw'
job_c2dw_2['reset_dataset']['remove_pat_enc'] = False
c2dw_a_vs_c2dw = [
  {
    'name': 'test_c2dw_a',
    'engine': Restore(db_name='test_c2dw_a', file='~/clarity-db-staging/c2dw_a/2017-04-05.sql'),
    'pipeline': {
      # 'populate_db': True,
    }
  },
  # {
  #   'name': 'test_c2dw_a',
  #   'engine': EngineC2dw,
  #   'job': job_c2dw_a,
  #   'pipeline': {
  #     # 'load_clarity': {'folder': '~/clarity-db-staging/2017-04-05/'},
  #     # 'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
  #     # 'populate_db': True,
  #   },
  # },
  {
    'name': 'test_c2dw',
    'engine': EngineC2dw,
    'job': job_c2dw_2,
    'pipeline': {
      'load_clarity': {'folder': '~/clarity-db-staging/2017-04-05/'},
      'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
      'copy_pat_enc': True,
      'populate_db': True,
    },
    'db_compare': {
      'srcdid': None,
      'srcmid': None,
      'dstdid': 1,
      'dstmid': 1,
      'cmp_remote_server': 'test_c2dw_a',
      'counts': False,
      'date': '2017-04-03',
      'feature_set': 'online'
    }
  }
]
##########################################################



class DBCompareTest():
  def __init__(self, db_pair):
    self.db_pair = db_pair
    self.passed = False

  def _init_(self):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run_init())

  async def run_init(self):
    self.epic2op_pool = await asyncpg.create_pool(
      database = self.epic2op.config.db_name,
      user     = self.epic2op.config.db_user,
      password = self.epic2op.config.db_pass,
      host     = self.epic2op.config.db_host,
      port     = self.epic2op.config.db_port
    )

  def run(self):
    self.db_setup(self.db_pair[0])
    self.db_setup(self.db_pair[1])

  def db_setup(self, db_config):
    if db_config.get('pipeline', False):
      pipeline = db_config['pipeline']
      if pipeline.get('load_clarity', False):
        self.load_clarity(pipeline['load_clarity'], db_config['name'])
      if pipeline.get('clean_db', False):
        self.clean_db(db_config)
      print(pipeline.get('copy_pat_enc', False))
      if pipeline.get('copy_pat_enc', False):
        print(pipeline.get('copy_pat_enc', False))
        self.copy_pat_enc(db_config)
      if pipeline.get('populate_db', False):
        self.populate_db(db_config)
    if db_config.get('db_compare', False):
      self.db_compare(db_config)

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

  def clean_db(self, db_config):
    print("clean_db: %s" % db_config['name'])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run_clean_db(db_config))

  async def run_clean_db(self, db_config):
    if 'job' in db_config:
      job = db_config['job']
      # job['db_name'] = 'test_c2dw_a'
      engine = db_config['engine'](job)
    else:
      engine = db_config['engine']
    await engine.init()
    rm_data = [
      "delete from cdm_s;",
      "delete from cdm_t;",
      "delete from cdm_twf;",
      "delete from criteria_meas;",
      "select drop_tables_pattern('workspace', 'job_etl_');",
    ]
    rm_pats = "delete from pat_enc;"
    reset_seq = "select setval('pat_enc_enc_id_seq', 1);"
    async with engine.pool.acquire() as conn:
      if 'rm_data' in db_config['pipeline']['clean_db']:
        for query in rm_data:
          pglog = await conn.execute(query)
          print(pglog)
      if 'rm_pats' in db_config['pipeline']['clean_db']:
        pglog = await conn.execute(rm_pats)
        print(pglog)
      if 'reset_seq' in db_config['pipeline']['clean_db']:
        pglog = await conn.execute(reset_seq)
        print(pglog)

  def populate_db(self, db_config):
    print("populate_db: %s" % db_config['name'])
    if 'job' in db_config:
      job = db_config['job']
      engine = db_config['engine'](job)
    else:
      engine = db_config['engine']
    engine.main()

  def copy_pat_enc(self, db_config):
    print("copy_pat_enc")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run_copy_pat_enc(db_config))

  async def run_copy_pat_enc(self, db_config):
    job = db_config['job']
    engine = db_config['engine'](job)
    await engine.init()
    pool = engine.pool
    sql = '''
    insert into pat_enc (dataset_id, enc_id, pat_id, visit_id)
    (
      select * from dblink('%s', $OPDB$
            select 1, enc_id, pat_id, visit_id from pat_enc
          $OPDB$) as pe (dataset_id int, enc_id int, pat_id text, visit_id text)
    );
    ''' % self.db_pair[0]['name']
    print(sql)
    async with pool.acquire() as conn:
      result = await conn.execute(sql)
      print(result)

  def db_compare(self, db_config):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run_db_compare(db_config))

  async def run_db_compare(self, db_config):
    print("db_compare %s vs %s" % (self.db_pair[0]['name'], self.db_pair[1]['name']))
    job = db_config['job']
    engine = db_config['engine'](job)
    await engine.init()
    dbpool = engine.pool
    args = db_config['db_compare']
    src_dataset_id = args['srcdid']
    src_model_id   = args['srcmid']
    dst_dataset_id = args['dstdid']
    dst_model_id   = args['dstmid']
    src_server = args['cmp_remote_server']
    counts = args['counts']
    dst_tsp_shift = args['dst_tsp_shift'] if 'dst_tsp_shift' in args else None
    online = True if 'feature_set' in args else False
    if 'date' in args:
      date = args['date']
    else:
      date = (dt.datetime.now() - dt.timedelta(days=3)).strftime('%Y-%m-%d')
    tsp_range = " tsp > '%(date)s 10:00:00 utc'::timestamptz and tsp < '%(date)s 20:00:00 utc'::timestamptz" % {'date': date}

    select_enc_ids_to_compare = '''
    select pat_enc.enc_id from pat_enc inner join cdm_s on cdm_s.enc_id = pat_enc.enc_id
          inner join dblink('%s', $OPDB$ select enc_id from cdm_s where cdm_s.fid = 'age' $OPDB$) as remote (enc_id int) on remote.enc_id = pat_enc.enc_id
          where cdm_s.fid = 'age'
          order by pat_enc.enc_id
          limit 50
    ''' % src_server
    print(select_enc_ids_to_compare)
    async with dbpool.acquire() as conn:
      enc_ids = await conn.fetch(select_enc_ids_to_compare)
    enc_id_range = 'enc_id in (%s)' % ','.join([str(e['enc_id']) for e in enc_ids])
    # enc_id_range = 'enc_id < 31'

    cdm_s_online_features = ['age','gender',
    'heart_failure_hist', 'chronic_pulmonary_hist', 'emphysema_hist',
    'heart_arrhythmias_prob',
    'esrd_prob' 'esrd_diag', 'chronic_bronchitis_diag', 'heart_arrhythmias_diag', 'heart_failure_diag']
    cdm_t_online_features = ['urine_output', 'dobutamine_dose',
    'epinephrine_dose',
    'levophed_infusion_dose',
    'dopamine_dose','vent','fluids_intake',]
    cdm_twf_online_features = ['rass', 'resp_rate',  'nbp_sys', 'gcs', 'temperature', 'amylase',    'weight', 'pao2', 'nbp_dias', 'hemoglobin',  'wbc', 'bilirubin', 'lipase', 'sodium', 'creatinine',  'spo2',  'heart_rate', 'paco2', 'bun', 'platelets', 'fio2']

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
      # 'weight': ['(round(value::numeric, 4))', '='],
      # 'nbp_mean': ['(round(value::numeric, 4))', '='],
      # 'mapm': ['(round(value::numeric, 4))', '='],
      # 'pao2_to_fio2': ['(round(value, 4))', '='],
      'temperature': ['(round(temperature::numeric, 0)) as temperature', '='],
    }
    for field in cdm_twf_fields:
      if field[0] in cdm_twf_dependent_expr_map:
        field[0] = cdm_twf_dependent_expr_map[field[0]][0]

    cdm_dependent_fields = {
      'value': ('fid', cdm_dependent_expr_map)
    }

    cdm_twf_dependent_fields = {
      'value': ('fid', cdm_twf_dependent_expr_map)
    }

    cdm_s_fields1 = [
      ['enc_id'                             , 'enc_id',     'integer'     ],
      ['fid'                                , 'fid',        'varchar(50)' ],
      ['value'           , 'text',        ],
      # ['confidence'                         , 'confidence', 'integer'     ],
    ]
    cdm_s_query1 = (cdm_s_fields1, (cdm_s_range + ' and ' if cdm_s_range is not None else '') + enc_id_range, 'fid, enc_id', cdm_s_dependent_fields)

    cdm_t_fields1 = [
      ['enc_id'                             , 'enc_id',     'integer'     ],
      ['tsp'                                , 'tsp',        'timestamptz' ],
      ['fid'                                , 'fid',        'varchar(50)' ],
      ["(value::json)#>>'{dose}'"           , 'dose',       'text'        ],
      ["(value::json)#>>'{action}'"         , 'action',     'text'        ],
      ["(value::json)#>>'{order_tsp}'"      , 'order_tsp',  'text'        ],
      # ['confidence'                         , 'confidence', 'integer'     ],
    ]
    cdm_t_query1 = (cdm_t_fields1, 'fid like \'%_dose\' and ' + (cdm_t_range + ' and ' if cdm_t_range is not None else '') + enc_id_range, 'fid, enc_id, tsp', cdm_t_dose_dependent_fields)

    cdm_t_fields2 = [
      ['enc_id'          ,'enc_id'          , 'integer',     ],
      ['tsp'             ,'tsp'             , 'timestamptz', ],
      ['fid'             ,'fid'             , 'varchar(50)', ],
      ['value'           ,'value'           , 'text',        ],
      # ['confidence'      ,'confidence'      , 'integer',     ],
    ]

    cdm_t_query2 = (cdm_t_fields2, 'fid !~ \'dose|inhosp|bacterial_culture|_proc|culture_order|pneumonia_sepsis|uro_sepsis|biliary_sepsis|intra_abdominal_sepsis\' and ' + (cdm_t_range + ' and ' if cdm_t_range is not None else '')+ enc_id_range, 'fid, enc_id, tsp', cdm_t_dependent_fields)

    cdm_t_fields3 = [
      ['enc_id'                             , 'enc_id',     'integer'     ],
      ['tsp'                                , 'tsp',        'timestamptz' ],
      ['fid'                                , 'fid',        'varchar(50)' ],
      ["(value::json)#>>'{diagname}'"           , 'diagname',       'text'        ],
      ["(value::json)#>>'{ischronic}'"         , 'ischronic',     'text'        ],
      ["""(value::json)#>>'{"present on admission"}'"""      , 'present_on_admission',  'text'        ],
      # ['confidence'                         , 'confidence', 'integer'     ],
    ]
    cdm_t_query3 = (cdm_t_fields3, 'fid like \'%_inhosp|pneumonia_sepsis|uro_sepsis|biliary_sepsis|intra_abdominal_sepsis\' and ' + (cdm_t_range + ' and ' if cdm_t_range is not None else '') + enc_id_range, 'fid, enc_id, tsp', cdm_dependent_fields)

    cdm_t_fields4 = [
      ['enc_id'                             , 'enc_id',     'integer'     ],
      ['tsp'                                , 'tsp',        'timestamptz' ],
      ['fid'                                , 'fid',        'varchar(50)' ],
      ["(value::json)#>>'{status}'"           , 'status',       'text'        ],
      ["(value::json)#>>'{name}'"         , 'name',     'text'        ],
      # ['confidence'                         , 'confidence', 'integer'     ],
    ]
    cdm_t_query4 = (cdm_t_fields4, 'fid like \'bacterial_culture|_proc|culture_order\' and ' + (cdm_t_range + ' and ' if cdm_t_range is not None else '') + enc_id_range, 'fid, enc_id, tsp', cdm_dependent_fields)

    cdm_twf_field_index = [
      ['enc_id'                             , 'enc_id',     'integer'     ],
      ['tsp'                                , 'tsp',        'timestamptz' ],
    ]

    confidence_range = '%s < 8'

    cdm_twf_queries = [(cdm_twf_field_index + [cdm_twf_fields[2*i]], enc_id_range + ' and ' + tsp_range + ' and ' + (confidence_range % cdm_twf_fields[2*i+1][0]) + ' and ' + after_admission_constraint.format(cdm='cdm_twf'), 'enc_id, tsp', cdm_twf_dependent_fields) for i in range(len(cdm_twf_fields)//2)]

    tables_to_compare = {
      # 'datalink'                 : ('dataset', []),
      'cdm_function'             : ('dataset', []),
      'cdm_feature'              : ('dataset', []),
      # 'datalink_feature_mapping' : ('dataset', []),
      'pat_enc'                  : ('dataset', [pat_enc_query]),
      'cdm_g'                    : ('both'   , []),
      'cdm_s'                    : ('dataset', [cdm_s_query1]),
      # 'cdm_m'                    : ('dataset', []),
      'cdm_t'                    : ('dataset', [cdm_t_query1, cdm_t_query2, cdm_t_query3, cdm_t_query4]),
      # 'criteria_meas'            : ('dataset', []),
      # 'criteria'                 : ('dataset', []),
      # 'criteria_events'          : ('dataset', []),
      # 'criteria_log'             : ('dataset', []),
      # 'criteria_meas_archive'    : ('dataset', []),
      # 'criteria_archive'         : ('dataset', []),
      # 'criteria_default'         : ('dataset', []),
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
      if queries:
        for field_map, predicate, sort_field, dependent_fields in queries:
          c = TableComparator(src_server,
                              src_dataset_id, src_model_id,
                              dst_dataset_id, dst_model_id,
                              tbl, src_pred=predicate,
                              field_map=field_map, dependent_fields=dependent_fields,
                              version_extension=version_type,
                              as_count_result=counts, sort_field=sort_field, dst_tsp_shift=dst_tsp_shift)
          records = await c.run(dbpool)
          results.append(records)
      else:
        c = TableComparator(src_server,
                            src_dataset_id, src_model_id,
                            dst_dataset_id, dst_model_id,
                            tbl, version_extension=version_type, as_count_result=counts, dst_tsp_shift=dst_tsp_shift)
        records = await c.run(dbpool)
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
    return self.passed



if __name__ == '__main__':
  if len(sys.argv) == 2:
    db_pair_name = sys.argv[1]
    if db_pair_name == 'daily_compare':
      db_pair = daily_compare
    elif db_pair_name == 'epic2op_vs_c2dw':
      db_pair = epic2op_vs_c2dw
    elif db_pair_name == 'c2dw_a_vs_c2dw':
      db_pair = c2dw_a_vs_c2dw
    else:
      print('unkown db_pair: {}'.format(db_pair_name))
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
  exit(test.passed)