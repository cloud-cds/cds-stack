from etl.clarity2dw.engine import Engine as EngineC2dw
from etl.epic2op.engine import Engine as EngineEpic2op
import os
from etl.clarity2dw.engine import job_test_c2dw
import asyncio
import asyncpg
from compare_cdm import TableComparator
from cdm_feature import cdm_twf_field
import datetime as dt

db_pair = [
  {
    'name': 'test_epic2op',
    'engine': EngineEpic2op(db_name='test_epic2op'),
    'pipeline': {
      'clean_db': ['rm_data'],
      'populate_db': {},
    },
  },
  {
    'name': 'test_c2dw',
    'engine': EngineC2dw(job_test_c2dw),
    'pipeline': {
      # 'clean_db': ['rm_data', 'rm_pats', 'reset_seq'],
      # 'copy_pat_enc': {},
      # 'populate_db': {},
    },
    'db_compare': {
      'srcdid': None,
      'srcmid': None,
      'dstdid': 1,
      'dstmid': 1,
      'cmp_remote_server': 'test_epic2op',
      'counts': False,
    }
  }
]





class DBCompareTest():
  def __init__(self, db_pair):
    self.db_pair = db_pair

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
    pipeline = db_config['pipeline']
    if 'clean_db' in pipeline:
      self.clean_db(db_config)
    if 'copy_pat_enc' in pipeline:
      self.copy_pat_enc(db_config)
    if 'populate_db' in pipeline:
      self.populate_db(db_config)
    if 'db_compare' in db_config:
      self.db_compare(db_config)

  def clean_db(self, db_config):
    print("clean_db: %s" % db_config['name'])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run_clean_db(db_config))

  async def run_clean_db(self, db_config):
    engine = db_config['engine']
    await engine.init()
    rm_data = """
    delete from cdm_s;
    delete from cdm_t;
    delete from cdm_twf;
    delete from criteria_meas;
    select drop_tables_pattern('workspace', 'job_etl_');
    """
    rm_pats = "delete from pat_enc;"
    reset_seq = "select setval('pat_enc_enc_id_seq', 1);"
    async with engine.pool.acquire() as conn:
      if 'rm_data' in db_config['pipeline']['clean_db']:
        await conn.execute(rm_data)
      if 'rm_pats' in db_config['pipeline']['clean_db']:
        await conn.execute(rm_pats)
      if 'reset_seq' in db_config['pipeline']['clean_db']:
        await conn.execute(reset_seq)

  def populate_db(self, db_config):
    print("populate_db: %s" % db_config['name'])
    engine = db_config['engine']
    engine.main()

  def copy_pat_enc(self, db_config):
    print("copy_pat_enc")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run_copy_pat_enc(db_config))

  async def run_copy_pat_enc(self, db_config):
    pool = db_config['engine'].pool
    sql = '''
    insert into pat_enc (dataset_id, enc_id, pat_id, visit_id)
    (
      select * from dblink('%s', $OPDB$
            select 1, enc_id, pat_id, visit_id from pat_enc
          $OPDB$) as pe (dataset_id int, enc_id int, pat_id text, visit_id text)
    );
    ''' % self.pair[0]['name']
    async with pool.acquire() as conn:
      await conn.execute(sql)

  def db_compare(self, db_config):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run_db_compare(db_config))

  async def run_db_compare(self, db_config):
    print("db_compare %s vs %s" % (self.db_pair[0]['name'], self.db_pair[1]['name']))
    engine = db_config['engine']
    await engine.init()
    dbpool = engine.pool
    args = db_config['db_compare']
    src_dataset_id = args['srcdid']
    src_model_id   = args['srcmid']
    dst_dataset_id = args['dstdid']
    dst_model_id   = args['dstmid']
    src_server = args['cmp_remote_server']
    counts = args['counts']
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
    pat_enc_query = (pat_enc_fields, enc_id_range, 'enc_id', None)

    cdm_s_range = 'fid ~ \'%s\'' % '|'.join(cdm_s_online_features)
    cdm_t_range = 'fid ~ \'%s\'' % '|'.join(cdm_t_online_features)
    cdm_t_range += ' and ' + tsp_range
    cdm_twf_online_fields = [row for row in cdm_twf_field if (row[0][:-2] if row[0].endswith('_c') else row[0]) in cdm_twf_online_features]

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
      'temperature': ['(round(temperature::numeric, 0))', '='],
    }
    for field in cdm_twf_online_fields:
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
    cdm_s_query1 = (cdm_s_fields1, cdm_s_range + ' and ' + enc_id_range, 'fid, enc_id', cdm_s_dependent_fields)

    cdm_t_fields1 = [
      ['enc_id'                             , 'enc_id',     'integer'     ],
      ['tsp'                                , 'tsp',        'timestamptz' ],
      ['fid'                                , 'fid',        'varchar(50)' ],
      ["(value::json)#>>'{dose}'"           , 'dose',       'text'        ],
      ["(value::json)#>>'{action}'"         , 'action',     'text'        ],
      ["(value::json)#>>'{order_tsp}'"      , 'order_tsp',  'text'        ],
      # ['confidence'                         , 'confidence', 'integer'     ],
    ]
    cdm_t_query1 = (cdm_t_fields1, 'fid like \'%_dose\' and ' + cdm_t_range + ' and ' + enc_id_range, 'fid, enc_id, tsp', cdm_t_dose_dependent_fields)

    cdm_t_fields2 = [
      ['enc_id'          ,'enc_id'          , 'integer',     ],
      ['tsp'             ,'tsp'             , 'timestamptz', ],
      ['fid'             ,'fid'             , 'varchar(50)', ],
      ['value'           ,'value'           , 'text',        ],
      # ['confidence'      ,'confidence'      , 'integer',     ],
    ]

    cdm_t_query2 = (cdm_t_fields2, 'fid !~ \'dose|inhosp|bacterial_culture|_proc|culture_order|pneumonia_sepsis|uro_sepsis|biliary_sepsis|intra_abdominal_sepsis\' and ' + cdm_t_range + ' and '+ enc_id_range, 'fid, enc_id, tsp', cdm_t_dependent_fields)

    cdm_t_fields3 = [
      ['enc_id'                             , 'enc_id',     'integer'     ],
      ['tsp'                                , 'tsp',        'timestamptz' ],
      ['fid'                                , 'fid',        'varchar(50)' ],
      ["(value::json)#>>'{diagname}'"           , 'diagname',       'text'        ],
      ["(value::json)#>>'{ischronic}'"         , 'ischronic',     'text'        ],
      ["""(value::json)#>>'{"present on admission"}'"""      , 'present_on_admission',  'text'        ],
      # ['confidence'                         , 'confidence', 'integer'     ],
    ]
    cdm_t_query3 = (cdm_t_fields3, 'fid like \'%_inhosp|pneumonia_sepsis|uro_sepsis|biliary_sepsis|intra_abdominal_sepsis\' and ' + cdm_t_range + ' and ' + enc_id_range, 'fid, enc_id, tsp', cdm_dependent_fields)

    cdm_t_fields4 = [
      ['enc_id'                             , 'enc_id',     'integer'     ],
      ['tsp'                                , 'tsp',        'timestamptz' ],
      ['fid'                                , 'fid',        'varchar(50)' ],
      ["(value::json)#>>'{status}'"           , 'status',       'text'        ],
      ["(value::json)#>>'{name}'"         , 'name',     'text'        ],
      # ['confidence'                         , 'confidence', 'integer'     ],
    ]
    cdm_t_query4 = (cdm_t_fields4, 'fid like \'bacterial_culture|_proc|culture_order\' and ' + cdm_t_range + ' and ' + enc_id_range, 'fid, enc_id, tsp', cdm_dependent_fields)

    cdm_twf_field_index = [
      ['enc_id'                             , 'enc_id',     'integer'     ],
      ['tsp'                                , 'tsp',        'timestamptz' ],
    ]

    confidence_range = '%s < 8'

    cdm_twf_queries = [(cdm_twf_field_index + [cdm_twf_online_fields[2*i]], enc_id_range + ' and ' + tsp_range + ' and ' + (confidence_range % cdm_twf_online_fields[2*i+1][0]), 'enc_id, tsp', cdm_twf_dependent_fields) for i in range(len(cdm_twf_online_fields)//2)]

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



    # results = []
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
                              as_count_result=counts, sort_field=sort_field)
          records = await c.run(dbpool)
          # results += records
      else:
        c = TableComparator(src_server,
                            src_dataset_id, src_model_id,
                            dst_dataset_id, dst_model_id,
                            tbl, version_extension=version_type, as_count_result=counts)
        records = await c.run(dbpool)
        # results += records




if __name__ == '__main__':
  test = DBCompareTest(db_pair)
  test.run()