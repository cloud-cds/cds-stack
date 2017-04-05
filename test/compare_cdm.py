import os
import asyncio
import asyncpg
import json
import copy
import logging
import argparse
from functools import partial
from cdm_feature import cdm_twf_field



logging.basicConfig(level=logging.INFO)

host          = os.environ['db_host']
port          = os.environ['db_port']
db            = os.environ['db_name']
user          = os.environ['db_user']
pw            = os.environ['db_password']
if 'cmp_remote_server' in os.environ:
  src_server    = os.environ['cmp_remote_server']

enc_id_range = 'enc_id < 31'
tsp_range = " tsp > '2017-04-01 08:00:00 EDT'::timestamptz and tsp < '2017-04-01 16:00:00 EDT'::timestamptz"

cdm_s_online_features = ['age','gender',
'heart_failure_hist', 'chronic_pulmonary_hist', 'emphysema_hist',
'heart_arrhythmias_prob',
'esrd_prob' 'esrd_diag', 'chronic_bronchitis_diag', 'heart_arrhythmias_diag', 'heart_failure_diag']
cdm_t_online_features = ['urine_output', 'dobutamine_dose',
'epinephrine_dose',
'levophed_infusion_dose',
'dopamine_dose','vent','fluids_intake',]
cdm_twf_online_features = ['rass', 'resp_rate',  'nbp_sys', 'gcs', 'temperature', 'amylase',    'weight', 'pao2', 'nbp_dias', 'hemoglobin',  'wbc', 'bilirubin', 'lipase', 'sodium', 'creatinine',  'spo2',  'heart_rate', 'paco2', 'bun', 'platelets', 'fio2']


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
  'shock_idx': ['(round(value::numeric, 4))', '='],
  'weight': ['(round(value::numeric, 4))', '='],
  'nbp_mean': ['(round(value::numeric, 4))', '='],
  'mapm': ['(round(value::numeric, 4))', '='],
  'pao2_to_fio2': ['(round(value, 4))', '='],
  'temperature': ['(round(value, 2))', '='],
}

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
  ['confidence'                         , 'confidence', 'integer'     ],
]
cdm_t_query1 = (cdm_t_fields1, 'fid like \'%_dose\' and ' + cdm_t_range + ' and ' + enc_id_range, 'fid, enc_id, tsp', cdm_t_dose_dependent_fields)

cdm_t_fields2 = [
  ['enc_id'          , 'integer',     ],
  ['tsp'             , 'timestamptz', ],
  ['fid'             , 'varchar(50)', ],
  ['value'           , 'text',        ],
  ['confidence'      , 'integer',     ],
]

cdm_t_query2 = (cdm_t_fields2, 'fid !~ \'dose|inhosp|bacterial_culture|_proc|culture_order|pneumonia_sepsis|uro_sepsis|biliary_sepsis|intra_abdominal_sepsis\' and ' + cdm_t_range + ' and '+ enc_id_range, 'fid, enc_id, tsp', cdm_t_dependent_fields)

cdm_t_fields3 = [
  ['enc_id'                             , 'enc_id',     'integer'     ],
  ['tsp'                                , 'tsp',        'timestamptz' ],
  ['fid'                                , 'fid',        'varchar(50)' ],
  ["(value::json)#>>'{diagname}'"           , 'diagname',       'text'        ],
  ["(value::json)#>>'{ischronic}'"         , 'ischronic',     'text'        ],
  ["""(value::json)#>>'{"present on admission"}'"""      , 'present_on_admission',  'text'        ],
  ['confidence'                         , 'confidence', 'integer'     ],
]
cdm_t_query3 = (cdm_t_fields3, 'fid like \'%_inhosp|pneumonia_sepsis|uro_sepsis|biliary_sepsis|intra_abdominal_sepsis\' and ' + cdm_t_range + ' and ' + enc_id_range, 'fid, enc_id, tsp', cdm_dependent_fields)

cdm_t_fields4 = [
  ['enc_id'                             , 'enc_id',     'integer'     ],
  ['tsp'                                , 'tsp',        'timestamptz' ],
  ['fid'                                , 'fid',        'varchar(50)' ],
  ["(value::json)#>>'{status}'"           , 'status',       'text'        ],
  ["(value::json)#>>'{name}'"         , 'name',     'text'        ],
  ['confidence'                         , 'confidence', 'integer'     ],
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
  # 'cdm_function'             : ('dataset', []),
  # 'cdm_feature'              : ('dataset', []),
  # 'datalink_feature_mapping' : ('dataset', []),
  # 'pat_enc'                  : ('dataset', []),
  # 'cdm_g'                    : ('both'   , []),
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

unsupported_types = ['json', 'jsonb']

class TableComparator:
  def __init__(self, src_server,
                     src_dataset_id, src_model_id,
                     dst_dataset_id, dst_model_id,
                     src_tbl, dst_tbl=None,
                     src_pred=None, dst_pred=None,
                     field_map=None, dependent_fields=None,
                     version_extension='dataset', as_count_result=True, sort_field=None):

    self.src_server     = src_server
    self.src_dataset_id = src_dataset_id
    self.src_model_id   = src_model_id
    self.dst_dataset_id = dst_dataset_id
    self.dst_model_id   = dst_model_id

    self.src_table = src_tbl
    self.dst_table = dst_tbl if dst_tbl is not None else src_tbl

    self.src_pred = src_pred
    self.dst_pred = dst_pred if dst_pred is not None else src_pred

    self.field_map = field_map
    self.dependent_fields = dependent_fields

    self.version_extension = version_extension
    self.as_count_result = as_count_result
    self.sort_field = sort_field

  def version_extension_ids(self):
    if self.version_extension == 'dataset':
      return ['dataset_id']
    elif self.version_extension == 'model':
      return ['model_id']
    elif self.version_extension == 'both':
      return ['dataset_id', 'model_id']
    return []

  def split_fields(self):
    src_fields = []
    dst_fields = []

    if type(self.field_map) is list:
      src_fields = copy.deepcopy(self.field_map)
      dst_fields = copy.deepcopy(self.field_map)

    else:
      for s, t in self.field_map.items():
        src_fields.append(s)
        dst_fields.append(t)

    return (src_fields, dst_fields)

  async def get_field_map(self, pool):
    async with pool.acquire() as conn:
      remote_fields_query = \
      '''
      select *
      from dblink('%(remote_server)s', $OPDB$
        select column_name, data_type from information_schema.columns
        where table_name = '%(remote_table)s' and table_schema = 'public'
      $OPDB$) AS remote_fields (column_name text, data_type text)
      ''' % { 'remote_server': self.src_server, 'remote_table': self.src_table }

      logging.info('Loading remote schema for {}'.format(self.src_table))
      remote_fields = await conn.fetch(remote_fields_query)

      if len(remote_fields) > 0:
        extension_ids = self.version_extension_ids()
        self.field_map = [[f['column_name'], f['data_type']] for f in remote_fields \
                             if f['column_name'] not in extension_ids and f['data_type'] not in unsupported_types]

        schema_desc = '\n'.join(map(lambda x: ': '.join(x), self.field_map))
        logging.info('Found remote schema for {}:\n{}'.format(self.src_table, schema_desc))

      else:
        logging.info('No remote schema found for {}'.format(self.src_table))
        self.field_map = []


  async def compare_query(self, pool, src_tbl, src_fields, dst_tbl, dst_fields, dst_tsp_shift='4 hours'):

    src_version_map = { 'model_id': self.src_model_id, 'dataset_id': self.src_dataset_id }
    dst_version_map = { 'model_id': self.dst_model_id, 'dataset_id': self.dst_dataset_id }

    extension_ids = self.version_extension_ids()
    src_extension_vals = filter(lambda x: x is not None, map(lambda x: src_version_map[x], extension_ids))
    dst_extension_vals = filter(lambda x: x is not None, map(lambda x: dst_version_map[x], extension_ids))
    src_pred_list = [] if self.src_pred is None else [self.src_pred]
    dst_pred_list = [] if self.dst_pred is None else [self.dst_pred]
    with_src_extension = ' and '.join(src_pred_list + list(map(lambda v: '{} = {}'.format(v[0], v[1]), zip(extension_ids, src_extension_vals))))
    with_dst_extension = ' and '.join(dst_pred_list + list(map(lambda v: '{} = {}'.format(v[0], v[1]), zip(extension_ids, dst_extension_vals))))

    with_src_extension = 'where ' + with_src_extension if with_src_extension else ''
    with_dst_extension = 'where ' + with_dst_extension if with_dst_extension else ''

    def project_expr(field_map_entry, mode='expr'):
      expr = None
      name = None
      typ  = None

      if len(field_map_entry) == 2:
        name = field_map_entry[0]
        expr = name
        typ = field_map_entry[1]
      else:
        expr, name, typ = field_map_entry

      if self.dependent_fields is not None and name in self.dependent_fields:
        dep_field, dep_expr_map = self.dependent_fields[name]
        when_exprs = [ 'when %(dep_field)s %(op)s \'%(dep_val)s\' then (%(dep_expr)s)::%(ty)s' \
                          % {'dep_field': dep_field, 'dep_val': dep_val, 'dep_expr': dep_expr[0], 'ty': typ, 'op': dep_expr[1] } \
                        for dep_val, dep_expr in dep_expr_map.items() ]
        expr = '(case %(whens)s else (%(expr)s)::%(ty)s end) as %(name)s' % { 'whens': '\n'.join(when_exprs), 'expr': expr, 'ty': typ, 'name': name }

      if mode == 'expr':
        return expr
      elif mode == 'name':
        return name
      else:
        return '%s %s' % (name, typ)

    remote_query = \
      'select %(remote_fields)s from %(remote_table)s %(with_src_extension)s' \
        % { 'remote_table'       : src_tbl,
            'remote_fields'      : ', '.join(map(partial(project_expr), src_fields)),
            'with_src_extension' : with_src_extension }

    query_finalizer = ''
    if self.as_count_result:
      query_finalizer = '''
      SELECT (SELECT count(*) FROM A_DIFF_B) + (SELECT count(*) FROM B_DIFF_A) as diffs
      '''

    else:
      query_finalizer = '''
      SELECT * FROM (
        SELECT true as missing_remotely, * FROM A_DIFF_B
        UNION
        SELECT false as missing_remotely, * FROM B_DIFF_A
      ) R
      %s
      ''' % ('' if self.sort_field is None else ('ORDER BY %s' % self.sort_field))
    compare_to_remote_query = \
    '''
    WITH A_DIFF_B AS (
      SELECT %(local_exprs)s FROM %(local_table)s %(with_dst_extension)s
      EXCEPT
      SELECT %(local_fields)s
      FROM dblink('%(srv)s', $OPDB$
        %(query)s
      $OPDB$) AS %(local_table)s_compare (%(local_fields_and_types)s)
    ), B_DIFF_A AS (
      SELECT %(local_fields)s
      FROM dblink('%(srv)s', $OPDB$
        %(query)s
      $OPDB$) AS %(local_table)s_compare (%(local_fields_and_types)s)
      EXCEPT
      SELECT %(local_exprs)s FROM %(local_table)s %(with_dst_extension)s
    )
    %(finalizer)s
    ''' % {
      'srv'                    : self.src_server,
      'query'                  : remote_query,
      'local_table'            : dst_tbl,
      'local_exprs'            : ', '.join(map(partial(project_expr), dst_fields if dst_tsp_shift is None else [(f if f[0] != 'tsp' or len(f) < 3 else ["(tsp + '%s'::interval) tsp" % dst_tsp_shift, f[1], f[2]])  for f in dst_fields])),
      'local_fields'           : ', '.join(map(partial(project_expr, mode='name'), dst_fields)),
      'local_fields_and_types' : ', '.join(map(partial(project_expr, mode='nametype'), dst_fields)),
      'with_dst_extension': with_dst_extension.replace("tsp", "tsp + '%s'::interval" % dst_tsp_shift) if 'tsp' in with_dst_extension and dst_tsp_shift is not None else with_dst_extension ,
      'finalizer'              : query_finalizer
    }

    logging.info('Query to execute:\n{}'.format(compare_to_remote_query))
    async with pool.acquire() as conn:
      if self.as_count_result:
        r = await conn.fetchrow(compare_to_remote_query)
        logging.info('# DIFFS: %s' % r['diffs'])
        if r['diffs'] > 0:
          logging.warning('Table %s differs from %s.%s (%s rows)' % (self.dst_table, self.src_server, self.src_table, r['diffs']))
      else:
        results = await conn.fetch(compare_to_remote_query)
        for r in results:
          logging.info(dict(r))
        return results


  async def run(self, pool):
    if not self.field_map:
      await self.get_field_map(pool)

    if self.field_map:
        src_fields, dst_fields = self.split_fields()
        await self.compare_query(pool, self.src_table, src_fields, self.dst_table, dst_fields)

    else:
      logging.warning('Skipping table comparison for {}, no remote schema found'.format(self.src_table))


async def run():
  logging.info("Running CDM DB Comparison")
  dbpool = await asyncpg.create_pool(database=db, user=user, password=pw, host=host, port=port)

  parser = argparse.ArgumentParser()
  parser.add_argument("--srcdid", type=int, default=None, help="Source dataset id")
  parser.add_argument("--dstdid", type=int, default=None, help="Dest dataset id")
  parser.add_argument("--srcmid", type=int, default=None, help="Source model id")
  parser.add_argument("--dstmid", type=int, default=None, help="Dest model id")
  parser.add_argument("--counts", default=False, help="Show count of differing rows instead of values", action="store_true")
  args = parser.parse_args()

  src_dataset_id = args.srcdid
  src_model_id   = args.srcmid

  dst_dataset_id = args.dstdid
  dst_model_id   = args.dstmid

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
                            as_count_result=args.counts, sort_field=sort_field)
        await c.run(dbpool)
    else:
      c = TableComparator(src_server,
                          src_dataset_id, src_model_id,
                          dst_dataset_id, dst_model_id,
                          tbl, version_extension=version_type, as_count_result=args.counts)
      await c.run(dbpool)

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run())

