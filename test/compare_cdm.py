import os
import asyncio
import asyncpg
import json
import copy
import logging

logging.basicConfig(level=logging.INFO)

host          = os.environ['db_host']
port          = os.environ['db_port']
db            = os.environ['db_name']
user          = os.environ['db_user']
pw            = os.environ['db_password']
src_server    = os.environ['cmp_remote_server']

tables_to_compare = {
  'datalink'                 : 'dataset',
  'cdm_function'             : 'dataset',
  'cdm_feature'              : 'dataset',
  'datalink_feature_mapping' : 'dataset',
  'pat_enc'                  : 'dataset',
  'cdm_g'                    : 'both',
  'cdm_s'                    : 'dataset',
  'cdm_m'                    : 'dataset',
  'cdm_t'                    : 'dataset',
  'criteria_meas'            : 'dataset',
  'criteria'                 : 'dataset',
  'criteria_events'          : 'dataset',
  'criteria_log'             : 'dataset',
  'criteria_meas_archive'    : 'dataset',
  'criteria_archive'         : 'dataset',
  'criteria_default'         : 'dataset',
  'notifications'            : 'dataset',
  'parameters'               : 'dataset',
  'trews_scaler'             : 'model',
  'trews_feature_weights'    : 'model',
  'trews_parameters'         : 'model',
  'cdm_twf'                  : 'dataset',
  'trews'                    : 'dataset',
  'pat_status'               : 'dataset',
  'deterioration_feedback'   : 'dataset',
  'feedback_log'             : 'dataset',
}

class TableComparator:
  def __init__(self, src_server,
                     src_dataset_id, src_model_id,
                     dst_dataset_id, dst_model_id,
                     src_tbl, dst_tbl=None,
                     field_map=None, version_extension='dataset', as_count_result=True):

    self.src_server     = src_server
    self.src_dataset_id = src_dataset_id
    self.src_model_id   = src_model_id
    self.dst_dataset_id = dst_dataset_id
    self.dst_model_id   = dst_model_id

    self.src_table = src_tbl
    self.dst_table = dst_tbl if dst_tbl is not None else src_tbl

    self.field_map = field_map
    self.version_extension = version_extension
    self.as_count_result = as_count_result

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
      ''' % { 'remote_server': self.src_server, 'remote_table': self.source_table }

      logging.info('Loading remote schema for {}'.format(self.source_table))
      remote_fields = await conn.fetch(remote_fields_query)

      if len(remote_fields) > 0:
        extension_ids = self.version_extension_ids()
        self.field_map = [[f['column_name'], f['data_type']] for f in remote_fields if f['column_name'] not in extension_ids]

        schema_desc = '\n'.join(map(lambda x: ': '.join(x), self.field_map))
        logging.info('Found remote schema for {}:\n{}'.format(self.source_table, schema_desc))

      else:
        logging.info('No remote schema found for {}'.format(self.source_table))
        self.field_map = []


  async def compare_query(self, pool, src_tbl, src_fields, dst_tbl, dst_fields):

    src_version_map = { 'model_id': self.src_model_id, 'dataset_id': self.src_dataset_id }
    dst_version_map = { 'model_id': self.dst_model_id, 'dataset_id': self.dst_dataset_id }

    extension_ids = self.version_extension_ids()
    src_extension_vals = map(lambda x: src_version_map[x], extension_ids)
    dst_extension_vals = map(lambda x: dst_version_map[x], extension_ids)

    with_src_extension = ' and '.join(map(lambda v: '{} = {}'.format(v[0], v[1]), zip(extension_ids, src_extension_vals)))
    with_dst_extension = ' and '.join(map(lambda v: '{} = {}'.format(v[0], v[1]), zip(extension_ids, dst_extension_vals)))

    with_src_extension = 'where ' + with_src_extension if with_src_extension else ''
    with_dst_extension = 'where ' + with_dst_extension if with_dst_extension else ''

    remote_query = \
      'select %(remote_fields)s from %(remote_table)s %(with_src_extension)s' \
        % { 'remote_table'       : src_tbl,
            'remote_fields'      : ', '.join(map(lambda nt: nt[0], src_fields)),
            'with_src_extension' : with_src_extension }

    query_finalizer = ''
    if self.as_count_result:
      query_finalizer = '''
      SELECT count(*) FROM A_DIFF_B UNION B_DIFF_A
      '''

    else:
      query_finalizer = '''
      SELECT true as missing_remotely, * FROM A_DIFF_B
      UNION
      SELECT false as missing_remotely, * FROM B_DIFF_A
      '''

    compare_to_remote_query = \
    '''
    WITH A_DIFF_B AS (
      SELECT %(local_field)s FROM %(local_table)s %(with_dst_extension)s
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
      SELECT %(local_field)s FROM %(local_table)s %(with_dst_extension)s
    )
    %(finalizer)s
    ''' % {
      'srv'                    : self.src_server,
      'query'                  : remote_query,
      'local_table'            : dst_tbl,
      'local_fields'           : ', '.join(map(lambda nt: nt[0], dst_fields)),
      'local_fields_and_types' : ', '.join(map(lambda nt: ' '.join(nt), dst_fields)),
      'with_dst_extension'     : with_dst_extension,
      'finalizer'              : query_finalizer
    }

    logging.info('Query to execute:\n{}'.format(remote_load_sql))
    async with pool.acquire() as conn:
      status = await conn.execute(remote_load_sql)
      logging.info('Remote load status: {}'.format(status))


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

  src_dataset_id = 1
  src_model_id   = 1

  dst_dataset_id = 2
  dst_model_id   = 2

  for tbl, version_type in tables_to_compare.items():
    c = TableComparator(src_server,
                        src_dataset_id, src_model_id,
                        dst_dataset_id, dst_model_id,
                        tbl, version_extension=version_type)
    await c.run(dbpool)

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run())

