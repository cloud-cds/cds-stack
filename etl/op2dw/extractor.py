import asyncio
import asyncpg
import json
import copy
import logging

class Extractor:
  def __init__(self, remote_server, dataset_id, model_id,
                     source_tbl, dest_tbl=None,
                     field_map=None, primary_key=None,
                     as_model_extension=False):
    self.remote_server = remote_server
    self.dataset_id = dataset_id
    self.model_id = model_id
    self.source_table = source_tbl
    self.dest_table = dest_tbl if dest_tbl is not None else source_tbl
    self.field_map = field_map
    self.primary_key = primary_key
    self.as_model_extension = as_model_extension

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

  def remote_query(self, conn, query, tbl_alias, tbl_fields):
    return \
      '''
      select * from dblink('%(srv)s', %(query)s) as %(tbl)s(%(fields)s)
      ''' % {
        'srv'    : self.remote_server,
        'query'  : query,
        'tbl'    : tbl_alias,
        'fields' : tbl_fields
      }

  async def get_primary_key(self, pool, table, as_remote=False):
    async with pool.acquire() as conn:
      primary_key_query = \
      '''
      select c.column_name, c.data_type
      from information_schema.key_column_usage k
      inner join information_schema.columns c on (k.table_name = c.table_name and k.column_name = c.column_name)
      where c.table_name = '%(tbl)s' and c.table_schema = 'public'
      and k.constraint_name like '%%_pkey'
      ''' % { 'tbl': table }

      logging.info('Loading primary key for {}'.format(table))
      key_fields = []

      if as_remote:
        remote_primary_key_query = self.remote_query(conn, primary_key_query, 'pkey_tmp', 'column_name text, data_type text')
        key_fields = await conn.fetch(remote_primary_key_query)
      else:
        key_fields = await conn.fetch(primary_key_query)

      if len(key_fields) > 0:
        extension_id = 'model_id' if self.as_model_extension else 'dataset_id'
        self.primary_key = [[f['column_name'], f['data_type']] for f in key_fields if f['column_name'] != extension_id]

        schema_desc = '\n'.join(map(lambda x: ': '.join(x), self.primary_key))
        logging.info('Found primary key for {}:\n{}'.format(table, schema_desc))

      else:
        logging.info('No primary key found for {}'.format(table))
        self.primary_key = []


  async def get_field_map(self, pool):
    async with pool.acquire() as conn:
      remote_fields_query = \
      '''
      select *
      from dblink('%(remote_server)s', $OPDB$
        select column_name, data_type from information_schema.columns
        where table_name = '%(remote_table)s' and table_schema = 'public'
      $OPDB$) AS remote_fields (column_name text, data_type text)
      ''' % { 'remote_server': self.remote_server, 'remote_table': self.source_table }

      logging.info('Loading remote schema for {}'.format(self.source_table))
      remote_fields = await conn.fetch(remote_fields_query)

      if len(remote_fields) > 0:
        self.field_map = [[f['column_name'], f['data_type']] for f in remote_fields]

        schema_desc = '\n'.join(map(lambda x: ': '.join(x), self.field_map))
        logging.info('Found remote schema for {}:\n{}'.format(self.source_table, schema_desc))

      else:
        logging.info('No remote schema found for {}'.format(self.source_table))
        self.field_map = []


  async def load_query(self, pool, src_tbl, src_fields, dst_tbl, dst_fields, dst_key_fields):
    non_key_assignments = [[snt[0], 'excluded.{}'.format(dnt[0])] \
                              for snt, dnt in zip(src_fields, dst_fields) if dnt not in dst_key_fields]

    remote_query = \
      'select %(remote_fields)s from %(remote_table)s' \
        % {'remote_table': src_tbl, 'remote_fields': ', '.join(map(lambda nt: nt[0], src_fields)) }

    extension_id = 'model_id' if self.as_model_extension else 'dataset_id'
    extension_val = str(self.model_id if self.as_model_extension else self.dataset_id)
    extension_expr = '{} as {}'.format(extension_val, extension_id)

    upsert_clause = \
    '''
    ON CONFLICT (%(extension_id)s, %(primary_key)s) DO UPDATE SET
      %(non_key_assignments)s
    ''' % {
      'extension_id': extension_id,
      'primary_key': ', '.join(map(lambda nt: nt[0], dst_key_fields)),
      'non_key_assignments': ', '.join(map(lambda assign: ' = '.join(assign), non_key_assignments))
    }

    remote_load_sql = \
    '''
    INSERT INTO %(local_table)s (%(extension_id)s, %(local_fields)s)
    SELECT %(extension_expr)s, %(local_table)s_tmp.*
    FROM dblink('%(srv)s', $OPDB$
      %(query)s
    $OPDB$) AS %(local_table)s_tmp (%(local_fields_and_types)s)
    %(upsert_clause)s
    ''' % {
      'srv': self.remote_server,
      'extension_id': extension_id,
      'extension_expr': extension_expr,
      'query': remote_query,
      'local_table': dst_tbl,
      'local_fields': ', '.join(map(lambda nt: nt[0], dst_fields)),
      'local_fields_and_types': ', '.join(map(lambda nt: ' '.join(nt), dst_fields)),
      'upsert_clause': upsert_clause if dst_key_fields else ''
    }

    logging.info('Query to execute:\n{}'.format(remote_load_sql))
    async with pool.acquire() as conn:
      status = await conn.execute(remote_load_sql)
      logging.info('Remote load status: {}'.format(status))


  async def run(self, pool):
    if not self.field_map:
      await self.get_field_map(pool)

    if not self.primary_key:
      await self.get_primary_key(pool, self.dest_table)

    if self.field_map:
      # TODO: change when migrated to split cdm_twf and cdm_twf_c tables.
      with_split_cdm_twf = False

      if with_split_cdm_twf and self.source_table == 'cdm_twf':
        src_fields, dst_fields = self.split_fields()

        common_fields = ['enc_id', 'tsp']
        src_vfields = list(filter(lambda nt: nt[0] in common_fields or not(nt[0].endswith('_c')), src_fields))
        dst_vfields = list(filter(lambda nt: nt[0] in common_fields or not(nt[0].endswith('_c')), dst_fields))

        await self.load_query(pool, self.source_table, src_vfields, self.dest_table, dst_vfields, self.primary_key)

        src_cfields = list(filter(lambda nt: nt[0] in common_fields or nt[0].endswith('_c'), src_fields))
        dst_cfields = list(filter(lambda nt: nt[0] in common_fields or nt[0].endswith('_c'), dst_fields))

        await self.load_query(pool, self.source_table, src_cfields, 'cdm_twf_c', dst_cfields, self.primary_key)

      else:
        src_fields, dst_fields = self.split_fields()
        await self.load_query(pool, self.source_table, src_fields, self.dest_table, dst_fields, self.primary_key)

    else:
      logging.warning('Skipping ETL for {}, no remote schema found'.format(self.source_table))
