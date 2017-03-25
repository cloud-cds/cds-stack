import asyncio
import asyncpg
import json
import copy
import logging

class Extractor:
  def __init__(self, remote_server, db_id, etl_id, source_tbl, dest_tbl=None, field_map=None):
    self.remote_server = remote_server
    self.db_id = db_id
    self.etl_id = etl_id
    self.source_table = source_tbl
    self.dest_table = dest_tbl if dest_tbl is not None else source_tbl
    self.field_map = field_map

  async def get_field_map(self, pool):
    remote_fields_query = \
    '''
    select *
    from dblink('%(remote_server)s', $OPDB$
      select column_name, data_type from information_schema.columns where table_name = '%(remote_table)s'
    $OPDB$) AS remote_fields (column_name text, data_type::text)
    ''' % { 'remote_server': self.remote_server, 'remote_table': self.source_table }

    logging.info('Loading remote schema for {}'.format(self.source_table))
    remote_fields = await conn.fetch(remote_fields_query)

    if len(remote_fields) > 0:
      self.field_map = []
      for f in remote_fields:
        self.field_map.append([f['column_name'], f['data_type']])

      schema_desc = '\n'.join(map(self.field_map, lambda x: ': '.join(x)))
      logging.info('Found remote schema for {}:\n{}'.format(self.source_table, schema_desc))

    else:
      logging.info('No remote schema found for {}'.format(self.source_table))

  async def run(self, pool):
    local_fields = []
    remote_fields = []

    if not self.field_map:
      await self.get_field_map(pool)

    if type(self.field_map) is list:
      remote_fields = copy.deepcopy(self.field_map)
      local_fields = copy.deepcopy(self.field_map)

    else:
      for s, t in self.field_map.items():
        remote_fields.append(s)
        local_fields.append(t)

    remote_query =
      'select %(remote_fields)s from %(remote_table)s'
        % {'remote_table': self.source_table, 'remote_fields': ','.join(map(lambda nt: nt[0], remote_fields)) }

    remote_load_sql = \
    '''
    INSERT INTO %(local_table)s (%(local_fields)s)
    SELECT as %(db_id)s as db_id, %(etl_id)s as etl_id, %(local_table)s_tmp.*
    FROM dblink('%(srv)s', $OPDB$
      %(query)s
    $OPDB$) AS %(local_table)s_tmp (%(local_fields_and_types)s)
    ''' % {
      'srv': self.remote_server,
      'db_id': self.db_id,
      'etl_id': self.etl_id,
      'query': remote_query,
      'local_table': self.dest_table,
      'local_fields': ','.join(map(lambda nt: nt[0], local_fields)),
      'local_fields_and_types': ','.join(map(lambda nt: ' '.join(nt), local_fields))
    }

    logging.info('Query to execute:\n{}'.format(remote_load_sql))
    # async with pool.acquire() as conn:
    #   status = await conn.execute(remote_load_sql)
    #   logging.info('Remote load status: {}'.format(status))
