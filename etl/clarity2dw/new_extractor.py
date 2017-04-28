import functools
import time

class Extractor:
  def __init__(self, **kwargs):
    self.dataset_id = kwargs.get('dataset_id')

  def extract(f):
    @functools.wraps(f)
    def wrapper(*args, **kwds):
      print('Calling decorated task')
      return f(*args, **kwds)
    return wrapper

  @extract
  def example(self, *args, **kwds):
    print('example task')
    time.sleep(2)

  async def extract_init(self, ctxt, config):
    async with ctxt.db_pool.acquire() as conn:
      await self.query_cdm_feature_dict(conn)
      await self.reset_dataset(conn, ctxt, config)
      return 1

  async def query_cdm_feature_dict(self, conn):
    sql = "select * from cdm_feature where dataset_id = %s" % self.dataset_id
    cdm_feature = await conn.fetch(sql)
    cdm_feature_dict = {f['fid']:f for f in cdm_feature}
    return cdm_feature_dict

  async def reset_dataset(self, conn, ctxt, config):
    ctxt.log.warn("reset_dataset")
    reset_sql = ''
    if config.get('remove_data', False):
      reset_sql += '''
      delete from cdm_s where dataset_id = %(dataset_id)s;
      delete from cdm_t where dataset_id = %(dataset_id)s;
      delete from cdm_twf where dataset_id = %(dataset_id)s;
      delete from criteria_meas where dataset_id = %(dataset_id)s;
      ''' % {'dataset_id': self.dataset_id}
    if config.get('remove_pat_enc', False):
      reset_sql += '''
      delete from pat_enc where dataset_id = %(dataset_id)s;
      ''' % {'dataset_id': self.dataset_id}
    if 'start_enc_id' in config:
      reset_sql += "select setval('pat_enc_enc_id_seq', %s);" % config['start_enc_id']
    ctxt.log.debug("ETL init sql: " + reset_sql)
    result = await conn.execute(reset_sql)
    ctxt.log.info("ETL Init: " + result)

  async def populate_patients(self, ctxt, config):
    async with ctxt.db_pool.acquire() as conn:
      limit = config.get('limit', None)
      sql = '''
      insert into pat_enc (dataset_id, visit_id, pat_id)
      SELECT %(dataset_id)s, demo."CSN_ID" visit_id, demo."pat_id"
      FROM "Demographics" demo left join pat_enc pe on demo."CSN_ID"::text = pe.visit_id::text
      where pe.visit_id is null %(limit)s
      ''' % {'dataset_id': self.dataset_id, 'limit': 'limit {}'.format(limit) if limit else ''}
      ctxt.log.debug("ETL populate_patients sql: " + sql)
      result = await conn.execute(sql)
      return result
    ctxt.log.info("ETL populate_patients: " + result)