import os
import asyncio
import functools
import logging
import time
from etl.core.engine import Engine

TEST_LOG_FMT = '%(asctime)s|%(funcName)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=TEST_LOG_FMT)

db_config = {
  'db_name': os.environ['db_name'],
  'db_user': os.environ['db_user'],
  'db_pass': os.environ['db_password'],
  'db_host': os.environ['db_host'],
  'db_port': os.environ['db_port']
}

def a(ctxt):
  logging.info('a sleeping 2 secs')
  time.sleep(2)
  logging.info('a woke up')
  return 1

def b(ctxt):
  logging.info('b sleeping 2 secs')
  time.sleep(2)
  logging.info('b woke up')
  return 2

def c(ctxt, x, y):
  logging.info('c sleeping 2 secs')
  time.sleep(2)
  logging.info('c woke up %s %s' % (x, y))
  return 3

g = {
  'a': ([],         {'config': db_config, 'fn': a}),
  'b': ([],         {'config': db_config, 'fn': b}),
  'c': (['a', 'b'], {'config': db_config, 'fn': c})
}

e = Engine(name='engine1', tasks=g)
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(e.run())
loop.close()

class TestClass:
  async def test_query(self, ctxt, x, y):
    async with ctxt.db_pool.acquire() as conn:
      print(x, y)
      sql = 'select count(*) as cnt from pat_enc where dataset_id = 1;'
      ctxt.log.info('Test query: %s' % sql)
      result = await conn.fetchval(sql)
      ctxt.log.info('Query result: %s' % result)
      return result

  async def test_timeout_query(self, ctxt, x, y):
    async with ctxt.db_pool.acquire() as conn:
      print(x, y)
      sql = 'select count(*) as cnt from cdm_twf;'
      ctxt.log.info('Test query: %s' % sql)
      try:
        result = await conn.fetchval(sql, timeout=0.1)
      except Exception as e:
        ctxt.log.warn('error: %s' % str(e))
        result = await conn.fetchval(sql)
      ctxt.log.info('Query result: %s' % result)
      return result

  async def test_transaction(self, ctxt, _):
    async with ctxt.db_pool.acquire() as conn:
      sql = 'select enc_id from pat_enc where dataset_id = 1 limit 10;'
      ctxt.log.info('Test query: %s' % sql)
      async with conn.transaction():
        async for row in conn.cursor(sql):
          ctxt.log.info(row)
      ctxt.log.info('transaction end')
      return None

  async def test_transaction_w2(self, ctxt, _):
    async with ctxt.db_pool.acquire() as conn:
      sql = 'select enc_id from pat_enc where dataset_id =1 limit 10;'
      ctxt.log.info('Test query: %s' % sql)
      futures = []
      async with conn.transaction():
        async for row in conn.cursor(sql):
          ctxt.log.info(row['enc_id'])
          sql_insert = "insert into cdm_s (dataset_id, enc_id, fid, value, confidence) values (1, {}, 'age', 2, 0) on conflict (dataset_id, enc_id, fid) do update set value = Excluded.value, confidence = Excluded.confidence".format(row['enc_id'])
          ctxt.log.info(sql_insert)
          futures.append(conn.execute(sql_insert))
      await asyncio.wait(futures)
      ctxt.log.info('transaction end')
      return None

  async def test_transaction_w3(self, ctxt, _):
    async with ctxt.db_pool.acquire() as conn:
      sql = 'select enc_id from pat_enc where dataset_id =1 limit 10;'
      ctxt.log.info('Test query: %s' % sql)
      futures = []
      raw = await conn.fetch(sql)
      transformed = [(1, row['enc_id'], 'age', 2, 0) for row in raw]
      upsert_sql = "insert into cdm_s (dataset_id, enc_id, fid, value, confidence) values ($1, $2, $3, $4, $5) on conflict (dataset_id, enc_id, fid) do update set value = Excluded.value, confidence = Excluded.confidence"
      result = await conn.executemany(upsert_sql, transformed)
      ctxt.log.info(result)
      return None

  async def test_transaction_w(self, ctxt, _):
    async with ctxt.db_pool.acquire() as conn:
      sql = 'select enc_id from pat_enc where dataset_id =1 limit 10;'
      ctxt.log.info('Test query: %s' % sql)
      async with conn.transaction():
        cur = await conn.cursor(sql)
        rows = await cur.fetch(2)
        while rows:
          ctxt.log.info(rows)
          async with conn.transaction():
            for row in rows:
              sql_insert = "insert into cdm_s (dataset_id, enc_id, fid, value, confidence) values (1, {}, 'age', 2, 0) on conflict (dataset_id, enc_id, fid) do update set value = Excluded.value, confidence = Excluded.confidence".format(row['enc_id'])
              ctxt.log.info(sql_insert)
              await conn.execute(sql_insert)
          rows = await cur.fetch(2)
      ctxt.log.info('transaction end')
      return None

  async def test_multi_tasks(self, ctxt, _):
    # async with ctxt.db_pool.acquire() as conn:
    futures = []
    for i in range(2):
      futures.append(self.test_transaction_w(ctxt, _))
    print(futures)
    await asyncio.wait(futures)
    return None

  def cls_a(self, ctxt, x):
    ctxt.log.info('cls_a sleeping {} secs'.format(x))
    time.sleep(x)
    ctxt.log.info('cls_a woke up')
    return 1

  def cls_b(self, ctxt):
    ctxt.log.info('cls_b sleeping 5 secs')
    time.sleep(2)
    ctxt.log.info('cls_b woke up')
    return 2

  def cls_c(self, ctxt, x, y):
    ctxt.log.info('cls_c sleeping 5 secs')
    time.sleep(2)
    ctxt.log.info('cls_c woke up %s %s' % (x, y))
    return 3

t = TestClass()

g2 = {
  'a': ([],         {'config': db_config, 'fn': functools.partial(t.cls_a, x=1)}),
  'b': ([],         {'config': db_config, 'fn': t.cls_b}),
  'c': (['a', 'b'], {'config': db_config, 'fn': t.cls_c}),
  'd': (['c'],      {'config': db_config, 'coro': t.test_timeout_query, 'args': [1]}),
  # 'e': (['c'],      {'config': db_config, 'coro': t.test_multi_tasks})
}

e2 = Engine(name='engine2', tasks=g2)
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(e2.run())
loop.close()
