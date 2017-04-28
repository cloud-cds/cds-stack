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
  logging.info('a sleeping 5 secs')
  time.sleep(5)
  logging.info('a woke up')
  return 1

def b(ctxt):
  logging.info('b sleeping 5 secs')
  time.sleep(5)
  logging.info('b woke up')
  return 2

def c(ctxt, x, y):
  logging.info('c sleeping 5 secs')
  time.sleep(5)
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
      sql = 'select count(*) as cnt from pat_enc;'
      ctxt.log.info('Test query: %s' % sql)
      result = await conn.fetchval(sql)
      ctxt.log.info('Query result: %s' % result)
      return result

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
  'd': (['c'],      {'config': db_config, 'coro': t.test_query, 'args': [1]})
}

e2 = Engine(name='engine2', tasks=g2)
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(e2.run())
loop.close()