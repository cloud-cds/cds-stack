import asyncio
import logging
import time
from etl.core.engine import Engine

logging.basicConfig(level=logging.INFO)

def a():
  print('a sleeping 5 secs')
  time.sleep(5)
  print('a woke up')
  return 1

def b():
  print('b sleeping 5 secs')
  time.sleep(5)
  print('b woke up')
  return 2

def c(x, y):
  print('c sleeping 5 secs')
  time.sleep(5)
  print('c woke up', x, y)
  return 3

g = {
  'a': ([], a),
  'b': ([], b),
  'c': (['a', 'b'], c)
}

e = Engine(name='engine1', tasks=g)
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(e.run())
loop.close()

class TestClass:
  def cls_a(self):
    print('cls_a sleeping 5 secs')
    time.sleep(5)
    print('cls_a woke up')
    return 1

  def cls_b(self):
    print('cls_b sleeping 5 secs')
    time.sleep(5)
    print('cls_b woke up')
    return 2

  def cls_c(self, x, y):
    print('cls_c sleeping 5 secs')
    time.sleep(5)
    print('cls_c woke up', x, y)
    return 3

t = TestClass()

g2 = {
  'a': ([], t.cls_a),
  'b': ([], t.cls_b),
  'c': (['a', 'b'], t.cls_c)
}

e2 = Engine(name='engine2', tasks=g2)
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(e2.run())
loop.close()