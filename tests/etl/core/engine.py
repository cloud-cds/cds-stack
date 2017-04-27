from etl.core.engine import Engine

def a():
  print('a')
  return 1

def b():
  print('b')
  return 2

def c(x, y):
  print('c', x, y)
  return 3

g = {
  'a': ([], a),
  'b': ([], b),
  'c': (['a', 'b'], c)
}

e = Engine(tasks=g)
loop = asyncio.get_event_loop()
loop.run_until_complete(e.run())
loop.close()