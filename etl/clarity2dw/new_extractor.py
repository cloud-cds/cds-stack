import functools
import time

class Extractor:
  def __init__(self, **kwargs):
    pass

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