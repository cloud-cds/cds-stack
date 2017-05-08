from inspect import signature
'''
Task class - a Plan is made up of these

Attributes:
  name (str): Name of the task.
  deps (list, optional): List of dependencies.
  coro (function, optional): Asyncronous function to run.
  fn   (function, optional): Asyncronous function to run.
  args (list, optional): List of arguments to pass to the coro or fn.
'''

class Task:
  def __init__(self, name, deps=[], coro=None, fn=None, args=None):
    # Make sure a either a coroutine or function is set
    if (not coro and not fn) or (coro and fn):
      raise ValueError("Task {} needs a coroutine or function (but not both)".format(name))

    # Make sure that 'ctxt' is the first argument of the function or coroutine
    params = signature(coro).parameters if coro else signature(fn).parameters
    if 'ctxt' != list(params)[0]:
      raise RuntimeError("Task {} function or coroutine needs 'ctxt' as first argument".format(name))

    # Make sure the number of arguments is valid
    # Subtract 1 because of the 'ctxt'
    if ((len(args) if args else 0) + len(deps)) != (len(params) - 1) and \
      "args" not in params:
      error_str = "Task '{}' ".format(name)
      error_str += "function or coroutine has an incorrect number of arguments or dependencies"
      error_str += "\n   Expected: {},  Found: {}".format(
        (len(args) if args else 0) + len(deps), len(params) - 1)
      raise RuntimeError(error_str)

    self.name = name
    self.deps = deps
    self.func = {'coro': coro} if coro else {'fn': fn}
    self.args = {'args': args} if args else {}



  def __str__(self):
    ''' String representation of the task '''
    for f in self.func.items():
      f_type = f[0]
      f_func = f[1]
    task_str = "{}:".format(self.name)
    task_str += "\n  deps: {}".format(self.deps if self.deps else "-")
    task_str += "\n  {}: {}".format(f_type, f_func)
    task_str += "\n  args: {}".format(self.args if self.args else "-")
    return task_str
