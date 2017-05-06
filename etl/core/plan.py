''' 
Plan Class 

Attributes:
  name (str): Name of the plan.
  config (dict): Dictionary of the plan configuration that all tasks inherit.

Properties:
  plan: Dictionary of plan.
'''

class Plan:
  def __init__(self, name, config):
    self.name = name
    self.config = config
    self.tasks = []


  def __str__(self):
    ''' Returns a string representation of the plan '''
    plan_str = "{}:".format(self.name)
    for t in self.tasks:
      plan_str += "\n {}".format(t)
    return plan_str


  def add(self, task):
    ''' Adds another task to the plan '''
    self.tasks.append(task)


  @property
  def plan(self):
    ''' Returns the plan as a dictionary '''
    plan = {} 
    for t in self.tasks:
      plan[t.name] = (t.deps, {'config': self.config, **t.func, **t.args})
    return plan
