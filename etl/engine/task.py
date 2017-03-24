class Task:
  num_tasks = 0

  def __init__(self, **kwargs):
    self.task_id = Task.num_tasks
    self.task_name = kwargs.get('name', '')
    self.body = kwargs.get('run', None)
    Task.num_tasks += 1

  def run(self):
    """
    Returns a list of next tasks to run.

    Task subclasses should override this method (and need not call this method)
    """
    if body is not None:
      return self.body()
    return []