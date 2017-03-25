import os, logging
from collections import deque

class Engine:
  """
  ETL Engine, responsible for running ETL tasks.

  For now, this is a simple serial engine.
  """

  def __init__(self, **kwargs):
    # An engine identifier.
    self.name = kwargs.get('name', 'etl-engine')

    # A dictionary of tasks, indexed by task name.
    self.tasks = {}

    # A queue of tasks waiting to run.
    self.run_queue = deque([])

  def run(self):
    while self.run_queue:
      queue_ids = [task.task_name if task.task_name else task.task_id for task in self.run_queue]
      logging.debug('Queue {}'.format(str(queue_ids)))

      task = self.run_queue.popleft()

      logging.info('Running [{}]{}'.format(task.task_id, task.task_name))
      next_tasks = task.run()
      for t in next_tasks:
        logging.debug('Scheduled [{}]{}'.format(t.task_id, t.task_name))
        self.schedule(t)

  def schedule(self, task):
    self.run_queue.append(task)
