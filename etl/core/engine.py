import os, logging
from collections import deque

import asyncio
import concurrent.futures
import functools.partial

class Engine:
  """
  An engine that runs a DAG of Python coroutines
  """
  def __init__(self, **kwargs):
    # An engine identifier.
    self.name = kwargs.get('name', 'etl-engine')

    # A dictionary of tasks, mapping task name => (task sources, task coroutine)
    # Sources are other task names.
    self.tasks = kwargs.get('tasks', {})

    # Compute reverse dependencies (i.e., task destinations)
    self.reverse_dependencies = {}
    self.gc_counters = {}

    for task_id, task_data in self.tasks.items():
      dependencies, _ = task_data
      for d in dependencies:
        self.reverse_dependencies[d] = self.reverse_dependencies.get(d, []) + [task_id]
        self.gc_counters[d] = self.gc_counters.get(d, 0) + 1

    # A dictionary of task futures, mapping task names => list of dependency futures.
    # This allows us to wait on the last future to complete as a dependency barrier.
    self.task_futures = {}

    # A dictionary of task results, mapping task names => result object.
    # These results are garbage collected when no longer needed by any downstream task.
    self.task_results = {}

    # Executor
    self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=2)

    # A queue of tasks ready, and waiting to run.
    self.pending_queue = deque()

    # Prepare initial tasks.
    self.schedule()


  # Return if a task has completed
  def completed(self, dependencies):
    dones = [task_id in self.task_futures \
              and self.task_futures[task_id].done() \
              and task_id in self.task_results \
              for task_id in dependencies]

    return all(dones)

  # Adds tasks to the queue based on their readiness.
  def schedule(self, just_ran=None):
    # Schedule downstream neighbors of a task if it just ran,
    # otherwise attempt to schedule all tasks.
    if just_ran:
      schedulable_tasks = [ \
         next_task \
          for task_ran in just_ran \
            if task_ran in self.tasks and task_ran in self.reverse_dependencies \
              for next_task in self.reverse_dependencies[task_ran]
      ]

    else:
      schedulable_tasks = self.tasks.keys()

    for task_id in schedulable_tasks:
      if not completed([task_id]):
        # Check if task is ready, as either task has no dependencies,
        # or all dependencies have completed futures and results.
        dependencies, _ = self.tasks[task_id]
        if (not dependencies) or completed(dependencies):
          logging.info('Enqueueing "%s"' % task_id)
          self.pending_queue.append(task_id)
      else:
        logging.info('Scheduler skipping completed task "%s"' % task_id)

  # Task result garbage collection.
  # Releases dependencies' results when all downstreams of dependencies are complete.
  def gc_dependencies(self, task_id):
    if task_id in self.tasks:
      dependencies, _ = self.tasks[task_id]
      for d in dependencies:
        self.gc_counters[d] -= 1
        if self.gc_counters[d] <= 0:
          self.task_results[d] = None
          logging.info('Engine garbage collection %s' % d)
    else:
      logging.error('Invalid task id "%s"' % task_id)

  # Executes a task using outputs from dependencies.
  def run_task(self, task_id):
    if task_id in self.tasks:
      dependencies, task_fn = self.tasks[task_id]
      args = [self.task_results[d] if d in self.task_results else None for d in dependencies]
      logging.info('Starting task %s' % task_id)
      self.task_results[task_id] = task_fn(*args)
      self.gc_dependencies(task_id)
    else:
      logging.error('Invalid task id "%s"' % task_id)

  # Runs all (independent) pending tasks
  def run_block(self):
    ids_and_futures = []
    while self.pending_queue:
      task_id = self.pending_queue.popleft()
      logging.info('Engine dispatching task: %s' % task_id)

      future = asyncio.get_event_loop().run_in_executor(self.executor, run_task, task_id)
      self.task_futures[task_id] = future
      ids_and_futures.append((task_id, future))

    return ids_and_futures

  async def run(self):
    iteration = 0
    while len(self.task_results) != len(self.tasks):
      # Run a block of tasks, and wait for the first completion.
      ids_and_futures = self.run_block()
      await asyncio.wait([idf[1] for idf in ids_and_futures], return_when=asyncio.FIRST_COMPLETED)

      # Schedule next tasks
      completed = [idf[0] for idf in ids_and_futures if idf[1].done()]
      logging.info('Engine (iter %s) completed %s' % (iteration, str(completed)))

      if completed:
        self.schedule(just_ran=completed)
        logging.info('Queue (iter %s) %s' % (iteration, str(list(self.pending_queue))))
      else:
        logging.info('Scheduler no-op, no new tasks completed')

      logging.engine('Engine progress: %s / %s tasks completed' % (len(self.task_results), len(self.tasks)))

    logging.info('Engine completed.')

