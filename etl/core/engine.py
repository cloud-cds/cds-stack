from etl.core.plan import Plan

import os, logging
from collections import deque

import asyncio
import asyncpg
import concurrent.futures
import functools
import faulthandler

ENGINE_LOG_FMT = '%(asctime)s|%(name)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'

import code, traceback, signal

#####################
# Debug methods
def debug(sig, frame):
    """Interrupt running process, and provide a python prompt for
    interactive debugging."""
    d={'_frame':frame}         # Allow access to frame object.
    d.update(frame.f_globals)  # Unless shadowed by global
    d.update(frame.f_locals)

    i = code.InteractiveConsole(d)
    message  = "Signal received : entering python shell.\nTraceback:\n"
    message += ''.join(traceback.format_stack(frame))
    i.interact(message)

def listen():
    signal.signal(signal.SIGUSR2, debug)  # Register handler

####################
# Utility methods.
class TaskContext:
  def __init__(self, name, config):
    self.name = name
    self.config = config
    self.log = logging.getLogger(self.name)
    self.log.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    formatter = logging.Formatter(ENGINE_LOG_FMT)
    sh.setFormatter(formatter)
    self.log.addHandler(sh)
    self.log.propagate = False

  async def async_init(self, loop=None):
    if loop:
      self.db_pool = await asyncpg.create_pool( \
                      database=self.config['db_name'], \
                      user=self.config['db_user'], \
                      password=self.config['db_pass'], \
                      host=self.config['db_host'], \
                      port=self.config['db_port'], \
                      loop=loop)
    else:
      self.db_pool = await asyncpg.create_pool( \
                      database=self.config['db_name'], \
                      user=self.config['db_user'], \
                      password=self.config['db_pass'], \
                      host=self.config['db_host'], \
                      port=self.config['db_port'])


def run_fn_with_context(fn, name, config, *args):
  ctxt = TaskContext(name, config)
  ctxt.loop = asyncio.new_event_loop()
  ctxt.loop.run_until_complete(ctxt.async_init(ctxt.loop))
  result = fn(ctxt, *args)
  ctxt.loop.close()
  return result

def run_coro_with_context(coro, name, config, *args):
  ctxt = TaskContext(name, config)
  ctxt.loop = asyncio.new_event_loop()
  ctxt.loop.run_until_complete(ctxt.async_init(ctxt.loop))
  result = ctxt.loop.run_until_complete(coro(ctxt, *args))
  ctxt.loop.close()
  return result


#################
#
class Engine:
  """
  An engine that runs a DAG of Python coroutines
  """
  def __init__(self, plan, name='etl-engine', nprocs=2, loglevel=logging.INFO):
    faulthandler.register(signal.SIGUSR1)
    # An engine identifier.
    self.name = name

    # Number of processes
    self.nprocs = nprocs

    # Configure engine logging.
    self.log = logging.getLogger(self.name)
    self.log.setLevel(loglevel)

    sh = logging.StreamHandler()
    formatter = logging.Formatter(ENGINE_LOG_FMT)
    sh.setFormatter(formatter)
    self.log.addHandler(sh)
    self.log.propagate = False

    # Plan
    if type(plan) != Plan:
      raise TypeError("First argument to Engine must be a plan of type 'Plan'")
    self.tasks = plan.plan

    # Downstreams are reverse dependencies (i.e., task name => task destinations)
    # and represent parent->child task edges.
    self.downstreams = {}

    # Counters for pending parents, i.e., task names => # unfinished parents
    self.dep_counters = {}

    # Counters for pending downstreams, i.e., task names => # unfinished downstreams
    self.gc_counters = {}

    # Initialize downstreams and counters from task graph.
    for task_id, task_data in self.tasks.items():
      dependencies, _ = task_data
      self.dep_counters[task_id] = len(dependencies)
      for d in dependencies:
        self.downstreams[d] = self.downstreams.get(d, []) + [task_id]
        self.gc_counters[d] = self.gc_counters.get(d, 0) + 1

    # A dictionary of task futures, mapping task names => future.
    self.task_futures = {}

    # A dictionary of task results, mapping task names => result object.
    # These results are garbage collected when no longer needed by any downstream task.
    self.task_results = {}

    # Executor
    self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=self.nprocs)

    # A queue of tasks ready, and waiting to run.
    self.pending_queue = deque()

    # Prepare initial tasks.
    self.schedule()

  def __str__(self):
    '''Returns a string representation of the engine'''
    _engine = '%s:' % self.name

    _engine += '\n  Tasks:'
    sorted_task_ids = sorted(self.task_futures)

    for t in sorted_task_ids:
      f = self.task_futures[t]
      task_status = 'Running'

      if f.done() and t in self.task_results:
        task_status = 'Done, with result'
      if f.done():
        task_status = 'Done, no result'
      if f.cancelled():
        task_status = 'Cancelled'

      _engine += '\n    Task: %s %s [%s %s %s]' \
                    % (t, task_status, \
                       self.dep_counters[t], \
                       self.downstreams[t] if t in self.downstreams else [], \
                       self.gc_counters[t] if t in self.gc_counters else 0)

    return _engine


  # Return if a task has completed
  def completed(self, dependencies):
    dones = [task_id in self.task_futures \
              and self.task_futures[task_id].done() \
              and task_id in self.task_results \
              for task_id in dependencies]

    return all(dones)

  # Check if task is ready, as either task has no dependencies,
  # or all dependencies have completed futures and results.
  def schedule_task(self, task_id):
    if not self.completed([task_id]):
      dependencies, _ = self.tasks[task_id]
      if (not dependencies) or self.completed(dependencies):
        self.log.debug('Enqueueing "%s"' % task_id)
        self.pending_queue.append(task_id)
      else:
        self.log.debug('Scheduler passing on %s (not ready)' % task_id)
    else:
      self.log.debug('Scheduler passing on %s (completed)' % task_id)

  # Adds tasks to the queue based on their readiness.
  def schedule(self, just_ran=None):
    # Schedule downstream neighbors of a task if it just ran,
    # otherwise attempt to schedule all tasks.
    schedulable_tasks = []
    if just_ran:
      for task_ran in just_ran:
        if task_ran in self.tasks and task_ran in self.downstreams:
          for next_task in self.downstreams[task_ran]:
            self.log.debug('Attempting to schedule %s, barrier: %s' % (next_task, self.dep_counters[next_task]))
            self.dep_counters[next_task] -= 1
            if self.dep_counters[next_task] <= 0:
              schedulable_tasks.append(next_task)

    else:
      schedulable_tasks = [k for k,v in self.dep_counters.items() if v == 0]

    for task_id in schedulable_tasks:
      self.schedule_task(task_id)

  # Task result garbage collection.
  # Releases dependencies' results when all downstreams of dependencies are complete.
  def gc_dependencies(self, task_id):
    if task_id in self.tasks:
      dependencies, _ = self.tasks[task_id]
      for d in dependencies:
        self.gc_counters[d] -= 1
        if self.gc_counters[d] <= 0:
          self.task_results[d] = None
          self.log.info('Engine garbage collecting %s' % d)
    else:
      self.log.error('Invalid task id "%s"' % task_id)


  # Runs all (independent) pending tasks
  def run_block(self):
    ids_and_futures = []
    while self.pending_queue:
      task_id = self.pending_queue.popleft()

      self.log.debug('Engine dispatching task: %s' % task_id)

      # Execute task_id using outputs from dependencies.
      if task_id in self.tasks:
        dependencies, task_body = self.tasks[task_id]
        args = [self.task_results[d] if d in self.task_results else None for d in dependencies]
        if 'args' in task_body:
          args += task_body['args']

        if 'fn' in task_body:
          self.log.debug('Starting task function %s' % task_id)
          future = asyncio.get_event_loop().run_in_executor( \
                    self.executor, run_fn_with_context, \
                    task_body['fn'], task_id, task_body['config'], *args)

        elif 'coro' in task_body:
          self.log.debug('Starting task coroutine %s' % task_id)
          future = asyncio.get_event_loop().run_in_executor( \
                    self.executor, run_coro_with_context, \
                    task_body['coro'], task_id, task_body['config'], *args)

        else:
          self.log.error('Invalid function body for %s' % task_id)

        self.task_futures[task_id] = future
        ids_and_futures.append((task_id, future))

      else:
        self.log.error('Invalid task id "%s"' % task_id)

    return ids_and_futures

  async def run(self):
    iteration = 0

    self.log.debug('Running ' + str(self))

    while len(self.task_results) != len(self.tasks):
      # Launch a group of parallel tasks.
      active = self.run_block()
      finished = []

      # If we did not launch anything, retrieve the currently running tasks.
      if not active:
        for task_id, future in self.task_futures.items():
          if not(future.done() or future.cancelled()):
            active.append((task_id, future))
          elif task_id not in self.task_results:
            finished.append((task_id, future))

        self.log.debug('Active Queue (iter %s) %s' % (iteration, str([idf[1] for idf in active])))
        self.log.debug('Recently Completed (iter %s) %s' % (iteration, str([idf[1] for idf in finished])))

      if not(active or finished):
        self.log.error('Engine has no tasks to wait on (no tasks launched/active/recently finished)')
        self.log.error(str(self))
        break

      if active:
        # Wait for the first completion amongst the launched or running tasks.
        await asyncio.wait([idf[1] for idf in active], return_when=asyncio.FIRST_COMPLETED)

        # Track finished tasks.
        finished.extend([idf for idf in active if idf[1].done()])

      self.log.info('Engine (iter %s) completed %s' % (iteration, str([f[0] for f in finished])))

      for task_id, future in finished:
        self.task_results[task_id] = future.result()
        self.gc_dependencies(task_id)

      # Schedule next tasks
      if finished:
        self.schedule(just_ran=[f[0] for f in finished])
        self.log.debug('Queue (iter %s) %s' % (iteration, str(list(self.pending_queue))))
      else:
        self.log.debug('Scheduler no-op, no new tasks completed')

      self.log.info('Engine progress (iter %s): %s / %s tasks completed' % (iteration, len(self.task_results), len(self.tasks)))
      iteration += 1

    if len(self.task_results) == len(self.tasks):
      self.log.info('Engine completed.')

    else:
      self.log.info('Engine failed to complete tasks, only %s / %s finished.' % (len(self.task_results), len(self.tasks)))

