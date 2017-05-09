from etl.core.task import Task
from etl.core.plan import Plan
from etl.core.engine import Engine
from inspect import signature
import logging
import asyncio
import os
TEST_LOG_FMT = '%(asctime)s|%(funcName)s|%(process)s-%(thread)s|%(levelname)s|%(message)s'
logging.basicConfig(level=logging.INFO, format=TEST_LOG_FMT)


def abc(ctxt, arg1, arg2):
    ctxt.log.info("{} {}".format(arg1, arg2))
    return None

t1 = Task(name='task_a', fn=abc, args=["hello", "world"])

print(t1)


p1 = Plan(name='plan_a', config={
  'db_name': os.environ['db_name'],
  'db_user': os.environ['db_user'],
  'db_pass': os.environ['db_password'],
  'db_host': os.environ['db_host'],
  'db_port': os.environ['db_port']
})
p1.add(t1)

print(p1)
print(p1.plan)

engine = Engine(
    plan = p1,
    name = "etl-engine",
    nprocs = 1,
    loglevel = logging.DEBUG,
)

loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(engine.run())
loop.close()
