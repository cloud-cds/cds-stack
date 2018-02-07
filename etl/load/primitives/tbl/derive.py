"""
derive functions
"""
import sys
import etl.mappings.confidence as confidence
import datetime
from etl.transforms.primitives.row.transform import STOPPED_ACTIONS
# from etl.load.primitives.tbl.acute_liver_failure_update import *
# from etl.load.primitives.tbl.acute_pancreatitis_update import *
from etl.load.primitives.tbl.admit_weight_update import *
from etl.load.primitives.tbl.change_since_last_measured import *
from etl.load.primitives.tbl.hemorrhage_update import *
from etl.load.primitives.tbl.hemorrhagic_shock_update import *
from etl.load.primitives.tbl.septic_shock_iii_update import *
from etl.load.primitives.tbl.time_since_last_measured import *
from etl.load.primitives.row import load_row
from etl.load.primitives.tbl import clean_tbl
from etl.load.primitives.tbl.derive_helper import *
from etl.load.primitives.tbl.cardiogenic_shock_feats import *


def derive(fid, func_id, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  this_mod = sys.modules[__name__]
  func = getattr(this_mod, func_id)
  return func(fid, fid_input, conn, log, dataset_id, derive_feature_addr,
              cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours)

def with_ds(dataset_id, table_name=None, conjunctive=True):
  if dataset_id is not None:
    return '%s %sdataset_id = %s' % (' and' if conjunctive else ' where', '' if table_name is None else table_name+'.', dataset_id)
  return ''

async def lookup_population_mean(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  check if fid_popmean exists or not;
  if not, calculate it
  NOTE: if the fid does not exist for fid_popmean, then fid_popmean is null in
  cdm_g
  """
  select_sql = "SELECT value FROM cdm_g WHERE fid = '%(fid)s'%(with_ds)s" % {'fid': fid, 'with_ds': with_ds(dataset_id) }
  popmean = await conn.fetchrow(select_sql)
  if popmean is not None:
    log.info("lookup_population_mean %s %s" % (fid, popmean['value']))
  else:
    log.warn("cannot find %s in cdm_g" % fid)
    calculate_popmean_sql = \
      "SELECT avg(%(fid)s) FROM %(twf_table)s where %(fid)s is not null%(with_ds)s" \
        % {'fid':fid_input, 'twf_table':twf_table, 'with_ds': with_ds(dataset_id)}
    popmean = await conn.fetchrow(calculate_popmean_sql)
    if popmean is not None:
      await load_row.insert_g([fid, popmean[0], confidence.POPMEAN])
      log.info("lookup_population_mean %s %s" \
        % (fid, popmean[0]))
    else:
      log.error("lookup_population_mean %s" % fid)

# Same as any_continuous_dose_update (special case)
async def any_antibiotics_order_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid should be any_antibiotics_order (T, boolean)
  fid_input should be a list of dose
  """
  assert fid == 'any_antibiotics_order', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  for i in range(len(fid_input_items)):
    assert fid_input_items[i].endswith('dose'), \
      'wrong fid_input %s' % fid_input
  await any_med_order_update(fid, fid_input, conn, log, dataset_id, incremental, cdm_t_target, cdm_t_lookbackhours)

async def any_pressor_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid should be any_pressor (T, boolean)
  fid_input should be a list of dose
  """
  assert fid == 'any_pressor', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  for i in range(len(fid_input_items)):
    assert fid_input_items[i].endswith('dose'), \
      'wrong fid_input %s' % fid_input
  for dose in fid_input_items:
    await any_continuous_dose_update(fid, dose, conn, log, dataset_id=dataset_id, incremental=incremental, cdm_t_target=cdm_t_target)

async def any_inotrope_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid should be any_inotrope (T, boolean)
  fid_input should be a list of dose
  """
  assert fid == 'any_inotrope', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  for i in range(len(fid_input_items)):
    assert fid_input_items[i].endswith('dose'), \
      'wrong fid_input %s' % fid_input
  if dataset_id and not incremental:
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id=dataset_id,
                                           incremental=incremental, cdm_t_target=cdm_t_target))
  for dose in fid_input_items:
    await any_continuous_dose_update(fid, dose, conn, log,
                                     dataset_id=dataset_id,
                                     incremental=incremental, cdm_t_target=cdm_t_target)

# Special case
async def any_continuous_dose_update(fid, dose, conn, log, dataset_id=None,
                                     incremental=False, cdm_t_target='cdm_t'):
  global STOPPED_ACTIONS
  select_sql = """
    select enc_id, tsp, value::json->>'action' as action, confidence
    from %(cdm_t)s as cdm_t where fid = '%(dose)s'%(with_ds)s %(incremental_enc_id_in)s
    order by enc_id, tsp
  """ % {'cdm_t': cdm_t_target,'dose': dose, 'with_ds': with_ds(dataset_id),
         'incremental_enc_id_in': incremental_enc_id_in(' and ', 'cdm_t', dataset_id,
                                                        incremental)}
  log.info("any_continuous_dose_update: sql: {}".format(select_sql))
  records = await conn.fetch(select_sql)
  block = {'enc_id':None, 'start_tsp':None, 'end_tsp':None,
       'start_c': 0, 'end_c': 0}
  for rec in records:
    enc_id = rec['enc_id']
    tsp = rec['tsp']
    action = rec['action']
    c = rec['confidence']
    if block['enc_id'] is None and not action in STOPPED_ACTIONS:
      block['enc_id'] = enc_id
      block['start_tsp'] = tsp
      block['start_c'] = c
    elif block['enc_id'] == enc_id:
      if action in STOPPED_ACTIONS:
        block['end_tsp'] = tsp
        block['end_c'] = c
        # block is reaty to update
        await update_continuous_dose_block(fid, block, conn, log,
                                           dataset_id, cdm_t_target)
        block = {'enc_id':None, 'start_tsp':None, 'end_tsp':None,
             'start_c': 0, 'end_c': 0}
    elif block['enc_id'] != enc_id and not action in STOPPED_ACTIONS:
      # update current block
      await update_continuous_dose_block(fid, block, conn, log,
                                         dataset_id, cdm_t_target)
      # create new block
      block = {'enc_id':enc_id, 'start_tsp':tsp, 'end_tsp':None,
           'start_c': 0, 'end_c': 0}

# Special case
async def update_continuous_dose_block(fid, block, conn, log, dataset_id, cdm_t_target):
  select_sql = """
    select value from %s cdm_t where enc_id = %s and fid = '%s'%s
    and tsp <= timestamptz '%s'
    order by tsp DESC
  """ % (cdm_t_target, block['enc_id'], fid, with_ds(dataset_id), block['start_tsp'])
  prev = await conn.fetchrow(select_sql)
  if prev is None or prev['value'] == 'False':
    await load_row.upsert_t(conn, [block['enc_id'], block['start_tsp'], fid,
            True, block['start_c']], dataset_id=dataset_id)
  if block['end_tsp'] is None:
    delete_sql = """
      delete from %s cdm_t where enc_id = %s and fid = '%s'%s
      and tsp > timestamptz '%s'
    """ % (cdm_t_target, block['enc_id'], fid, with_ds(dataset_id), block['start_tsp'])
    await conn.execute(delete_sql)
  else:
    delete_sql = """
      delete from %s cdm_t where enc_id = %s and fid = '%s'%s
      and tsp > timestamptz '%s' and tsp <= timestamptz '%s'
    """ % (cdm_t_target, block['enc_id'], fid, with_ds(dataset_id), block['start_tsp'], block['end_tsp'])
    await conn.execute(delete_sql)
    select_sql = """
      select value from %s cdm_t where enc_id = %s and fid = '%s'%s
      and tsp >= timestamptz '%s' order by tsp
    """ % (cdm_t_target, block['enc_id'], fid, with_ds(dataset_id), block['end_tsp'])
    post = await conn.fetchrow(select_sql)
    if post is None or post['value'] == 'True':
      await load_row.upsert_t(conn, [block['enc_id'], block['end_tsp'], fid,
              False, block['end_c']], dataset_id = dataset_id, cdm_t_target=cdm_t_target)



# Special case
async def any_med_order_update(fid, fid_input, conn, log, dataset_id=None,
                               incremental=False, cdm_t_target='cdm_t', cdm_t_lookbackhours=None):
  # Updated on 3/19/2016
  if dataset_id and not incremental:
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id=dataset_id,
                                           incremental=incremental, cdm_t_target=cdm_t_target))
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  doses = '|'.join(fid_input_items)
  lookbackhours = " and now() - cdm_t.tsp <= '{}'::interval".format(cdm_t_lookbackhours) if cdm_t_lookbackhours is not None else ''
  if dataset_id:
    select_sql = """
      SELECT distinct enc_id,
        value::json->>'order_tsp' order_tsp,
        max(confidence) confidence FROM %s cdm_t
      WHERE fid ~ '%s'%s AND cast(value::json->>'dose' as numeric) > 0
      %s %s
      group by enc_id, order_tsp
    """ % (cdm_t_target, doses, with_ds(dataset_id),
           incremental_enc_id_in(' and ', 'cdm_t', dataset_id, incremental),
           lookbackhours)
  else:
    orders = '|'.join(f[:-5] for f in fid_input_items)
    select_sql = """
      SELECT distinct enc_id,
        tsp order_tsp,
        max(confidence) confidence FROM %s cdm_t
      WHERE fid ~ '^(%s)_dose_order$' AND value::numeric > 0
      %s
      group by enc_id, order_tsp
    """ % (cdm_t_target, orders, lookbackhours)
  log.info("query: {}".format(select_sql))
  rows = await conn.fetch(select_sql)
  for row in rows:
    if row['order_tsp']:
      values = [row['enc_id'], row['order_tsp'], fid, "True",
            row['confidence']]
      await load_row.upsert_t(conn, values, dataset_id=dataset_id, cdm_t_target=cdm_t_target)







# Special case
async def suspicion_of_infection_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  # 03/20/2016
  assert fid == 'suspicion_of_infection', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid_input_items[0] == 'culture_order' \
    and fid_input_items[1] == 'any_antibiotics_order', \
    'wrong fid_input %s' % fid_input
  if dataset_id and not incremental:
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id=dataset_id, incremental=incremental))
  select_sql = """
    select t1.enc_id, t1.tsp, t1.confidence|t2.confidence confidence
      from %s t1
    inner join %s t2 on t1.enc_id = t2.enc_id
      and t2.fid = 'any_antibiotics_order'
      and t2.tsp >= t1.tsp and t2.tsp <= t1.tsp + interval '72 hours'
    where t1.fid = 'culture_order'%s%s%s
  """ % (cdm_t_target, cdm_t_target,
         with_ds(dataset_id, table_name='t1'),
         with_ds(dataset_id, table_name='t2'),
         incremental_enc_id_in(' and ', 't1', dataset_id, incremental),
         )
  log.info(select_sql)
  rows = await conn.fetch(select_sql)
  for row in rows:
    values = [row['enc_id'], row['tsp'], fid, "True",
          row['confidence']]
    await load_row.upsert_t(conn, values, dataset_id)
  select_sql = """
    select t1.enc_id, t1.tsp, t1.confidence|t2.confidence confidence
      from %s t1
    inner join %s t2 on t1.enc_id = t2.enc_id
      and t2.fid = 'culture_order'
      and t2.tsp >= t1.tsp and t2.tsp <= t1.tsp + interval '24 hours'
    where t1.fid = 'any_antibiotics_order'%s%s%s
  """ % (cdm_t_target, cdm_t_target,
         with_ds(dataset_id, table_name='t1'),
         with_ds(dataset_id, table_name='t2'),
         incremental_enc_id_in(' and ', 't1', dataset_id, incremental),
         )
  rows = await conn.fetch(select_sql)
  for row in rows:
    values = [row['enc_id'], row['tsp'], fid, "True",
          row['confidence']]
    await load_row.upsert_t(conn, values, dataset_id, cdm_t_target)











# Special subquery (subquery with if-then-else cases)
async def hypotension_intp_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  assert fid == 'hypotension_intp', 'wrong fid %s' % fid
  assert fid_input == 'hypotension_raw', 'wrong fid_input %s' % fid
  twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
  await conn.execute(clean_tbl.cdm_twf_clean(fid, value=False, dataset_id=dataset_id, twf_table=twf_table_temp, incremental=incremental))
  fid_input_twf_table_temp = derive_feature_addr[fid_input]['twf_table_temp']
  select_sql = """
  SELECT *
  FROM
    (SELECT enc_id, tsp, hypotension_raw, coalesce(hypotension_raw_c, 0) as
    hypotension_raw_c,
        lag(enc_id) over w as enc_id_prev,
        lag(tsp) over w as tsp_prev,
        lag(hypotension_raw) over w as hypotension_raw_prev
     FROM %(twf_table)s%(with_ds)s %(incremental_enc_id_in)s
     WINDOW w as (order by enc_id,tsp)
     ORDER by enc_id, tsp) as query
  WHERE (query.hypotension_raw OR query.hypotension_raw_prev) OR
      query.enc_id IS DISTINCT FROM query.enc_id_prev
  """ % {'twf_table': fid_input_twf_table_temp,
         'with_ds': with_ds(dataset_id, table_name=fid_input_twf_table_temp, \
            conjunctive=False),
         'incremental_enc_id_in': incremental_enc_id_in(' and ' if dataset_id else ' where ', \
            fid_input_twf_table_temp, dataset_id, incremental)}
  log.debug(select_sql)
  records = await conn.fetch(select_sql)
  block_start = None
  block_end = None
  block_c = 0
  block_enc_id = None
  for rec in records:
    if rec['enc_id'] != rec['enc_id_prev']:
      # new enc_id
      # if previous block stands long enough, update
      if block_start:
        block_end = rec['tsp_prev']
        duration = block_end - block_start
        if duration.total_seconds() >= 30*60: # 30 minutes
          # update
          update_sql = """
          UPDATE %(twf_table_temp)s SET hypotension_intp = 'True',
          hypotension_intp_c = %(confidence)s
          WHERE enc_id = %(enc_id)s%(with_ds)s
          AND tsp >= timestamptz '%(tsp_start)s'
          AND tsp <= timestamptz '%(tsp_end)s'
          """ % {'confidence': block_c, 'enc_id': block_enc_id,
               'tsp_start': block_start, 'tsp_end': block_end,
               'twf_table_temp': twf_table_temp, 'with_ds': with_ds(dataset_id)}
          await conn.execute(update_sql)
      # clear the block
      block_start = None
      block_end = None
      block_c = 0
      block_enc_id = rec['enc_id']
    if rec['hypotension_raw'] and not rec['hypotension_raw_prev']:
      # from false to true, start a new block
      block_start = rec['tsp']
      block_c = block_c | rec['hypotension_raw_c']
    elif not rec['hypotension_raw'] and rec['hypotension_raw_prev']:
      # from true to false, end a block
      block_end = rec['tsp_prev']
      if block_start:
        # need test
        duration = block_end - block_start
        if duration.total_seconds() >= 30*60: # 30 minutes
          # update
          update_sql = """
          UPDATE %(twf_table_temp)s SET hypotension_intp = 'True',
          hypotension_intp_c = %(confidence)s
          WHERE enc_id = %(enc_id)s%(with_ds)s
          AND tsp >= timestamptz '%(tsp_start)s'
          AND tsp <= timestamptz '%(tsp_end)s'
          """ % {'confidence': block_c, 'enc_id': block_enc_id,
               'tsp_start': block_start, 'tsp_end': block_end,
               'twf_table_temp': twf_table_temp, 'with_ds': with_ds(dataset_id)}
          await conn.execute(update_sql)
      block_start = None
      block_end = None
      block_c = 0
    else:
      # both current and previous hypotension_raw are 1
      block_c = block_c | rec['hypotension_raw_c']

# Special subquery (multiple subqueries)
async def sirs_intp_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  SIRS_INTP 30m version
  """
  # update 4/17/2016
  assert fid == 'sirs_intp', 'wrong fid %s' % fid
  assert fid_input == 'sirs_raw', 'wrong fid_input %s' % fid
  twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
  await conn.execute(clean_tbl.cdm_twf_clean(fid, value=False, confidence=0, twf_table=twf_table_temp, dataset_id=dataset_id, incremental=incremental))
  # 1. Identify all periods of time where sirs_raw=1 for
  # at least 1 consecutive hours and set sirs_intp=1
  fid_input_twf_table_temp = derive_feature_addr[fid_input]['twf_table_temp']
  select_sql = """
  SELECT *
  FROM
    (SELECT enc_id, tsp, sirs_raw, sirs_raw_c,
        lag(enc_id) over w as enc_id_prev,
        lag(tsp) over w as tsp_prev,
        lag(sirs_raw) over w as sirs_raw_prev
     FROM %(twf_table)s%(with_ds)s
     %(incremental_enc_id_in)s
     WINDOW w as (order by enc_id,tsp)
     ORDER by enc_id, tsp) as query
  WHERE (query.sirs_raw OR query.sirs_raw_prev) OR
      query.enc_id IS DISTINCT FROM query.enc_id_prev
  """ % {'twf_table': fid_input_twf_table_temp,
         'with_ds': with_ds(dataset_id, table_name=fid_input_twf_table_temp, \
            conjunctive=False),
         'incremental_enc_id_in': incremental_enc_id_in(' and ' if dataset_id else ' where ', \
            fid_input_twf_table_temp, dataset_id, incremental)}
  log.info(select_sql)
  records = await conn.fetch(select_sql)
  block_start = None
  block_end = None
  block_c = 0
  block_enc_id = None
  for rec in records:
    if rec['enc_id'] != rec['enc_id_prev']:
      # new enc_id
      # if previous block stands long enough, update
      if block_start:
        block_end = rec['tsp_prev']
        duration = block_end - block_start
        if duration.total_seconds() >= 30*60: # half an hour
          # update
          update_sql = """
          UPDATE %(twf_table)s SET sirs_intp = 'True',
          sirs_intp_c = %(confidence)s
          WHERE enc_id = %(enc_id)s%(with_ds)s
          AND tsp >= timestamptz '%(tsp_start)s'
          AND tsp <= timestamptz '%(tsp_end)s'
          """ % {'confidence': block_c, 'enc_id': block_enc_id,
               'tsp_start': block_start, 'tsp_end': block_end,
               'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)}
          await conn.execute(update_sql)
      # clear the block
      block_start = None
      block_end = None
      block_c = 0
      block_enc_id = rec['enc_id']
    if rec['sirs_raw'] and not rec['sirs_raw_prev']:
      # from false to true, start a new block
      block_start = rec['tsp']
      if rec['sirs_raw_c']:
        block_c = block_c | rec['sirs_raw_c']
    elif not rec['sirs_raw'] and rec['sirs_raw_prev']:
      # from true to false, end a block
      block_end = rec['tsp_prev']
      if block_start:
        # need test
        duration = block_end - block_start
        if duration.total_seconds() >= 30*60: # half an hour
          # update
          update_sql = """
          UPDATE %(twf_table)s SET sirs_intp = 'True',
          sirs_intp_c = %(confidence)s
          WHERE enc_id = %(enc_id)s%(with_ds)s
          AND tsp >= timestamptz '%(tsp_start)s'
          AND tsp <= timestamptz '%(tsp_end)s'
          """ % {'confidence': block_c, 'enc_id': block_enc_id,
               'tsp_start': block_start, 'tsp_end': block_end,
               'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)}
          await conn.execute(update_sql)
      block_start = None
      block_end = None
      block_c = 0
    else:
      # both current and previous sirs_raw are 1
      if rec['sirs_raw_c']:
        block_c = block_c | rec['sirs_raw_c']
  # 2. If two periods where sirs_intp=1 are within 1 hours of each other,
  # set sirs_intp=1 at all sample times between the two periods
  select_sql = """
  SELECT *
  FROM
    (SELECT enc_id, tsp, sirs_intp, sirs_intp_c,
        lag(enc_id) over w as enc_id_prev,
        lag(tsp) over w as tsp_prev,
        lag(sirs_intp) over w as sirs_intp_prev,
        lag(sirs_intp_c) over w as sirs_intp_c_prev
     FROM %(twf_table)s%(with_ds)s
     %(incremental_enc_id_in)s
     WINDOW w as (order by enc_id,tsp)
     ORDER by enc_id, tsp) as query
  WHERE (query.sirs_intp or query.sirs_intp_prev) OR
      (query.enc_id IS DISTINCT FROM query.enc_id_prev)
  """ % {'twf_table': twf_table_temp,
         'with_ds': with_ds(dataset_id, table_name=twf_table_temp, \
            conjunctive=False),
         'incremental_enc_id_in': incremental_enc_id_in(' and ' if dataset_id else ' where ', \
            twf_table_temp, dataset_id, incremental)}
  log.info(select_sql)
  records = await conn.fetch(select_sql)
  interval_start = None
  interval_end = None
  interval_c = 0
  interval_enc_id = -1
  for rec in records:
    if interval_enc_id != rec['enc_id']:
      # new enc_id
      interval_start = None
      interval_end = None
      interval_c = 0
      interval_enc_id = rec['enc_id']
    if rec['sirs_intp_prev'] and not rec['sirs_intp']:
      # new interval
      interval_start = rec['tsp_prev']
      if rec['sirs_intp_c_prev']:
        interval_c = interval_c | rec['sirs_intp_c_prev']
    elif not rec['sirs_intp_prev'] and rec['sirs_intp']:
      # interval end
      interval_end = rec['tsp']
      if rec['sirs_intp_c']:
        interval_c = interval_c | rec['sirs_intp_c']
      if interval_start is not None \
        and (interval_end - interval_start).total_seconds() \
          <= 1800: # half an hour
        # update
        update_sql = """
          UPDATE %(twf_table)s SET sirs_intp = 'True',
          sirs_intp_c = %(confidence)s
          WHERE enc_id = %(enc_id)s%(with_ds)s
          AND tsp > timestamptz '%(tsp_start)s'
          AND tsp < timestamptz '%(tsp_end)s'
          """ % {'confidence': interval_c, 'enc_id': interval_enc_id,
               'tsp_start': interval_start, 'tsp_end': interval_end,
               'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)}
        await conn.execute(update_sql)
      interval_start = None
      interval_end = None
      interval_c = 0


# Subquery chain
async def septic_shock_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  # UPDATE 8/19/2016
  assert fid == 'septic_shock', 'wrong fid %s' % fid
  select_sql = """
  select enc_id, tsp, coalesce(%(conf)s,0) conf from %(twf_table)s
  where %(condition)s %(with_ds)s
  """

  update_clause = """
  UPDATE %(twf_table)s SET septic_shock = coalesce(septic_shock, 0) | %(flag)s,
    septic_shock_c = coalesce(septic_shock_c, 0) | %(conf)s
    WHERE
    enc_id = %(enc_id)s
    and tsp >= timestamptz '%(tsp)s'
    and tsp < timestamptz '%(tsp)s' + interval '6 hours'
    %(with_ds)s
    ;
  """
  severe_sepsis_twf_table_temp = derive_feature_addr['severe_sepsis']['twf_table_temp'] if 'severe_sepsis' in derive_feature_addr else get_src_twf_table(derive_feature_addr)
  twf_table_temp = derive_feature_addr['septic_shock']['twf_table_temp']
  records = \
    await conn.fetch((select_sql + ' %(incremental_enc_id_in)s') \
      % {'condition':'severe_sepsis is true',
         'conf': 'severe_sepsis_c',
         'twf_table': severe_sepsis_twf_table_temp,
         'with_ds': with_ds(dataset_id),
         'incremental_enc_id_in': incremental_enc_id_in(' and ', \
            severe_sepsis_twf_table_temp, dataset_id, incremental)})
  for rec in records:
    await conn.execute(update_clause \
      % {'conf':rec['conf'],
         'flag':1,
         'enc_id': rec['enc_id'],
         'tsp': rec['tsp'],
         'twf_table': twf_table_temp,
         'with_ds': with_ds(dataset_id)})

  subquery = get_select_table_joins(['hypotension_intp', 'lactate'], derive_feature_addr, cdm_feature_dict, dataset_id, incremental)
  records = \
    await conn.fetch(select_sql \
      % {'condition':'hypotension_intp is true or lactate > 4',
         'conf': 'hypotension_intp_c | lactate_c',
         'twf_table':'(' + subquery + ') source',
         'with_ds': with_ds(dataset_id)})
  for rec in records:
    await conn.execute(update_clause \
      % {'conf': rec['conf'],
         'flag':2,
         'enc_id': rec['enc_id'],
         'tsp': rec['tsp'],
         'twf_table': twf_table_temp,
         'with_ds': with_ds(dataset_id)})
  subquery = get_select_table_joins(['fluid_resuscitation', 'vasopressor_resuscitation'], derive_feature_addr, cdm_feature_dict, dataset_id, incremental)
  records = \
    await conn.fetch(select_sql % \
      {'condition':'fluid_resuscitation is true or vasopressor_resuscitation is true',
       'conf': 'fluid_resuscitation_c | vasopressor_resuscitation_c',
       'twf_table': '(' + subquery + ') source',
       'with_ds': with_ds(dataset_id)})
  for rec in records:
    await conn.execute(update_clause \
      % {'conf':rec['conf'],
         'flag':4,
         'enc_id': rec['enc_id'],
         'tsp': rec['tsp'],
         'twf_table': twf_table_temp,
         'with_ds': with_ds(dataset_id)})

  update_clause = """
  UPDATE %(twf_table)s SET septic_shock = ( case when septic_shock = 7 then 1
    else 0 end)
  %(with_ds)s
  """ % {'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id, conjunctive=False)}
  await conn.execute(update_clause)



# Special case (mini-pipeline)
async def resp_sofa_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid should be resp_sofa
  fid_input should be (vent (T), pao2_to_fio2 (TWF))
  """
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid == 'resp_sofa', 'wrong fid %s' % fid
  assert fid_input_items[0] == 'vent' \
    and fid_input_items[1] == 'pao2_to_fio2', 'wrong fid_input %s' % fid_input
  twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
  await conn.execute(clean_tbl.cdm_twf_clean(fid, value=0, twf_table=twf_table_temp, dataset_id=dataset_id, incremental=incremental))
  update_clause = """
  INSERT INTO %(twf_table_temp)s (%(dataset_id)s
    enc_id, tsp, resp_sofa, resp_sofa_c)
  (
    WITH vent as (select enc_id,tsp,fid from %(cdm_t)s as cdm_t where fid = 'vent'
    %(with_ds)s %(incremental_enc_id_in)s)
    SELECT %(dataset_id)s source.enc_id, source.tsp,
      (CASE
      WHEN pao2_to_fio2 < 100 and fid is not null THEN 4
      WHEN pao2_to_fio2 < 200 and fid is not null THEN 3
      WHEN pao2_to_fio2 < 300 THEN 2
      WHEN pao2_to_fio2 < 400 THEN 1
      ELSE 0 END) %(fid)s,
      pao2_to_fio2_c %(fid)s_c
    FROM (
      %(twf_table_join)s
    ) source left join vent on source.enc_id = vent.enc_id
    and source.tsp = vent.tsp
  )
  ON CONFLICT (%(dataset_id)s enc_id,tsp) DO UPDATE SET
  %(fid)s = excluded.%(fid)s,
  %(fid)s_c = excluded.%(fid)s_c
  ;
  """ % {'fid':fid, 'cdm_t': cdm_t_target,
         'with_ds': with_ds(dataset_id, table_name='cdm_t'),
         'twf_table_temp': twf_table_temp,
         'dataset_id': 'dataset_id,' if dataset_id else '',
         'twf_table_join': get_select_table_joins(['pao2_to_fio2'], \
            derive_feature_addr, cdm_feature_dict, dataset_id, incremental),
         'incremental_enc_id_in': \
            incremental_enc_id_in(' and ', 'cdm_t', dataset_id, incremental)}
  log.info(update_clause)
  await conn.execute(update_clause)

# Special case (mini-pipeline)
async def cardio_sofa_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours=None):
  """
  fid should be cardio_sofa
  03/20/2016
  """
  global STOPPED_ACTIONS
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid == 'cardio_sofa', 'wrong fid %s' % fid
  assert fid_input_items[0] == 'map' and \
    fid_input_items[1] == 'dopamine_dose' \
    and fid_input_items[2] == 'epinephrine_dose'\
    and fid_input_items[3] == 'dobutamine_dose'\
    and fid_input_items[4] == 'levophed_infusion_dose' \
    and fid_input_items[5] == 'weight', 'wrong fid_input %s' \
    % fid_input
  src_twf_table = get_src_twf_table(derive_feature_addr)
  twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
  await conn.execute(clean_tbl.cdm_twf_clean(fid, value=0, twf_table=twf_table_temp, dataset_id=dataset_id, incremental=incremental))

  # update cardio_sofa based on map
  update_clause = """
  INSERT INTO %(twf_table_temp)s (%(dataset_id)s enc_id,tsp, cardio_sofa, cardio_sofa_c)
  SELECT %(dataset_id)s enc_id, tsp, 1 as cardio_sofa, map_c as cardio_sofa_c
  FROM %(map_table)s where map < 70%(with_ds)s %(incremental_enc_id_in)s
  ON CONFLICT (%(dataset_id)s enc_id,tsp) DO UPDATE SET
  cardio_sofa = excluded.cardio_sofa,
  cardio_sofa_c = excluded.cardio_sofa_c
  """ % {'fid':fid,
         'twf_table_temp': twf_table_temp,
         'with_ds': with_ds(dataset_id),
         'map_table': src_twf_table,
         'dataset_id': 'dataset_id, ' if dataset_id else '',
         'incremental_enc_id_in': incremental_enc_id_in(' and ', \
            src_twf_table, dataset_id, incremental)}
  log.info(update_clause)
  await conn.execute(update_clause)

  select_sql = """
  SELECT enc_id, tsp,
    value::json->>'action' as action, value::json->>'dose' as dose,
    confidence
    FROM %s cdm_t
  WHERE fid = '%s'%s %s %s ORDER BY enc_id, tsp
  """

  update_clause = """
  UPDATE %(twf_table)s SET cardio_sofa = %(threshold)s
  , cardio_sofa_c = %(confidence)s
  WHERE enc_id = %(enc_id)s%(with_ds)s
  AND cardio_sofa >= 1 AND cardio_sofa < %(threshold)s
  AND tsp >= timestamptz '%(tsp)s'
  ;
  """

  update_clause_with_max_tsp = """
  UPDATE %(twf_table)s SET cardio_sofa = %(threshold)s
  , cardio_sofa_c = %(confidence)s
  WHERE enc_id = %(enc_id)s%(with_ds)s
  AND cardio_sofa >= 1 AND cardio_sofa < %(threshold)s
  AND tsp >= timestamptz '%(tsp)s'
  AND tsp <= timestamptz '%(max_tsp)s'
  ;
  """
  lookbackhours = " and now() - cdm_t.tsp <= '{}'::interval".format(cdm_t_lookbackhours) if cdm_t_lookbackhours is not None else ''
  # update cardio_sofa based on dopamine_dose
  records = await conn.fetch(select_sql % \
      (cdm_t_target, 'dopamine_dose', with_ds(dataset_id),
       incremental_enc_id_in(' and ', 'cdm_t', dataset_id, incremental),
       lookbackhours))
  for i, rec in enumerate(records):
    action = rec['action']
    if not action in STOPPED_ACTIONS and rec['dose'] is not None:
      dopamine = float(rec['dose'])
      dopamine_c = rec['confidence']
      threshold = 0
      if dopamine > 15:
        threshold = 4
      elif dopamine > 5:
        threshold = 3
      else:
        threshold = 2
      if threshold > 0:
        if i+1 < len(records) \
          and rec['enc_id'] == records[i+1]['enc_id']:
          await conn.execute(update_clause_with_max_tsp \
            % {'threshold':threshold, 'confidence':dopamine_c,
               'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
               'max_tsp': records[i+1]['tsp'],
               'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})
        else:
          await conn.execute(update_clause \
            % {'threshold':threshold, 'confidence':dopamine_c,
               'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
               'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})

  # update cardio_sofa based on epinephrine_dose
  # need to check the unit first
  get_unit_sql = "SELECT unit FROM cdm_feature WHERE fid = '%s'%s" % ('epinephrine_dose', with_ds(dataset_id))
  unit = await conn.fetchrow(get_unit_sql)
  unit = unit['unit']
  if unit == 'mcg/kg/min':
    records = await conn.fetch(select_sql \
      % (cdm_t_target, 'epinephrine_dose',
         with_ds(dataset_id)),
         incremental_enc_id_in(' and ', 'cdm_t', dataset_id, incremental),
         lookbackhours)
  elif unit == 'mcg/min':
    select_sql_with_weight = """
      select sub.enc_id, sub.tsp,
        sub.value::json->>'action' as action,
        cast(sub.value::json->>'dose' as numeric)/last(weight) as dose,
        sub.confidence
      from
      (SELECT cdm_t.enc_id, cdm_t.tsp, cdm_t.value,
        cdm_t.confidence, twf.weight
        FROM %s cdm_t
        inner join %s twf
          on cdm_t.fid = '%s' and twf.enc_id = cdm_t.enc_id
          and cdm_t.tsp >= twf.tsp
        %s %s %s
        ORDER BY cdm_t.enc_id, cdm_t.tsp, twf.tsp
      ) as sub
      group by sub.enc_id, sub.tsp, sub.value, sub.confidence
    """
    lookbackhours2 = ((" where" if dataset_id is None and not incremental else " and") + " now() - cdm_t.tsp <= '{}'::interval".format(cdm_t_lookbackhours)) if cdm_t_lookbackhours is not None else ''
    sql = select_sql_with_weight % \
      (cdm_t_target, 'cdm_twf' if dataset_id else twf_table_temp, 'epinephrine_dose',
       with_ds(dataset_id, table_name='cdm_t', conjunctive=False),
       incremental_enc_id_in(' and ' if dataset_id else ' where ', 'twf', dataset_id, incremental), lookbackhours2)
    log.info("select_sql_with_weight:%s" % sql)
    records = await conn.fetch(sql)
  for i, rec in enumerate(records):
    action = rec['action']
    if not action in STOPPED_ACTIONS and rec['dose'] is not None:
      dose = float(rec['dose'])
      dose_c = rec['confidence']
      threshold = 0
      if dose > 0.1:
        threshold = 4
      elif dose <= 0.1:
        threshold = 3
      if threshold > 0:
        if i+1 < len(records) \
          and rec['enc_id'] == records[i+1]['enc_id']:
          await conn.execute(update_clause_with_max_tsp \
            % {'threshold':threshold, 'confidence':dose_c,
               'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
               'max_tsp': records[i+1]['tsp'],
               'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})
        else:
          await conn.execute(update_clause \
            % {'threshold':threshold, 'confidence':dose_c,
               'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
               'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})
  # update cardio_sofa based on dobutamine_dose
  records = await conn.fetch(select_sql % (cdm_t_target, 'dobutamine_dose', with_ds(dataset_id),incremental_enc_id_in(' and ', 'cdm_t', dataset_id, incremental), lookbackhours))
  for i, rec in enumerate(records):
    action = rec['action']
    if not action in STOPPED_ACTIONS and rec['dose'] is not None:
      if float(rec['dose']) > 0:
        dose_c = rec['confidence']
        if i+1 < len(records) \
          and rec['enc_id'] == records[i+1]['enc_id']:
          await conn.execute(update_clause_with_max_tsp \
            % {'threshold':2, 'confidence':dose_c,
               'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
               'max_tsp': records[i+1]['tsp'],
               'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})
        else:
          await conn.execute(update_clause \
            % {'threshold':2, 'confidence':dose_c,
               'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
               'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})
  # update cardio_sofa based on levophed_infusion_dose
  # need to check the unit of levophed_infusion_dose
  get_unit_sql = "SELECT unit FROM cdm_feature WHERE fid = '%s'%s" % ('levophed_infusion_dose', with_ds(dataset_id))
  unit = await conn.fetchrow(get_unit_sql)
  unit = unit['unit']
  if unit == 'mcg/kg/min':
    records = await conn.fetch(select_sql % (cdm_t_target, 'levophed_infusion_dose', with_ds(dataset_id),incremental_enc_id_in(' and ', 'cdm_t', dataset_id, incremental),lookbackhours))
  elif unit == 'mcg/min':
    select_sql_with_weight = """
      select sub.enc_id, sub.tsp,
        sub.value::json->>'action' as action,
        cast(sub.value::json->>'dose' as numeric)/last(weight) as dose,
        sub.confidence
      from
      (SELECT cdm_t.enc_id, cdm_t.tsp, cdm_t.value,
        cdm_t.confidence, twf.weight
        FROM %s cdm_t
        inner join %s twf
          on cdm_t.fid = '%s' and twf.enc_id = cdm_t.enc_id
          and cdm_t.tsp >= twf.tsp
        %s %s %s
        ORDER BY cdm_t.enc_id, cdm_t.tsp, twf.tsp
      ) as sub
      group by sub.enc_id, sub.tsp, sub.value, sub.confidence
    """
    sql = select_sql_with_weight % \
      (cdm_t_target, twf_table_temp, 'levophed_infusion_dose',
       with_ds(dataset_id, table_name='cdm_t', conjunctive=False),
       incremental_enc_id_in(' and ' if dataset_id else ' where ', 'twf', dataset_id,
                             incremental), lookbackhours2)
    log.info("select_sql_with_weight:%s" % sql)
    records = await conn.fetch(sql)

  for i, rec in enumerate(records):
    action = rec['action']
    if not action in STOPPED_ACTIONS and rec['dose'] is not None:
      dose = float(rec['dose'])
      if dose <= 0.1:
        threshold = 3
      else:
        threshold = 4
      dose_c = rec['confidence']
      if i+1 < len(records) \
        and rec['enc_id'] == records[i+1]['enc_id']:
        await conn.execute(update_clause_with_max_tsp \
          % {'threshold':threshold, 'confidence':dose_c,
             'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
             'max_tsp': records[i+1]['tsp'],
             'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})
      else:
        await conn.execute(update_clause \
          % {'threshold':threshold, 'confidence':dose_c,
             'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
             'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})


# Special case (mini-pipeline)
async def vasopressor_resuscitation_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid should be vasopressor_resuscitation (TWF, boolean)
  fid_input should be levophed_infusion_dose and dopamine_dose
  """
  assert fid == 'vasopressor_resuscitation', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid_input_items[0] == 'levophed_infusion_dose'\
    and fid_input_items[1] == 'dopamine_dose', \
    'wrong fid_input %s' % fid_input
  twf_table_temp = derive_feature_addr[fid]['twf_table_temp']

  await conn.execute(clean_tbl.cdm_twf_clean(fid, value=False, confidence=0, twf_table=twf_table_temp, dataset_id=dataset_id, incremental=incremental))
  select_sql = """
  select enc_id, tsp, value::json->>'action' as action from %s cdm_t
  where fid = '%s'%s %s order by enc_id, tsp;
  """

  update_sql_with_stop = """
  update %(twf_table)s set vasopressor_resuscitation = True
  where enc_id = %(enc_id)s%(with_ds)s and tsp >= timestamptz '%(begin)s' and tsp < timestamptz '%(end)s'
  """

  update_sql_wo_stop = """
  update %(twf_table)s set vasopressor_resuscitation = True
  where enc_id = %(enc_id)s%(with_ds)s and tsp >= timestamptz '%(begin)s'
  """

  records = await conn.fetch(\
    select_sql % (cdm_t_target, 'levophed_infusion_dose',
                  with_ds(dataset_id),
                  incremental_enc_id_in(' and ', 'cdm_t', dataset_id, incremental)))
  enc_id_cur = None
  start_tsp = None
  stop_tsp = None
  for rec in records:
    enc_id = rec['enc_id']
    tsp = rec['tsp']
    action = rec['action']
    if enc_id_cur is not None and enc_id_cur != enc_id:
      # update current enc_id
      await conn.execute(update_sql_wo_stop % {'enc_id':enc_id_cur,
                           'begin':start_tsp,
                           'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})
      enc_id_cur = None
      start_tsp = None
      stop_tsp = None
    if action == 'Stopped':
      stop_tsp = tsp
      if enc_id_cur is not None:
        # log.info(update_sql_with_stop % (enc_id_cur,\
        #     start_tsp, stop_tsp))
        await conn.execute(update_sql_with_stop % {'enc_id':enc_id_cur,
                               'begin':start_tsp,
                               'end':stop_tsp,
                               'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})
      enc_id_cur = None
      start_tsp = None
      stop_tsp = None
    else:
      if enc_id_cur is None:
        enc_id_cur = enc_id
        start_tsp = tsp
  if enc_id_cur is not None:
    await conn.execute(update_sql_wo_stop % {'enc_id':enc_id_cur,
                         'begin':start_tsp,
                         'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})

  records = await conn.fetch(\
    select_sql % (cdm_t_target, 'dopamine_dose',
                  with_ds(dataset_id),
                  incremental_enc_id_in(' and ', 'cdm_t', dataset_id, incremental)))
  enc_id_cur = None
  start_tsp = None
  stop_tsp = None
  for rec in records:
    enc_id = rec['enc_id']
    tsp = rec['tsp']
    action = rec['action']
    if enc_id_cur is not None and enc_id_cur != enc_id:
      # update current enc_id
      await conn.execute(update_sql_wo_stop % {'enc_id':enc_id_cur,
                           'begin':start_tsp,
                           'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})
      enc_id_cur = None
      start_tsp = None
      stop_tsp = None
    if action == 'Stopped':
      stop_tsp = tsp
      if enc_id_cur is not None:
        await conn.execute(update_sql_with_stop % {'enc_id':enc_id_cur,
                               'begin':start_tsp,
                               'end':stop_tsp,
                               'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})
      enc_id_cur = None
      start_tsp = None
      stop_tsp = None
    else:
      if enc_id_cur is None:
        enc_id_cur = enc_id
        start_tsp = tsp
  if enc_id_cur is not None:
    await conn.execute(update_sql_wo_stop % {'enc_id':enc_id_cur,
                         'begin':start_tsp,
                         'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})



# Special case (mini-pipeline)
async def heart_attack_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  NEED TO MODIFY
  fid_input should be heart_attack_inhosp, ekg_proc, troponin
  fid should be heart_attack (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'heart_attack', 'fid %s is heart_attack' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'heart_attack_inhosp' == fid_input_items[0] \
    and 'ekg_proc' == fid_input_items[1] \
    and 'troponin' == fid_input_items[2], \
    "fid_input error: %s" % fid_input_items
  if dataset_id and not incremental:
    # clean previous values
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id=dataset_id, incremental=incremental, cdm_t_target=cdm_t_target))
  # Retrieve all records of heart_attack_inhosp
  select_sql = """
  SELECT * FROM %s cdm_t
  WHERE  fid = 'heart_attack_inhosp'%s %s
  ORDER BY enc_id, tsp;
  """
  records = await conn.fetch(\
    select_sql % (cdm_t_target, with_ds(dataset_id),
                  incremental_enc_id_in(' and ', 'cdm_t', dataset_id, incremental)))

  # Retrieve troponin above threshold
  # and EKG procedure order times to corroborate time of diagnosis
  select_ekg = """
  SELECT * FROM %(cdm_t)s as cdm_t
  WHERE
    cdm_t.fid ~ 'ekg_proc'
    and cdm_t.enc_id = %(enc_id)s%(with_ds)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
    %(incremental_enc_id_in)s
  ORDER BY tsp
  """
  select_troponin = """
  SELECT tsp FROM %(twf_table)s
  WHERE troponin > 0
    and enc_id = %(enc_id)s%(with_ds)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
    %(incremental_enc_id_in)s
  ORDER BY tsp
  """
  # For each instance of heart attack
  # Set diagnosis time to be min (time of troponin extreme value, time of EKG
  # Update cdm_t with heart_attack=TRUE at diagnosis time
  twf_table_troponin = get_src_twf_table(derive_feature_addr)
  for record in records:
    enc_id = record['enc_id']
    tsp = record['tsp']
    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    evidence = await conn.fetch(\
      select_troponin % \
        {'enc_id':enc_id, 'tsp':tsp,
         'twf_table': twf_table_troponin,
         'with_ds': with_ds(dataset_id),
         'incremental_enc_id_in': \
            incremental_enc_id_in(' and ',
                                  twf_table_troponin,dataset_id,
                                  incremental)})
    t1 = tsp
    if len(evidence) > 0:
      t1 = evidence[0]['tsp']

    evidence = await conn.fetch(select_ekg \
      % {'cdm_t': cdm_t_target,'enc_id':enc_id, 'tsp':tsp, 'with_ds': with_ds(dataset_id),
         'incremental_enc_id_in': \
            incremental_enc_id_in(' and ', 'cdm_t', dataset_id,incremental)})
    t2 = tsp
    if len(evidence) > 0:
      t2 = evidence[0]['tsp']

    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = min(t1, t2)
    conf=confidence.NO_TRANSFORM

    await load_row.upsert_t(conn, [enc_id, tsp_first, fid, 'True', conf],
                            dataset_id = dataset_id, cdm_t_target=cdm_t_target)

# Special case (mini-pipeline)
async def stroke_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid_input should be stroke_inhosp, ct_proc, mri_proc
  fid should be heart_attack (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'stroke', 'fid %s is stroke' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'stroke_inhosp' == fid_input_items[0] and 'ct_proc' == fid_input_items[1] \
      and 'mri_proc' == fid_input_items[2], "fid_input error: %s" % fid_input_items
  lookbackhours = " and now() - cdm_t.tsp <= '{}'::interval".format(cdm_t_lookbackhours) if cdm_t_lookbackhours is not None else ''
  if dataset_id and not incremental:
    # clean previous values
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id=dataset_id, incremental=incremental, cdm_t_target=cdm_t_target))

  # Retrieve all records of stroke_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM %s cdm_t
  WHERE  fid = 'stroke_inhosp'%s
  %s %s
  ORDER BY enc_id,  tsp;
  """
  records = await conn.fetch(select_sql \
    % (cdm_t_target, with_ds(dataset_id), incremental_enc_id_in(' and ', 'cdm_t', dataset_id,incremental), lookbackhours))

  # Retrieve CT and MRI order times to corroborate time of diagnosis
  select_sql = """
  SELECT * FROM %(cdm_t)s as cdm_t
  WHERE
    cdm_t.fid ~ 'ct_proc|mri_proc'
    and cdm_t.enc_id = %(enc_id)s%(with_ds)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
    %(lookbackhours)s
  ORDER BY tsp
  """

  # For each instance of stroke
  # Set diagnosis time to be min (time of CT, time of  MRI)
  # Update cdm_t with stroke=TRUE at diagnosis time
  for record in records:
    enc_id = record['enc_id']
    tsp = record['tsp']

    evidence = await conn.fetch(select_sql % {'cdm_t': cdm_t_target, 'enc_id':enc_id, 'tsp':tsp, 'with_ds': with_ds(dataset_id), 'lookbackhours':lookbackhours})


    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = tsp
    conf=confidence.NO_TRANSFORM

    if len(evidence) > 0:
      tsp_first = evidence[0]['tsp']

    await load_row.upsert_t(conn, [enc_id, tsp_first, fid, 'True', conf], dataset_id=dataset_id)

# Special case (mini-pipeline)
async def gi_bleed_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid_input should be gi_bleed_inhosp, ct_proc, mri_proc
  fid should be gi_bleed (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'gi_bleed', 'fid %s is gi_bleed' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'gi_bleed_inhosp' == fid_input_items[0] and 'ct_proc' == fid_input_items[1] \
      and 'mri_proc' == fid_input_items[2], "fid_input error: %s" % fid_input_items
  lookbackhours = " and now() - cdm_t.tsp <= '{}'::interval".format(cdm_t_lookbackhours) if cdm_t_lookbackhours is not None else ''
  if dataset_id and not incremental:
    # clean previous values
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id=dataset_id, incremental=incremental, cdm_t_target=cdm_t_target))
  # Retrieve all records of gi_bleed_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM %s cdm_t
  WHERE  fid = 'gi_bleed_inhosp'%s %s %s
  ORDER BY enc_id,  tsp;
  """
  records = await conn.fetch(select_sql % \
    (cdm_t_target, with_ds(dataset_id), incremental_enc_id_in(' and ', 'cdm_t',dataset_id, incremental), lookbackhours))

  # Retrieve CT and MRI order times to corroborate time of diagnosis
  select_sql = """
  SELECT * FROM %(cdm_t)s as cdm_t
  WHERE
    cdm_t.fid ~ 'ct_proc|mri_proc'
    and cdm_t.enc_id = %(enc_id)s %(with_ds)s %(lookbackhours)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
  ORDER BY tsp;
  """

  # For each instance of gi_bleed
  # Set diagnosis time to be min (time of CT, time of  MRI)
  # Update cdm_t with gi_bleed=TRUE at diagnosis time
  for record in records:
    enc_id = record['enc_id']
    tsp = record['tsp']
    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though

    evidence = await conn.fetch(select_sql % {'cdm_t': cdm_t_target,'enc_id':enc_id, 'tsp':tsp, 'with_ds': with_ds(dataset_id), 'lookbackhours':lookbackhours})

    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = tsp
    conf=confidence.NO_TRANSFORM

    if len(evidence) > 0:
      tsp_first = evidence[0]['tsp']

    await load_row.upsert_t(conn, [enc_id, tsp_first, fid, 'True', conf], dataset_id=dataset_id, cdm_t_target='cdm_t' if 'select' in cdm_t_target else cdm_t_target)




# Special case (mini-pipeline)
async def severe_pancreatitis_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid_input should be severe_pancreatitis_inhosp, ct_proc, mri_proc
  fid should be heart_attack (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'severe_pancreatitis', 'fid %s is severe_pancreatitis' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'severe_pancreatitis_inhosp' == fid_input_items[0] and 'ct_proc' == fid_input_items[1] \
      and 'mri_proc' == fid_input_items[2], "fid_input error: %s" % fid_input_items
  if dataset_id and not incremental:
    # clean previous values
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id=dataset_id, incremental=incremental, cdm_t_target=cdm_t_target))
  # Retrieve all records of severe_pancreatitis_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM %s cdm_t
  WHERE  fid = 'severe_pancreatitis_inhosp'%s %s
  ORDER BY enc_id, tsp;
  """
  records = await conn.fetch(select_sql \
    % (cdm_t_target, with_ds(dataset_id), incremental_enc_id_in(' and ', 'cdm_t', dataset_id,incremental)))

  # Retrieve CT and MRI order times to corroborate time of diagnosis
  select_sql = """
  SELECT * FROM %(cdm_t)s as cdm_t
  WHERE
    cdm_t.fid ~ 'ct_proc|mri_proc'
    and cdm_t.enc_id = %(enc_id)s%(with_ds)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
  ORDER BY tsp
  """

  # For each instance of stroke
  # Set diagnosis time to be min (time of CT, time of  MRI)
  # Update cdm_t with stroke=TRUE at diagnosis time
  for record in records:
    enc_id = record['enc_id']
    tsp = record['tsp']

    evidence = await conn.fetch(select_sql % {'cdm_t': cdm_t_target,'enc_id':enc_id, 'tsp':tsp, 'with_ds': with_ds(dataset_id)})

    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = tsp
    conf=confidence.NO_TRANSFORM

    if len(evidence) > 0:
      tsp_first = evidence[0]['tsp']

    await load_row.upsert_t(conn, [enc_id, tsp_first, fid, 'True', conf], dataset_id=dataset_id, cdm_t_target=cdm_t_target)

# Special case (mini-pipeline)
async def pulmonary_emboli_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid_input should be pulmonary_emboli_inhosp, ct_proc, ekg_proc
  fid should be heart_attack (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'pulmonary_emboli', 'fid %s is pulmonary_emboli' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'pulmonary_emboli_inhosp' == fid_input_items[0] and 'ct_proc' == fid_input_items[1] \
      and 'ekg_proc' == fid_input_items[2], "fid_input error: %s" % fid_input_items
  if dataset_id and not incremental:
    # clean previous values
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id=dataset_id, incremental=incremental, cdm_t_target=cdm_t_target))
  # Retrieve all records of pulmonary_emboli_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM %s cdm_t
  WHERE  fid = 'pulmonary_emboli_inhosp'%s %s
  ORDER BY enc_id,  tsp;
  """
  records = await conn.fetch(select_sql \
    % (cdm_t_target, with_ds(dataset_id), incremental_enc_id_in(' and ', 'cdm_t',dataset_id, incremental)))

  # Retrieve CT and MRI order times to corroborate time of diagnosis
  select_sql = """
  SELECT * FROM %(cdm_t)s as cdm_t
  WHERE
    cdm_t.fid ~ 'ct_proc|mri_proc'
    and cdm_t.enc_id = %(enc_id)s%(with_ds)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
  ORDER BY tsp
  """

  # For each instance of stroke
  # Set diagnosis time to be min (time of CT, time of  MRI)
  # Update cdm_t with stroke=TRUE at diagnosis time
  for record in records:
    enc_id = record['enc_id']
    tsp = record['tsp']

    evidence = await conn.fetch(select_sql % {'cdm_t':cdm_t_target, 'enc_id':enc_id, 'with_ds': with_ds(dataset_id), 'tsp':tsp})

    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = tsp
    conf=confidence.NO_TRANSFORM

    if len(evidence) > 0:
      tsp_first = evidence[0]['tsp']

    await load_row.upsert_t(conn, [enc_id, tsp_first, fid, 'True', conf], dataset_id=dataset_id, cdm_t_target=cdm_t_target)

# Special case (mini-pipeline)
async def bronchitis_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid_input should be bronchitis_inhosp, chest_xray, bacterial_culture
  fid should be heart_attack (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'bronchitis', 'fid %s is bronchitis' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'bronchitis_inhosp' == fid_input_items[0] and 'chest_xray' == fid_input_items[1] \
      and 'bacterial_culture' == fid_input_items[2], "fid_input error: %s" % fid_input_items
  if dataset_id and not incremental:
    # clean previous values
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id=dataset_id, incremental=incremental, cdm_t_target=cdm_t_target))
  # Retrieve all records of bronchitis_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM %s cdm_t
  WHERE  fid = 'bronchitis_inhosp'%s %s
  ORDER BY enc_id,  tsp;
  """
  records = await conn.fetch(select_sql \
    % (cdm_t_target, with_ds(dataset_id), incremental_enc_id_in(' and ', 'cdm_t', dataset_id,incremental)))

  # Retrieve chest x-ray and bacterial culture order times to corroborate time of diagnosis
  select_sql = """
  SELECT * FROM %(cdm_t)s as cdm_t
  WHERE
    cdm_t.fid ~ 'chest_xray|bacterial_culture'
    and cdm_t.enc_id = %(enc_id)s%(with_ds)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
  ORDER BY tsp
  """

  # For each instance of stroke
  # Set diagnosis time to be min (time of CT, time of  MRI)
  # Update cdm_t with stroke=TRUE at diagnosis time
  for record in records:
    enc_id = record['enc_id']
    tsp = record['tsp']

    evidence = await conn.fetch(select_sql % {'cdm_t':cdm_t_target,'enc_id':enc_id, 'with_ds': with_ds(dataset_id), 'tsp':tsp})

    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = tsp
    conf=confidence.NO_TRANSFORM

    if len(evidence) > 0:
      tsp_first = evidence[0]['tsp']

    await load_row.upsert_t(conn, [enc_id, tsp_first, fid, 'True', conf], dataset_id=dataset_id, cdm_t_target=cdm_t_target)

# Special case (mini-pipeline)
async def acute_kidney_failure_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid_input should be
  acute_kidney_failure_inhosp, creatinine, urine_output_24hr, dialysis
  fid should be acute_kidney_failure (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'acute_kidney_failure', 'fid %s is acute_kidney_failure' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'acute_kidney_failure_inhosp' == fid_input_items[0] \
    and 'creatinine' == fid_input_items[1] \
    and 'urine_output_24hr' == fid_input_items[2] \
    and 'dialysis' == fid_input_items[3], \
    "fid_input error: %s" % fid_input_items
  if dataset_id and not incremental:
    # clean previous values
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id=dataset_id, incremental=incremental, cdm_t_target=cdm_t_target))

  sql = """
  WITH S as (
    SELECT enc_id, min(tsp) pat_min_tsp from %(twf_table)s
    %(dataset_id_equal)s
    group by enc_id
  )
  INSERT INTO %(cdm_t)s (%(dataset_id)s enc_id, tsp, fid, value, confidence)
  SELECT %(dataset_id)s enc_id, tsp, 'acute_kidney_failure' as fid, 'True' as value, max(conf) as confidence
  FROM (
    SELECT %(dataset_id_akfi)s akfi.enc_id, coalesce(least(cr.tsp, ur24.tsp, di.tsp), akfi.tsp) as tsp, greatest(cr.creatinine_c, ur24.urine_output_24hr_c, di.confidence) as conf
    FROM %(cdm_t)s as akfi
    LEFT JOIN %(twf_table)s cr on cr.tsp >= akfi.tsp and
      cr.tsp <= akfi.tsp + '24 hours'::interval and cr.enc_id = akfi.enc_id
    LEFT JOIN %(twf_table_ur24)s ur24 on ur24.tsp >= akfi.tsp and ur24.tsp <= akfi.tsp + '24 hours'::interval and akfi.enc_id = ur24.enc_id
    LEFT JOIN %(cdm_t)s as di on di.tsp >= akfi.tsp and di.tsp <= akfi.tsp + '24 hours'::interval and di.enc_id = akfi.enc_id
    where akfi.fid = 'acute_kidney_failure_inhosp'
      %(dataset_id_equal_akfi)s
      and cr.creatinine > 5 and cr.creatinine_c < 8 %(dataset_id_equal_cr)s
      and ur24.urine_output_24hr < 500 and ur24.tsp - (select pat_min_tsp from S where S.enc_id = ur24.enc_id) >= '24 hours'::interval %(dataset_id_equal_ur24)s
      and di.value = 'True' %(dataset_id_equal_di)s
      %(incremental_enc_id_in)s
  ) source
  GROUP BY %(dataset_id)s enc_id,tsp,fid
  ON CONFLICT (%(dataset_id)s enc_id,tsp,fid) DO UPDATE SET
    value = excluded.value, confidence = excluded.confidence
  """ % {
    'cdm_t': cdm_t_target,
    'twf_table': get_src_twf_table(derive_feature_addr),
    'dataset_id_equal': 'where dataset_id = {}'.format(dataset_id) if dataset_id else '',
    'dataset_id': 'dataset_id,' if dataset_id else '',
    'dataset_id_akfi': 'akfi.dataset_id,' if dataset_id else '',
    'twf_table_ur24': derive_feature_addr['urine_output_24hr']['twf_table_temp'],
    'dataset_id_equal_akfi': ' and akfi.dataset_id = {}'.format(dataset_id) if dataset_id else '',
    'dataset_id_equal_cr': ' and cr.dataset_id = {}'.format(dataset_id) if dataset_id else '',
    'dataset_id_equal_ur24': ' and ur24.dataset_id = {}'.format(dataset_id) if dataset_id else '',
    'dataset_id_equal_di': ' and di.dataset_id = {}'.format(dataset_id) if dataset_id else '',
    'incremental_enc_id_in': incremental_enc_id_in(' and ', 'akfi', dataset_id,incremental)
  }
  log.info(sql)
  await conn.execute(sql)








# Special case (mini-pipeline)
async def ards_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid_input should be ards_inhosp, pao2_to_fio2, vent
  fid should be ards (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'ards', 'fid %s is ards' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'ards_inhosp' == fid_input_items[0] \
    and 'pao2_to_fio2' == fid_input_items[1] \
    and 'vent' == fid_input_items[2], \
    "fid_input error: %s" % fid_input_items
  if dataset_id and not incremental:
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id=dataset_id, incremental=incremental))
  # Retrieve all records of ards_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM cdm_t
  WHERE  fid = 'ards_inhosp'%s %s
  ORDER BY enc_id,  tsp;
  """
  records = await conn.fetch(select_sql \
    % (with_ds(dataset_id), incremental_enc_id_in(' and ', 'cdm_t',dataset_id, incremental)))

  select_ptf = """
  SELECT tsp FROM %(twf_table_ptf)s
  WHERE pao2_to_fio2 < 100
    and enc_id = %(enc_id)s%(with_ds)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
  ORDER BY tsp
  """

  select_vent = """
  SELECT tsp, value FROM cdm_t
  WHERE fid = 'vent'
    and enc_id = %(enc_id)s%(with_ds)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
  ORDER BY tsp
  """

  # For each instance of stroke
  # Set diagnosis time to be min (time of CT, time of  MRI)
  # Update cdm_t with stroke=TRUE at diagnosis time
  twf_table_ptf = derive_feature_addr['pao2_to_fio2']['twf_table_temp']
  for record in records:
    enc_id = record['enc_id']
    tsp = record['tsp']

    evidence = await conn.fetch(select_ptf % {'enc_id':enc_id, 'tsp':tsp,
                  'twf_table_ptf': twf_table_ptf, 'with_ds': with_ds(dataset_id)})
    t1 = tsp
    if len(evidence) > 0:
      t1 = evidence[0]['tsp']

    evidence = await conn.fetch(select_vent % {'enc_id':enc_id, 'tsp':tsp, 'with_ds': with_ds(dataset_id)})
    t2 = tsp
    if len(evidence) > 0:
      if evidence[0]['value'] == 'True':
        t2 = evidence[0]['tsp']
    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = min(t1, t2)
    conf=confidence.NO_TRANSFORM

    await load_row.upsert_t(conn, [enc_id, tsp_first, fid, 'True', conf], dataset_id=dataset_id)

# Special case (mini-pipeline)
async def hepatic_failure_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  """
  fid_input should be hepatic_failure_inhosp, bilirubin
  fid should be hepatic_failure (T)
  Set (id, time) to 1 when hepatic_failure is diagnosed
  """

  assert fid == 'hepatic_failure', 'fid %s is hepatic_failure' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'hepatic_failure_inhosp' == fid_input_items[0] \
    and 'bilirubin' == fid_input_items[1] \
    , "fid_input error: %s" % fid_input_items
  if dataset_id and not incremental:
    # clean previous values
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id=dataset_id, incremental=incremental))
  # Retrieve all records of hepatic_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM cdm_t
  WHERE  fid = 'hepatic_failure_inhosp'%s %s
  ORDER BY enc_id,  tsp;
  """
  records = await conn.fetch(select_sql \
    % (with_ds(dataset_id), incremental_enc_id_in(' and ', 'cdm_t',dataset_id, incremental)))


  # Retrieve bilirubin order times to corroborate time of diagnosis
  select_sql = """
  SELECT tsp FROM %(twf_table)s
  WHERE
    bilirubin > 12
    and enc_id = %(enc_id)s%(with_ds)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
  ORDER BY tsp
  """
  twf_table_temp = get_src_twf_table(derive_feature_addr)
  # For each instance of stroke
  # Set diagnosis time to be min (time of CT, time of  MRI)
  # Update cdm_t with stroke=TRUE at diagnosis time
  for record in records:
    enc_id = record['enc_id']
    tsp = record['tsp']

    evidence = await conn.fetch(select_sql % {'enc_id':enc_id, 'tsp':tsp,
                  'twf_table': twf_table_temp, 'with_ds': with_ds(dataset_id)})


    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = tsp
    conf=confidence.NO_TRANSFORM

    if len(evidence) > 0:
      tsp_first = evidence[0]['tsp']

    await load_row.upsert_t(conn, [enc_id, tsp_first, fid, 'True', conf], dataset_id=dataset_id)
