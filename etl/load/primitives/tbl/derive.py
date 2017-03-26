"""
derive functions
TODO: remove cdm out and use conn
"""
import sys
import etl.confidence as confidence
import datetime
from etl.transforms.primitives.row.transform import STOPPED_ACTIONS
from etl.load.primitives.tbl.acute_liver_failure_update import *
from etl.load.primitives.tbl.acute_pancreatitis_update import *
from etl.load.primitives.tbl.admit_weight_update import *
from etl.load.primitives.tbl.change_since_last_measured import *
from etl.load.primitives.tbl.hemorrhage_update import *
from etl.load.primitives.tbl.hemorrhagic_shock_update import *
from etl.load.primitives.tbl.metabolic_acidosis_update import *
from etl.load.primitives.tbl.mi_update import *
from etl.load.primitives.tbl.septic_shock_iii_update import *
from etl.load.primitives.tbl.time_since_last_measured import *

def derive(fid, func_id, fid_input, conn, log, twf_table='cdm_twf'):
  this_mod = sys.modules[__name__]
  func = getattr(this_mod, func_id)
  return func(fid, fid_input, cdm, twf_table=twf_table)

def lookup_population_mean(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  check if fid_popmean exists or not;
  if not, calculate it
  NOTE: if the fid does not exist for fid_popmean, then fid_popmean is null in
  cdm_g
  """
  select_sql = "SELECT value FROM cdm_g WHERE fid = '%s'" % fid
  server_cursor = cdm.select_with_sql(select_sql)
  popmean = server_cursor.fetchone()
  server_cursor.close()
  if popmean is not None:
    cdm.log.info("lookup_population_mean %s %s" % (fid, popmean['value']))
  else:
    cdm.log.warn("cannont find %s in cdm_g" % fid)
    calculate_popmean_sql = \
      "SELECT avg(%(fid)s) FROM %(twf_table)s where %(fid)s is not null" \
        % {'fid':fid_input, 'twf_table':twf_table}
    server_cursor = cdm.select_with_sql(calculate_popmean_sql)
    popmean = server_cursor.fetchone()
    if popmean is not None:
      cdm.insert_g([fid, popmean[0], confidence.POPMEAN])
      cdm.log.info("lookup_population_mean %s %s" \
        % (fid, popmean[0]))
    else:
      cdm.log.error("lookup_population_mean %s" % fid)








# Subquery
def minutes_since_organ_fail_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be minutes_since_any_organ_fail (TWF)
  fid input should be any_organ_failure (TWF)
  Algorithm:
  For each enc_id, find the tsp when organ first fails , use that to calculate
  time difference
  """
  assert fid == 'minutes_since_any_organ_fail', 'wrong fid %s' % fid
  assert fid_input == 'any_organ_failure', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  # first, find all the times for all given enc_id where any_organ_failure
  # is True
  select_sql = """
  SELECT enc_id, tsp, any_organ_failure_c FROM %(twf_table)s
  WHERE any_organ_failure is TRUE
  ORDER BY enc_id, tsp;
  """ % {'twf_table': twf_table}
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()
  enc_id = -1
  tsp_first = None
  conf = 0
  for record in records:
    if enc_id != record['enc_id']:
      # update previous enc_id
      if enc_id != -1:
        update_sql = """
        UPDATE %(twf_table)s SET %(fid)s =
          EXTRACT(EPOCH FROM (tsp - timestamptz '%(tsp_first)s'))/60
        , %(fid)s_c = %(conf)s
        WHERE enc_id = %(enc_id)s
        """ % {'fid':fid, 'tsp_first':tsp_first, 'conf':conf,
             'enc_id': enc_id, 'twf_table': twf_table}
        cdm.update_twf_sql(update_sql)
      # new enc_id
      enc_id = record['enc_id']
      tsp_first = record['tsp']
      conf =  record['any_organ_failure_c']
  # finally, for invalid value, set to zero
  update_default = "update %(twf_table)s set %(fid)s = 0 where %(fid)s is null or %(fid)s < 0" % \
    {'fid':fid, 'twf_table': twf_table}
  cdm.update_twf_sql(update_default)

# Subquery
def minutes_since_any_organ_fail_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be minutes_since_any_organ_fail (TWF)
  fid input should be any_organ_failure (TWF)
  Algorithm:
  For each enc_id, find the tsp when organ first fails , use that to calculate
  time difference
  """
  assert fid == 'minutes_since_any_organ_fail', 'wrong fid %s' % fid
  assert fid_input == 'any_organ_failure', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  # first, find all the times for all given enc_id where any_organ_failure
  # is True
  select_sql = """
  SELECT enc_id, tsp, jh_organ_failure_c FROM %(twf_table)s
  WHERE any_organ_failure is TRUE
  ORDER BY enc_id, tsp;
  """ % {'twf_table': twf_table}
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()
  enc_id = -1
  tsp_first = None
  conf = 0
  for record in records:
    if enc_id != record['enc_id']:
      # update previous enc_id
      if enc_id != -1:
        update_sql = """
        UPDATE %(twf_table)s SET %(fid)s =
          EXTRACT(EPOCH FROM (tsp - timestamptz '%(tsp_first)s'))/60
        , %(fid)s_c = %(conf)s
        WHERE enc_id = %(enc_id)s
        """ % {'fid':fid, 'tsp_first':tsp_first, 'conf':conf,
             'enc_id': enc_id, 'twf_table': twf_table}
        cdm.update_twf_sql(update_sql)
      # new enc_id
      enc_id = record['enc_id']
      tsp_first = record['tsp']
      conf =  record['jh_organ_failure_c']
  # finally, for invalid value, set to zero
  update_default = "update %(twf_table)s set %(fid)s = 0 where %(fid)s is null or %(fid)s < 0" \
    % {'fid':fid, 'twf_table': twf_table}
  cdm.update_twf_sql(update_default)

# Subquery
def treatment_within_6_hours_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be treatment_within_6_hours_update (TWF)
  fid input should be all(T):
    any_antibiotics
    any_anticoagulant
    any_beta_blocker
    any_glucocorticoid
    any_inotrope
    any_pressor
    vent
  Algorithm:
  For each enc_id, find the tsp when organ first fails , use that to calculate
  time difference
  """
  assert fid == 'treatment_within_6_hours', 'wrong fid %s' % fid
  cdm.clean_twf(fid, twf_table=twf_table)
  select_sql = """
  select enc_id,tsp from cdm_t
    where
      fid = 'any_antibiotics'
      or fid = 'any_anticoagulant'
      or fid = 'any_beta_blocker'
      or fid = 'any_glucocorticoid'
      or fid = 'any_inotrope'
      or fid = 'any_pressor'
      or fid = 'vent' and value = 'True'
  """
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()
  update_sql = """
  update %(twf_table)s set treatment_within_6_hours = true
  where enc_id = %(enc_id)s
  and tsp > timestamptz '%(tsp)s'
  and tsp - timestamptz '%(tsp)s' < interval '6 hours'
  """
  for record in records:
    enc_id = record['enc_id']
    tsp = record['tsp']
    cdm.update_twf_sql(update_sql % {'enc_id':enc_id, 'tsp':tsp, 'twf_table': twf_table})


# Subquery
def minutes_since_any_antibiotics_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be minutes_since_any_antibiotics (TWF)
  fid input should be any_antibiotics (T)
  """
  assert fid == 'minutes_since_any_antibiotics', 'wrong fid %s' % fid
  assert fid_input == 'any_antibiotics', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  # first, find all the times for all given enc_id where any_organ_failure
  # is True
  select_sql = """
  SELECT * FROM cdm_t
  WHERE fid = 'any_antibiotics' and cast(value as boolean) is TRUE
  ORDER BY enc_id, tsp;
  """
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()
  enc_id = -1
  tsp_first = None
  conf = 0
  for record in records:
    if enc_id != record['enc_id']:
      # update previous enc_id
      if enc_id != -1:
        update_sql = """
        UPDATE %(twf_table)s SET %(fid)s =
          EXTRACT(EPOCH FROM (tsp - timestamptz '%(tsp_first)s'))/60
        , %(fid)s_c = %(conf)s
        WHERE enc_id = %(enc_id)s
        """ % {'fid':fid, 'tsp_first':tsp_first, 'conf':conf,
             'enc_id': enc_id, 'twf_table': twf_table}
        cdm.update_twf_sql(update_sql)
      # new enc_id
      enc_id = record['enc_id']
      tsp_first = record['tsp']
      conf =  record['confidence']
  # finally, for invalid value, set to default value 0
  update_default = "update %(twf_table)s set %(fid)s = 0 where %(fid)s is null or %(fid)s < 0" \
    % {'fid':fid, 'twf_table': twf_table}
  cdm.update_twf_sql(update_default)



# Subquery
def minutes_to_shock_onset_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be minutes_to_shock_onset (TWF)
  fid input should be septic_shock (TWF)
  Algorithm:
  For each enc_id, find the tsp of the first shock , use that to calculate
  time difference
  """
  assert fid == 'minutes_to_shock_onset', 'wrong fid %s' % fid
  assert fid_input == 'septic_shock', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  # first, find all the times for all given enc_id where any_organ_failure
  # is True
  select_sql = """
  SELECT enc_id, tsp, septic_shock_c FROM %(twf_table)s
  WHERE septic_shock is TRUE
  ORDER BY enc_id, tsp;
  """ % {'twf_table': twf_table}
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()
  enc_id = -1
  tsp_first = None
  conf = 0
  for record in records:
    if enc_id != record['enc_id']:
      # update previous enc_id
      if enc_id != -1:
        update_sql = """
        UPDATE %(twf_table)s SET %(fid)s =
          EXTRACT(EPOCH FROM (timestamptz '%(tsp_first)s' - tsp))/60
        , %(fid)s_c = %(conf)s
        WHERE enc_id = %(enc_id)s
        """ % {'fid':fid, 'tsp_first':tsp_first, 'conf':conf,
             'enc_id': enc_id, 'twf_table': twf_table}
        cdm.update_twf_sql(update_sql)
      # new enc_id
      enc_id = record['enc_id']
      tsp_first = record['tsp']
      conf =  record['septic_shock_c']






# Same as intake_output_duration (subquery)
def urine_output_24hr(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be urine_output_24hr (TWF)
  fid input should be urine_output (T)
  """
  assert fid == 'urine_output_24hr', 'wrong fid %s' % fid
  assert fid_input == 'urine_output', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  win_h = 24
  intake_output_duration(fid, fid_input, win_h, cdm, twf_table=twf_table)

# Same as intake_output_duration (subquery)
def urine_output_6hr(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be urine_output_6hr (TWF)
  fid input should be urine_output (T)
  """
  assert fid == 'urine_output_6hr', 'wrong fid %s' % fid
  assert fid_input == 'urine_output', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  win_h = 6
  intake_output_duration(fid, fid_input, win_h, cdm, twf_table=twf_table)

# Same as intake_output_duration (subquery)
def fluids_intake_24hr(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be fluids_intake_24hr (TWF)
  fid input should be fluids_intake (T)
  """
  assert fid == 'fluids_intake_24hr', 'wrong fid %s' % fid
  assert fid_input == 'fluids_intake', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  win_h = 24
  intake_output_duration(fid, fid_input, win_h, cdm, twf_table=twf_table)

# Same as intake_output_duration (subquery)
def fluids_intake_1hr(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be fluids_intake_1hr (TWF)
  fid input should be fluids_intake (T)
  """
  assert fid == 'fluids_intake_1hr', 'wrong fid %s' % fid
  assert fid_input == 'fluids_intake', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  win_h = 1
  intake_output_duration(fid, fid_input, win_h, cdm, twf_table=twf_table)

# Same as intake_output_duration (subquery)
def fluids_intake_3hr(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be fluids_intake_3hr (TWF)
  fid input should be fluids_intake (T)
  """
  assert fid == 'fluids_intake_3hr', 'wrong fid %s' % fid
  assert fid_input == 'fluids_intake', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  win_h = 3
  intake_output_duration(fid, fid_input, win_h, cdm, twf_table=twf_table)

# Subquery
def intake_output_duration(fid, fid_input, win_h, cdm, twf_table='cdm_twf'):
  cdm.clean_twf(fid, value=0, confidence=0,twf_table=twf_table)
  update_sql = """
  update %(twf_table)s set %(fid)s = x.sum_v, %(fid)s_c = x.max_c
  from (
    select twf.enc_id, twf.tsp,
    sum(t.value::float) sum_v, max(t.confidence) max_c
    from %(twf_table)s twf
    inner join cdm_t t
    on t.enc_id = twf.enc_id and t.tsp <= twf.tsp
    and t.tsp > twf.tsp - interval '%(win_h)s hours'
    and fid = '%(fid_input)s'
    group by twf.enc_id, twf.tsp
    ) as x
  where %(twf_table)s.enc_id = x.enc_id and %(twf_table)s.tsp = x.tsp
  """ % {'fid': fid, 'win_h':win_h, 'fid_input': fid_input, 'twf_table': twf_table}
  cdm.log.info(update_sql)
  cdm.update_twf_sql(update_sql)

# Same as any_continuous_dose_update (special case)
def any_antibiotics_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be any_antibiotics (T, boolean)
  fid_input should be a list of dose
  """
  assert fid == 'any_antibiotics', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  for i in range(len(fid_input_items)):
    assert fid_input_items[i].endswith('dose'), \
      'wrong fid_input %s' % fid_input
  any_dose_update(fid, fid_input, cdm, twf_table=twf_table)

# Same as any_continuous_dose_update (special case)
def any_antibiotics_order_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be any_antibiotics_order (T, boolean)
  fid_input should be a list of dose
  """
  assert fid == 'any_antibiotics_order', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  for i in range(len(fid_input_items)):
    assert fid_input_items[i].endswith('dose'), \
      'wrong fid_input %s' % fid_input
  any_med_order_update(fid, fid_input, cdm, twf_table=twf_table)

# Same as any_continuous_dose_update (special case)
def any_pressor_update(fid, fid_input, cdm, twf_table='cdm_twf'):
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
    any_continuous_dose_update(fid, dose, cdm, twf_table=twf_table)

# Same as any_continuous_dose_update (special case)
def any_inotrope_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be any_inotrope (T, boolean)
  fid_input should be a list of dose
  """
  assert fid == 'any_inotrope', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  for i in range(len(fid_input_items)):
    assert fid_input_items[i].endswith('dose'), \
      'wrong fid_input %s' % fid_input
  cdm.clean_t(fid)
  for dose in fid_input_items:
    any_continuous_dose_update(fid, dose, cdm, twf_table=twf_table)

# Special case
def any_continuous_dose_update(fid, dose, cdm, twf_table='cdm_twf'):
  global STOPPED_ACTIONS
  select_sql = """
    select enc_id, tsp, value::json->>'action' as action, confidence
    from cdm_t where fid = '%s' order by enc_id, tsp
  """ % dose
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()
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
        update_continuous_dose_block(fid, block, cdm)
        block = {'enc_id':None, 'start_tsp':None, 'end_tsp':None,
             'start_c': 0, 'end_c': 0}
    elif block['enc_id'] != enc_id and not action in STOPPED_ACTIONS:
      # update current block
      update_continuous_dose_block(fid, block, cdm)
      # create new block
      block = {'enc_id':enc_id, 'start_tsp':tsp, 'end_tsp':None,
           'start_c': 0, 'end_c': 0}

# Special case
def update_continuous_dose_block(fid, block, cdm, twf_table='cdm_twf'):
  select_sql = """
    select value from cdm_t where enc_id = %s and fid = '%s'
    and tsp <= timestamptz '%s' order by tsp DESC
  """ % (block['enc_id'], fid, block['start_tsp'])
  server_cursor = cdm.select_with_sql(select_sql)
  prev = server_cursor.fetchone()
  server_cursor.close()
  if prev is None or prev['value'] == 'False':
    cdm.insert_t([block['enc_id'], block['start_tsp'], fid,
            True, block['start_c']])
  if block['end_tsp'] is None:
    delete_sql = """
      delete from cdm_t where enc_id = %s and fid = '%s'
      and tsp > timestamptz '%s'
    """ % (block['enc_id'], fid, block['start_tsp'])
    cdm.query_with_sql(delete_sql)
  else:
    delete_sql = """
      delete from cdm_t where enc_id = %s and fid = '%s'
      and tsp > timestamptz '%s' and tsp <= timestamptz '%s'
    """ % (block['enc_id'], fid, block['start_tsp'], block['end_tsp'])
    cdm.query_with_sql(delete_sql)
    select_sql = """
      select value from cdm_t where enc_id = %s and fid = '%s'
      and tsp >= timestamptz '%s' order by tsp
    """ % (block['enc_id'], fid, block['end_tsp'])
    server_cursor = cdm.select_with_sql(select_sql)
    post = server_cursor.fetchone()
    server_cursor.close()
    if post is None or post['value'] == 'True':
      cdm.insert_t([block['enc_id'], block['end_tsp'], fid,
              False, block['end_c']])

# Same as any_dose_update (special case)
def glucocorticoid_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be glucocorticoid (T, boolean)
  fid_input should be a list of dose
  """
  assert fid == 'any_glucocorticoid', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  for i in range(len(fid_input_items)):
    assert fid_input_items[i].endswith('dose'), \
      'wrong fid_input %s' % fid_input
  any_dose_update(fid, fid_input, cdm)

# Same as any_dose_update (special case)
def beta_blocker_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be any_beta_blocker (T, boolean)
  fid_input should be a list of dose
  """
  assert fid == 'any_beta_blocker', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  for i in range(len(fid_input_items)):
    assert fid_input_items[i].endswith('dose'), \
      'wrong fid_input %s' % fid_input
  any_dose_update(fid, fid_input, cdm)

# Same as any_dose_update (special case)
def anticoagulant_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be anticoagulant (T, boolean)
  fid_input should be a list of dose
  """
  assert fid == 'any_anticoagulant', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  for i in range(len(fid_input_items)):
    assert fid_input_items[i].endswith('dose'), \
      'wrong fid_input %s' % fid_input
  any_dose_update(fid, fid_input, cdm)

# Special case
def any_dose_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  # Updated on 3/19/2016
  cdm.clean_t(fid)
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  doses = '|'.join(fid_input_items)
  select_sql = """
    SELECT distinct enc_id, tsp, max(confidence) confidence FROM cdm_t
    WHERE fid ~ '%s' AND cast(value::json->>'dose' as numeric) > 0
    group by enc_id, tsp
  """ % doses
  server_cursor = cdm.select_with_sql(select_sql)
  rows = server_cursor.fetchall()
  server_cursor.close()
  for row in rows:
    values = [row['enc_id'], row['tsp'], fid, "True",
          row['confidence']]
    cdm.upsert_t(values)

# Special case
def any_med_order_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  # Updated on 3/19/2016
  cdm.clean_t(fid)
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  doses = '|'.join(fid_input_items)
  select_sql = """
    SELECT distinct enc_id,
      value::json->>'order_tsp' order_tsp,
      max(confidence) confidence FROM cdm_t
    WHERE fid ~ '%s' AND cast(value::json->>'dose' as numeric) > 0
    group by enc_id, order_tsp
  """ % doses
  server_cursor = cdm.select_with_sql(select_sql)
  rows = server_cursor.fetchall()
  server_cursor.close()
  for row in rows:
    if row['order_tsp']:
      values = [row['enc_id'], row['order_tsp'], fid, "True",
            row['confidence']]
      cdm.upsert_t(values)







# Special case
def suspicion_of_infection_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  # 03/20/2016
  assert fid == 'suspicion_of_infection', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid_input_items[0] == 'culture_order' \
    and fid_input_items[1] == 'any_antibiotics_order', \
    'wrong fid_input %s' % fid_input
  cdm.clean_t(fid)
  select_sql = """
    select t1.enc_id, t1.tsp, t1.confidence|t2.confidence confidence
      from cdm_t t1
    inner join cdm_t t2 on t1.enc_id = t2.enc_id
      and t2.fid = 'any_antibiotics_order'
      and t2.tsp >= t1.tsp and t2.tsp <= t1.tsp + interval '72 hours'
    where t1.fid = 'culture_order'
  """
  server_cursor = cdm.select_with_sql(select_sql)
  rows = server_cursor.fetchall()
  server_cursor.close()
  for row in rows:
    values = [row['enc_id'], row['tsp'], fid, "True",
          row['confidence']]
    cdm.upsert_t(values)
  select_sql = """
    select t1.enc_id, t1.tsp, t1.confidence|t2.confidence confidence
      from cdm_t t1
    inner join cdm_t t2 on t1.enc_id = t2.enc_id
      and t2.fid = 'culture_order'
      and t2.tsp >= t1.tsp and t2.tsp <= t1.tsp + interval '24 hours'
    where t1.fid = 'any_antibiotics_order'
  """
  server_cursor = cdm.select_with_sql(select_sql)
  rows = server_cursor.fetchall()
  server_cursor.close()
  for row in rows:
    values = [row['enc_id'], row['tsp'], fid, "True",
          row['confidence']]
    cdm.upsert_t(values)











# Special subquery (subquery with if-then-else cases)
def hypotension_intp_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  assert fid == 'hypotension_intp', 'wrong fid %s' % fid
  assert fid_input == 'hypotension_raw', 'wrong fid_input %s' % fid
  cdm.clean_twf(fid, value=False)
  select_sql = """
  SELECT *
  FROM
    (SELECT enc_id, tsp, hypotension_raw, hypotension_raw_c,
        lag(enc_id) over w as enc_id_prev,
        lag(tsp) over w as tsp_prev,
        lag(hypotension_raw) over w as hypotension_raw_prev
     FROM %(twf_table)s
     WINDOW w as (order by enc_id,tsp)
     ORDER by enc_id, tsp) as query
  WHERE (query.hypotension_raw OR query.hypotension_raw_prev) OR
      query.enc_id IS DISTINCT FROM query.enc_id_prev
  """ % {'twf_table': twf_table}
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()
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
          UPDATE %(twf_table)s SET hypotension_intp = 'True',
          hypotension_intp_c = %(confidence)s
          WHERE enc_id = %(enc_id)s
          AND tsp >= timestamptz '%(tsp_start)s'
          AND tsp <= timestamptz '%(tsp_end)s'
          """ % {'confidence': block_c, 'enc_id': block_enc_id,
               'tsp_start': block_start, 'tsp_end': block_end,
               'twf_table': twf_table}
          cdm.update_twf_sql(update_sql)
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
          UPDATE %(twf_table)s SET hypotension_intp = 'True',
          hypotension_intp_c = %(confidence)s
          WHERE enc_id = %(enc_id)s
          AND tsp >= timestamptz '%(tsp_start)s'
          AND tsp <= timestamptz '%(tsp_end)s'
          """ % {'confidence': block_c, 'enc_id': block_enc_id,
               'tsp_start': block_start, 'tsp_end': block_end,
               'twf_table': twf_table}
          cdm.update_twf_sql(update_sql)
      block_start = None
      block_end = None
      block_c = 0
    else:
      # both current and previous hypotension_raw are 1
      block_c = block_c | rec['hypotension_raw_c']

# Special subquery (multiple subqueries)
def sirs_intp_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  SIRS_INTP 30m version
  """
  # update 4/17/2016
  assert fid == 'sirs_intp', 'wrong fid %s' % fid
  assert fid_input == 'sirs_raw', 'wrong fid_input %s' % fid
  cdm.clean_twf(fid, value=False, confidence=0, twf_table=twf_table)
  # 1. Identify all periods of time where sirs_raw=1 for
  # at least 1 consecutive hours and set sirs_intp=1
  select_sql = """
  SELECT *
  FROM
    (SELECT enc_id, tsp, sirs_raw, sirs_raw_c,
        lag(enc_id) over w as enc_id_prev,
        lag(tsp) over w as tsp_prev,
        lag(sirs_raw) over w as sirs_raw_prev
     FROM %(twf_table)s
     WINDOW w as (order by enc_id,tsp)
     ORDER by enc_id, tsp) as query
  WHERE (query.sirs_raw OR query.sirs_raw_prev) OR
      query.enc_id IS DISTINCT FROM query.enc_id_prev
  """ % {'twf_table': twf_table}
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()
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
          WHERE enc_id = %(enc_id)s
          AND tsp >= timestamptz '%(tsp_start)s'
          AND tsp <= timestamptz '%(tsp_end)s'
          """ % {'confidence': block_c, 'enc_id': block_enc_id,
               'tsp_start': block_start, 'tsp_end': block_end,
               'twf_table': twf_table}
          cdm.update_twf_sql(update_sql)
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
          WHERE enc_id = %(enc_id)s
          AND tsp >= timestamptz '%(tsp_start)s'
          AND tsp <= timestamptz '%(tsp_end)s'
          """ % {'confidence': block_c, 'enc_id': block_enc_id,
               'tsp_start': block_start, 'tsp_end': block_end,
               'twf_table': twf_table}
          cdm.update_twf_sql(update_sql)
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
     FROM %(twf_table)s
     WINDOW w as (order by enc_id,tsp)
     ORDER by enc_id, tsp) as query
  WHERE (query.sirs_intp or query.sirs_intp_prev) OR
      (query.enc_id IS DISTINCT FROM query.enc_id_prev)
  """ % {'twf_table': twf_table}
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()
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
          WHERE enc_id = %(enc_id)s
          AND tsp > timestamptz '%(tsp_start)s'
          AND tsp < timestamptz '%(tsp_end)s'
          """ % {'confidence': interval_c, 'enc_id': interval_enc_id,
               'tsp_start': interval_start, 'tsp_end': interval_end,
               'twf_table': twf_table}
        cdm.update_twf_sql(update_sql)
      interval_start = None
      interval_end = None
      interval_c = 0







# Simple (w/ complex expressions)
def renal_sofa_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be renal_sofa (TWF)
  fid input should be creatinine, urine_output_24hr (both TWF)
  """
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid == 'renal_sofa', 'wrong fid %s' % fid
  assert fid_input_items[0] == 'creatinine' \
    and fid_input_items[1] == 'urine_output_24hr', \
      'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, value=0, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = (CASE
    WHEN %(creat)s > 5 THEN 4
    WHEN %(creat)s >= 3.5 THEN 3
    WHEN %(creat)s >= 2 THEN 2
    WHEN %(creat)s >= 1.2 THEN 1
    ELSE 0
  END)
  , %(fid)s_c = coalesce(%(creat)s_c, 0);
  """ % {'fid':fid, 'creat':fid_input_items[0], 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = (CASE
      WHEN %(urine_output)s < 200 THEN 4
      WHEN %(fid)s < 3 AND %(urine_output)s < 500 THEN 3
      ELSE %(fid)s
    END)
    , %(fid)s_c = coalesce(%(fid)s_c,0) | coalesce(%(urine_output)s_c,0)
    WHERE %(urine_output)s < 500
      AND
      tsp - (SELECT min(cdm_twf_2.tsp) FROM %(twf_table)s cdm_twf_2
           WHERE cdm_twf_2.enc_id = %(twf_table)s.enc_id)
      >= interval '24 hours'
    ;
  """ % {'fid':fid, 'urine_output':fid_input_items[1], 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)





# Subquery chain
def septic_shock_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  # UPDATE 8/19/2016
  assert fid == 'septic_shock', 'wrong fid %s' % fid

  cdm.clean_twf(fid, value=0, confidence=0,twf_table=twf_table)

  select_sql = """
  select enc_id, tsp from %(twf_table)s
  where %(condition)s
  """

  update_clause = """
  UPDATE %(twf_table)s SET septic_shock = septic_shock | %(flag)s,
    septic_shock_c = septic_shock_c | %(id)s_c
    WHERE
    enc_id = %(enc_id)s
    and tsp >= timestamptz '%(tsp)s'
    and tsp < timestamptz '%(tsp)s' + interval '6 hours'
    ;
  """

  server_cursor = \
    cdm.select_with_sql(select_sql % {'condition':'severe_sepsis is true',
                      'twf_table': twf_table})
  records = server_cursor.fetchall()
  server_cursor.close()
  for rec in records:
    cdm.update_twf_sql(update_clause % {'id':'severe_sepsis',
                      'flag':1,
                      'enc_id': rec['enc_id'],
                      'tsp': rec['tsp'],
                      'twf_table': twf_table})

  server_cursor = \
    cdm.select_with_sql(select_sql % {'condition':'hypotension_intp is true or lactate > 4',
                      'twf_table':twf_table})
  records = server_cursor.fetchall()
  server_cursor.close()
  for rec in records:
    cdm.update_twf_sql(update_clause % {'id':'severe_sepsis',
                      'flag':2,
                      'enc_id': rec['enc_id'],
                      'tsp': rec['tsp'],
                      'twf_table': twf_table})

  server_cursor = \
    cdm.select_with_sql(select_sql % \
      {'condition':'fluid_resuscitation is true or vasopressor_resuscitation is true',
       'twf_table': twf_table})
  records = server_cursor.fetchall()
  server_cursor.close()
  for rec in records:
    cdm.update_twf_sql(update_clause % {'id':'severe_sepsis',
                      'flag':4,
                      'enc_id': rec['enc_id'],
                      'tsp': rec['tsp'],
                      'twf_table': twf_table})

  update_clause = """
  UPDATE %(twf_table)s SET septic_shock = ( case when septic_shock = 7 then 1
    else 0 end)
  """ % {'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)



# Special case (mini-pipeline)
def resp_sofa_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be resp_sofa
  fid_input should be (vent (T), pao2_to_fio2 (TWF))
  """
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid == 'resp_sofa', 'wrong fid %s' % fid
  assert fid_input_items[0] == 'vent' \
    and fid_input_items[1] == 'pao2_to_fio2', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, value=0, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = (CASE
    WHEN pao2_to_fio2 < 300 THEN 2
    WHEN pao2_to_fio2 < 400 THEN 1
    ELSE 0
  END),
  %(fid)s_c = pao2_to_fio2_c
  ;
  """ % {'fid':fid, 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

  select_sql = """
  SELECT * FROM cdm_t WHERE fid = 'vent' ORDER BY enc_id, tsp
  """
  server_cursor = cdm.select_with_sql(select_sql)
  block_enc_id = -1
  block_start = None
  block_end = None
  block_c = 0
  update_sql = """
  UPDATE %(twf_table)s SET resp_sofa = (CASE
    WHEN pao2_to_fio2 < 100 THEN 4
    WHEN pao2_to_fio2 < 200 THEN 3
    ELSE resp_sofa
    END),
    resp_sofa_c = resp_sofa_c | %(block_c)s
  WHERE enc_id = %(enc_id)s
  AND tsp >= timestamptz '%(tsp_start)s'
  """
  records = server_cursor.fetchall()
  for rec in records:
    if block_enc_id != rec['enc_id']:
      # new enc_id
      # update last block
      if block_start is not None:
        block_update_sql = update_sql \
          % {'enc_id':block_enc_id, 'block_c':block_c,
             'tsp_start':block_start, 'twf_table': twf_table}
        cdm.update_twf_sql(block_update_sql)
      # start new block
      block_start = None
      block_end = None
      block_c = 0
      block_enc_id = rec['enc_id']
    if rec['value'] == 'True':
      if block_start is None:
        block_start = rec['tsp']
        block_c = int(rec['confidence'])
      else:
        block_c = block_c | int(rec['confidence'])
    else:
      block_end = rec['tsp']
      block_c = block_c | int(rec['confidence'])
      if block_start is not None:
        block_update_sql = (update_sql + \
          "AND tsp < timestamptz '%(tsp_end)s'") \
          % {'enc_id':block_enc_id, 'block_c':block_c,
             'tsp_start':block_start, 'tsp_end':block_end,
             'twf_table': twf_table}
        cdm.update_twf_sql(block_update_sql)
      block_start = None
      block_end = None
      block_c = 0
  if block_start is not None:
    block_update_sql = update_sql \
      % {'enc_id':block_enc_id, 'block_c':block_c,
         'tsp_start':block_start, 'twf_table': twf_table}
    cdm.update_twf_sql(block_update_sql)

# Special case (mini-pipeline)
def cardio_sofa_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be cardio_sofa
  03/20/2016
  """
  global STOPPED_ACTIONS
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid == 'cardio_sofa', 'wrong fid %s' % fid
  assert fid_input_items[0] == 'mapm' and \
    fid_input_items[1] == 'dopamine_dose' \
    and fid_input_items[2] == 'epinephrine_dose'\
    and fid_input_items[3] == 'dobutamine_dose'\
    and fid_input_items[4] == 'levophed_infusion_dose' \
    and fid_input_items[5] == 'weight', 'wrong fid_input %s' \
    % fid_input
  cdm.clean_twf(fid, value=0, twf_table=twf_table)

  # update cardio_sofa based on mapm
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = 1, %(fid)s_c = mapm_c WHERE mapm < 70
  """ % {'fid':fid, 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

  select_sql = """
  SELECT enc_id, tsp,
    value::json->>'action' as action, value::json->>'dose' as dose,
    confidence
    FROM cdm_t
  WHERE fid = '%s' ORDER BY enc_id, tsp
  """

  update_clause = """
  UPDATE %(twf_table)s SET cardio_sofa = %(threshold)s
  , cardio_sofa_c = %(confidence)s
  WHERE enc_id = %(enc_id)s
  AND cardio_sofa >= 1 AND cardio_sofa < %(threshold)s
  AND tsp >= timestamptz '%(tsp)s'
  ;
  """

  update_clause_with_max_tsp = """
  UPDATE %(twf_table)s SET cardio_sofa = %(threshold)s
  , cardio_sofa_c = %(confidence)s
  WHERE enc_id = %(enc_id)s
  AND cardio_sofa >= 1 AND cardio_sofa < %(threshold)s
  AND tsp >= timestamptz '%(tsp)s'
  AND tsp <= timestamptz '%(max_tsp)s'
  ;
  """

  # update cardio_sofa based on dopamine_dose
  server_cursor = cdm.select_with_sql(select_sql % 'dopamine_dose')
  records = server_cursor.fetchall()
  server_cursor.close()
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
          cdm.update_twf_sql(update_clause_with_max_tsp \
            % {'threshold':threshold, 'confidence':dopamine_c,
               'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
               'max_tsp': records[i+1]['tsp'],
               'twf_table': twf_table})
        else:
          cdm.update_twf_sql(update_clause \
            % {'threshold':threshold, 'confidence':dopamine_c,
               'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
               'twf_table': twf_table})

  # update cardio_sofa based on epinephrine_dose
  # need to check the unit first
  unit = cdm.get_feature_unit('epinephrine_dose')
  if unit == 'mcg/kg/min':
    server_cursor = cdm.select_with_sql(select_sql % 'epinephrine_dose')
    records = server_cursor.fetchall()
    server_cursor.close()
  elif unit == 'mcg/min':
    select_sql_with_weight = """
      select sub.enc_id, sub.tsp,
        sub.value::json->>'action' as action,
        cast(sub.value::json->>'dose' as numeric)/last(weight) as dose,
        sub.confidence
      from
      (SELECT t.enc_id, t.tsp, t.value,
        t.confidence, twf.weight
        FROM cdm_t t
        inner join cdm_twf twf
          on t.fid = '%s' and twf.enc_id = t.enc_id
          and t.tsp >= twf.tsp
        ORDER BY t.enc_id, t.tsp, twf.tsp
      ) as sub
      group by sub.enc_id, sub.tsp, sub.value, sub.confidence
    """
    cdm.log.info("select_sql_with_weight:%s" \
      % (select_sql_with_weight % 'epinephrine_dose'))
    server_cursor = cdm.select_with_sql(select_sql_with_weight \
      % 'epinephrine_dose')
    records = server_cursor.fetchall()
    server_cursor.close()

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
          cdm.update_twf_sql(update_clause_with_max_tsp \
            % {'threshold':threshold, 'confidence':dose_c,
               'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
               'max_tsp': records[i+1]['tsp'],
               'twf_table': twf_table})
        else:
          cdm.update_twf_sql(update_clause \
            % {'threshold':threshold, 'confidence':dose_c,
               'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
               'twf_table': twf_table})
  # update cardio_sofa based on dobutamine_dose
  server_cursor = cdm.select_with_sql(select_sql % 'dobutamine_dose')
  records = server_cursor.fetchall()
  server_cursor.close()
  for i, rec in enumerate(records):
    action = rec['action']
    if not action in STOPPED_ACTIONS and rec['dose'] is not None:
      if float(rec['dose']) > 0:
        dose_c = rec['confidence']
        if i+1 < len(records) \
          and rec['enc_id'] == records[i+1]['enc_id']:
          cdm.update_twf_sql(update_clause_with_max_tsp \
            % {'threshold':2, 'confidence':dose_c,
               'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
               'max_tsp': records[i+1]['tsp'],
               'twf_table': twf_table})
        else:
          cdm.update_twf_sql(update_clause \
            % {'threshold':2, 'confidence':dose_c,
               'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
               'twf_table': twf_table})
  # update cardio_sofa based on levophed_infusion_dose
  # need to check the unit of levophed_infusion_dose
  unit = cdm.get_feature_unit('levophed_infusion_dose')
  if unit == 'mcg/kg/min':
    server_cursor = cdm.select_with_sql(select_sql % 'levophed_infusion_dose')
    records = server_cursor.fetchall()
    server_cursor.close()
  elif unit == 'mcg/min':
    select_sql_with_weight = """
      select sub.enc_id, sub.tsp,
        sub.value::json->>'action' as action,
        cast(sub.value::json->>'dose' as numeric)/last(weight) as dose,
        sub.confidence
      from
      (SELECT t.enc_id, t.tsp, t.value,
        t.confidence, twf.weight
        FROM cdm_t t
        inner join cdm_twf twf
          on t.fid = '%s' and twf.enc_id = t.enc_id
          and t.tsp >= twf.tsp
        ORDER BY t.enc_id, t.tsp, twf.tsp
      ) as sub
      group by sub.enc_id, sub.tsp, sub.value, sub.confidence
    """
    cdm.log.info("select_sql_with_weight:%s" \
      % (select_sql_with_weight % 'levophed_infusion_dose'))
    server_cursor = \
      cdm.select_with_sql(select_sql_with_weight % 'levophed_infusion_dose')
    records = server_cursor.fetchall()
    server_cursor.close()
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
        cdm.update_twf_sql(update_clause_with_max_tsp \
          % {'threshold':threshold, 'confidence':dose_c,
             'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
             'max_tsp': records[i+1]['tsp'],
             'twf_table': twf_table})
      else:
        cdm.update_twf_sql(update_clause \
          % {'threshold':threshold, 'confidence':dose_c,
             'enc_id': rec['enc_id'], 'tsp':rec['tsp'],
             'twf_table': twf_table})


# Special case (mini-pipeline)
def vasopressor_resuscitation_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be vasopressor_resuscitation (TWF, boolean)
  fid_input should be levophed_infusion_dose and dopamine_dose
  """
  assert fid == 'vasopressor_resuscitation', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid_input_items[0] == 'levophed_infusion_dose'\
    and fid_input_items[1] == 'dopamine_dose', \
    'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, value=False, confidence=0, twf_table=twf_table)
  select_sql = """
  select enc_id, tsp, value::json->>'action' as action from cdm_t
  where fid = '%s' order by enc_id, tsp;
  """

  update_sql_with_stop = """
  update %(twf_table)s set vasopressor_resuscitation = True
  where enc_id = %(enc_id)s and tsp >= timestamptz '%(begin)s' and tsp < timestamptz '%(end)s'
  """

  update_sql_wo_stop = """
  update %(twf_table)s set vasopressor_resuscitation = True
  where enc_id = %(enc_id)s and tsp >= timestamptz '%(begin)s'
  """

  server_cursor = \
    cdm.select_with_sql(select_sql % 'levophed_infusion_dose')
  records = server_cursor.fetchall()
  server_cursor.close()
  enc_id_cur = None
  start_tsp = None
  stop_tsp = None
  for rec in records:
    enc_id = rec['enc_id']
    tsp = rec['tsp']
    action = rec['action']
    if enc_id_cur is not None and enc_id_cur != enc_id:
      # update current enc_id
      cdm.update_twf_sql(update_sql_wo_stop % {'enc_id':enc_id_cur,
                           'begin':start_tsp,
                           'twf_table': twf_table})
      enc_id_cur = None
      start_tsp = None
      stop_tsp = None
    if action == 'Stopped':
      stop_tsp = tsp
      if enc_id_cur is not None:
        # cdm.log.info(update_sql_with_stop % (enc_id_cur,\
        #     start_tsp, stop_tsp))
        cdm.update_twf_sql(update_sql_with_stop % {'enc_id':enc_id_cur,
                               'begin':start_tsp,
                               'end':stop_tsp,
                               'twf_table': twf_table})
      enc_id_cur = None
      start_tsp = None
      stop_tsp = None
    else:
      if enc_id_cur is None:
        enc_id_cur = enc_id
        start_tsp = tsp
  if enc_id_cur is not None:
    cdm.update_twf_sql(update_sql_wo_stop % {'enc_id':enc_id_cur,
                         'begin':start_tsp,
                         'twf_table': twf_table})

  server_cursor = \
    cdm.select_with_sql(select_sql % 'dopamine_dose')
  records = server_cursor.fetchall()
  server_cursor.close()
  enc_id_cur = None
  start_tsp = None
  stop_tsp = None
  for rec in records:
    enc_id = rec['enc_id']
    tsp = rec['tsp']
    action = rec['action']
    if enc_id_cur is not None and enc_id_cur != enc_id:
      # update current enc_id
      cdm.update_twf_sql(update_sql_wo_stop % {'enc_id':enc_id_cur,
                           'begin':start_tsp,
                           'twf_table': twf_table})
      enc_id_cur = None
      start_tsp = None
      stop_tsp = None
    if action == 'Stopped':
      stop_tsp = tsp
      if enc_id_cur is not None:
        cdm.update_twf_sql(update_sql_with_stop % {'enc_id':enc_id_cur,
                               'begin':start_tsp,
                               'end':stop_tsp,
                               'twf_table': twf_table})
      enc_id_cur = None
      start_tsp = None
      stop_tsp = None
    else:
      if enc_id_cur is None:
        enc_id_cur = enc_id
        start_tsp = tsp
  if enc_id_cur is not None:
    cdm.update_twf_sql(update_sql_wo_stop % {'enc_id':enc_id_cur,
                         'begin':start_tsp,
                         'twf_table': twf_table})



# Special case (mini-pipeline)
def heart_attack_update(fid, fid_input, cdm, twf_table='cdm_twf'):
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
  # clean previous values
  cdm.delete_feature_values(fid)
  # Retrieve all records of heart_attack_inhosp
  select_sql = """
  SELECT * FROM cdm_t
  WHERE  fid = 'heart_attack_inhosp'
  ORDER BY enc_id,  tsp;
  """
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()

  # Retrieve troponin above threshold
  # and EKG procedure order times to corroborate time of diagnosis
  select_ekg = """
  SELECT * FROM cdm_t
  WHERE
    cdm_t.fid ~ 'ekg_proc'
    and cdm_t.enc_id = %(enc_id)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
  ORDER BY tsp
  """
  select_troponin = """
  SELECT tsp FROM %(twf_table)s
  WHERE troponin > 0
    and enc_id = %(enc_id)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
  ORDER BY tsp
  """
  # For each instance of heart attack
  # Set diagnosis time to be min (time of troponin extreme value, time of EKG
  # Update cdm_t with heart_attack=TRUE at diagnosis time
  for record in records:
    enc_id = record['enc_id']
    tsp = record['tsp']
    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    server_cursor = cdm.select_with_sql(select_troponin % {'enc_id':enc_id,
                                 'tsp':tsp,
                                 'twf_table': twf_table})
    evidence = server_cursor.fetchall()
    server_cursor.close()
    t1 = tsp
    if len(evidence) > 0:
      t1 = evidence[0]['tsp']

    server_cursor = cdm.select_with_sql(select_ekg % {'enc_id':enc_id,
                              'tsp':tsp})
    evidence = server_cursor.fetchall()
    server_cursor.close()
    t2 = tsp
    if len(evidence) > 0:
      t2 = evidence[0]['tsp']

    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = min(t1, t2)
    conf=confidence.NO_TRANSFORM

    cdm.upsert_t([enc_id, tsp_first, fid, 'True', conf])

# Special case (mini-pipeline)
def stroke_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid_input should be stroke_inhosp, ct_proc, mri_proc
  fid should be heart_attack (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'stroke', 'fid %s is stroke' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'stroke_inhosp' == fid_input_items[0] and 'ct_proc' == fid_input_items[1] \
      and 'mri_proc' == fid_input_items[2], "fid_input error: %s" % fid_input_items

  # clean previous values
  cdm.delete_feature_values(fid)

  # Retrieve all records of stroke_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM cdm_t
  WHERE  fid = 'stroke_inhosp'
  ORDER BY enc_id,  tsp;
  """
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()

  # Retrieve CT and MRI order times to corroborate time of diagnosis
  select_sql = """
  SELECT * FROM cdm_t
  WHERE
    cdm_t.fid ~ 'ct_proc|mri_proc'
    and cdm_t.enc_id = %(enc_id)s
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

    server_cursor = cdm.select_with_sql(select_sql % {'enc_id':enc_id,
                              'tsp':tsp})
    evidence = server_cursor.fetchall()
    server_cursor.close()


    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = tsp
    conf=confidence.NO_TRANSFORM

    if len(evidence) > 0:
      tsp_first = evidence[0]['tsp']

    cdm.upsert_t([enc_id, tsp_first, fid, 'True', conf])

# Special case (mini-pipeline)
def gi_bleed_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid_input should be gi_bleed_inhosp, ct_proc, mri_proc
  fid should be gi_bleed (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'gi_bleed', 'fid %s is gi_bleed' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'gi_bleed_inhosp' == fid_input_items[0] and 'ct_proc' == fid_input_items[1] \
      and 'mri_proc' == fid_input_items[2], "fid_input error: %s" % fid_input_items

  # clean previous values
  cdm.delete_feature_values(fid)
  # Retrieve all records of gi_bleed_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM cdm_t
  WHERE  fid = 'gi_bleed_inhosp'
  ORDER BY enc_id,  tsp;
  """
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()

  # Retrieve CT and MRI order times to corroborate time of diagnosis
  select_sql = """
  SELECT * FROM cdm_t
  WHERE
    cdm_t.fid ~ 'ct_proc|mri_proc'
    and cdm_t.enc_id = %(enc_id)s
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

    server_cursor = cdm.select_with_sql(select_sql % {'enc_id':enc_id,
                              'tsp':tsp})
    evidence = server_cursor.fetchall()
    server_cursor.close()

    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = tsp
    conf=confidence.NO_TRANSFORM

    if len(evidence) > 0:
      tsp_first = evidence[0]['tsp']

    cdm.upsert_t([enc_id, tsp_first, fid, 'True', conf])




# Special case (mini-pipeline)
def severe_pancreatitis_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid_input should be severe_pancreatitis_inhosp, ct_proc, mri_proc
  fid should be heart_attack (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'severe_pancreatitis', 'fid %s is severe_pancreatitis' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'severe_pancreatitis_inhosp' == fid_input_items[0] and 'ct_proc' == fid_input_items[1] \
      and 'mri_proc' == fid_input_items[2], "fid_input error: %s" % fid_input_items
  # clean previous values
  cdm.delete_feature_values(fid)
  # Retrieve all records of severe_pancreatitis_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM cdm_t
  WHERE  fid = 'severe_pancreatitis_inhosp'
  ORDER BY enc_id,  tsp;
  """
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()

  # Retrieve CT and MRI order times to corroborate time of diagnosis
  select_sql = """
  SELECT * FROM cdm_t
  WHERE
    cdm_t.fid ~ 'ct_proc|mri_proc'
    and cdm_t.enc_id = %(enc_id)s
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

    server_cursor = cdm.select_with_sql(select_sql % {'enc_id':enc_id,
                              'tsp':tsp})
    evidence = server_cursor.fetchall()
    server_cursor.close()


    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = tsp
    conf=confidence.NO_TRANSFORM

    if len(evidence) > 0:
      tsp_first = evidence[0]['tsp']

    cdm.upsert_t([enc_id, tsp_first, fid, 'True', conf])

# Special case (mini-pipeline)
def pulmonary_emboli_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid_input should be pulmonary_emboli_inhosp, ct_proc, ekg_proc
  fid should be heart_attack (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'pulmonary_emboli', 'fid %s is pulmonary_emboli' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'pulmonary_emboli_inhosp' == fid_input_items[0] and 'ct_proc' == fid_input_items[1] \
      and 'ekg_proc' == fid_input_items[2], "fid_input error: %s" % fid_input_items
  # clean previous values
  cdm.delete_feature_values(fid)
  # Retrieve all records of pulmonary_emboli_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM cdm_t
  WHERE  fid = 'pulmonary_emboli_inhosp'
  ORDER BY enc_id,  tsp;
  """
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()

  # Retrieve CT and MRI order times to corroborate time of diagnosis
  select_sql = """
  SELECT * FROM cdm_t
  WHERE
    cdm_t.fid ~ 'ct_proc|mri_proc'
    and cdm_t.enc_id = %(enc_id)s
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

    server_cursor = cdm.select_with_sql(select_sql % {'enc_id':enc_id,
                              'tsp':tsp})
    evidence = server_cursor.fetchall()
    server_cursor.close()


    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = tsp
    conf=confidence.NO_TRANSFORM

    if len(evidence) > 0:
      tsp_first = evidence[0]['tsp']

    cdm.upsert_t([enc_id, tsp_first, fid, 'True', conf])

# Special case (mini-pipeline)
def bronchitis_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid_input should be bronchitis_inhosp, chest_xray, bacterial_culture
  fid should be heart_attack (T)
  Set (id, time) to 1 when heart attack is diagnosed
  """

  assert fid == 'bronchitis', 'fid %s is bronchitis' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'bronchitis_inhosp' == fid_input_items[0] and 'chest_xray' == fid_input_items[1] \
      and 'bacterial_culture' == fid_input_items[2], "fid_input error: %s" % fid_input_items
  # clean previous values
  cdm.delete_feature_values(fid)
  # Retrieve all records of bronchitis_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM cdm_t
  WHERE  fid = 'bronchitis_inhosp'
  ORDER BY enc_id,  tsp;
  """
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()

  # Retrieve chest x-ray and bacterial culture order times to corroborate time of diagnosis
  select_sql = """
  SELECT * FROM cdm_t
  WHERE
    cdm_t.fid ~ 'chest_xray|bacterial_culture'
    and cdm_t.enc_id = %(enc_id)s
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

    server_cursor = cdm.select_with_sql(select_sql % {'enc_id':enc_id,
                              'tsp':tsp})
    evidence = server_cursor.fetchall()
    server_cursor.close()


    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = tsp
    conf=confidence.NO_TRANSFORM

    if len(evidence) > 0:
      tsp_first = evidence[0]['tsp']

    cdm.upsert_t([enc_id, tsp_first, fid, 'True', conf])

# Special case (mini-pipeline)
def acute_kidney_failure_update(fid, fid_input, cdm, twf_table='cdm_twf'):
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
  # clean previous values
  cdm.delete_feature_values(fid)
  # Retrieve all records of acute_kidney_failure_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM cdm_t
  WHERE  fid = 'acute_kidney_failure_inhosp'
  ORDER BY enc_id,  tsp;
  """
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()


  select_cr = """
  SELECT tsp FROM %(twf_table)s
  WHERE creatinine > 5
    and enc_id = %(enc_id)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
  ORDER BY tsp
  """

  select_uo = """
  SELECT tsp FROM %(twf_table)s
  WHERE urine_output_24hr < 500
    and enc_id = %(enc_id)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
    AND
      tsp - (SELECT min(cdm_twf_2.tsp) FROM %(twf cdm_twf_2_table)s
           WHERE cdm_twf_2.enc_id = cdm_twf.enc_id)
      >= interval '24 hours'
  ORDER BY tsp
  """

  select_di = """
  SELECT tsp, value FROM cdm_t
  WHERE fid = 'dialysis'
    and enc_id = %(enc_id)s
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

    server_cursor = cdm.select_with_sql(select_cr % {'enc_id':enc_id,
                             'tsp':tsp,'twf_table': twf_table})
    evidence = server_cursor.fetchall()
    server_cursor.close()
    t1 = tsp
    if len(evidence) > 0:
      t1 = evidence[0]['tsp']

    server_cursor = cdm.select_with_sql(select_uo % {'enc_id':enc_id,
                             'tsp':tsp,'twf_table': twf_table})
    evidence = server_cursor.fetchall()
    server_cursor.close()
    t2 = tsp
    if len(evidence) > 0:
      t2 = evidence[0]['tsp']

    server_cursor = cdm.select_with_sql(select_di % {'enc_id':enc_id,
                             'tsp':tsp})
    evidence = server_cursor.fetchall()
    server_cursor.close()
    t3 = tsp
    if len(evidence) > 0:
      if evidence[0]['value'] == 'True':
        t3 = evidence[0]['tsp']
    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = min(t1, t2, t3)
    conf=confidence.NO_TRANSFORM

    cdm.upsert_t([enc_id, tsp_first, fid, 'True', conf])




# Special case (mini-pipeline)
def ards_update(fid, fid_input, cdm, twf_table='cdm_twf'):
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
  cdm.delete_feature_values(fid)
  # Retrieve all records of ards_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM cdm_t
  WHERE  fid = 'ards_inhosp'
  ORDER BY enc_id,  tsp;
  """
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()

  select_ptf = """
  SELECT tsp FROM %(twf_table)s
  WHERE pao2_to_fio2 < 100
    and enc_id = %(enc_id)s
    and tsp >= timestamptz '%(tsp)s'
    and timestamptz '%(tsp)s' <= tsp + interval '24 hours'
  ORDER BY tsp
  """

  select_vent = """
  SELECT tsp, value FROM cdm_t
  WHERE fid = 'vent'
    and enc_id = %(enc_id)s
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

    server_cursor = cdm.select_with_sql(select_ptf % {'enc_id':enc_id,
                             'tsp':tsp,'twf_table': twf_table})
    evidence = server_cursor.fetchall()
    server_cursor.close()
    t1 = tsp
    if len(evidence) > 0:
      t1 = evidence[0]['tsp']

    server_cursor = cdm.select_with_sql(select_vent % {'enc_id':enc_id,
                             'tsp':tsp})
    evidence = server_cursor.fetchall()
    server_cursor.close()
    t2 = tsp
    if len(evidence) > 0:
      if evidence[0]['value'] == 'True':
        t2 = evidence[0]['tsp']
    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = min(t1, t2)
    conf=confidence.NO_TRANSFORM

    cdm.upsert_t([enc_id, tsp_first, fid, 'True', conf])

# Special case (mini-pipeline)
def hepatic_failure_update(fid, fid_input, cdm, twf_table='cdm_twf'):
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
  # clean previous values
  cdm.delete_feature_values(fid)
  # Retrieve all records of hepatic_inhosp
  select_sql = """
  SELECT distinct enc_id, tsp FROM cdm_t
  WHERE  fid = 'hepatic_failure_inhosp'
  ORDER BY enc_id,  tsp;
  """
  server_cursor = cdm.select_with_sql(select_sql)
  records = server_cursor.fetchall()
  server_cursor.close()


  # Retrieve bilirubin order times to corroborate time of diagnosis
  select_sql = """
  SELECT tsp FROM %(twf_table)s
  WHERE
    bilirubin > 12
    and enc_id = %(enc_id)s
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

    server_cursor = cdm.select_with_sql(select_sql % {'enc_id':enc_id,
                              'tsp':tsp,'twf_table': twf_table})
    evidence = server_cursor.fetchall()
    server_cursor.close()


    # By default set datetime of diagnosis to time given in ProblemList table
    # This datetime only specifies date though
    tsp_first = tsp
    conf=confidence.NO_TRANSFORM

    if len(evidence) > 0:
      tsp_first = evidence[0]['tsp']

    cdm.upsert_t([enc_id, tsp_first, fid, 'True', conf])
