"""
derive functions
TODO: remove cdm out and use conn
"""
import sys
import etl.confidence as confidence
import datetime
from etl.transforms.primitives.row.transform import STOPPED_ACTIONS
from etl.load.primitives.tbl.acute_organ_failure_update import *
from etl.load.primitives.tbl.acute_pancreatitis_update import *
from etl.load.primitives.tbl.admit_weight_update import *
from etl.load.primitives.tbl.change_since_last_measured import *
from etl.load.primitives.tbl.hemorrhage_update import *
from etl.load.primitives.tbl.hemorrhagic_shock_update import *
from etl.load.primitives.tbl.metabolic_acidosis_update import *
from etl.load.primitives.tbl.mi_update import *
from etl.load.primitives.tbl.septic_shock_iii_update import *
from etl.load.primitives.tbl.septic_shock_iii_update_queries import *
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

def bun_to_cr_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  assert fid == 'bun_to_cr', 'fid %s is not bun_to_cr' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'bun' == fid_input_items[0] and 'creatinine' == fid_input_items[1], \
    "fid_input error: %s" % fid_input_items
  cdm.clean_twf(fid, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = %(bun)s/%(creatinine)s,
    %(fid)s_c = %(creatinine)s_c | %(bun)s_c
  """ % {'fid':fid, 'bun':fid_input_items[0], 
       'creatinine':fid_input_items[1], 'twf_table':twf_table}
  cdm.update_twf_sql(update_clause)

def pao2_to_fio2_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid_input should be pao2 and fio2 (both are TWF)
  fid should be pao2_to_fio2 (TWF)
  """
  assert fid == 'pao2_to_fio2', 'fid %s is not pao2_to_fio2' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'pao2' == fid_input_items[0] and 'fio2' == fid_input_items[1], \
    "fid_input error: %s" % fid_input_items
  cdm.clean_twf(fid, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = %(pao2)s/%(fio2)s*100,
    %(fid)s_c = %(fio2)s_c | %(pao2)s_c
  """ % {'fid':fid, 'pao2':fid_input_items[0], 'fio2':fid_input_items[1], 
       'twf_table':twf_table}
  cdm.update_twf_sql(update_clause)

def hepatic_sofa_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid_input should be bilirubin (TWF), unit is mg/dl
  fid should be hepatic_sofa (TWF)
  """
  assert fid == 'hepatic_sofa', 'wrong fid %s' % fid
  assert fid_input == 'bilirubin', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, value=0, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = (CASE
    WHEN (%(fid_input)s > 12.0) THEN 4
    WHEN (%(fid_input)s >= 6.0) THEN 3
    WHEN (%(fid_input)s >= 2.0) THEN 2
    WHEN (%(fid_input)s >= 1.2) THEN 1
    ELSE 0
  END), %(fid)s_c = %(fid_input)s_c;
  """ % {'fid':fid, 'fid_input':fid_input, 'twf_table':twf_table}
  cdm.log.info("hepatic_sofa_update:%s" % update_clause)
  cdm.update_twf_sql(update_clause)

def qsofa_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid_input should be resp_rate, sbpm, gcs (TWF)
  fid should be qsofa (TWF)
  """
  assert fid == 'qsofa', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert 'resp_rate' == fid_input_items[0] \
    and 'sbpm' == fid_input_items[1]\
    and 'gcs' == fid_input_items[2], \
    "fid_input error: %s" % fid_input_items
  cdm.clean_twf(fid, value=0, confidence=0,twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET qsofa = qsofa + 1, qsofa_c = qsofa_c | resp_rate_c
  where resp_rate >= 22 and based_on_popmean(resp_rate_c) = 0;
  """ % {'twf_table': twf_table}
  cdm.log.info("qsofa_update:%s" % update_clause)
  cdm.update_twf_sql(update_clause)
  update_clause = """
  UPDATE %(twf_table)s SET qsofa = qsofa + 1, qsofa_c = qsofa_c | sbpm_c
  where sbpm <= 100 and based_on_popmean(sbpm_c) = 0;
  """ % {'twf_table': twf_table}
  cdm.log.info("qsofa_update:%s" % update_clause)
  cdm.update_twf_sql(update_clause)
  update_clause = """
  UPDATE %(twf_table)s SET qsofa = qsofa + 1, qsofa_c = qsofa_c | gcs_c
  where gcs < 13 and based_on_popmean(gcs_c) = 0;
  """ % {'twf_table': twf_table}
  cdm.log.info("qsofa_update:%s" % update_clause)
  cdm.update_twf_sql(update_clause)

def neurologic_sofa_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid_input should be gcs (TWF)
  fid should be neurologic_sofa (TWF)
  """
  assert fid == 'neurologic_sofa', 'wrong fid %s' % fid
  assert fid_input == 'gcs', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, value=0, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = (CASE
    WHEN (%(fid_input)s < 6) THEN 4
    WHEN (%(fid_input)s < 10) THEN 3
    WHEN (%(fid_input)s < 13) THEN 2
    WHEN (%(fid_input)s < 15) THEN 1
    ELSE 0
  END), %(fid)s_c = %(fid_input)s_c;
  """ % {'fid':fid, 'fid_input':fid_input, 'twf_table':twf_table}
  cdm.log.info("neurologic_sofa_update:%s" % update_clause)
  cdm.update_twf_sql(update_clause)

def hematologic_sofa_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid_input should be platelets (TWF)
  fid should be hematologic_sofa (TWF)
  """
  assert fid == 'hematologic_sofa', 'wrong fid %s' % fid
  assert fid_input == 'platelets', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, value=0, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = (CASE
    WHEN (%(fid_input)s < 20) THEN 4
    WHEN (%(fid_input)s < 50) THEN 3
    WHEN (%(fid_input)s < 100) THEN 2
    WHEN (%(fid_input)s < 150) THEN 1
    ELSE 0
  END), %(fid)s_c = %(fid_input)s_c;
  """ % {'fid':fid, 'fid_input':fid_input, 'twf_table':twf_table}
  cdm.log.info("hematologic_sofa_update:%s" % update_clause)
  cdm.update_twf_sql(update_clause)



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
# def minutes_since_organ_fail_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     """
#     TODO
#     fid should be minutes_since_organ_fail (TWF)
#     fid input should be any_organ_failure (TWF)
#     """
#     assert fid == 'minutes_since_organ_fail', 'wrong fid %s' % fid
#     assert fid_input == 'any_organ_failure', 'wrong fid_input %s' % fid_input
#     cdm.clean_twf(fid, twf_table=twf_table)
#     # first, find all the times for all given enc_id where any_organ_failure
#     # is True
#     select_sql = """
#     SELECT enc_id, tsp, jh_organ_failure_c FROM %(twf_table)s
#     WHERE any_organ_failure is TRUE
#     ORDER BY enc_id, tsp;
#     """
#     server_cursor = cdm.select_with_sql(select_sql)
#     block_start_tsp = None
#     block_end_tsp = None
#     confidence = None
#     enc_id = ''
#     update_sql = """
#     UPDATE %(twf_table)s SET minutes_since_organ_fail =
#     EXTRACT(EPOCH FROM (tsp - %(start)s))/60,
#     minutes_since_organ_fail_c = %(confidence)s
#     WHERE enc_id = %(enc_id)s and tsp > %(start)s
#     """
#     records = server_cursor.fetchall()
#     server_cursor.close()
#     for record in records:
#         # second, for each block update the fid value
#         if enc_id != record['enc_id']:
#             # new enc_id
#             # update for last enc_id
#             if block_start_tsp:
#                 if block_end_tsp is None:
#                     cdm.update_twf_sql(update_sql \
#                         % {'start': block_start_tsp, 'enc_id': enc_id,
#                            'confidence': confidence})
#                 else:
#                     cdm.update_twf_sql(update_sql \
#                         % {'start': block_start_tsp, 'enc_id': enc_id,
#                            'confidence': confidence} \
#                         + ' and tsp < %s' % block_end_tsp)
#             enc_id = record['enc_id']
#             block_start_tsp = None
#             confidence = None
#         if block_start_tsp:
#             block_end_tsp = record['tsp']
#             cdm.update_twf_sql(update_sql \
#                 % {'start': block_start_tsp, 'enc_id': enc_id,
#                    'confidence': confidence} \
#                 + ' and tsp < %s' % block_end_tsp)
#         block_start_tsp = record['tsp']
#         confidence = record['jh_organ_failure_c']
#         block_end_tsp = None


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

# def minutes_since_antibiotics_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     """
#     TODO
#     fid should be minutes_since_antibiotics (TWF)
#     fid input should be any_antibiotics (T)
#     Null cases:
#     1. if an encounter has not taken any antibiotics, then all values of
#     this feature is null
#     2. if no antibiotics was taken before, the rest values of this feature
#     are null
#     """
#     assert fid == 'minutes_since_antibiotics', 'wrong fid %s' % fid
#     assert fid_input == 'any_antibiotics', 'wrong fid_input %s' % fid_input
#     cdm.clean_twf(fid, twf_table=twf_table)
#     # first, find all timestamps for all given enc_id that any_antibiotics = 1
#     select_sql = """
#     SELECT * FROM cdm_t
#     WHERE fid = 'any_antibiotics' and cast(value as boolean) is TRUE
#     ORDER BY enc_id, tsp;
#     """
#     server_cursor = cdm.select_with_sql(select_sql)
#     block_start_tsp = None
#     block_end_tsp = None
#     confidence = None
#     enc_id = ''
#     update_sql = """
#     UPDATE %(twf_table)s SET minutes_since_antibiotics =
#     EXTRACT(EPOCH FROM (tsp - '%(start)s'::timestamp without time zone))/60,
#     minutes_since_antibiotics_c = %(confidence)s
#     WHERE enc_id = %(enc_id)s and tsp >= '%(start)s'::timestamp without time zone
#     """
#     records = server_cursor.fetchall();
#     server_cursor.close()
#     for record in records:
#         # second, for each block update the fid value
#         if enc_id != record['enc_id']:
#             # new enc_id
#             # update for last enc_id
#             if block_start_tsp:
#                 if block_end_tsp is None:
#                     cdm.update_twf_sql(update_sql \
#                         % {'start': block_start_tsp, 'enc_id': enc_id,
#                            'confidence': confidence})
#                 else:
#                     cdm.update_twf_sql(update_sql \
#                         % {'start': block_start_tsp, 'enc_id': enc_id,
#                            'confidence': confidence} \
#                         + " and tsp < '%s'::timestamp without time zone" \
#                             % block_end_tsp)
#             enc_id = record['enc_id']
#             block_start_tsp = None
#             confidence = None
#         if block_start_tsp:
#             block_end_tsp = record['tsp']
#             cdm.update_twf_sql(update_sql \
#                 % {'start': block_start_tsp, 'enc_id': enc_id,
#                    'confidence': confidence} \
#                 + " and tsp < '%s'::timestamp without time zone" % block_end_tsp)
#         block_start_tsp = record['tsp']
#         confidence = record['confidence']
#         block_end_tsp = None


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
# def minutes_to_shock_onset_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     """
#     TODO
#     fid should be minutes_to_shock_onset (TWF)
#     fid input should be septic_shock (TWF)
#     """
#     assert fid == 'minutes_to_shock_onset', 'wrong fid %s' % fid
#     assert fid_input == 'septic_shock', 'wrong fid_input %s' % fid_input

#     select_sql = """
#     SELECT *
#     FROM
#         (SELECT enc_id, tsp, %(fid_input)s, %(fid_input)s_c,
#                 lag(enc_id) over w as enc_id_prev,
#                 lag(%(fid_input)s) over w as %(fid_input)s_prev
#          FROM %(twf_table)s
#          WINDOW w as (order by enc_id,tsp)
#          ORDER by enc_id, tsp) as query
#     WHERE query.%(fid_input)s IS DISTINCT FROM query.%(fid_input)s_prev OR
#           query.enc_id IS DISTINCT FROM query.enc_id_prev
#     """ % {'fid_input': fid_input}
#     update_sql_positive = """
#     UPDATE %(twf_table)s SET minutes_to_shock_onset =
#     EXTRACT(EPOCH FROM (%(end)s - tsp))/60,
#     minutes_to_shock_onset_c = %(confidence)s
#     WHERE enc_id = %(enc_id)s and tsp >= %(start)s
#     """
#     update_sql_negative = """
#     UPDATE %(twf_table)s SET minutes_to_shock_onset =
#     (EXTRACT(EPOCH FROM (tsp - %(start)s))/60)*-1,
#     minutes_to_shock_onset_c = %(confidence)s
#     WHERE enc_id = %(enc_id)s and tsp >= %(start)s
#     """
#     server_cursor = cdm.select_with_sql(select_sql)
#     block_start_tsp = None
#     block_end_tsp = None
#     septic_shock = None
#     confidence = None
#     enc_id = ''
#     for record in server_cursor.fetchall():
#         # second, for each block update the fid value
#         if enc_id != record['enc_id']:
#             # new enc_id
#             # update for last enc_id
#             if block_start_tsp:
#                 if block_end_tsp is None:
#                     if septic_shock:
#                         cdm.update_twf_sql(update_sql_negative \
#                             % {'start': block_start_tsp, 'enc_id': enc_id,
#                                'confidence': confidence})
#             enc_id = record['enc_id']
#             septic_shock = record['septic_shock']
#             confidence = record['septic_shock_c']
#             block_start_tsp = None
#         if block_start_tsp:
#             block_end_tsp = record['tsp']
#             if septic_shock:
#                 cdm.update_twf_sql(update_sql_negative \
#                     % {'start': block_start_tsp, 'enc_id': enc_id,
#                        'confidence': confidence} \
#                     + ' and tsp < %s' % block_end_tsp)
#             else:
#                 cdm.update_twf_sql(update_sql_positive \
#                     % {'start': block_start_tsp, 'enc_id': enc_id,
#                        'confidence': confidence} \
#                     + ' and tsp < %s' % block_end_tsp)
#             septic_shock = record['septic_shock']
#             confidence = record['septic_shock_c']
#         block_start_tsp = record['tsp']
#         block_end_tsp = None
#     # In case that the last episode of the last patient has septic shock
#     if septic_shock:
#         cdm.update_twf_sql(update_sql_negative \
#             % {'start': block_start_tsp, 'enc_id': enc_id})
#     server_cursor.close()




def nbp_mean_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be nbp_mean
  fid_input should be nbp_sys and nbp_dias
  NOTE: how to update confidence value
  """
  assert fid == 'nbp_mean', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid_input_items[0].strip() == 'nbp_dias' \
    and fid_input_items[1].strip() == 'nbp_sys', \
      'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET nbp_mean = nbp_sys/3 + nbp_dias/3*2,
  nbp_mean_c = nbp_sys_c | nbp_dias_c
  ;
  """ % {'twf_table': twf_table}
  cdm.log.info("nbp_mean_update:%s" % update_clause)
  cdm.update_twf_sql(update_clause)


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




def sirs_wbc_oor_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be sirs_wbc_oor (TWF)
  fid input should be wbc (TWF)
  the unit of wbc is * 1K/mm3
  """
  assert fid == 'sirs_wbc_oor', 'wrong fid %s' % fid
  assert fid_input == 'wbc', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = (%(wbc)s < 4 OR %(wbc)s > 12),
  %(fid)s_c = %(wbc)s_c
  ;
  """ % {'fid':fid, 'wbc':fid_input, 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)


def obstructive_pe_shock_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be obstructive_pe_shock (TWF)
  fid input should be lactate, ddimer, spo2, heart_rate (TWF)
  """
  assert fid == 'obstructive_pe_shock', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid_input_items[0] == 'lactate' \
    and fid_input_items[1] == 'ddimer' \
    and fid_input_items[2] == 'spo2' \
    and fid_input_items[3] == 'heart_rate' , \
    'wrong fid_input %s' % fid_input

  cdm.clean_twf(fid, value=0, confidence=0,twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET obstructive_pe_shock = 1,
    obstructive_pe_shock_c = lactate_c | ddimer_c | spo2_c | heart_rate_c
    where lactate > 2
    and ddimer > 0.5
    and spo2 < 95
    and heart_rate > 100    ;
  """ % {'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

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


def sirs_hr_oor_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be sirs_hr_oor (TWF)
  fid input should be heart_rate (TWF)
  """
  assert fid == 'sirs_hr_oor', 'wrong fid %s' % fid
  assert fid_input == 'heart_rate', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = %(hr)s > 90, %(fid)s_c = %(hr)s_c;
  """ % {'fid':fid, 'hr':fid_input, 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

def sirs_resp_oor_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be sirs_resp_oor (TWF)
  fid input should be resp_rate and paco2 (TWF)
  update on 2/2/2016
  """
  assert fid == 'sirs_resp_oor', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid_input_items[0] == 'resp_rate' and fid_input_items[1] == 'paco2', \
    'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, value=False, confidence=0, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET
  sirs_resp_oor = (resp_rate > 20 OR paco2 <= 32),
  sirs_resp_oor_c =
  (case
    when resp_rate > 20 and cast(1-based_on_popmean(resp_rate_c) as bool) then resp_rate_c
    when paco2 <= 32 and cast(1-based_on_popmean(paco2_c) as bool) then paco2_c
    ELSE
    resp_rate_c | paco2_c
  end)
  """ % {'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

def sirs_temperature_oor_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be sirs_temprature_oor (TWF)
  fid input should be temperature (TWF)
  """
  assert fid == 'sirs_temperature_oor', 'wrong fid %s' % fid
  assert fid_input == 'temperature', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = (%(temp)s < 96.8 OR %(temp)s > 100.4),
  %(fid)s_c = %(temp)s_c;
  """ % {'fid':fid, 'temp':fid_input, 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

def shock_idx_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be shock_idx (TWF)
  fid input should be hr, sbpm (both TWF)
  """
  assert fid == 'shock_idx', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid_input_items[0] == 'heart_rate' and fid_input_items[1] == 'sbpm', \
    'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = %(hr)s/%(sbpm)s
  , %(fid)s_c = %(hr)s_c | %(sbpm)s_c
  """ % {'fid':fid, 'hr':fid_input_items[0], 'sbpm':fid_input_items[1],
       'value_tran': confidence.VALUE_TRANSFORMED, 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

# def hypotension_raw_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     """
#     fid should be hypotension_raw (TWF)
#     fid input should be sbpm and mapm (TWF)
#     """
#     assert fid == 'hypotension_raw', 'wrong fid %s' % fid
#     fid_input_items = [item.strip() for item in fid_input.split(',')]
#     assert fid_input_items[0] == 'sbpm' and fid_input_items[1] == 'mapm', \
#         'wrong fid_input %s' % fid_input
#     cdm.clean_twf(fid, value=False, confidence=0, twf_table=twf_table)
#     update_clause = """
#     UPDATE %(twf_table)s SET %(fid)s =
#         (sbpm < 90 and based_on_popmean(sbpm_c) != 1)
#         OR
#         (mapm < 65 and based_on_popmean(mapm_c) != 1),
#     %(fid)s_c = coalesce(%(sbpm)s_c,0) | coalesce(%(mapm)s_c,0);
#     """ % {'fid':fid, 'sbpm':fid_input_items[0], 'mapm':fid_input_items[1]}
#     cdm.update_twf_sql(update_clause)

def hypotension_raw_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  UPDATE 2/22/2016
  fid should be hypotension_raw (TWF)
  fid input should be sbpm and mapm (TWF)
  """
  assert fid == 'hypotension_raw', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid_input_items[0] == 'sbpm' and fid_input_items[1] == 'mapm', \
    'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, value=False, confidence=0, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = true,
    %(fid)s_c = coalesce(%(sbpm)s_c,0) | coalesce(%(mapm)s_c,0)
  where (sbpm < 90 and based_on_popmean(sbpm_c) != 1)
    OR
    (mapm < 65 and based_on_popmean(mapm_c) != 1);
  """ % {'fid':fid, 'sbpm':fid_input_items[0], 'mapm':fid_input_items[1],
       'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

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

# def sirs_intp_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     # update 2/17/2016
#     assert fid == 'sirs_intp', 'wrong fid %s' % fid
#     assert fid_input == 'sirs_raw', 'wrong fid_input %s' % fid
#     cdm.clean_twf(fid, value=False, confidence=0, twf_table=twf_table)
#     # 1. Identify all periods of time where sirs_raw=1 for
#     # at least 5 consecutive hours and set sirs_intp=1
#     select_sql = """
#     SELECT *
#     FROM
#         (SELECT enc_id, tsp, sirs_raw, sirs_raw_c,
#                 lag(enc_id) over w as enc_id_prev,
#                 lag(tsp) over w as tsp_prev,
#                 lag(sirs_raw) over w as sirs_raw_prev
#          FROM %(twf_table)s
#          WINDOW w as (order by enc_id,tsp)
#          ORDER by enc_id, tsp) as query
#     WHERE (query.sirs_raw OR query.sirs_raw_prev) OR
#           query.enc_id IS DISTINCT FROM query.enc_id_prev
#     """
#     server_cursor = cdm.select_with_sql(select_sql)
#     records = server_cursor.fetchall()
#     server_cursor.close()
#     block_start = None
#     block_end = None
#     block_c = 0
#     block_enc_id = None
#     for rec in records:
#         if rec['enc_id'] != rec['enc_id_prev']:
#             # new enc_id
#             # if previous block stands long enough, update
#             if block_start:
#                 block_end = rec['tsp_prev']
#                 duration = block_end - block_start
#                 if duration.total_seconds() >= 5*60*60: # 5 hours
#                     # update
#                     update_sql = """
#                     UPDATE %(twf_table)s SET sirs_intp = 'True',
#                     sirs_intp_c = %(confidence)s
#                     WHERE enc_id = %(enc_id)s
#                     AND tsp >= timestamptz '%(tsp_start)s'
#                     AND tsp <= timestamptz '%(tsp_end)s'
#                     """ % {'confidence': block_c, 'enc_id': block_enc_id,
#                            'tsp_start': block_start, 'tsp_end': block_end}
#                     cdm.update_twf_sql(update_sql)
#             # clear the block
#             block_start = None
#             block_end = None
#             block_c = 0
#             block_enc_id = rec['enc_id']
#         if rec['sirs_raw'] and not rec['sirs_raw_prev']:
#             # from false to true, start a new block
#             block_start = rec['tsp']
#             if rec['sirs_raw_c']:
#                 block_c = block_c | rec['sirs_raw_c']
#         elif not rec['sirs_raw'] and rec['sirs_raw_prev']:
#             # from true to false, end a block
#             block_end = rec['tsp_prev']
#             if block_start:
#                 # need test
#                 duration = block_end - block_start
#                 if duration.total_seconds() >= 5*60*60: # 5 hours
#                     # update
#                     update_sql = """
#                     UPDATE %(twf_table)s SET sirs_intp = 'True',
#                     sirs_intp_c = %(confidence)s
#                     WHERE enc_id = %(enc_id)s
#                     AND tsp >= timestamp '%(tsp_start)s'
#                     AND tsp <= timestamp '%(tsp_end)s'
#                     """ % {'confidence': block_c, 'enc_id': block_enc_id,
#                            'tsp_start': block_start, 'tsp_end': block_end}
#                     cdm.update_twf_sql(update_sql)
#             block_start = None
#             block_end = None
#             block_c = 0
#         else:
#             # both current and previous sirs_raw are 1
#             if rec['sirs_raw_c']:
#                 block_c = block_c | rec['sirs_raw_c']
#     # 2. If two periods where sirs_intp=1 are within 6 hours of each other,
#     # set sirs_intp=1 at all sample times between the two periods
#     select_sql = """
#     SELECT *
#     FROM
#         (SELECT enc_id, tsp, sirs_intp, sirs_intp_c,
#                 lag(enc_id) over w as enc_id_prev,
#                 lag(tsp) over w as tsp_prev,
#                 lag(sirs_intp) over w as sirs_intp_prev,
#                 lag(sirs_intp_c) over w as sirs_intp_c_prev
#          FROM %(twf_table)s
#          WINDOW w as (order by enc_id,tsp)
#          ORDER by enc_id, tsp) as query
#     WHERE (query.sirs_intp or query.sirs_intp_prev) OR
#           (query.enc_id IS DISTINCT FROM query.enc_id_prev)
#     """
#     server_cursor = cdm.select_with_sql(select_sql)
#     records = server_cursor.fetchall()
#     server_cursor.close()
#     interval_start = None
#     interval_end = None
#     interval_c = 0
#     interval_enc_id = -1
#     for rec in records:
#         if interval_enc_id != rec['enc_id']:
#             # new enc_id
#             interval_start = None
#             interval_end = None
#             interval_c = 0
#             interval_enc_id = rec['enc_id']
#         if rec['sirs_intp_prev'] and not rec['sirs_intp']:
#             # new interval
#             interval_start = rec['tsp_prev']
#             if rec['sirs_intp_c_prev']:
#                 interval_c = interval_c | rec['sirs_intp_c_prev']
#         elif not rec['sirs_intp_prev'] and rec['sirs_intp']:
#             # interval end
#             interval_end = rec['tsp']
#             if rec['sirs_intp_c']:
#                 interval_c = interval_c | rec['sirs_intp_c']
#             if interval_start is not None \
#                 and (interval_end - interval_start).total_seconds() \
#                     <= 6 * 3600: # 6 hours
#                 # update
#                 update_sql = """
#                     UPDATE %(twf_table)s SET sirs_intp = 'True',
#                     sirs_intp_c = %(confidence)s
#                     WHERE enc_id = %(enc_id)s
#                     AND tsp > timestamp '%(tsp_start)s'
#                     AND tsp < timestamp '%(tsp_end)s'
#                     """ % {'confidence': interval_c, 'enc_id': interval_enc_id,
#                            'tsp_start': interval_start, 'tsp_end': interval_end}
#                 cdm.update_twf_sql(update_sql)
#             interval_start = None
#             interval_end = None
#             interval_c = 0

def sepsis_related_organ_failure_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be sepsis_related_organ_failure
  fid input should be inr, platelets, creatinine, bilirubin,
            urine_output_24hr, lactate, pao2_to_fio2,
            hypotension_intp all (TWF)
  """
  assert fid == 'sepsis_related_organ_failure', 'wrong fid %s' % fid
  cdm.clean_twf(fid, value=False)
  # hardcoded input
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = TRUE
    , %(fid)s_c = coalesce(inr_c,0)
      | coalesce(platelets_c,0) | coalesce(creatinine_c,0)
      | coalesce(bilirubin_c,0)
      | coalesce(lactate_c,0) | coalesce(pao2_to_fio2_c,0)
      | coalesce(hypotension_intp_c,0)
    WHERE
      (inr > 1.5 and based_on_popmean(inr_c) != 1)
      OR (platelets < 100 and based_on_popmean(platelets_c) != 1)
      OR (lactate < 4.46 and based_on_popmean(lactate_c) != 1)
      OR (lactate > 13.39 and based_on_popmean(lactate_c) != 1)
      OR hypotension_intp is TRUE
      OR (creatinine > 2 and based_on_popmean(creatinine_c) != 1 and
        coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'chronic_kidney_hist' limit 1)
        , False) = False)
      OR (bilirubin > 2 and based_on_popmean(bilirubin_c) != 1 and
        coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'liver_disease_hist' limit 1)
        , False) = False)
      OR (pao2_to_fio2 < 100 and based_on_popmean(pao2_to_fio2_c) != 1 and
        coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'chronic_pulmonary_hist' limit 1)
        , False) = False)
    ;
  """ % {'fid':fid, 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = TRUE
    , %(fid)s_c = coalesce(urine_output_24hr_c,0)
    WHERE %(fid)s is FALSE
      AND urine_output_24hr < 500 AND
      tsp - (SELECT min(cdm_twf_2.tsp) FROM %(twf cdm_twf_2_table)s
           WHERE cdm_twf_2.enc_id = cdm_twf.enc_id)
      >= interval '24 hours'
    ;
  """ % {'fid':fid, 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

def acute_organ_failure_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be acute_organ_failure
  fid input should be inr, platelets, creatinine, chronic_kidney_hist,
    bilirubin, liver_disease_hist, urine_output_24hr, lactate,
    pao2_to_fio2, chronic_pulmonary_hist, hypotension_intp
  UPDATE: 3/21/2017
  """
  assert fid == 'acute_organ_failure', 'wrong fid %s' % fid
  cdm.clean_twf(fid, value=False)
  # hardcoded input
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = TRUE
    , %(fid)s_c = coalesce(inr_c,0)
      | coalesce(platelets_c,0) | coalesce(creatinine_c,0)
      | coalesce(bilirubin_c,0)
      | coalesce(lactate_c,0) | coalesce(pao2_to_fio2_c,0)
      | coalesce(hypotension_intp_c,0)
    WHERE
      (inr > 1.5 and based_on_popmean(inr_c) != 1)
      OR (platelets < 100 and based_on_popmean(platelets_c) != 1)
      OR (lactate > 4.0 and based_on_popmean(lactate_c) != 1)
      OR hypotension_intp is TRUE
      OR (creatinine > 2 and based_on_popmean(creatinine_c) != 1 and
        coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'chronic_kidney_hist' limit 1)
        , False) = False)
      OR (bilirubin > 2 and based_on_popmean(bilirubin_c) != 1 and
        coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'liver_disease_hist' limit 1)
        , False) = False)
      OR (pao2_to_fio2 < 100 and based_on_popmean(pao2_to_fio2_c) != 1 and
        coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'chronic_pulmonary_hist' limit 1)
        , False) = False)
    ;
  """ % {'fid':fid, 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = TRUE
    , %(fid)s_c = coalesce(urine_output_24hr_c,0)
    WHERE %(fid)s is FALSE
      AND
        urine_output_24hr < 500
      AND
        tsp - (SELECT min(cdm_twf_2.tsp) FROM %(twf_table)s cdm_twf_2
             WHERE cdm_twf_2.enc_id = cdm_twf.enc_id)
        >= interval '24 hours'
      AND
        coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'chronic_kidney_hist' limit 1)
        , False) = False
    ;
  """ % {'fid':fid, 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

def severe_sepsis_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  update 4/17/2016
  USE sirs_raw INSTEAD OF sirs_intp
  fid should be severe_sepsis
  fid input should be sirs_intp, acute_organ_failure (TWF)
  and infections_angus_hist, infections_angus_diag, sepsis_note (S)
  """
  assert fid == 'severe_sepsis', 'wrong fid %s' % fid
  cdm.clean_twf(fid, value=False)
  # hardcoded input
  update_clause = """
  UPDATE %(twf_table)s SET severe_sepsis = TRUE
    , severe_sepsis_c = coalesce(acute_organ_failure_c,0)
      | coalesce(sirs_raw_c, 0)
    WHERE sirs_raw is true
    AND acute_organ_failure is true
    AND enc_id in
    (
      SELECT distinct enc_id FROM cdm_s WHERE
      (fid = 'infections_angus_diag' AND value like 'True')
      OR (fid = 'infections_angus_hist' AND value like 'True')
      OR (fid = 'sepsis_note' AND value like 'True')
    )
    ;
  """ % {'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

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


def cmi_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  update on 3/21/2017
  fid should be cmi
  fid input should be severe_sepsis, fluid_resuscitation,
            vasopressor_resuscitation, and fluids_intake_1hr (TWF)
  """
  assert fid == 'cmi', 'wrong fid %s' % fid
  cdm.clean_twf(fid, value='False', confidence=confidence.NULL, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET cmi = (CASE
    WHEN
      (
        (fluid_resuscitation is TRUE
          or vasopressor_resuscitation is true)
        OR fluids_intake_1hr > 250
      )
      AND
      (select bool_or(severe_sepsis) from %(twf_table)s twf
           where twf.enc_id = cdm_twf.enc_id
           and twf.tsp <= cdm_twf.tsp
           and cdm_twf.tsp - twf.tsp < interval '6 hours'
      )
      THEN TRUE
    ELSE FALSE
    END),
    cmi_c = cmi_c | coalesce(severe_sepsis_c,0)
      | coalesce(fluid_resuscitation_c,0)
      | coalesce(vasopressor_resuscitation_c,0)
      | coalesce(fluids_intake_1hr_c,0)
    ;
  """ % {'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)
# def cmi_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     """
#     fid should be cmi
#     fid input should be sirs_intp, sufficient_fluid_replacement, (TWF)
#     infections_angus, sepsis_note (S)
#     NOTE: here I include infections_angus_hist!
#     """
#     assert fid == 'cmi', 'wrong fid %s' % fid
#     cdm.clean_twf(fid, value='False', confidence=confidence.NULL, twf_table=twf_table)
#     # first select enc_id has infections_angus and sepsis_note
#     select_sql = """
#         SELECT distinct enc_id FROM cdm_s WHERE
#         (fid = 'infections_angus_diag' AND value like 'True')
#         OR (fid = 'infections_angus_hist' AND value like 'True')
#         OR (fid = 'sepsis_note' AND value like 'True')
#     """
#     server_cursor = cdm.select_with_sql(select_sql)
#     encounters = server_cursor.fetchall()
#     server_cursor.close()
#     for rec in encounters:
#         # hardcoded input
#         update_clause = """
#         UPDATE %(twf_table)s SET %(fid)s = (CASE
#             WHEN sirs_intp is TRUE AND sufficient_fluid_replacement is TRUE
#                 THEN TRUE
#             ELSE FALSE
#             END), %(fid)s_c = %(fid)s_c | coalesce(sirs_intp_c,0)
#                 | coalesce(sufficient_fluid_replacement_c,0)
#             WHERE enc_id = %(enc_id)s
#             ;
#         """ % {'fid':fid, 'enc_id': rec['enc_id']}
#         cdm.update_twf_sql(update_clause)

# def cmi_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     """
#     update on 2/1/2016
#     fid should be cmi
#     fid input should be severe_sepsis, sufficient_fluid_replacement, and
#     fluids_intake_1hr (TWF)
#     """
#     assert fid == 'cmi', 'wrong fid %s' % fid
#     cdm.clean_twf(fid, value='False', confidence=confidence.NULL, twf_table=twf_table)

#     update_clause = """
#     UPDATE %(twf_table)s SET cmi = (CASE
#         WHEN severe_sepsis is TRUE
#         AND (sufficient_fluid_replacement is TRUE OR fluids_intake_1hr > 250)
#             THEN TRUE
#         ELSE FALSE
#         END),
#         cmi_c = cmi_c | coalesce(severe_sepsis_c,0)
#             | coalesce(sufficient_fluid_replacement_c,0)
#             | coalesce(fluids_intake_1hr_c,0)
#         ;
#     """
#     cdm.update_twf_sql(update_clause)

# def septic_shock_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     assert fid == 'septic_shock', 'wrong fid %s' % fid
#     # hard code the fid_input
#     cdm.clean_twf(fid, value='False', confidence=confidence.NULL, twf_table=twf_table)
#     # first select enc_id has infections_angus and sepsis_note
#     select_sql = """
#         SELECT distinct enc_id FROM cdm_s WHERE
#         (fid = 'infections_angus_diag' AND value like 'True')
#         OR (fid = 'infections_angus_hist' AND value like 'True')
#     """
#     server_cursor = cdm.select_with_sql(select_sql)
#     encounters = server_cursor.fetchall()
#     server_cursor.close()
#     for rec in encounters:
#         # hardcoded input
#         update_clause = """
#         UPDATE %(twf_table)s SET %(fid)s = (CASE
#             WHEN severe_sepsis is TRUE AND sufficient_fluid_replacement is TRUE
#                 AND hypotension_intp is TRUE
#                 THEN TRUE
#             ELSE FALSE
#             END), %(fid)s_c = %(fid)s_c | coalesce(sirs_intp_c,0)
#                 | coalesce(sufficient_fluid_replacement_c,0)
#                 | coalesce(hypotension_intp_c,0)
#             WHERE enc_id = %(enc_id)s
#             ;
#         """ % {'fid':fid, 'enc_id': rec['enc_id']}
#         cdm.update_twf_sql(update_clause)

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



# def septic_shock_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     # UPDATE 4/27/2016
#     assert fid == 'septic_shock', 'wrong fid %s' % fid
#     # hard code the fid_input
#     cdm.clean_twf(fid, value='False', confidence=confidence.NULL, twf_table=twf_table)
#     # hardcoded input
#     update_clause = """
#     UPDATE %(twf_table)s SET %(fid)s = TRUE ,
#         %(fid)s_c = %(fid)s_c | coalesce(sirs_intp_c,0)
#             | coalesce(sufficient_fluid_replacement_c,0)
#             | coalesce(hypotension_intp_c,0)

#         WHERE
#         severe_sepsis is TRUE
#         AND sufficient_fluid_replacement is TRUE
#         AND hypotension_intp is TRUE
#         ;
#     """ % {'fid':fid}
#     cdm.update_twf_sql(update_clause)

#     select_sql = """
#     select enc_id, tsp, value::json->>'action' as action from cdm_t
#     where fid = '%s' order by enc_id, tsp;
#     """

#     update_sql_with_stop = """
#     update %(twf_table)s set septic_shock = True
#     where enc_id = %s and tsp >= timestamp '%s' and tsp < timestamp '%s'
#         AND severe_sepsis is TRUE
#         AND hypotension_intp is TRUE
#     """

#     update_sql_wo_stop = """
#     update %(twf_table)s set septic_shock = True
#     where enc_id = %s and tsp >= timestamp '%s'
#         AND severe_sepsis is TRUE
#         AND hypotension_intp is TRUE
#     """

#     server_cursor = \
#         cdm.select_with_sql(select_sql % 'levophed_infusion_dose')
#     records = server_cursor.fetchall()
#     server_cursor.close()
#     enc_id_cur = None
#     start_tsp = None
#     stop_tsp = None
#     for rec in records:
#         enc_id = rec['enc_id']
#         tsp = rec['tsp']
#         action = rec['action']
#         if enc_id_cur is not None and enc_id_cur != enc_id:
#             # update current enc_id
#             cdm.update_twf_sql(update_sql_wo_stop % (enc_id, start_tsp))
#             enc_id_cur = None
#             start_tsp = None
#             stop_tsp = None
#         if action == 'Stopped':
#             stop_tsp = tsp
#             if enc_id_cur is not None:
#                 cdm.log.info(update_sql_with_stop % (enc_id_cur,\
#                     start_tsp, stop_tsp))
#                 cdm.update_twf_sql(update_sql_with_stop % (enc_id_cur,\
#                     start_tsp, stop_tsp))
#             enc_id_cur = None
#             start_tsp = None
#             stop_tsp = None
#         else:
#             if enc_id_cur is None:
#                 enc_id_cur = enc_id
#                 start_tsp = tsp
#     if enc_id_cur is not None:
#         cdm.update_twf_sql(update_sql_wo_stop % (enc_id, start_tsp))

#     server_cursor = \
#         cdm.select_with_sql(select_sql % 'dopamine_dose')
#     records = server_cursor.fetchall()
#     server_cursor.close()
#     enc_id_cur = None
#     start_tsp = None
#     stop_tsp = None
#     for rec in records:
#         enc_id = rec['enc_id']
#         tsp = rec['tsp']
#         action = rec['action']
#         if enc_id_cur is not None and enc_id_cur != enc_id:
#             # update current enc_id
#             cdm.update_twf_sql(update_sql_wo_stop % (enc_id, start_tsp))
#             enc_id_cur = None
#             start_tsp = None
#             stop_tsp = None
#         if action == 'Stopped':
#             stop_tsp = tsp
#             if enc_id_cur is not None:
#                 cdm.update_twf_sql(update_sql_with_stop % (enc_id_cur,\
#                     start_tsp, stop_tsp))
#             enc_id_cur = None
#             start_tsp = None
#             stop_tsp = None
#         else:
#             if enc_id_cur is None:
#                 enc_id_cur = enc_id
#                 start_tsp = tsp
#     if enc_id_cur is not None:
#         cdm.update_twf_sql(update_sql_wo_stop % (enc_id, start_tsp))

def worst_sofa_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be worst_sofa_update
  fid input should be resp_sofa, hepatic_sofa, hematologic_sofa,
            cardio_sofa, neurologic_sofa, renal_sofa
            all (TWF)
  Note currently all inputs are hardcoded below
  """
  assert fid == 'worst_sofa', 'wrong fid %s' % fid
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  for sofa in fid_input_items:
    assert fid.endswith('sofa'), 'wrong fid_input %s' % sofa

  cdm.clean_twf(fid, value=0, twf_table=twf_table)
  sofa_value = ",".join(['%(sofa)s * (1-based_on_popmean(%(sofa)s_c))' \
    % {'sofa':sofa} for sofa in fid_input_items])
  sofa_c_list = "|".join(['coalesce('+sofa+',0)' for sofa in fid_input_items])
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s =
  GREATEST(%(sofa_list)s)
  , %(fid)s_c = %(sofa_c_list)s;
  """ % {'fid':fid, 'sofa_list': fid_input, 'sofa_c_list': sofa_c_list,
       'twf_table': twf_table}
  cdm.log.debug('sql:' + update_clause)
  cdm.update_twf_sql(update_clause)

def any_organ_failure_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid_input should be worst_sofa (TWF)
  fid should be any_organ_failure (TWF)
  """
  assert fid == 'any_organ_failure', 'wrong fid %s' % fid
  assert fid_input == 'worst_sofa', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, value=False)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s = (CASE
    WHEN (%(fid_input)s = 4) THEN TRUE
    ELSE FALSE
  END), %(fid)s_c = %(fid_input)s_c;
  """ % {'fid':fid, 'fid_input':fid_input, 'twf_table': twf_table}
  cdm.log.info("neurologic_sofa_update:%s" % update_clause)
  cdm.update_twf_sql(update_clause)



def sirs_raw_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  fid should be sirs_raw (TWF)
  fid input sirs_temperature_oor, sirs_hr_oor, sirs_resp_oor, sirs_wbc_oor
  (all TWF)
  """
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid == 'sirs_raw', 'wrong fid %s' % fid
  assert fid_input_items[0] == 'sirs_temperature_oor' \
    and fid_input_items[1] == 'sirs_hr_oor' \
    and fid_input_items[2] == 'sirs_resp_oor' \
    and fid_input_items[3] == 'sirs_wbc_oor', \
      'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET %(fid)s =
    (cast(sirs_temperature_oor as int)
      * (1-based_on_popmean(sirs_temperature_oor_c))
    + cast(sirs_hr_oor as int) * (1-based_on_popmean(sirs_hr_oor_c))
    + cast(sirs_resp_oor as int) * (1-based_on_popmean(sirs_resp_oor_c))
    + cast(sirs_wbc_oor as int) * (1-based_on_popmean(sirs_wbc_oor_c))
  >= 2)
  , %(fid)s_c = sirs_temperature_oor_c | sirs_hr_oor_c | sirs_resp_oor_c
  | sirs_wbc_oor_c
  ;
  """ % {'fid':fid, 'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

# def mapm_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     fid_input_items = [item.strip() for item in fid_input.split(',')]
#     assert fid == 'mapm', 'wrong fid %s' % fid
#     assert fid_input_items[0] == 'abp_mean' \
#         and fid_input_items[1] == 'nbp_mean', \
#             'wrong fid_input %s' % fid_input
#     cdm.clean_twf(fid, twf_table=twf_table)
#     update_clause = """UPDATE %(twf_table)s SET mapm = coalesce(abp_mean, nbp_mean),
#     mapm_c = coalesce(abp_mean_c, 0) | coalesce(nbp_mean_c, 0)
#     """
#     cdm.update_twf_sql(update_clause)

def mapm_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  # UPDATE 2/22/2016
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid == 'mapm', 'wrong fid %s' % fid
  assert fid_input_items[0] == 'abp_mean' \
    and fid_input_items[1] == 'nbp_mean', \
      'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET
    mapm = case
      when based_on_popmean(abp_mean_c) = 0 and abp_mean is not null
        then abp_mean
      when based_on_popmean(nbp_mean_c) = 0 and nbp_mean is not null
        then nbp_mean
      else coalesce(abp_mean, nbp_mean)
    end
    ,
    mapm_c =
    case
      when based_on_popmean(abp_mean_c) = 0 and abp_mean is not null
        then abp_mean_c
      when based_on_popmean(nbp_mean_c) = 0 and nbp_mean is not null
        then nbp_mean_c
      else coalesce(abp_mean_c, 0) | coalesce(nbp_mean_c, 0)
    end;
  """ % {'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

# def dbpm_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     """
#     fid: dbpm, diastolic blood pressure (combines invasive and noninvasive)
#     fid_input: abp_dias and nbp_dias
#     deprecated!!!
#     """
#     fid_input_items = [item.strip() for item in fid_input.split(',')]
#     assert fid == 'dbpm', 'wrong fid %s' % fid
#     assert fid_input_items[0] == 'abp_dias' \
#         and fid_input_items[1] == 'nbp_dias', 'wrong fid_input %s' % fid_input
#     cdm.clean_twf(fid, twf_table=twf_table)
#     update_clause = """UPDATE %(twf_table)s SET dbpm = (CASE
#         WHEN (abp_dias is null) or (abp_dias > 0.15 * nbp_dias) THEN nbp_dias
#         ELSE abp_dias
#     END)
#     , dbpm_c = coalesce(abp_dias_c, 0) | coalesce(nbp_dias_c, 0);"""
#     cdm.update_twf_sql(update_clause)


def dbpm_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  # UPDATE 2/22/2016
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid == 'dbpm', 'wrong fid %s' % fid
  assert fid_input_items[0] == 'abp_dias' \
    and fid_input_items[1] == 'nbp_dias', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET dbpm = (CASE
    WHEN based_on_popmean(abp_dias_c) = 1 or abp_dias is null THEN nbp_dias
    WHEN (abp_dias > 0.15 * nbp_dias)
      and based_on_popmean(abp_dias_c) = 0
      and based_on_popmean(nbp_dias_c) = 0 THEN nbp_dias
    ELSE abp_dias
  END)
  , sbpm_c = (CASE
    WHEN based_on_popmean(abp_dias_c) = 1 or abp_dias is null THEN nbp_dias_c
    WHEN (abp_dias > 0.15 * nbp_dias)
      and based_on_popmean(abp_dias_c) = 0
      and based_on_popmean(nbp_dias_c) = 0 THEN nbp_dias_c | abp_dias_c
    ELSE abp_dias_c
  END);
  """ % {'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

# def sbpm_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     fid_input_items = [item.strip() for item in fid_input.split(',')]
#     assert fid == 'sbpm', 'wrong fid %s' % fid
#     assert fid_input_items[0] == 'abp_sys' \
#         and fid_input_items[1] == 'nbp_sys', 'wrong fid_input %s' % fid_input
#     cdm.clean_twf(fid, twf_table=twf_table)
#     update_clause = """UPDATE %(twf_table)s SET sbpm = (CASE
#         WHEN (abp_sys is null) or (abp_sys < 0.15 * nbp_sys) THEN nbp_sys
#         ELSE abp_sys
#     END)
#     , sbpm_c = coalesce(abp_sys_c, 0) | coalesce(nbp_sys_c, 0);"""
#     cdm.update_twf_sql(update_clause)

def sbpm_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  # UPDATE 2/22/2016
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid == 'sbpm', 'wrong fid %s' % fid
  assert fid_input_items[0] == 'abp_sys' \
    and fid_input_items[1] == 'nbp_sys', 'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET sbpm = (CASE
    WHEN based_on_popmean(abp_sys_c) = 1 or abp_sys is null THEN nbp_sys
    WHEN (abp_sys < 0.15 * nbp_sys)
      and based_on_popmean(abp_sys_c) = 0
      and based_on_popmean(nbp_sys_c) = 0 THEN nbp_sys
    ELSE abp_sys
  END)
  , sbpm_c = (CASE
    WHEN based_on_popmean(abp_sys_c) = 1 or abp_sys is null THEN nbp_sys_c
    WHEN (abp_sys < 0.15 * nbp_sys)
      and based_on_popmean(abp_sys_c) = 0
      and based_on_popmean(nbp_sys_c) = 0 THEN nbp_sys_c | abp_sys_c
    ELSE abp_sys_c
  END);
  """ % {'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)




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


def fluid_resuscitation_update(fid, fid_input, cdm, twf_table='cdm_twf'):
  """
  8/19/2016
  fid is fluid_resuscitation
  fid_input is fluids_intake_3hr and weight
  Definition: fluid_resuscitation is true when pt took >= 30cc/kg in 3 hours
  """
  fid_input_items = [item.strip() for item in fid_input.split(',')]
  assert fid == 'fluid_resuscitation', 'wrong fid %s' % fid
  assert fid_input_items[0] == 'fluids_intake_3hr'\
    and fid_input_items[1] == 'weight', \
    'wrong fid_input %s' % fid_input
  cdm.clean_twf(fid, value=False, confidence=0, twf_table=twf_table)
  update_clause = """
  UPDATE %(twf_table)s SET fluid_resuscitation = (CASE
    WHEN fluids_intake_3hr/weight >= 30
    THEN True
    ELSE False
    END),
    fluid_resuscitation_c = (CASE
    WHEN fluids_intake_3hr/weight >= 30
      THEN fluids_intake_3hr_c | weight_c
    ELSE 0
    END)
  """ % {'twf_table': twf_table}
  cdm.update_twf_sql(update_clause)

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

# def sufficient_fluid_replacement_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     """
#     DEPRECATED!
#     5/18/2016
#     Include levophed_infusion_dose and dopamine_dose
#     fid is sufficient_fluid_replacement
#     fid_input is fluids_intake_3hr, levophed_infusion_dose and dopamine_dose
#     """
#     fid_input_items = [item.strip() for item in fid_input.split(',')]
#     assert fid == 'sufficient_fluid_replacement', 'wrong fid %s' % fid
#     assert fid_input_items[0] == 'fluids_intake_3hr'\
#         and fid_input_items[1] == 'levophed_infusion_dose' \
#         and fid_input_items[2] == 'dopamine_dose', \
#         'wrong fid_input %s' % fid_input
#     cdm.clean_twf(fid, value=False, confidence=0, twf_table=twf_table)
#     update_clause = """
#     UPDATE %(twf_table)s SET sufficient_fluid_replacement = (CASE
#         WHEN fluids_intake_3hr > 250
#         THEN True
#         ELSE False
#         END),
#         sufficient_fluid_replacement_c = (CASE
#         WHEN fluids_intake_3hr > 250 THEN fluids_intake_3hr_c
#         ELSE 0
#         END)
#     """
#     cdm.update_twf_sql(update_clause)

#     select_sql = """
#     select enc_id, tsp, value::json->>'action' as action from cdm_t
#     where fid = '%s' order by enc_id, tsp;
#     """

#     update_sql_with_stop = """
#     update %(twf_table)s set sufficient_fluid_replacement = True
#     where enc_id = %s and tsp >= timestamp '%s' and tsp < timestamp '%s'
#     """

#     update_sql_wo_stop = """
#     update %(twf_table)s set sufficient_fluid_replacement = True
#     where enc_id = %s and tsp >= timestamp '%s'
#     """

#     server_cursor = \
#         cdm.select_with_sql(select_sql % 'levophed_infusion_dose')
#     records = server_cursor.fetchall()
#     server_cursor.close()
#     enc_id_cur = None
#     start_tsp = None
#     stop_tsp = None
#     for rec in records:
#         enc_id = rec['enc_id']
#         tsp = rec['tsp']
#         action = rec['action']
#         if enc_id_cur is not None and enc_id_cur != enc_id:
#             # update current enc_id
#             cdm.update_twf_sql(update_sql_wo_stop % (enc_id_cur, start_tsp))
#             enc_id_cur = None
#             start_tsp = None
#             stop_tsp = None
#         if action == 'Stopped':
#             stop_tsp = tsp
#             if enc_id_cur is not None:
#                 cdm.log.info(update_sql_with_stop % (enc_id_cur,\
#                     start_tsp, stop_tsp))
#                 cdm.update_twf_sql(update_sql_with_stop % (enc_id_cur,\
#                     start_tsp, stop_tsp))
#             enc_id_cur = None
#             start_tsp = None
#             stop_tsp = None
#         else:
#             if enc_id_cur is None:
#                 enc_id_cur = enc_id
#                 start_tsp = tsp
#     if enc_id_cur is not None:
#         cdm.update_twf_sql(update_sql_wo_stop % (enc_id_cur, start_tsp))

#     server_cursor = \
#         cdm.select_with_sql(select_sql % 'dopamine_dose')
#     records = server_cursor.fetchall()
#     server_cursor.close()
#     enc_id_cur = None
#     start_tsp = None
#     stop_tsp = None
#     for rec in records:
#         enc_id = rec['enc_id']
#         tsp = rec['tsp']
#         action = rec['action']
#         if enc_id_cur is not None and enc_id_cur != enc_id:
#             # update current enc_id
#             cdm.update_twf_sql(update_sql_wo_stop % (enc_id_cur, start_tsp))
#             enc_id_cur = None
#             start_tsp = None
#             stop_tsp = None
#         if action == 'Stopped':
#             stop_tsp = tsp
#             if enc_id_cur is not None:
#                 cdm.update_twf_sql(update_sql_with_stop % (enc_id_cur,\
#                     start_tsp, stop_tsp))
#             enc_id_cur = None
#             start_tsp = None
#             stop_tsp = None
#         else:
#             if enc_id_cur is None:
#                 enc_id_cur = enc_id
#                 start_tsp = tsp
#     if enc_id_cur is not None:
#         cdm.update_twf_sql(update_sql_wo_stop % (enc_id_cur, start_tsp))


# def sufficient_fluid_replacement_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     """
#     Legacy -- as implemented in MIMIC2
#     fid is sufficient_fluid_replacement
#     fid_input is fluids_intake_24hr and weight
#     """
#     fid_input_items = [item.strip() for item in fid_input.split(',')]
#     assert fid == 'sufficient_fluid_replacement', 'wrong fid %s' % fid
#     assert fid_input_items[0] == 'fluids_intake_24hr' and \
#         fid_input_items[1] == 'weight', 'wrong fid_input %s' % fid_input
#     cdm.clean_twf(fid, value=False, confidence=0, twf_table=twf_table)
#     update_clause = """
#     UPDATE %(twf_table)s SET sufficient_fluid_replacement = (CASE
#         WHEN fluids_intake_24hr > 1200
#         OR fluids_intake_24hr/weight > 20 THEN True
#         ELSE False
#         END),
#         sufficient_fluid_replacement_c = (CASE
#         WHEN fluids_intake_24hr > 1200 THEN fluids_intake_24hr_c
#         WHEN fluids_intake_24hr/weight > 20 THEN
#             fluids_intake_24hr_c | weight_c
#         ELSE 0
#         END)
#     """
#     cdm.update_twf_sql(update_clause)





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


# def hemorrhage_update(fid, fid_input, cdm, twf_table='cdm_twf'):
#     """
#     fid_input should be hemorrhage_inhosp, ct_proc, mri_proc
#     fid should be heart_attack (T)
#     Set (id, time) to 1 when heart attack is diagnosed
#     """

#     assert fid == 'hemorrhage', 'fid %s is hemorrhage' % fid
#     fid_input_items = [item.strip() for item in fid_input.split(',')]
#     assert 'hemorrhage_inhosp' == fid_input_items[0] and 'ct_proc' == fid_input_items[1] \
#             and 'mri_proc' == fid_input_items[2], "fid_input error: %s" % fid_input_items
#     # clean previous values
#     cdm.delete_feature_values(fid)
#     # Retrieve all records of hemorrhage_inhosp
#     select_sql = """
#     SELECT distinct enc_id, tsp FROM cdm_t
#     WHERE  fid = 'hemorrhage_inhosp'
#     ORDER BY enc_id,  tsp;
#     """
#     server_cursor = cdm.select_with_sql(select_sql)
#     records = server_cursor.fetchall()
#     server_cursor.close()

#     # Retrieve CT and MRI order times to corroborate time of diagnosis
#     select_sql = """
#     SELECT * FROM cdm_t
#     WHERE
#         cdm_t.fid ~ 'ct_proc|mri_proc'
#         and cdm_t.enc_id = %(enc_id)s
#         and tsp >= timestamp '%(tsp)s'
#         and timestamp '%(tsp)s' <= tsp + interval '24 hours'
#     ORDER BY tsp
#     """

#     # For each instance of stroke
#     # Set diagnosis time to be min (time of CT, time of  MRI)
#     # Update cdm_t with stroke=TRUE at diagnosis time
#     for record in records:
#         enc_id = record['enc_id']
#         tsp = record['tsp']

#         server_cursor = cdm.select_with_sql(select_sql % {'enc_id':enc_id,
#                                                           'tsp':tsp})
#         evidence = server_cursor.fetchall()
#         server_cursor.close()


#         # By default set datetime of diagnosis to time given in ProblemList table
#         # This datetime only specifies date though
#         tsp_first = tsp
#         conf=confidence.NO_TRANSFORM

#         if len(evidence) > 0:
#             tsp_first = evidence[0]['tsp']

#         cdm.upsert_t([enc_id, tsp_first, fid, 'True', conf])


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
