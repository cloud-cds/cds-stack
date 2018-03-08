"""
dashan_query.py
"""
import os, sys, traceback
import functools
import json
import datetime
import logging
import pytz
import requests
import asyncio
import asyncpg

from jhapi_io import JHAPI
from monitoring import APIMonitor

import dashan_universe.transforms as transforms
from time import sleep
import random

# Globals.
EPIC_SERVER         = os.environ['epic_server'] if 'epic_server' in os.environ else 'prod'
NOTIFICATION_SERVER = os.environ['notification_server'] if 'notification_server' in os.environ else None
v1_flowsheets       = os.environ['v1_flowsheets'].lower() == 'true' if 'v1_flowsheets' in os.environ else False
soi_flowsheet       = os.environ['soi_flowsheet'].lower() == 'true' if 'soi_flowsheet' in os.environ else False

api_monitor = APIMonitor()
if api_monitor.enabled:
  api_monitor.register_metric('EpicNotificationSuccess', 'Count', [('API', api_monitor.monitor_target)])
  api_monitor.register_metric('EpicNotificationFailures', 'Count', [('API', api_monitor.monitor_target)])
  if v1_flowsheets:
    flowsheet_names = ['count','score','threshold','flag','version']
  else:
    flowsheet_names = ['count','score']
  for flowsheet_name in flowsheet_names:
    api_monitor.register_metric('FSPush%sSuccess'  % flowsheet_name.capitalize(), 'Count', [('API', api_monitor.monitor_target)])

logging.basicConfig(format='%(levelname)s|%(asctime)s.%(msecs)03d|%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

epic_notifications = os.environ['epic_notifications']
client_id          = os.environ['jhapi_client_id']
client_secret      = os.environ['jhapi_client_secret']
NUM_RETRY          = int(os.environ['JHAPI_NUM_RETRY']) if 'JHAPI_NUM_RETRY' in os.environ else 5
BASE               = int(os.environ['JHAPI_BACKOFF_BASE']) if 'JHAPI_BACKOFF_BASE' in os.environ else 2
MAX_BACKOFF        = int(os.environ['JHAPI_BACKOFF_MAX']) if 'JHAPI_BACKOFF_MAX' in os.environ else 60

model_in_use       = os.environ['model_in_use']

excluded_units     = os.environ['excluded_units'].split(',') \
                      if 'excluded_units' in os.environ \
                      else ['HCGH LABOR & DELIVERY', 'HCGH EMERGENCY-PEDS', 'HCGH 2C NICU', 'HCGH 1CX PEDIATRICS', 'HCGH 2N MCU']
excluded_units     = ', '.join(map(lambda unit: "'%s'" % unit, excluded_units))

##########################################
# Compact query implementations.
# These pull out multiple component TREWS data and metadata
# components in fewer queries, reducing the # DB roundtrips.
#

# For a patient, returns time series of:
# - trews scores
# - top 3 features that contributed to the each time point
async def get_trews_contributors(db_pool, pat_id, start_hrs=6, start_day=2, end_day=7, sample_mins=30, sample_hrs=12):
  contributor_fn = 'calculate_lmc_contributors' if model_in_use == 'lmc' else 'calculate_trews_contributors'

  rank_limit = 3
  sample_start_hrs = start_hrs
  sample_start_day = start_day
  sample_end_day = end_day
  sample_hr_mins = sample_mins
  sample_day_hrs = sample_hrs

  get_contributors_sql = \
  '''
  with trews_contributors as (
    select enc_id, tsp, trewscore, fid, cdm_value, rnk,
           (case
              when tsp <= now() - interval '%(sample_start_day)s days'
              then date_trunc('day', tsp) + (interval '%(sample_day_hrs)s hours' * floor(date_part('hour', tsp)::float / %(sample_day_hrs)s))
              when tsp <= now() - interval '%(sample_start_hrs)s hours'
              then date_trunc('hour', tsp) + (interval '%(sample_hr_mins)s minutes' * floor(date_part('minute', tsp)::float / %(sample_hr_mins)s))
              else null
              end) as tsp_bucket
    from %(fn)s('%(pid)s', %(rank_limit)s)
          as R(enc_id, tsp, trewscore, fid, trews_value, cdm_value, rnk)
    where tsp >= now() - interval '%(sample_end_day)s days'
    order by tsp_bucket, rnk
  ),
  latest_2_enc_ids as (
      select enc_id, max(tsp) - min(tsp) as duration
      from trews_contributors
      group by enc_id
      order by enc_id desc limit 2
  ),
  desired_enc_ids as (
      select enc_id from latest_2_enc_ids
      order by enc_id desc
      limit (
          select ( case when (
                      select max(duration) from latest_2_enc_ids
                      group by enc_id
                      order by enc_id desc limit 1
                  ) > interval '24 hours'
          then 1 else 2 end )
      )
  )
  select * from (
    select tsp, trewscore, fid, cdm_value, rnk
    from trews_contributors
    where enc_id in (select enc_id from desired_enc_ids)
    and tsp_bucket is null
    union all (
      select R.tsp,
             avg(R.trewscore) over (partition by R.tsp),
             R.fid, R.cdm_value, R.rnk
      from (
        select tsp_bucket as tsp,
               avg(trewscore) as trewscore,
               first(fid order by trewscore desc) as fid,
               first(cdm_value order by trewscore desc) as cdm_value,
               rnk
        from trews_contributors
        where enc_id in (select enc_id from desired_enc_ids)
        and tsp_bucket is not null
        group by tsp_bucket, rnk
      ) R
    )
  ) R
  order by tsp, rnk;
  ''' % { 'fn'               : contributor_fn,
          'pid'              : pat_id,
          'rank_limit'       : rank_limit,
          'sample_start_hrs' : sample_start_hrs,
          'sample_start_day' : sample_start_day,
          'sample_end_day'   : sample_end_day,
          'sample_hr_mins'   : sample_hr_mins,
          'sample_day_hrs'   : sample_day_hrs
         }

  # get_contributors_sql = \
  # '''
  # select tsp, trewscore, fid, cdm_value, rnk
  # from calculate_trews_contributors('%(pid)s', %(rank_limit)s)
  #         as R(enc_id, tsp, trewscore, fid, trews_value, cdm_value, rnk)
  # order by tsp
  # ''' % {'pid': pat_id, 'rank_limit': rank_limit}

  async with db_pool.acquire() as conn:
    result = await conn.fetch(get_contributors_sql)

    timestamps = []
    trewscores = []
    tf_names   = [[] for i in range(rank_limit)]
    tf_values  = [[] for i in range(rank_limit)]

    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)

    for row in result:
      rnk = int(row['rnk'])
      if rnk <= rank_limit:
        try:
            v = float(row['cdm_value'])
        except ValueError:
            v = str(row['cdm_value'])

        timestamps.append((row['tsp'] - epoch).total_seconds())
        trewscores.append(float(row['trewscore']))
        tf_names[rnk-1].append(str(row['fid']))
        tf_values[rnk-1].append(v)
      else:
        logging.warning("Invalid trews contributor rank: {}" % rnk)

    return {
        'timestamp'  : timestamps,
        'trewscore'  : trewscores,
        'tf_1_name'  : tf_names[0],
        'tf_1_value' : tf_values[0],
        'tf_2_name'  : tf_names[1],
        'tf_2_value' : tf_values[1],
        'tf_3_name'  : tf_names[2],
        'tf_3_value' : tf_values[2]
    }

# Retrieves a sampled/bucketed time series of JIT scores.
#
async def get_trews_jit_score(db_pool, pat_id, start_hrs=6, start_day=2, end_day=7, sample_mins=30, sample_hrs=12):

  sample_start_hrs = start_hrs
  sample_start_day = start_day
  sample_end_day = end_day
  sample_hr_mins = sample_mins
  sample_day_hrs = sample_hrs

  # old_get_score_sql = \
  # '''
  # with jit_scores as (
  #   select tjs.enc_id, tjs.tsp, tjs.score, tjs.odds_ratio,
  #          (case
  #           when tjs.tsp <= now() - interval '%(sample_start_day)s days'
  #           then date_trunc('day', tjs.tsp) + (interval '%(sample_day_hrs)s hours' * floor(date_part('hour', tjs.tsp)::float / %(sample_day_hrs)s))
  #           when tjs.tsp <= now() - interval '%(sample_start_hrs)s hours'
  #           then date_trunc('hour', tjs.tsp) + (interval '%(sample_hr_mins)s minutes' * floor(date_part('minute', tjs.tsp)::float / %(sample_hr_mins)s))
  #           else null
  #           end) as tsp_bucket
  #   from trews_jit_score tjs
  #   where tjs.enc_id = (select * from pat_id_to_enc_id('%(pat_id)s'::text))
  #   and tjs.model_id = get_trews_parameter('trews_jit_model_id')
  #   and tjs.tsp >= now() - interval '%(sample_end_day)s days'
  #   order by tsp_bucket
  # )
  # select * from (
  #   select tsp, score, odds_ratio
  #   from jit_scores
  #   where tsp_bucket is null
  #   union all
  #   select tsp_bucket as tsp,
  #          avg(score) as score,
  #          avg(odds_ratio) as odds_ratio
  #   from jit_scores
  #   where tsp_bucket is not null
  #   group by enc_id, tsp_bucket
  # ) R
  # order by tsp;
  # ''' % {
  #   'pat_id'           : pat_id,
  #   'sample_start_hrs' : sample_start_hrs,
  #   'sample_start_day' : sample_start_day,
  #   'sample_end_day'   : sample_end_day,
  #   'sample_hr_mins'   : sample_hr_mins,
  #   'sample_day_hrs'   : sample_day_hrs
  # }

  get_score_sql = \
  '''
  select tjs.enc_id, tjs.tsp, tjs.score, tjs.odds_ratio
  from trews_jit_score tjs
  where tjs.enc_id = (select * from pat_id_to_enc_id('%(pat_id)s'::text))
  and tjs.model_id = get_trews_parameter('trews_jit_model_id')
  and tjs.tsp >= now() - interval '%(sample_end_day)s days'
  order by tjs.tsp;
  ''' % {
    'pat_id'           : pat_id,
    'sample_start_hrs' : sample_start_hrs,
    'sample_start_day' : sample_start_day,
    'sample_end_day'   : sample_end_day,
    'sample_hr_mins'   : sample_hr_mins,
    'sample_day_hrs'   : sample_day_hrs
  }

  async with db_pool.acquire() as conn:
    result = await conn.fetch(get_score_sql)

    timestamps  = []
    trewscores  = []
    odds_ratios = []

    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)

    for row in result:
      timestamps.append((row['tsp'] - epoch).total_seconds())
      trewscores.append(float(row['score']))
      odds_ratios.append(float(row['odds_ratio']))

    return {
        'timestamp'  : timestamps,
        'trewscore'  : trewscores,
        'odds_ratio' : odds_ratios
    }


# Retrieves TREWS alert and orgdf intervals for the timeline view.
#
async def get_trews_intervals(db_pool, pat_id):
  get_intervals_sql = \
  '''
  select * from get_trews_orgdf_intervals(case when pat_id_to_enc_id('%(pat_id)s'::text) is null then -1 else pat_id_to_enc_id('%(pat_id)s'::text) end);
  ''' % { 'pat_id' : pat_id }

  async with db_pool.acquire() as conn:
    result = await conn.fetch(get_intervals_sql)

    intervals = {}
    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)

    for row in result:
      n = row['name']
      intervals[n] = { 'name': n, 'intervals': json.loads(row['intervals']) if row['intervals'] is not None else [] }

    return intervals


# Single roundtrip retrieval of both notifications and history events.
async def get_patient_events(db_pool, pat_id):
  get_events_sql = \
  '''
  select 0 as event_type,
         notification_id as evt_id,
         null as tsp,
         message as payload
  from notifications where enc_id = (select * from pat_id_to_enc_id('%(pat_id)s'::text))
    and (message#>>'{model}' = '%(model)s' or not message::jsonb ? 'model')
  union all
  select 1 as event_type,
         log_id as evt_id,
         date_part('epoch', tsp) as tsp,
         event as payload
  from criteria_log where enc_id = (select * from pat_id_to_enc_id('%(pat_id)s'::text))
  ''' % { 'pat_id': pat_id, 'model': model_in_use }

  async with db_pool.acquire() as conn:
    result = await conn.fetch(get_events_sql)

    notifications = []
    history = []

    for row in result:
      if row['event_type'] == 0:
        notification = json.loads(row['payload']) if row['payload'] is not None else {}
        if notification['timestamp'] is not None:
          notification['timestamp'] = int(notification['timestamp'])
          notification['id'] = row['evt_id']
          notifications.append(notification)
        else:
          logging.error('Invalid patient event, no timestamp found: %s' % str(row))
      else:
        audit = json.loads(row['payload']) if row['payload'] is not None else {}
        audit['log_id'] = row['evt_id']
        audit['pat_id'] = pat_id
        audit['timestamp'] = row['tsp']
        history.append(audit)

    return (notifications, history)


# For a patient, returns the:
# - trews threshold
# - admit time
# - activated/deactivated status
# - deterioration feedback timestamp, statuses and uid
#
async def get_patient_profile(db_pool, pat_id):
  threshold_param_key = 'lmc_threshold' if model_in_use == 'lmc' else 'trews_jit_threshold'
  get_patient_profile_sql = \
  '''
  select * from
  (
      select value as trews_threshold
      from trews_parameters where name = '%(threshold_param_key)s' limit 1
  ) TT
  full outer join
  (
      select value::timestamptz as admit_time
      from cdm_s
      where enc_id = (select * from pat_id_to_enc_id('%(pid)s'::text)) and fid = 'admittime'
      order by value::timestamptz desc limit 1
  ) ADT on true
  full outer join
  (
      select deactivated, deactivated_tsp from pat_status where enc_id = (select * from pat_id_to_enc_id('%(pid)s'::text)) limit 1
  ) DEACT on true
  full outer join
  (
      select date_part('epoch', tsp) detf_tsp, deterioration, uid as detf_uid
      from deterioration_feedback where enc_id = (select * from pat_id_to_enc_id('%(pid)s'::text)) limit 1
  ) DETF on true
  full outer join
  (
      select max(value) as age
      from cdm_s
      where enc_id = (select * from pat_id_to_enc_id('%(pid)s'::text)) and fid = 'age'
  ) AGE on true
  full outer join
  (
    select min(update_date) as refresh_time from criteria where enc_id = (select * from pat_id_to_enc_id('%(pid)s'::text))
  ) REFRESH on true
  full outer join
  (
    select count(*) > 0 as excluded
    from cdm_t
    where enc_id = (select * from pat_id_to_enc_id('%(pid)s'::text))
    and fid = 'care_unit'
    and value in ( %(excluded_units)s )
  ) EXCLUDED_UNITS on true
  full outer join
  (
    select creatinine     as baseline_creatinine,
           inr            as baseline_inr,
           bilirubin      as baseline_bilirubin,
           platelets      as baseline_platelets,
           creatinine_tsp as baseline_creatinine_tsp,
           inr_tsp        as baseline_inr_tsp,
           bilirubin_tsp  as baseline_bilirubin_tsp,
           platelets_tsp  as baseline_platelets_tsp
    from orgdf_baselines where pat_id = '%(pid)s'::text
  ) TREWS_BASELINES on true
  ''' % { 'pid': pat_id, 'threshold_param_key': threshold_param_key, 'excluded_units': excluded_units }

  async with db_pool.acquire() as conn:
    result = await conn.fetch(get_patient_profile_sql)

    profile = {
        'trews_threshold'      : None,
        'admit_time'           : None,
        'deactivated'          : None,
        'deactivated_tsp'      : None,
        'detf_tsp'             : None,
        'deterioration'        : None,
        'detf_uid'             : None,
        'age'                  : None,
        'refresh_time'         : None,
        'excluded_units'       : None,
        'orgdf_baselines'      : None
    }

    if len(result) == 1:
      epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)

      profile['trews_threshold']      = float("{:.9f}".format(float(result[0][0])))
      profile['admit_time']           = (result[0][1] - epoch).total_seconds() if result[0][1] is not None else None
      profile['deactivated']          = result[0][2]
      profile['deactivated_tsp']      = (result[0][3] - epoch).total_seconds() if result[0][3] is not None else None
      profile['detf_tsp']             = result[0][4]
      profile['deterioration']        = json.loads(result[0][5]) if result[0][5] is not None else None
      profile['detf_uid']             = result[0][6]
      profile['age']                  = result[0][7]
      profile['refresh_time']         = (result[0][8] - epoch).total_seconds() if result[0][8] is not None else None
      profile['excluded_units']       = result[0][9]
      profile['orgdf_baselines']      = {
                                          'baseline_creatinine'     : result[0][10]                           if result[0][10]  is not None else None,
                                          'baseline_inr'            : result[0][11]                           if result[0][11] is not None else None,
                                          'baseline_bilirubin'      : result[0][12]                           if result[0][12] is not None else None,
                                          'baseline_platelets'      : result[0][13]                           if result[0][13] is not None else None,
                                          'baseline_creatinine_tsp' : (result[0][14] - epoch).total_seconds() if result[0][14] is not None else None,
                                          'baseline_inr_tsp'        : (result[0][15] - epoch).total_seconds() if result[0][15] is not None else None,
                                          'baseline_bilirubin_tsp'  : (result[0][16] - epoch).total_seconds() if result[0][16] is not None else None,
                                          'baseline_platelets_tsp'  : (result[0][17] - epoch).total_seconds() if result[0][17] is not None else None
                                        }

    return profile


async def get_criteria(db_pool, eid):
  get_criteria_sql = \
  '''
  select * from get_criteria(case when pat_id_to_enc_id('%(pid)s'::text) is null then -1 else pat_id_to_enc_id('%(pid)s'::text) end)
  ''' % {'pid': eid}
  async with db_pool.acquire() as conn:
    result = await conn.fetch(get_criteria_sql)
    return result


async def get_criteria_log(db_pool, eid):
  get_criteria_log_sql = \
  '''
  select log_id, enc_id, date_part('epoch', tsp) epoch, event from criteria_log
  where enc_id = (select * from pat_id_to_enc_id('%s'::text)) order by tsp desc limit 25
  ''' % eid

  async with db_pool.acquire() as conn:
    result = await conn.fetch(get_criteria_log_sql)

    auditlist = []
    for row in result:
        audit = json.loads(row['event']) if row['event'] is not None else {}
        audit['log_id'] = row['log_id']
        audit['pat_id'] = eid
        audit['timestamp'] = row['epoch']
        auditlist.append(audit)
    return auditlist


async def get_notifications(db_pool, eid):
  get_notifications_sql = \
  '''
  select * from notifications
  where enc_id = (select * from pat_id_to_enc_id('%s'::text))
  and (message#>>'{model}' = '%s' or not message::jsonb ? 'model')
  ''' % (eid, model_in_use)

  async with db_pool.acquire() as conn:
    result = await conn.fetch(get_notifications_sql)

    notifications = []
    for row in result:
        notification = json.loads(row['message']) if row['message'] is not None else {}
        if notification['timestamp'] is not None:
          notification['timestamp'] = int(notification['timestamp'])
          notification['id'] = row['notification_id']
          notifications.append(notification)

    return notifications


async def get_order_detail(db_pool, eid):
  # NOTE: currently, we only query all cms_antibiotics
  get_order_detail_sql = \
  '''
  select T.tsp, initcap(regexp_replace(regexp_replace(T.fid, '_dose', ''), '_', ' ', 'g')) as fid, T.value
  from get_criteria(pat_id_to_enc_id('%s'::text)) C
  inner join cdm_t T on C.enc_id = T.enc_id
  where
  C.name = 'antibiotics_order'
  and T.fid in (
    'azithromycin_dose','aztreonam_dose','cefepime_dose','ceftriaxone_dose','gentamicin_dose','levofloxacin_dose',
    'metronidazole_dose','moxifloxacin_dose','piperacillin_tazobac_dose','vancomycin_dose',
    'aminoglycosides_dose', 'cephalosporins_1st_gen_dose', 'cephalosporins_2nd_gen_dose',
    'ciprofloxacin_dose', 'clindamycin_dose', 'daptomycin_dose',
    'glycopeptides_dose', 'linezolid_dose', 'macrolides_dose', 'penicillin_g_dose'
  )
  and coalesce(C.measurement_time, C.override_time, now()) - T.tsp <= interval '27 hours';
  ''' % eid

  async with db_pool.acquire() as conn:
    result = await conn.fetch(get_order_detail_sql)

    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)

    order_details = []
    for row in result:
        order_detail = {
            'timestamp' : int((row['tsp'] - epoch).total_seconds()),
            'order_name': row['fid'],
            'dose'      : row['value']
        }
        order_details.append(order_detail)

    return order_details


async def find_active_orders(db_pool, eid, orders):
  ''' Find any active orders from a list of order types and order placement times.
      The orders argument should be a list of (order type, placement time) tuples.
  '''

  # Check order_type value
  valid_order_types = ['antibiotics_order',
                       'blood_culture_order',
                       'crystalloid_fluid_order',
                       'initial_lactate_order',
                       'repeat_lactate_order',
                       'vasopressors_order']

  validated_orders = list(filter(lambda o: o[0] in valid_order_types, orders))

  if len(validated_orders) != len(orders):
    logging.error('Invalid orders to check in argument: ' % str(orders))
    return []

  logging.info('Checking active orders for %s from %s' % (eid, str(validated_orders)))

  # Get patient info needed for extract
  csn = None
  hospital = None
  async with db_pool.acquire() as conn:
    sql = \
    '''
    select P.visit_id, S.value
    from pat_enc P
    inner join cdm_s S on P.enc_id = S.enc_id
    where P.enc_id = pat_id_to_enc_id('%(pid)s'::text)
    and S.fid = 'hospital'
    ''' % {'pid': eid}

    row = await conn.fetchrow(sql)
    csn = row['visit_id']
    hospital = row['value']

  if not (csn and hospital):
    logging.info('Invalid CSN and hospital for %s: csn=%s hospital=%s' % (eid, csn, hospital))
    return []

  # Extract and transform orders
  jhapi_loader = JHAPI(EPIC_SERVER, client_id, client_secret)
  lab_orders, med_orders = jhapi_loader.extract_orders(eid, csn, hospital)

  logging.debug("Patient %s visit %s pre-transform lab_orders: %s" % (eid, csn, lab_orders))
  logging.debug("Patient %s visit %s pre-transform med_orders: %s" % (eid, csn, med_orders))

  lab_orders = transforms.transform_lab_orders(lab_orders)
  med_orders = transforms.transform_med_orders(med_orders)
  all_orders = {**lab_orders, **med_orders}

  logging.debug("Patient %s visit %s post-transform lab_orders: %s" % (eid, csn, lab_orders))
  logging.debug("Patient %s visit %s post-transform med_orders: %s" % (eid, csn, med_orders))
  logging.debug("Patient %s visit %s post-transform all_orders: %s" % (eid, csn, all_orders))

  active_orders = []

  for (order_type, order_time) in validated_orders:
    order_key = order_type.replace('_order', '').replace('repeat_', '').replace('initial_', '')
    if order_key in all_orders:
      logging.debug('Checking %s / %s with tsp: %s in %s' % (order_type, order_key, order_time, all_orders[order_key]))
      for tsp in all_orders[order_key]:

        logging.debug('Processing TSP %s' % tsp)
        tsp = tsp[:-3]+tsp[-2:] if ":" == tsp[-3:-2] else tsp # Format tz for parsing
        tsp = datetime.datetime.strptime(tsp, '%Y-%m-%dT%H:%M:%S%z')
        logging.debug('Parsed TSP %s' % tsp)

        order_time_tz = order_time.replace(tzinfo=pytz.UTC)
        logging.debug('Checking %s / %s : %s > %s = %s and %s' \
          % (order_type, order_key, tsp, order_time_tz, tsp > order_time_tz, (order_type, order_time) not in active_orders))

        if tsp > order_time_tz and (order_type, order_time) not in active_orders:
          logging.debug("Found an active %s: %s is past the order time of %s" % (order_type, tsp, order_time))
          active_orders.append((order_type, order_time))

  logging.info('find_active_orders result for %s %s: in=%s out=%s' % (eid, csn, orders, active_orders))

  return active_orders


async def toggle_notification_read(db_pool, eid, notification_id, as_read):
  toggle_notifications_sql = \
  '''
  with update_notifications as
  (   update notifications
      set message = jsonb_set(message::jsonb, '{read}'::text[], '%(val)s'::jsonb, false)
      where enc_id = (select * from pat_id_to_enc_id('%(pid)s'::text)) and notification_id = %(nid)s
      RETURNING *
  )
  insert into criteria_log (pat_id, tsp, event, update_date)
  select
          pat_id_to_enc_id('%(pid)s'::text),
          now(),
          json_build_object('event_type', 'toggle_notifications', 'message', n.message),
          now()
  from update_notifications n
  ''' % {'pid': eid, 'nid': notification_id, 'val': str(as_read).lower()}
  logging.info("toggle_notifications_read:" + toggle_notifications_sql)

  async with db_pool.acquire() as conn:
    await conn.execute(toggle_notifications_sql)
    await push_notifications_to_epic(db_pool, eid)


def temp_c_to_f(c):
  return c * 1.8 + 32

async def override_criteria(db_pool, eid, name, value='[{}]', user='user', clear=False, is_met=None, override_pre_offset_secs=None):
  if name == 'sirs_temp' and not clear:
      value[0]['lower'] = temp_c_to_f(float(value[0]['lower']))
      value[0]['upper'] = temp_c_to_f(float(value[0]['upper']))

  time_offset_suffix = (" - interval '%s seconds'" % override_pre_offset_secs) if override_pre_offset_secs else ''

  params = {
      'set_is_met': 'is_met = %s,' % is_met if is_met is not None else '',
      'time': 'now()' + time_offset_suffix if not clear else 'null',
      'user': ("'" + user + "'") if not clear else 'null',
      'val': ("'" + (json.dumps(value) if isinstance(value, list) else value) + "'") if not clear else 'null',
      'name': name if name != 'sus-edit' else 'suspicion_of_infection',
      'pid': eid,
      'user_log': user,
      'val_log': (json.dumps(value) if isinstance(value, list) else value) if not clear else 'null',
      'clear_log': 'true' if clear else 'false'
  }

  override_sql = \
  '''
  update criteria set
      %(set_is_met)s
      override_time = %(time)s,
      update_date = now(),
      override_value = %(val)s,
      override_user = %(user)s
  where enc_id = (select * from pat_id_to_enc_id('%(pid)s'::text)) and name = '%(name)s';
  insert into criteria_log (enc_id, tsp, event, update_date)
  values (
          pat_id_to_enc_id('%(pid)s'::text),
          now(),
          '{"event_type": "override", "name":"%(name)s", "uid":"%(user_log)s", "override_value":%(val_log)s, "clear":%(clear_log)s}',
          now()
      );
  select override_criteria_snapshot(pat_id_to_enc_id('%(pid)s'::text));
  ''' % params
  logging.info("override_criteria sql:" + override_sql)

  async with db_pool.acquire() as conn:
    await conn.execute(override_sql)
    await push_notifications_to_epic(db_pool, eid)


async def reset_patient(db_pool, eid, uid='user', event_id=None):
  reset_sql = """
  select * from reset_patient(pat_id_to_enc_id('{}'){});
  """.format(eid, ',{}'.format(event_id) if event_id is not None else '')
  logging.info("reset_patient:" + reset_sql)

  async with db_pool.acquire() as conn:
    await conn.execute(reset_sql)
    await push_notifications_to_epic(db_pool, eid)


async def deactivate(db_pool, eid, uid, deactivated):
  deactivate_sql = '''
  select * from deactivate(pat_id_to_enc_id('%(pid)s'::text), %(deactivated)s);
  insert into criteria_log (pat_id, tsp, event, update_date)
  values (
          pat_id_to_enc_id('%(pid)s'::text),
          now(),
          '{"event_type": "deactivate", "uid":"%(uid)s", "deactivated": %(deactivated)s}',
          now()
      );
  ''' % {'pid': eid, "deactivated": 'true' if deactivated else "false", "uid":uid}
  logging.info("deactivate user:" + deactivate_sql)

  async with db_pool.acquire() as conn:
    await conn.execute(deactivate_sql)
    await push_notifications_to_epic(db_pool, eid)


async def get_deactivated(db_pool, eid):
  async with db_pool.acquire() as conn:
    deactivated = await conn.fetch("select deactivated from pat_status where enc_id = (select * from pat_id_to_enc_id('%s'::text))" % eid)
    return ( len(deactivated) == 1 and deactivated[0][0] is True )


async def set_deterioration_feedback(db_pool, eid, deterioration_feedback, uid):
  deterioration_sql = '''
  select * from set_deterioration_feedback(pat_id_to_enc_id('%(pid)s'::text), now(), '%(deterioration)s', '%(uid)s');
  ''' % {'pid': eid, 'deterioration': json.dumps(deterioration_feedback), 'uid':uid}
  logging.info("set_deterioration_feedback user:" + deterioration_sql)
  async with db_pool.acquire() as conn:
    await conn.execute(deterioration_sql)


async def get_deterioration_feedback(db_pool, eid):
  get_deterioration_feedback_sql = \
  '''
  select enc_id, date_part('epoch', tsp) tsp, deterioration, uid
  from deterioration_feedback where enc_id = (select * from pat_id_to_enc_id('%s'::text)) limit 1
  ''' % eid
  async with db_pool.acquire() as conn:
    df = await conn.fetch(get_deterioration_feedback_sql)
    if len(df) == 1:
      return {
        "tsp": df[0][1],
        "deterioration": json.loads(df[0][2]) if df[0][2] is not None else None,
        "uid": df[0][3]
      }


async def get_feature_mapping(db_pool):
  get_mapping_sql = \
  '''
  select value from parameters where name='trews_jit_interpretability_mapping'
  '''
  try:
    async with db_pool.acquire() as conn:
      df = await conn.fetch(get_mapping_sql)
      return json.loads(df[0][0]) 
  except Exception as e:
    #print("Exception: " + str(e) + " in get_feature_mapping query")
    return {}

async def get_explanations(db_pool, eid):
  org_dfs = ['creatinine_orgdf', 'bilirubin_orgdf', 'platelets_orgdf','gcs_orgdf', 'inr_orgdf','hypotension_orgdf','lactate_orgdf']
  get_explanations_sql = \
  '''
  select feature_relevance, twf_raw_values,s_raw_values,%s
  from trews_jit_score where enc_id = (select * from pat_id_to_enc_id('%s'::text))
  and tsp = ( select measurement_time from criteria_events where enc_id= (select * from pat_id_to_enc_id('%s'::text)) and name ='trews_subalert' and flag::numeric>0)::timestamptz
  and model_id = (select value from trews_parameters where name='trews_jit_model_id')
  order by (orgdf_details::json->>'pred_time')::timestamptz desc
  limit 1;
  ''' %(','.join(org_dfs), eid, eid)
  try:
    async with db_pool.acquire() as conn:
      df = await conn.fetch(get_explanations_sql)
      result = {"feature_relevance":json.loads(df[0][0]),
              "twf_raw_values":json.loads(df[0][1]),
              "s_raw_values":json.loads(df[0][2]),
              "orgdfs": { orgdf: val for orgdf,val in zip(org_dfs, df[0][2:])}}
      return result
  except Exception as e:
    #print("Exception: " + str(e) + " in get_explanations query")
    result = {"feature_relevance": {},
            "twf_raw_values": {},
            "s_raw_values": {},
            "orgdfs" : {orgdf:0 for orgdf in org_dfs}}
    return result
    

async def get_nursing_eval(db_pool,eid):
  get_eval_str = \
  '''
  select eval,uid, date_part('epoch', tsp) tsp from nurse_eval where enc_id = (select * from pat_id_to_enc_id('%s'::text)) order by tsp::timestamptz desc limit 1;
  '''%(eid)
  try:
    async with db_pool.acquire() as conn:
      df = await conn.fetch(get_eval_str)
      data = {'eval': json.loads(df[0][0]),
              'uid': df[0][1],
              'tsp': df[0][2]}
      logging.info("nurse eval: " + str(data))
      return data;
  except Exception as e:
    logging.info("Exception: " + str(e) + " in get_nursing_eval")
    return {}

async def update_nursing_eval(db_pool,eid, data,uid):
    insert_str = \
    '''
    INSERT INTO nurse_eval (enc_id, tsp, uid, eval)
    VALUES ((select * from pat_id_to_enc_id('%s'::text)),now(), '%s', '%s');
    ''' %(eid,uid,json.dumps(data).replace("'", '"'))
    try:
      #print(insert_str)
      logging.info("Updated nurse_eval with command: %s" %insert_str)
      async with db_pool.acquire() as conn:
        await conn.execute(insert_str)
    except Exception as e:
      logging.info("Exception: " + str(e) + " in update_nursing_eval")
      return


async def push_notifications_to_epic(db_pool, eid, notify_future_notification=True):
  retries = 0
  max_retries = 5
  notifications = None
  async with db_pool.acquire() as conn:
    while retries < max_retries:
      retries += 1
      model = model_in_use
      notifications_sql = \
      '''
      select * from get_notifications_for_epic('%s', '%s');
      ''' % (eid, model)
      try:
        async with conn.transaction(isolation='serializable'):
          notifications = await conn.fetch(notifications_sql)
          logging.info('get_notifications_for_epic results %s' % len(notifications))
          break

      except asyncpg.exceptions.SerializationError:
        logging.info("Serialization error, retrying transaction")
        await asyncio.sleep(1)

      except:
        notifications = None
        break

    if notifications:
      logging.info("push notifications to epic (epic_notifications={}) for {}".format(epic_notifications, eid))
      #''' % (eid, model)
    try:
      async with conn.transaction(isolation='serializable'):
        notifications = await conn.fetch(notifications_sql)
    except:
      notifications = None

    if notifications:
      logging.info("push notifications to epic (epic_notifications={}) for {}".format(epic_notifications, eid))
      await load_epic_notifications(notifications)
      etl_channel = os.environ['etl_channel'] if 'etl_channel' in os.environ else None
      if etl_channel and notify_future_notification:
        notify_future_notification_sql = \
        '''
        select * from notify_future_notification('%s', '%s');
        ''' % (etl_channel, eid)
        logging.info("notify future notification: {}".format(notify_future_notification_sql))
        await conn.fetch(notify_future_notification_sql)
      elif etl_channel is None:
        logging.error("Unknown environ Error: etl_channel")
      else:
        logging.info("skipping notify_future_notification")
    else:
      logging.info("skipping notifications (e.g., no update or not in notifications_whitelist)")


async def load_epic_notifications(notifications):
  total = len(notifications)
  logging.info('load_epic_notifications call %s' % total)
  if total == 0:
    logging.info("No notification need to be updated to Epic")
    return

  if v1_flowsheets:
    flowsheet_ids = {
      'count'    : ('9490',  lambda x: str(x) if x is not None else '1'),
      'score'    : ('9485',  lambda x: format(float(x), '.4f').lstrip('0') if x is not None else '-1.0'),
      'threshold': ('94851', lambda x: format(float(x), '.4f').lstrip('0') if x is not None else '-1.0'),
      'flag'     : ('94852', lambda x: str(x) if x is not None else '0'),
      'version'  : ('94853', lambda x: model_in_use + '-v9' if model_in_use is not None else 'N/A')
    }
  else:
    flowsheet_ids = {
      'count'    : ('9490',  lambda x: str(x) if x is not None else '1'),
      'score'    : ('9485',  lambda x: format(float(x), '.4f').lstrip('0') if x is not None else '-1.0')
    }

  if epic_notifications is not None and int(epic_notifications):
    success = { k: 0 for k in flowsheet_ids }
    num_retry = 0
    push_tz = pytz.timezone('US/Eastern')
    push_tsp = datetime.datetime.utcnow().astimezone(push_tz)

    while min(success.values()) == 0 and num_retry < NUM_RETRY:
      for k in flowsheet_ids:
        if success[k] == 0:
          flowsheet_values = [{
              'pat_id'   : n['pat_id'],
              'visit_id' : n['visit_id'],
              'tsp'      : push_tsp,
              'value'    : flowsheet_ids[k][1](n[k] if k in n else None)
          } for n in notifications]

          jhapi_loader = JHAPI(NOTIFICATION_SERVER if NOTIFICATION_SERVER else EPIC_SERVER, client_id, client_secret)
          responses = jhapi_loader.load_flowsheet(flowsheet_values, flowsheet_id=flowsheet_ids[k][0])

          for val, response in zip(flowsheet_values, responses):
            if response is None:
              logging.error('Failed to push notifications: %s %s %s' % (val['pat_id'], val['visit_id'], val['value']))
            elif response.status_code != requests.codes.ok:
              logging.error('Failed to push notifications: %s %s %s HTTP %s' % (val['pat_id'], val['visit_id'], val['value'], response.status_code))
            elif response.status_code == requests.codes.ok:
              success[k] += 1
      if min(success.values()) > 0:
        break
      else:
        num_retry = num_retry + 1
        wait_time = min(((BASE**num_retry) + random.uniform(0, 1)), MAX_BACKOFF)
        logging.warn("retry jhapi to push notifications (%s s)".format(wait_time))
        sleep(wait_time)

    for k in success:
      logging.info('load_epic_notifications result %s %s' % (k, success[k]))
      api_monitor.add_metric('FSPush%sSuccess'  % k.capitalize(), value=success[k])
      api_monitor.add_metric('FSPush%sFailures' % k.capitalize(), value=total-success[k])

  else:
    logging.info('load_epic_notifications result skip')
    logging.info("Skipped pushing to Epic flowsheets")


async def load_epic_trewscores(trewscores):
  total = len(trewscores)
  if total == 0:
    logging.info("No trewscore need to be updated to Epic")
    return
  if epic_notifications is not None and int(epic_notifications):
    success = []
    patients = [{
        'pat_id': n['pat_id'],
        'visit_id': n['visit_id'],
        'value': format(n['trewscore'], '.2f').lstrip('0'),
        'tsp': n['tsp']
    } for n in trewscores]
    jhapi_loader = JHAPI(EPIC_SERVER, client_id, client_secret)
    responses = jhapi_loader.load_flowsheet(patients, flowsheet_id="9485")

    for pt, n, response in zip(patients, trewscores, responses):
      if response is None:
        logging.error('Failed to push trewscores: %s %s %s' % (pt['pat_id'], pt['visit_id'], pt['value']))
      elif response.status_code != requests.codes.ok:
        logging.error('Failed to push trewscores: %s %s %s HTTP %s' % (pt['pat_id'], pt['visit_id'], pt['value'], response.status_code))
      elif response.status_code == requests.codes.ok:
        success.append(n)
  else:
    logging.info("skip loading epic trewscores")
    success = trewscores
  api_monitor.add_metric('EpicTrewscoreSuccess', value=len(success))
  api_monitor.add_metric('EpicTrewscoreFailures', value=total-len(success))


async def load_epic_soi_for_bpa(pat_id, user_id, visit_id, soi):
  logging.info("push soi flowsheets to epic (epic_notifications={}) for {}".format(epic_notifications, pat_id))
  if epic_notifications is not None and int(epic_notifications) and v1_flowsheets and soi_flowsheet:
    push_tz = pytz.timezone('US/Eastern')
    push_tsp = datetime.datetime.utcnow().astimezone(push_tz)
    epic_tsp = push_tsp.replace(microsecond=0)

    flowsheets = {
      # username
      'username' : ('94854', { 'pat_id' : pat_id, 'visit_id' : visit_id, 'tsp' : push_tsp, 'value' : user_id }),

      # session start
      'session_start' : ('94855', { 'pat_id' : pat_id, 'visit_id' : visit_id, 'tsp' : push_tsp, 'value' : str(epic_tsp) }),

      # session end
      #'session_end' : ('94856', { 'pat_id' : pat_id, 'visit_id' : visit_id, 'tsp' : push_tsp, 'value' : push_tsp }),

      # soi_yn
      'soi_yn' : ('94857', { 'pat_id' : pat_id, 'visit_id' : visit_id, 'tsp' : push_tsp, 'value' : '1' }),

      # soi
      'soi' : ('94858', { 'pat_id' : pat_id, 'visit_id' : visit_id, 'tsp' : push_tsp, 'value' : 'null' if not soi else str(soi) }),
    }

    flowsheet_metrics = {
      'username'      : 'Username',
      'session_start' : 'SessionStart',
      #'session_end'   : 'SessionEnd',
      'soi_yn'        : 'SoiYN',
      'soi'           : 'SOI',
    }

    total = len(flowsheets.keys())
    success = { k: 0 for k in flowsheets }
    num_retry = 0

    while min(success.values()) == 0 and num_retry < NUM_RETRY:
      for k in flowsheets:
        if success[k] == 0:
          flowsheet_values = [flowsheets[k][1]]
          jhapi_loader = JHAPI(NOTIFICATION_SERVER if NOTIFICATION_SERVER else EPIC_SERVER, client_id, client_secret)
          responses = jhapi_loader.load_flowsheet(flowsheet_values, flowsheet_id=flowsheets[k][0])

          for val, response in zip(flowsheet_values, responses):
            if response is None:
              logging.error('Failed to push notifications: %s %s %s' % (val['pat_id'], val['visit_id'], val['value']))
            elif response.status_code != requests.codes.ok:
              logging.error('Failed to push notifications: %s %s %s HTTP %s' % (val['pat_id'], val['visit_id'], val['value'], response.status_code))
            elif response.status_code == requests.codes.ok:
              success[k] += 1
      if min(success.values()) > 0:
        break
      else:
        num_retry = num_retry + 1
        wait_time = min(((BASE**num_retry) + random.uniform(0, 1)), MAX_BACKOFF)
        logging.warn("retry jhapi to push notifications (%s s)".format(wait_time))
        sleep(wait_time)

    for k in success:
      api_monitor.add_metric('FSPush%sSuccess'  % flowsheet_metrics[k], value=success[k])
      api_monitor.add_metric('FSPush%sFailures' % flowsheet_metrics[k], value=total-success[k])

  else:
    logging.info("Skipped pushing to Epic SOI flowsheets")


async def eid_exist(db_pool, eid):
  async with db_pool.acquire() as conn:
    result = await conn.fetchrow("select * from pat_enc where pat_id = '%s' limit 1" % eid)
    return result is not None

async def eid_visit(db_pool, eid):
  async with db_pool.acquire() as conn:
    row = await conn.fetchrow("select pat_id_to_visit_id('%s'::text) as visit_id;" % eid)
    return row['visit_id'] if row else None


async def save_feedback(db_pool, doc_id, pat_id, dep_id, feedback):
  feedback_sql = '''
    INSERT INTO feedback_log (doc_id, tsp, enc_id, dep_id, feedback)
    VALUES ('%(doc)s', now(), pat_id_to_enc_id('%(pat)s'::text), '%(dep)s', '%(fb)s');
    ''' % {'doc': doc_id, 'pat': pat_id, 'dep': dep_id, 'fb': feedback}

  async with db_pool.acquire() as conn:
    await conn.execute(feedback_sql)


async def notify_pat_update(db_pool, channel, pat_id):
  notify_sql = "notify {}, 'invalidate_cache:{}:{}'".format(channel, pat_id, model_in_use)
  logging.info("notify_sql: " + notify_sql)
  async with db_pool.acquire() as conn:
    await conn.execute(notify_sql)

async def get_recent_pats_from_hosp(db_pool, hosp, model):
  sql = '''select distinct h.pat_id from pat_hosp() h
  inner join criteria_meas m on m.pat_id = h.pat_id
  where hospital = '{}' and now() - tsp < (select value::interval from parameters where name = 'lookbackhours')
  '''.format(hosp)
  async with db_pool.acquire() as conn:
    res = await conn.fetch(sql)
    return [row['pat_id'] for row in res]

async def invalidate_cache_batch(db_pool, pid, channel, serial_id, pat_cache):
  retries = 0
  max_retries = 5

  # run push_notifications_to_epic in a batch way
  logging.info('Invalidating patient cache serial_id %s (via channel %s)' % (serial_id, channel))
  sql = '''with notifications as (
    select * from get_notifications_for_refreshed_pats({serial_id}, '{model}')
  )
  select pat_id, visit_id, enc_id, count, score, threshold, flag from
  (select n.*, notify_future_notification('{channel}', pat_id) from notifications n) M;
  '''.format(serial_id=serial_id, model=model_in_use, channel=channel)
  pat_sql = 'select jsonb_array_elements_text(pats) pat_id from refreshed_pats where id = {}'.format(serial_id)

  try:
    async with db_pool.acquire() as conn:
      while retries < max_retries:
        retries += 1
        try:
          async with conn.transaction(isolation='serializable'):
            notifications = await conn.fetch(sql)
            logging.info('get_notifications_for_epic results %s' % len(notifications))
            await load_epic_notifications(notifications)
            pats = await conn.fetch(pat_sql)
            logging.info("Invalidating cache for %s" % ','.join(pat_id['pat_id'] for pat_id in pats))
            for pat_id in pats:
              asyncio.ensure_future(pat_cache.delete(pat_id['pat_id']))
            break

        except asyncpg.exceptions.SerializationError:
          logging.info("Serialization error, retrying transaction")
          await asyncio.sleep(1)

  except Exception as ex:
    logging.warning(str(ex))
    traceback.print_exc()
