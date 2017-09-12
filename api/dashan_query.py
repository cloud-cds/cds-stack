"""
dashan_query.py
"""
import os, sys, traceback
import json
import datetime
import logging
import pytz
import requests

from jhapi_io import JHAPI
import dashan_universe.transforms as transforms


logging.basicConfig(format='%(levelname)s|%(asctime)s.%(msecs)03d|%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

epic_notifications = os.environ['epic_notifications']
client_id          = os.environ['jhapi_client_id']
client_secret      = os.environ['jhapi_client_secret']
use_trews_lmc      = os.environ['use_trews_lmc'].lower() == 'true' if 'use_trews_lmc' in os.environ else False

##########################################
# Compact query implementations.
# These pull out multiple component TREWS data and metadata
# components in fewer queries, reducing the # DB roundtrips.
#

# For a patient, returns time series of:
# - trews scores
# - top 3 features that contributed to the each time point
async def get_trews_contributors(db_pool, pat_id, start_hrs=6, start_day=2, end_day=7, sample_mins=30, sample_hrs=12):
  contributor_fn = 'calculate_lmc_contributors' if use_trews_lmc else 'calculate_trews_contributors'

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


# Single roundtrip retrieval of both notifications and history events.
async def get_patient_events(db_pool, pat_id):
  get_events_sql = \
  '''
  select 0 as event_type,
         notification_id as evt_id,
         null as tsp,
         message as payload
  from notifications where pat_id = '%(pat_id)s'
    and (message#>>'{model}' = '%(model)s' or not message::jsonb ? 'model')
  union all
  select 1 as event_type,
         log_id as evt_id,
         date_part('epoch', tsp) as tsp,
         event as payload
  from criteria_log where pat_id = '%(pat_id)s'
  ''' % { 'pat_id': pat_id, 'model': 'lmc' if use_trews_lmc else 'trews' }

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
  threshold_param_key = 'lmc_threshold' if use_trews_lmc else 'trews_threshold'
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
      from cdm_s inner join pat_enc on pat_enc.enc_id = cdm_s.enc_id
      where pat_id = '%(pid)s' and fid = 'admittime'
      order by value::timestamptz desc limit 1
  ) ADT on true
  full outer join
  (
      select deactivated, deactivated_tsp from pat_status where pat_id = '%(pid)s' limit 1
  ) DEACT on true
  full outer join
  (
      select date_part('epoch', tsp) detf_tsp, deterioration, uid as detf_uid
      from deterioration_feedback where pat_id = '%(pid)s' limit 1
  ) DETF on true
  full outer join
  (
      select max(value) as age
      from cdm_s inner join pat_enc on pat_enc.enc_id = cdm_s.enc_id
      where pat_id = '%(pid)s' and fid = 'age'
  ) AGE on true
  full outer join
  (
    select min(update_date) as refresh_time from criteria where pat_id = '%(pid)s'
  ) REFRESH on true
  full outer join
  (
    select GREATEST(
                (array_agg(measurement_time order by measurement_time)  filter (where name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and is_met ) )[2],
                min(measurement_time) filter (where name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp','respiratory_failure','creatinine','bilirubin','platelet','inr','lactate') and is_met ))
            as first_sirs_orgdf_tsp
    from criteria_events where pat_id = '%(pid)s' and flag in (-990, 10)
  ) FIRST_SIRS_ORGDF on true
  ''' % { 'pid': pat_id, 'threshold_param_key': threshold_param_key }

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
        'first_sirs_orgdf_tsp' : None
    }

    if len(result) == 1:
      epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)

      profile['trews_threshold']      = float("{:.2f}".format(float(result[0][0])))
      profile['admit_time']           = (result[0][1] - epoch).total_seconds() if result[0][1] is not None else None
      profile['deactivated']          = result[0][2]
      profile['deactivated_tsp']      = (result[0][3] - epoch).total_seconds() if result[0][3] is not None else None
      profile['detf_tsp']             = result[0][4]
      profile['deterioration']        = json.loads(result[0][5]) if result[0][5] is not None else None
      profile['detf_uid']             = result[0][6]
      profile['age']                  = result[0][7]
      profile['refresh_time']         = (result[0][8] - epoch).total_seconds() if result[0][8] is not None else None
      profile['first_sirs_orgdf_tsp'] = (result[0][9] - epoch).total_seconds() if result[0][9] is not None else None

    return profile


async def get_criteria(db_pool, eid):
  get_criteria_sql = \
  '''
  select * from get_criteria('%s')
  ''' % eid
  async with db_pool.acquire() as conn:
    result = await conn.fetch(get_criteria_sql)
    return result


async def get_criteria_log(db_pool, eid):
  get_criteria_log_sql = \
  '''
  select log_id, pat_id, date_part('epoch', tsp) epoch, event from criteria_log
  where pat_id = '%s' order by tsp desc limit 25
  ''' % eid

  async with db_pool.acquire() as conn:
    result = await conn.fetch(get_criteria_log_sql)

    auditlist = []
    for row in result:
        audit = json.loads(row['event']) if row['event'] is not None else {}
        audit['log_id'] = row['log_id']
        audit['pat_id'] = row['pat_id']
        audit['timestamp'] = row['epoch']
        auditlist.append(audit)
    return auditlist


async def get_notifications(db_pool, eid):
  get_notifications_sql = \
  '''
  select * from notifications
  where pat_id = '%s'
  and (message#>>'{model}' = '%s' or not message::jsonb ? 'model')
  ''' % (eid, 'lmc' if use_trews_lmc else 'trews')

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
  select tsp, initcap(regexp_replace(regexp_replace(fid, '_dose', ''), '_', ' ')) as fid, value from criteria_meas
  where pat_id = '%s' and
  fid in (
    'azithromycin_dose','aztreonam_dose','cefepime_dose','ceftriaxone_dose','ciprofloxacin_dose','gentamicin_dose','levofloxacin_dose',
    'metronidazole_dose','moxifloxacin_dose','piperacillin_tazbac_dose','vancomycin_dose'
  )
  and now() - tsp < (select value::interval from parameters where name = 'lookbackhours');
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


async def is_order_placed(db_pool, eid, order_type, order_time):
  ''' See if an order for 'order_type' has been placed after 'order_time' '''

  # Check order_type value
  if order_type not in ['antibiotics_order', 'blood_culture_order',
                        'crystalloid_fluid_order', 'initial_lactate_order',
                        'repeat_lactate_order', 'vasopressors_order']:
    logging.error("Can't handle this order type: {}".format(order_type))
    await asyncio.sleep(1)
    return False
  logging.info("Checking if order has been placed for '{}'".format(order_type))

  # Get patient info needed for extract
  async with db_pool.acquire() as conn:
    row = await conn.fetchrow("SELECT * FROM pat_enc WHERE pat_id = '{}';".format(eid))
    csn = row['visit_id']
    hospital = await conn.fetchval("SELECT value FROM cdm_s WHERE enc_id = '{}' AND fid = 'hospital';".format(row['enc_id']))
  logging.info("Patient csn='{}', hosp='{}'".format(csn, hospital))

  # Extract and transform orders
  jhapi_loader = JHAPI('prod', client_id, client_secret)
  lab_orders, med_orders = jhapi_loader.extract_orders(eid, csn, hospital)
  lab_orders = transforms.transform_lab_orders(lab_orders)
  med_orders = transforms.transform_med_orders(med_orders)
  logging.info("Patients lab_orders: {}".format(lab_orders))
  logging.info("Patients med_orders: {}".format(med_orders))

  # Get all the timestamps for the correct order
  order_to_check = order_type.replace('_order', '').replace('repeat_', '').replace('initial_', '')
  all_orders = {**lab_orders, **med_orders}
  for order in all_orders:
    if order == order_to_check:
      tsps_to_check = all_orders[order]
  logging.info("Checking these {} orders: {}".format(order_to_check, tsps_to_check))

  # Parse the time stamps and compare to 'order_time'
  for tsp in tsps_to_check:
    tsp = tsp[:-3]+tsp[-2:] if ":" == tsp[-3:-2] else tsp # Format tz for parsing
    tsp = dt.datetime.strptime(tsp, '%Y-%m-%dT%H:%M:%S%z')
    if tsp > order_time:
      logging.info("{} is past the order time of {}".format(tsp, order_time))
      return True

  # Couldn't find an order after 'order_time'
  logging.info("No timestamp past {}".format(order_time))
  return False



async def toggle_notification_read(db_pool, eid, notification_id, as_read):
  toggle_notifications_sql = \
  '''
  with update_notifications as
  (   update notifications
      set message = jsonb_set(message::jsonb, '{read}'::text[], '%(val)s'::jsonb, false)
      where pat_id = '%(pid)s' and notification_id = %(nid)s
      RETURNING *
  )
  insert into criteria_log (pat_id, tsp, event, update_date)
  select
          '%(pid)s',
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

async def override_criteria(db_pool, eid, name, value='[{}]', user='user', clear=False):
  if name == 'sirs_temp' and not clear:
      value[0]['lower'] = temp_c_to_f(float(value[0]['lower']))
      value[0]['upper'] = temp_c_to_f(float(value[0]['upper']))

  params = {
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
      override_time = now(),
      update_date = now(),
      override_value = %(val)s,
      override_user = %(user)s
  where pat_id = '%(pid)s' and name = '%(name)s';
  insert into criteria_log (pat_id, tsp, event, update_date)
  values (
          '%(pid)s',
          now(),
          '{"event_type": "override", "name":"%(name)s", "uid":"%(user_log)s", "override_value":%(val_log)s, "clear":%(clear_log)s}',
          now()
      );
  select override_criteria_snapshot('%(pid)s');
  ''' % params
  logging.info("override_criteria sql:" + override_sql)

  async with db_pool.acquire() as conn:
    await conn.execute(override_sql)
    await push_notifications_to_epic(db_pool, eid)


async def reset_patient(db_pool, eid, uid='user', event_id=None):
  reset_sql = """
  select * from reset_patient('{}'{});
  """.format(eid, ',{}'.format(event_id) if event_id is not None else '')
  logging.info("reset_patient:" + reset_sql)

  async with db_pool.acquire() as conn:
    await conn.execute(reset_sql)
    await push_notifications_to_epic(db_pool, eid)


async def deactivate(db_pool, eid, uid, deactivated):
  deactivate_sql = '''
  select * from deactivate('%(pid)s', %(deactivated)s);
  insert into criteria_log (pat_id, tsp, event, update_date)
  values (
          '%(pid)s',
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
    deactivated = await conn.fetch("select deactivated from pat_status where pat_id = '%s'" % eid)
    return ( len(deactivated) == 1 and deactivated[0][0] is True )


async def set_deterioration_feedback(db_pool, eid, deterioration_feedback, uid):
  deterioration_sql = '''
  select * from set_deterioration_feedback('%(pid)s', now(), '%(deterioration)s', '%(uid)s');
  ''' % {'pid': eid, 'deterioration': json.dumps(deterioration_feedback), 'uid':uid}
  logging.info("set_deterioration_feedback user:" + deterioration_sql)
  async with db_pool.acquire() as conn:
    await conn.execute(deterioration_sql)


async def get_deterioration_feedback(db_pool, eid):
  get_deterioration_feedback_sql = \
  '''
  select pat_id, date_part('epoch', tsp) tsp, deterioration, uid
  from deterioration_feedback where pat_id = '%s' limit 1
  ''' % eid
  async with db_pool.acquire() as conn:
    df = await conn.fetch(get_deterioration_feedback_sql)
    if len(df) == 1:
      return {
        "tsp": df[0][1],
        "deterioration": json.loads(df[0][2]) if df[0][2] is not None else None,
        "uid": df[0][3]
      }


async def push_notifications_to_epic(db_pool, eid, notify_future_notification=True):
  async with db_pool.acquire() as conn:
    model = 'lmc' if use_trews_lmc else 'trews'
    notifications_sql = \
    '''
    select * from get_notifications_for_epic('%s', '%s');
    ''' % (eid, model)
    notifications = await conn.fetch(notifications_sql)
    if notifications:
      logging.info("push notifications to epic ({}) for {}".format(epic_notifications, eid))
      if epic_notifications is not None and int(epic_notifications):
        patients = [{
            'pat_id': n['pat_id'],
            'visit_id': n['visit_id'],
            'notifications': n['count']
        } for n in notifications]
        jhapi_loader = JHAPI('prod', client_id, client_secret)
        responses = jhapi_loader.load_notifications(patients)

        for pt, response in zip(patients, responses):
          if response is None:
            logging.error('Failed to push notifications: %s %s %s' % (pt['pat_id'], pt['visit_id'], pt['notifications']))
          elif response.status_code != requests.codes.ok:
            logging.error('Failed to push notifications: %s %s %s HTTP %s' % (pt['pat_id'], pt['visit_id'], pt['notifications'], response.status_code))

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
      logging.info("skipping notifications (e.g., not in notifications_whitelist)")

async def eid_exist(db_pool, eid):
  async with db_pool.acquire() as conn:
    result = await conn.fetchrow("select * from pat_enc where pat_id = '%s' limit 1" % eid)
    return result is not None


async def save_feedback(db_pool, doc_id, pat_id, dep_id, feedback):
  feedback_sql = '''
    INSERT INTO feedback_log (doc_id, tsp, pat_id, dep_id, feedback)
    VALUES ('%(doc)s', now(), '%(pat)s', '%(dep)s', '%(fb)s');
    ''' % {'doc': doc_id, 'pat': pat_id, 'dep': dep_id, 'fb': feedback}

  async with db_pool.acquire() as conn:
    await conn.execute(feedback_sql)


async def notify_pat_update(db_pool, channel, pat_id):
  notify_sql = "notify {}, 'invalidate_cache:{}:{}'".format(channel, pat_id,'lmc' if use_trews_lmc else 'trews')
  logging.info("notify_sql: " + notify_sql)
  async with db_pool.acquire() as conn:
    await conn.execute(notify_sql)

async def get_pats_from_hosp(db_pool, hosp):
  sql = "select pat_id from pat_hosp() where hospital = '{}'".format(hosp)
  async with db_pool.acquire() as conn:
    res = await conn.fetch(sql)
    return [row['pat_id'] for row in res]

async def invalidate_cache_hospital(db_pool, pid, channel, hospital, pat_cache):
  logging.info('Invalidating patient cache hospital %s (via channel %s)' % (hospital, channel))
  pat_ids = await get_pats_from_hosp(db_pool, hospital)
  for pat_id in pat_ids:
    logging.info("Invalidating cache for %s" % pat_id)
    await pat_cache.delete(pat_id)
    asyncio.ensure_future(\
        dashan_query.push_notifications_to_epic(db_pool, pat_id,
          notify_future_notification=False))