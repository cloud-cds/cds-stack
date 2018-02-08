import etl.load.primitives.tbl.load_table as primitives
from etl.core.task import Task
import datetime as dt
import asyncpg
import asyncio
from sqlalchemy import create_engine
from etl.load.pipelines.derive_main import derive_feature, get_derive_seq
import pandas as pd
import os
import logging
from etl.load.primitives.row import load_row
import json
from etl.io_config import server_protocol as protocol
import etl.io_config.core as core
import random
import pdb
from etl.io_config.cloudwatch import Cloudwatch
cloudwatch_logger = Cloudwatch()
WORKSPACE = core.get_environment_var('TREWS_ETL_WORKSPACE', 'event_workspace')

async def extract_non_discharged_patients(ctxt, hospital):
  '''
  Get all patient ids (EMRN) from pat_enc that don't have a discharge
  time in cdm_t.
  '''
  query_string = """
    SELECT DISTINCT pe.pat_id, pe.enc_id, pe.visit_id
    FROM pat_enc pe INNER JOIN cdm_s cs ON pe.enc_id = cs.enc_id
    inner join pat_hosp() h on pe.pat_id = h.pat_id
    WHERE cs.fid = 'admittime' AND cs.enc_id NOT IN (
      SELECT DISTINCT enc_id FROM cdm_t WHERE fid = 'discharge'
    ) and h.hospital = '{}'
  """.format(hospital)
  async with ctxt.db_pool.acquire() as conn:
    pats_no_discharge = await conn.fetch(query_string)
    pat_ids = [{
      'pat_id': str(x['pat_id']).strip(),
      'enc_id': str(x['enc_id']).strip(),
      'visit_id': str(x['visit_id']).strip(),
    } for x in pats_no_discharge]
    return pat_ids



async def load_discharge_times(ctxt, contacts_df):
  if contacts_df.empty:
    return
  discharged_df = contacts_df[contacts_df['discharge_date'] != '']
  def build_row(row):
    enc_id     = row['enc_id']
    tsp        = row['discharge_date']
    fid        = 'discharge'
    value      = json.dumps({
      'disposition':  row['discharge_disposition'],
      'department': row['department']
    })
    confidence = 1
    return [enc_id, tsp, fid, value, confidence]
  rows = discharged_df.apply(build_row, axis=1)
  async with ctxt.db_pool.acquire() as conn:
    await load_row.upsert_t(conn, rows, dataset_id=None, log=ctxt.log, many=True)

async def notify_data_ready_to_lmc_alert_server(ctxt, job_id):
  message = {
    'type'  : 'ETL',
    'time'  : str(dt.datetime.utcnow()),
    'hosp'  : job_id.split('_')[-2].upper(),
    'job_id': job_id
  }
  try:
    logging.info('Notify lmc alert server that the ETL is done: {}'.format(message))
    reader, writer = await asyncio.open_connection(protocol.LMC_ALERT_SERVER_IP, protocol.LMC_ALERT_SERVER_PORT, loop=ctxt.loop)
    await protocol.write_message(writer, message)
    writer.close()
  except Exception as e:
    ctxt.log.exception(e)
    ctxt.log.error("Fail to notify lmc alert server")

async def notify_data_ready_to_trews_alert_server(ctxt, *para):
  job_id = para[0]
  message = {
    'type'  : 'ETL',
    'time'  : str(dt.datetime.utcnow()),
    'hosp'  : job_id.split('_')[-2].upper(),
    'job_id': job_id
  }
  try:
    logging.info('Notify trews alert server that the ETL is done: {}'.format(message))
    reader, writer = await asyncio.open_connection(protocol.TREWS_ALERT_SERVER_IP, protocol.TREWS_ALERT_SERVER_PORT, loop=ctxt.loop)
    await protocol.write_message(writer, message)
    writer.close()
  except Exception as e:
    ctxt.log.exception(e)
    ctxt.log.error("Fail to notify trews alert server")

async def notify_delta_ready_to_trews_alert_server(ctxt, *para):
  job_id = para[0]
  message = {
    'type'  : 'ETL',
    'time'  : str(dt.datetime.utcnow()),
    'hosp'  : job_id.split('_')[-3].upper(),
    'job_id': job_id,
  }
  try:
    logging.info('Notify trews alert server that the push-based ETL is done: {}'.format(message))
    reader, writer = await asyncio.open_connection(protocol.TREWS_ALERT_SERVER_IP, protocol.TREWS_ALERT_SERVER_PORT, loop=ctxt.loop)
    await protocol.write_message(writer, message)
    writer.close()
  except Exception as e:
    ctxt.log.exception(e)
    ctxt.log.error("Fail to notify trews alert server")

# async def notify_criteria_ready_to_alert_server(ctxt, job_id, _):
#   message = {
#     'type': 'ETL', # change type
#     'time': str(dt.datetime.utcnow()),
#     'hosp': job_id.split('_')[-2].upper()
#   }
#   try:
#     reader, writer = await asyncio.open_connection(protocol.ALERT_SERVER_IP, protocol.ALERT_SERVER_PORT, loop=ctxt.loop)
#     await protocol.write_message(writer, message)
#     logging.info('Closing the socket')
#     writer.close()
#   except Exception as e:
#     ctxt.log.exception(e)
#     ctxt.log.error("Fail to notify alert server")


async def get_notifications_for_epic(ctxt, job_id, _):
  ''' Get all notifications to send to epic '''
  async with ctxt.db_pool.acquire() as conn:
    ctxt.log.info("getting notifications to push to epic")
    result = await conn.fetch("""
      SELECT n.* from get_notifications_for_epic(null) n
      inner join workspace.{}_bedded_patients_transformed bp
      on n.pat_id = bp.pat_id and n.visit_id = bp.visit_id
      """.format(job_id))
    return list(dict(x) for x in result)

async def notify_future_notification(ctxt, _):
  if 'etl_channel' in os.environ:
    ctxt.log.info("notify future notification")
    async with ctxt.db_pool.acquire() as conn:
      await conn.execute("select * from notify_future_notification('%s');" % os.environ['etl_channel'])
  else:
    ctxt.log.info("no etl channel found in the environment, skipping etl notifications")

async def epic_2_workspace_pull(ctxt, db_data, job_id, dtypes, workspace):
  ''' Push all the dataframes to a workspace table '''
  async with ctxt.db_pool.acquire() as conn:
    ctxt.log.info("enter epic_2_workspace")
    for df_name, df in db_data.items():
      await primitives.data_2_workspace(ctxt.log, conn, job_id, df_name, df, dtypes=dtypes, workspace=workspace)
    return job_id


async def epic_2_workspace(ctxt, db_data, job_id, dtypes, workspace, conn):
  ''' Push all the dataframes to a workspace table '''
  ctxt.log.debug("enter epic_2_workspace")
  for df_name, df in db_data.items():
    await primitives.data_2_workspace(ctxt.log, conn, job_id, df_name, df, dtypes=dtypes, workspace=workspace)
  return job_id


def test_data_2_workspace(ctxt, sqlalchemy_str, mode, job_id):
  engine = create_engine(sqlalchemy_str)
  for table in ('bedded_patients', 'flowsheet', 'lab_orders', 'lab_results',
    'med_orders', 'med_admin', 'location_history', 'active_procedures'):
    df = pd.read_sql_table("test_{}".format(table), engine).drop('index', axis=1)
    # Convert timestamps to correct format for ETL
    if not df.empty and 'tsp' in df.columns:
        df['tsp'] = df['tsp'].apply(lambda x: dt.datetime.utcfromtimestamp(float(x)).isoformat())
    df_name = "{}_transformed".format(table + 's' if table == 'flowsheet' else table)
    if_exists = 'append' if 'real' in mode else 'replace'
    primitives.data_2_workspace(ctxt.log, engine, job_id, df_name, df, dtypes=None, if_exists = if_exists)




async def workspace_to_cdm(ctxt, job_id, workspace, keep_delta_table=True):
  query = "select * from workspace_to_cdm('{}','{}','{}');".format(job_id, workspace, keep_delta_table)
  async with ctxt.db_pool.acquire() as conn:
    try:
      res = await conn.fetch(query)
      num_delta_t = res[0][0]
      cloudwatch_logger.push(
        dimension_name = 'ETL',
        metric_name    = 'num_delta_t',
        value          = num_delta_t,
        unit           = 'Count'
      )
    except asyncpg.exceptions.UndefinedTableError:
      logging.error("Workspace table does exist for {}".format(query))
    return job_id


async def workspace_to_cdm_delta(ctxt, job_id, workspace, keep_delta_table=False):
  query = "select * from workspace_to_cdm('{}','{}','{}');".format(job_id, workspace, keep_delta_table)
  async with ctxt.db_pool.acquire() as conn:
    try:
      res = await conn.fetch(query)
      num_delta_t = res[0][0]
      cloudwatch_logger.push(
        dimension_name = 'ETL',
        metric_name    = 'num_delta_t_push',
        value          = num_delta_t,
        unit           = 'Count'
      )
    except asyncpg.exceptions.UndefinedTableError:
      logging.error("Workspace table does exist for {}".format(query))
    return num_delta_t

async def workspace_to_cdm_delta(ctxt, job_id, workspace, conn, keep_delta_table=False):
  query = "select * from workspace_to_cdm('{}','{}','{}');".format(job_id, workspace, keep_delta_table)
  try:
    res = await conn.fetch(query)
    num_delta_t = res[0][0]
    cloudwatch_logger.push(
      dimension_name = 'ETL',
      metric_name    = 'num_delta_t_push',
      value          = num_delta_t,
      unit           = 'Count'
    )
  except asyncpg.exceptions.UndefinedTableError:
    logging.error("Workspace table does exist for {}".format(query))
  return num_delta_t

async def load_online_prediction_parameters(ctxt, job_id):
  async with ctxt.db_pool.acquire() as conn:
    ctxt.log.info("load online_prediction_features")
    # Load features needed for criteria
    query_criteria_feature = '''
    select unnest(string_to_array(value, ',')) fid from parameters where name = 'criteria_required_derive_fids'
    '''
    criteria_features = await conn.fetch(query_criteria_feature)
    criteria_features = [f['fid'] for f in criteria_features]
    # Load features needed for jit
    query_jit_feature = '''
    select unnest(string_to_array(value, ',')) fid from parameters where name = 'jit_required_twf_fids'
    '''
    jit_features = await conn.fetch(query_jit_feature)
    jit_features = [f['fid'] for f in jit_features]
    # Load features needed for lmc
    # query_lmc_feature = '''
    # select f.fid
    # from (
    #   select column_name fid
    #   from information_schema.columns
    #   where table_name = 'lmcscore') f
    # inner join cdm_feature cf on f.fid = cf.fid;
    # '''
    # lmc_features = await conn.fetch(query_lmc_feature)
    # lmc_features = [f['fid'] for f in lmc_features]
    # Load features weights from database
    # feature_weights = {}
    # trews_feature_weights = await conn.fetch("select * from trews_feature_weights")
    # for weight in trews_feature_weights:
    #   feature_weights[weight['fid']] = weight['weight']
    #   ctxt.log.debug("feature: {:30} weight: {}".format(weight['fid'], weight['weight']))
    # trews_parameters = await conn.fetch("select * from trews_parameters")
    # for parameter in trews_parameters:
    #   if parameter['name'] == 'max_score':
    #     max_score = parameter['value']
    #   if parameter['name'] == 'min_score':
    #     min_score = parameter['value']
    # ctxt.log.debug('set max_score to {} and min_score to {}'.format(max_score, min_score))

    # Get cdm feature dict
    cdm_feature = await conn.fetch("select * from cdm_feature")
    cdm_feature_dict = {f['fid']:dict(f) for f in cdm_feature}
    required_fids = criteria_features + jit_features
    # list the measured features for online prediction
    features_with_intermediates = get_features_with_intermediates(\
      required_fids, cdm_feature_dict)
    measured_features = [fid for fid in features_with_intermediates if cdm_feature_dict[fid]["is_measured"]]
    ctxt.log.debug("The measured features in online prediction: {}".format(
      _get_feature_description_report(measured_features, cdm_feature_dict)))

    # list the fillin features for online prediction
    fillin_features = [fid for fid in features_with_intermediates if \
      cdm_feature_dict[fid]["is_measured"] and cdm_feature_dict[fid]["category"] == "TWF"]
    ctxt.log.debug("The fillin features in online prediction: {}".format(fillin_features))

    # list the derive features for online prediction
    derive_features = [fid for fid in features_with_intermediates if not cdm_feature_dict[fid]["is_measured"]]
    ctxt.log.debug("The derive features in online prediction: {}".format(derive_features))

    return {
      'feature_weights'  : {}, #dict(feature_weights),
      'max_score'        : 0, #max_score,
      'min_score'        : 0, #min_score,
      'cdm_feature_dict' : cdm_feature_dict,
      'fillin_features'  : fillin_features,
      'derive_features'  : derive_features,
    }

async def load_online_prediction_parameters(ctxt, job_id, conn):
  ctxt.log.info("{} load online_prediction_features".format(job_id))
  # Load features needed for criteria
  query_criteria_feature = '''
  select unnest(string_to_array(value, ',')) fid from parameters where name = 'criteria_required_derive_fids'
  '''
  criteria_features = await conn.fetch(query_criteria_feature)
  criteria_features = [f['fid'] for f in criteria_features]
  # Load features needed for jit
  query_jit_feature = '''
  select unnest(string_to_array(value, ',')) fid from parameters where name = 'jit_required_twf_fids'
  '''
  jit_features = await conn.fetch(query_jit_feature)
  jit_features = [f['fid'] for f in jit_features]

  # Get cdm feature dict
  cdm_feature = await conn.fetch("select * from cdm_feature")
  cdm_feature_dict = {f['fid']:dict(f) for f in cdm_feature}
  required_fids = criteria_features + jit_features
  # list the measured features for online prediction
  features_with_intermediates = get_features_with_intermediates(\
    required_fids, cdm_feature_dict)
  measured_features = [fid for fid in features_with_intermediates if cdm_feature_dict[fid]["is_measured"]]
  ctxt.log.info("{}: The measured features in online prediction: {}".format(job_id,
    _get_feature_description_report(measured_features, cdm_feature_dict)))

  # list the fillin features for online prediction
  fillin_features = [fid for fid in features_with_intermediates if \
    cdm_feature_dict[fid]["is_measured"] and cdm_feature_dict[fid]["category"] == "TWF"]
  ctxt.log.info("{}: The fillin features in online prediction: {}".format(job_id, fillin_features))

  # list the derive features for online prediction
  derive_features = [fid for fid in features_with_intermediates if not cdm_feature_dict[fid]["is_measured"]]
  ctxt.log.info("{}: The derive features in online prediction: {}".format(job_id, derive_features))

  return {
    'feature_weights'  : {}, #dict(feature_weights),
    'max_score'        : 0, #max_score,
    'min_score'        : 0, #min_score,
    'cdm_feature_dict' : cdm_feature_dict,
    'fillin_features'  : fillin_features,
    'derive_features'  : derive_features,
  }


async def workspace_fillin(ctxt, prediction_params, job_id, workspace):
  ctxt.log.info("start fillin pipeline")
  # we run the optimized fillin in one run, e.g., update set all columns
  fillin_sql = '''
    SELECT * from workspace_fillin_delta({fillin_fids}, {twf_table}, 'cdm_t', '{job_id}', '{workspace}');
    '''.format(
      fillin_fids = 'array[{}]'.format(','.join(["'{}'".format(x) for x in prediction_params['fillin_features']])),
      twf_table   = "'{}.{}_cdm_twf'".format(workspace, job_id),
      job_id      = job_id,
      workspace   = workspace
    )
  async with ctxt.db_pool.acquire() as conn:
    ctxt.log.info("start fillin: {}".format(fillin_sql))
    result = await conn.execute(fillin_sql)
    ctxt.log.info(result)
    ctxt.log.info("fillin completed")
    return job_id


async def workspace_fillin_delta(ctxt, prediction_params, job_id, workspace):
  ctxt.log.info("start fillin pipeline")
  # we run the optimized fillin in one run, e.g., update set all columns
  fillin_sql = '''
    SELECT * from workspace_fillin_delta({fillin_fids}, {twf_table}, 'cdm_t', '{job_id}', '{workspace}');
    '''.format(
      fillin_fids = 'array[{}]'.format(','.join(["'{}'".format(x) for x in prediction_params['fillin_features']])),
      twf_table   = "'{}.{}_cdm_twf'".format(workspace, job_id),
      job_id      = job_id,
      workspace   = workspace
    )
  async with ctxt.db_pool.acquire() as conn:
    ctxt.log.debug("start fillin: {}".format(fillin_sql))
    result = await conn.execute(fillin_sql)
    ctxt.log.info(result)
    ctxt.log.info("fillin completed")
    return result[0][0]

async def workspace_fillin_delta(ctxt, prediction_params, job_id, workspace, conn):
  ctxt.log.info("{} start fillin pipeline".format(job_id))
  # we run the optimized fillin in one run, e.g., update set all columns
  fillin_sql = '''
    SELECT * from workspace_fillin_delta({fillin_fids}, {twf_table}, 'cdm_t', '{job_id}', '{workspace}');
    '''.format(
      fillin_fids = 'array[{}]'.format(','.join(["'{}'".format(x) for x in prediction_params['fillin_features']])),
      twf_table   = "'{}.{}_cdm_twf'".format(workspace, job_id),
      job_id      = job_id,
      workspace   = workspace
    )
  ctxt.log.debug("start fillin: {}".format(fillin_sql))
  result = await conn.execute(fillin_sql)
  ctxt.log.info(result)
  ctxt.log.info("{} fillin completed".format(job_id))
  return result[0][0]

async def workspace_derive(ctxt, prediction_params, job_id, workspace):
  cdm_feature_dict = prediction_params['cdm_feature_dict']
  derive_features = prediction_params['derive_features']
  ctxt.log.info("derive start")
  # get derive order based on the input derive_features
  derive_feature_dict = {fid: cdm_feature_dict[fid] for fid in derive_features}
  derive_feature_order = get_derive_seq(derive_feature_dict)
  twf_table = '{}.{}_cdm_twf'.format(workspace, job_id)
  cdm_t_target = '''
  (select * from cdm_t where enc_id in
    (select distinct enc_id from {workspace}.cdm_t wt
      where wt.job_id = '{job_id}'))
  '''.format(workspace=workspace, job_id=job_id)

  # get info for old function
  derive_feature_addr = {}
  for fid in derive_feature_order:
    table = twf_table if derive_feature_dict[fid]['category'] == 'TWF' else None
    derive_feature_addr[fid] = {
      'twf_table': table,
      'twf_table_temp': table,
      'category': derive_feature_dict[fid]['category'],
    }



  # derive the features sequentially
  # retry parameters
  base = 2
  max_backoff = 60

  async with ctxt.db_pool.acquire() as conn:
    res = await conn.fetchrow("select value from parameters where name = 'etl_workspace_lookbackhours'")
    lookbackhours = res['value']
    for fid in derive_feature_order:
      attempts = 0
      while True:
        try:
          ctxt.log.info("deriving fid {}".format(fid))
          await derive_feature(ctxt.log, fid, cdm_feature_dict, conn, derive_feature_addr=derive_feature_addr, cdm_t_lookbackhours=lookbackhours, workspace=workspace,job_id=job_id, cdm_t_target=cdm_t_target)
          break
        except Exception as e:
          attempts += 1
          ctxt.log.exception("PSQL Error derive: %s %s" % (fid if fid else 'run_derive', e))
          random_secs = random.uniform(0, 10)
          wait_time = min(((base**attempts) + random_secs), max_backoff)
          await asyncio.sleep(wait_time)
          ctxt.log.info("run_derive {} attempts {}".format(fid or '', attempts))
          if fid is None:
            raise Exception('batch derive stopped due to exception')
          continue
    ctxt.log.info("derive completed")
    return job_id


async def workspace_derive(ctxt, prediction_params, job_id, workspace, conn):
  cdm_feature_dict = prediction_params['cdm_feature_dict']
  derive_features = prediction_params['derive_features']
  ctxt.log.info("{} derive start".format(job_id))
  # get derive order based on the input derive_features
  derive_feature_dict = {fid: cdm_feature_dict[fid] for fid in derive_features}
  derive_feature_order = get_derive_seq(derive_feature_dict)
  twf_table = '{}.{}_cdm_twf'.format(workspace, job_id)
  cdm_t_target = '''
  (select * from cdm_t where enc_id in
    (select distinct enc_id from {workspace}.cdm_t wt
      where wt.job_id = '{job_id}'))
  '''.format(workspace=workspace, job_id=job_id)

  # get info for old function
  derive_feature_addr = {}
  for fid in derive_feature_order:
    table = twf_table if derive_feature_dict[fid]['category'] == 'TWF' else None
    derive_feature_addr[fid] = {
      'twf_table': table,
      'twf_table_temp': table,
      'category': derive_feature_dict[fid]['category'],
    }



  ctxt.log.info("{} derive the features sequentially: {}".format(job_id, derive_feature_order))
  # retry parameters
  base = 2
  max_backoff = 60

  res = await conn.fetchrow("select value from parameters where name = 'etl_workspace_lookbackhours'")
  lookbackhours = res['value']
  for fid in derive_feature_order:
    attempts = 0
    while True:
      try:
        ctxt.log.info("{} deriving fid {}".format(job_id, fid))
        await derive_feature(ctxt.log, fid, cdm_feature_dict, conn, derive_feature_addr=derive_feature_addr, cdm_t_lookbackhours=lookbackhours, workspace=workspace,job_id=job_id, cdm_t_target=cdm_t_target)
        break
      except Exception as e:
        attempts += 1
        ctxt.log.exception("PSQL Error derive: %s %s" % (fid if fid else 'run_derive', e))
        random_secs = random.uniform(0, 10)
        wait_time = min(((base**attempts) + random_secs), max_backoff)
        await asyncio.sleep(wait_time)
        ctxt.log.info("run_derive {} attempts {}".format(fid or '', attempts))
        if fid is None:
          raise Exception('batch derive stopped due to exception')
        continue
  return job_id


async def workspace_predict(ctxt, prediction_params, job_id, workspace):
  ctxt.log.info("predict start")
  feature_weights = prediction_params['feature_weights']
  cdm_feature_dict = prediction_params['cdm_feature_dict']
  min_score = prediction_params['min_score']
  max_score = prediction_params['max_score']
  twf_features = [fid for fid in feature_weights if cdm_feature_dict[fid]['category'] == 'TWF']
  s_features = [fid for fid in feature_weights if cdm_feature_dict[fid]['category'] == 'S']
  twf_features_times_weights = ['coalesce(((%s::numeric - (select coalesce(max(mean), 0) from trews_scaler where fid = E\'%s\')) / (select coalesce(max(case when scale = 0 then 1 else scale end), 1) from trews_scaler where fid = E\'%s\')) * %s, 0)' % (fid, fid, fid, feature_weights[fid]) \
      for fid in twf_features if cdm_feature_dict[fid]['data_type'] != 'Boolean' ] + \
       ['coalesce(((%s::int - (select coalesce(max(mean), 0) from trews_scaler where fid = E\'%s\') ) / ( select coalesce(max(case when scale = 0 then 1 else scale end), 1) from trews_scaler where fid = E\'%s\')) * %s, 0)' % (fid,fid, fid, feature_weights[fid]) \
      for fid in twf_features if cdm_feature_dict[fid]['data_type'] == 'Boolean']
  s_features_times_weights = ['coalesce((( coalesce(%s.value::numeric,0) - (select coalesce(max(mean), 0) from trews_scaler where fid = E\'%s\') ) / (select coalesce(max(case when scale = 0 then 1 else scale end), 1) from trews_scaler where fid = E\'%s\')) * %s, 0)' % (fid,fid,fid, feature_weights[fid]) \
      for fid in s_features if cdm_feature_dict[fid]['data_type'] != 'Boolean'] + \
      ['coalesce(((  coalesce(%s.value::bool::int,0) - (select coalesce(max(mean), 0) from trews_scaler where fid = E\'%s\') ) / (select coalesce(max(case when scale = 0 then 1 else scale end), 1) from trews_scaler where fid = E\'%s\')) * %s, 0)' % (fid, fid, fid, feature_weights[fid]) \
      for fid in s_features if cdm_feature_dict[fid]['data_type'] == 'Boolean']
  feature_weight_colnames = \
      [fid for fid in twf_features if cdm_feature_dict[fid]['data_type'] != 'Boolean' ] + \
      [fid for fid in twf_features if cdm_feature_dict[fid]['data_type'] == 'Boolean'] + \
      [fid for fid in s_features if cdm_feature_dict[fid]['data_type'] != 'Boolean'] + \
      [fid for fid in s_features if cdm_feature_dict[fid]['data_type'] == 'Boolean']
  feature_weight_values = [ "(%s - %s)/%s" % (val, min_score/len(feature_weights), max_score - min_score)\
      for val in (twf_features_times_weights + s_features_times_weights)]
  select_clause = ",".join(["%s %s" % (v,k) for k,v in zip(feature_weight_colnames, feature_weight_values)])
  # select feature values
  # calculate the trewscores
  weight_sum = "+".join(['coalesce(%s,0)'  % col for col in feature_weight_colnames])
  target    = '{}.{}'.format(workspace, job_id)
  table     = '{}_trews'.format(target)
  twf_table = '{}_cdm_twf'.format(target)
  sql = """
    DROP table if exists %(table)s;
    CREATE TABLE %(table)s AS
    SELECT %(twf_table)s.enc_id, tsp, %(cols)s, null::numeric trewscore
    FROM %(twf_table)s
  """ % {'cols':select_clause, 'table': table, 'twf_table': twf_table}
  for f in s_features:
    sql += """ left outer join cdm_s %(fid)s
              ON %(twf_table)s.enc_id = %(fid)s.enc_id
              AND %(fid)s.fid = '%(fid)s'
           """ % {'fid': f, 'twf_table': twf_table}
  sql += """
    ;update %(table)s set trewscore = %(sum)s;
    """ % {'sum':weight_sum, 'table':table, 'twf_table': twf_table}

  async with ctxt.db_pool.acquire() as conn:
    ctxt.log.info("predict trewscore:{}".format(sql))
    await conn.execute(sql)
    ctxt.log.info("predict completed")
    return job_id



async def workspace_submit_delta(ctxt, job_id, workspace):
  # submit to cdm_twf
  # submit to trews
  ctxt.log.info("{}: submitting results ...".format(job_id))
  submit_cdm = '''
  select * from workspace_submit_delta('%(workspace)s.%(job)s_cdm_twf');
  '''
  async with ctxt.db_pool.acquire() as conn:
    ctxt.log.debug(submit_cdm % {'job': job_id, 'workspace': workspace} )
    await conn.execute(submit_cdm % {'job': job_id, 'workspace': workspace} )
    ctxt.log.info("{}: results submitted".format(job_id))
    return job_id

async def workspace_submit_delta(ctxt, job_id, workspace, conn):
  # submit to cdm_twf
  # submit to trews
  ctxt.log.info("{}: submitting results ...".format(job_id))
  submit_cdm = '''
  select * from workspace_submit_delta('%(workspace)s.%(job)s_cdm_twf');
  '''
  ctxt.log.debug(submit_cdm % {'job': job_id, 'workspace': workspace} )
  await conn.execute(submit_cdm % {'job': job_id, 'workspace': workspace} )
  ctxt.log.info("{}: results submitted".format(job_id))
  return job_id

async def workspace_submit(ctxt, job_id, workspace, drop_workspace_table=True, trews=True):
  # submit to cdm_twf
  # submit to trews
  ctxt.log.info("{}: submitting results ...".format(job_id))
  select_all_colnames = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '%(table)s';
  """
  submit_cdm = """
    INSERT INTO cdm_twf
      (SELECT * FROM %(workspace)s.%(job)s_cdm_twf
       where now() - tsp < (select value::interval from parameters where name = 'etl_workspace_submit_hours')
      )
    ON conflict (enc_id, tsp) do UPDATE SET %(set_columns)s;
  """ + ("SELECT drop_tables('%(workspace)s', '%(job)s_cdm_twf');" if drop_workspace_table else '')
  submit_trews = """
    CREATE TABLE if not exists trews (LIKE %(workspace)s.%(job)s_trews,
        unique (enc_id, tsp)
        );
    INSERT into trews (enc_id, tsp, %(columns)s) (
        SELECT enc_id, tsp, %(columns)s FROM %(workspace)s.%(job)s_trews
        where now() - tsp < (select value::interval from parameters where name = 'etl_workspace_submit_hours')
        )
    ON conflict (enc_id, tsp)
        do UPDATE SET %(set_columns)s;
  """ + ("SELECT drop_tables('%(workspace)s', '%(job)s_trews');" if drop_workspace_table else '')
  async with ctxt.db_pool.acquire() as conn:
    records = await conn.fetch(select_all_colnames % {'table': 'cdm_twf'})
    colnames = [row[0] for row in records if row[0] != 'enc_id' and row[0] != 'tsp']
    twf_set_columns = ",".join([
        "%(col)s = excluded.%(col)s" % {'col': colname} for colname in colnames
    ])
    ctxt.log.debug(submit_cdm % {'job': job_id, 'set_columns': twf_set_columns, 'workspace': workspace} )
    await conn.execute(submit_cdm % {'job': job_id, 'set_columns': twf_set_columns, 'workspace': workspace} )
    if trews:
      records = await conn.fetch(select_all_colnames % {'table': '%s_trews' % job_id})
      colnames = [row[0] for row in records if row[0] != 'enc_id' and row[0] != 'tsp']
      trews_set_columns = ",".join([
          "%(col)s = excluded.%(col)s" % {'col': colname} for colname in colnames
      ])
      trews_columns = ",".join(colnames)
      ctxt.log.debug(submit_trews % {'job': job_id, 'set_columns': trews_set_columns, 'columns': trews_columns, 'workspace': workspace} )
      await conn.execute(submit_trews % {'job': job_id, 'set_columns': trews_set_columns, 'columns': trews_columns, 'workspace': workspace} )
    ctxt.log.info("{}: results submitted".format(job_id))
    return job_id

def get_features_with_intermediates(features, dictionary):
  output = set()
  feature_set = set(features)
  while len(feature_set) > 0:
    for fid in feature_set.copy():
      if not dictionary[fid]['is_measured']:
        fid_input_str = dictionary[fid]['derive_func_input']
        fid_input = [item.strip() for item in fid_input_str.split(',')]
        for fid_in in fid_input:
          feature_set.add(fid_in)
      output.add(fid)
      feature_set.remove(fid)
  return output




def _get_feature_description_report(features, dictionary):
  num = len(features)
  features_description = {}
  for fid in features:
    description = dictionary[fid]['description'].lower() if dictionary[fid]['description'] else 'null'
    if description in features_description:
      features_description[description].append(fid)
    else:
      features_description[description] = [fid]

  report = "All features: %s \n Total number of features:%s\n" % (features, num)
  for desc in features_description:
    report += desc + ":\n" + " ".join(features_description[desc]) + "\n"
  return report




def get_tasks(job_id, db_data_task, db_raw_data_task, mode, archive, sqlalchemy_str, deps=[], suppression=0):
  all_tasks = []
  if archive == 1:
    all_tasks.append(Task(
      name = 'epic_2_workspace_archive',
      deps = [db_raw_data_task],
      coro = epic_2_workspace,
      args = [job_id, 'unicode', WORKSPACE],
    ))
  if 'test' in mode:
    all_tasks += [Task(
      name = 'test_data_2_workspace',
      fn   = test_data_2_workspace,
      args = [sqlalchemy_str, mode, job_id],
    )]
  all_tasks += [
    Task(name = 'epic_2_workspace',
         deps = [db_data_task],
         coro = epic_2_workspace,
         args = [sqlalchemy_str, job_id, None, WORKSPACE]),
    Task(name = 'workspace_to_cdm',
         deps = ['epic_2_workspace'],
         coro = workspace_to_cdm,
         args = [WORKSPACE, True]),
    Task(name = 'load_online_prediction_parameters',
         deps = ['workspace_to_cdm'],
         coro = load_online_prediction_parameters),
    Task(name = 'workspace_fillin',
         deps = ['load_online_prediction_parameters', 'workspace_to_cdm'],
         coro = workspace_fillin,
         args = [WORKSPACE]),
    Task(name = 'workspace_derive',
         deps = ['load_online_prediction_parameters', 'workspace_fillin'],
         coro = workspace_derive,
         args = [WORKSPACE]),
    Task(name = 'workspace_predict',
         deps = ['load_online_prediction_parameters', 'workspace_derive'],
         coro = workspace_predict,
         args = [WORKSPACE]),
    Task(name = 'workspace_submit',
         deps = ['workspace_predict'],
         coro = workspace_submit_delta,
         args = [WORKSPACE]),
    Task(name = 'load_discharge_times',
         deps = ['contacts_transform'],
         coro = load_discharge_times),
        ]
  if suppression == 0:
    all_tasks += [
                  Task(name = 'get_notifications_for_epic',
                       deps = ['workspace_submit', 'advance_criteria_snapshot'],
                       coro = get_notifications_for_epic),
                  ]
  elif suppression == 1:
    all_tasks += [
                  Task(name = 'notify_data_ready_to_lmc_alert_server',
                       deps = ['workspace_submit'],
                       coro = notify_data_ready_to_lmc_alert_server),
                  Task(name = 'notify_data_ready_to_trews_alert_server',
                       deps = ['workspace_submit', 'advance_criteria_snapshot'],
                       coro = notify_data_ready_to_trews_alert_server)
                  ]
  elif suppression == 2:
    all_tasks += [
                  Task(name = 'notify_data_ready_to_lmc_alert_server',
                       deps = ['workspace_submit'],
                       coro = notify_data_ready_to_lmc_alert_server),
                  Task(name = 'notify_data_ready_to_trews_alert_server',
                       deps = ['workspace_submit'],
                       coro = notify_data_ready_to_trews_alert_server)
                  ]
  else:
    ctxt.log.error("Unknown suppression alert mode: {}".format(suppression))
  return all_tasks

def get_tasks_pat_only(job_id, db_data_task, db_raw_data_task, mode, archive, deps=[], suppression=0):
  all_tasks = [
    Task(name = 'epic_2_workspace',
         deps = [db_data_task],
         coro = epic_2_workspace_pull,
         args = [job_id, None, WORKSPACE]),
    Task(name = 'workspace_to_cdm',
         deps = ['epic_2_workspace'],
         coro = workspace_to_cdm,
         args = [WORKSPACE, True]),
        ]
  return all_tasks
