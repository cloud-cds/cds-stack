import etl.load.primitives.tbl.load_table as primitives
from etl.core.task import Task
import datetime as dt
import asyncpg
import asyncio
from sqlalchemy import create_engine
from etl.load.pipelines.derive_main import derive_feature, get_derive_seq
from etl.load.pipelines.fillin import fillin_pipeline
import pandas as pd
import os
import logging




async def get_notifications_for_epic(ctxt, job_id):
  ''' Get all notifications to send to epic '''
  async with ctxt.db_pool.acquire() as conn:
    ctxt.log.info("getting notifications to push to epic")
    return await conn.fetch("""
      SELECT n.* from get_notifications_for_epic(null) n
      inner join workspace.{}_bedded_patients_transformed bp
      on n.pat_id = bp.pat_id
      """.format(job_id))




def epic_2_workspace(ctxt, _, sqlalchemy_str, db_data, job_id, dtypes=None):
  ''' Push all the dataframes to a workspace table '''
  print(sqlalchemy_str)
  engine = create_engine(sqlalchemy_str)
  for df_name, df in db_data.items():
    if df is None or df.empty:
      ctxt.log.warning("Skipping table load for {} (invalid datafame)".format(df_name))
      continue
    primitives.data_2_workspace(engine, job_id, df_name, df, dtypes=dtypes)
  return job_id




def test_data_2_workspace(ctxt, mode, job_id):
  engine = create_engine(ctxt.get_db_conn_string_sqlalchemy())
  for table in ('bedded_patients', 'flowsheet', 'lab_orders', 'lab_results', 
    'med_orders', 'med_admin', 'location_history', 'active_procedures'):
    df = pd.read_sql_table("test_{}".format(table), engine).drop('index', axis=1)
    # Convert timestamps to correct format for ETL
    if not df.empty and 'tsp' in df.columns:
        df['tsp'] = df['tsp'].apply(lambda x: dt.datetime.utcfromtimestamp(float(x)).isoformat())
    df_name = "{}_transformed".format(table + 's' if table == 'flowsheet' else table)
    if_exists = 'append' if 'real' in mode else 'replace'
    primitives.data_2_workspace(engine, job_id, df_name, df, dtypes=None, if_exists = if_exists)




async def workspace_to_cdm(ctxt, job_id):
  async with ctxt.db_pool.acquire() as conn:
    await primitives.insert_new_patients(conn, job_id)
    await primitives.create_job_cdm_twf_table(conn, job_id)
    await primitives.workspace_bedded_patients_2_cdm_s(conn, job_id)
    await primitives.workspace_flowsheets_2_cdm_t(conn, job_id)
    await primitives.workspace_lab_results_2_cdm_t(conn, job_id)
    await primitives.workspace_location_history_2_cdm_t(conn, job_id)
    await primitives.workspace_medication_administration_2_cdm_t(conn, job_id)
    await primitives.workspace_fluids_intake_2_cdm_t(conn, job_id)
    await primitives.workspace_flowsheets_2_cdm_twf(conn, job_id)
    await primitives.workspace_lab_results_2_cdm_twf(conn, job_id)
    await primitives.workspace_medication_administration_2_cdm_twf(conn, job_id)
    await primitives.workspace_notes_2_cdm_notes(conn, job_id)
    # TODO load lab orders and active procedures to cdm
    return job_id




async def load_online_prediction_parameters(ctxt, job_id):
  async with ctxt.db_pool.acquire() as conn:
    ctxt.log.info("load online_prediction_features")

    # Load features weights from database
    feature_weights = {}
    trews_feature_weights = await conn.fetch("select * from trews_feature_weights")
    for weight in trews_feature_weights:
      feature_weights[weight['fid']] = weight['weight']
      ctxt.log.info("feature: %s\t weight: %s" % (weight['fid'], weight['weight']))
    trews_parameters = await conn.fetch("select * from trews_parameters")
    for parameter in trews_parameters:
      if parameter['name'] == 'max_score':
        max_score = parameter['value']
      if parameter['name'] == 'min_score':
        min_score = parameter['value']
    ctxt.log.info('set max_score to %s and min_score to %s' % (max_score, min_score))

    # Get cdm feature dict
    cdm_feature = await conn.fetch("select * from cdm_feature")
    cdm_feature_dict = {f['fid']:f for f in cdm_feature}

    # list the measured features for online prediction
    features_with_intermediates = get_features_with_intermediates(feature_weights.keys(), cdm_feature_dict)
    measured_features = [fid for fid in features_with_intermediates if cdm_feature_dict[fid]["is_measured"]]
    ctxt.log.info("The measured features in online prediction: {}".format(
      _get_feature_description_report(measured_features, cdm_feature_dict)))

    # list the fillin features for online prediction
    fillin_features = [fid for fid in features_with_intermediates if \
      cdm_feature_dict[fid]["is_measured"] and cdm_feature_dict[fid]["category"] == "TWF"]
    ctxt.log.info("The fillin features in online prediction: {}".format(fillin_features))

    # list the derive features for online prediction
    derive_features = [fid for fid in features_with_intermediates if not cdm_feature_dict[fid]["is_measured"]]
    ctxt.log.info("The derive features in online prediction: {}".format(derive_features))

    return {
      'feature_weights'  : feature_weights, 
      'max_score'        : max_score, 
      'min_score'        : min_score, 
      'cdm_feature_dict' : cdm_feature_dict, 
      'fillin_features'  : fillin_features, 
      'derive_features'  : derive_features,
    }




async def workspace_fillin(ctxt, prediction_params, job_id):
  ctxt.log.info("start fillin pipeline")
  fillin_table = 'workspace.{}_cdm_twf'.format(job_id)
  ctxt.log.info("start fillin pipeline")
  # we run the optimized fillin in one run, e.g., update set all columns
  fillin_sql = '''
    SELECT * from last_value_in_window({fillin_fids}, {twf_table});
    '''.format(
      fillin_fids='array[{}]'.format(', '.join(prediction_params['fillin_features'])),
      twf_table=fillin_table
    )
  async with ctxt.db_pool.acquire() as conn:
    ctxt.log.info("start fillin: {}".format(fillin_sql))
    result = await conn.execute(fillin_sql)
    ctxt.log.info(result, "\nfillin completed")
    ctxt.log.info("fillin completed")
    return job_id




async def workspace_derive(ctxt, prediction_params, job_id):
  cdm_feature_dict = prediction_params['cdm_feature_dict']
  derive_features = prediction_params['derive_features']
  ctxt.log.info("derive start")
  # get derive order based on the input derive_features
  derive_feature_dict = {fid: cdm_feature_dict[fid] for fid in derive_features}
  derive_feature_order = get_derive_seq(derive_feature_dict)
  # derive the features sequentially
  twf_table = 'workspace.{}_cdm_twf'.format(job_id)
  async with ctxt.db_pool.acquire() as conn:
    for fid in derive_feature_order:
      ctxt.log.info("deriving fid {}".format(fid))
      await derive_feature(ctxt.log, cdm_feature_dict[fid], conn, twf_table=twf_table)
    ctxt.log.info("derive completed")
    return job_id




async def workspace_predict(ctxt, prediction_params, job_id):
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
  target    = 'workspace.{}'.format(job_id)
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





async def workspace_submit(ctxt, job_id):
  # submit to cdm_twf
  # submit to trews
  ctxt.log.info("submit start")
  ctxt.log.info("{}: submitting results ...".format(job_id))
  select_all_colnames = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '%(table)s';
  """
  submit_twf = """
    INSERT INTO cdm_twf
      (SELECT * FROM workspace.%(job)s_cdm_twf)
    ON conflict (enc_id, tsp) do UPDATE SET %(set_columns)s;
    SELECT drop_tables('workspace', '%(job)s_cdm_twf');
  """
  submit_trews = """
    CREATE TABLE if not exists trews (LIKE workspace.%(job)s_trews,
        unique (enc_id, tsp)
        );
    INSERT into trews (enc_id, tsp, %(columns)s) (
        SELECT enc_id, tsp, %(columns)s FROM workspace.%(job)s_trews
        )
    ON conflict (enc_id, tsp)
        do UPDATE SET %(set_columns)s;
    SELECT drop_tables('workspace', '%(job)s_trews');
  """
  async with ctxt.db_pool.acquire() as conn:
    records = await conn.fetch(select_all_colnames % {'table': 'cdm_twf'})
    colnames = [row[0] for row in records if row[0] != 'enc_id' and row[0] != 'tsp']
    twf_set_columns = ",".join([
        "%(col)s = excluded.%(col)s" % {'col': colname} for colname in colnames
    ])
    ctxt.log.info(submit_twf % {'job': job_id, 'set_columns': twf_set_columns} )
    await conn.execute(submit_twf % {'job': job_id, 'set_columns': twf_set_columns} )
    records = await conn.fetch(select_all_colnames % {'table': '%s_trews' % job_id})
    colnames = [row[0] for row in records if row[0] != 'enc_id' and row[0] != 'tsp']
    trews_set_columns = ",".join([
        "%(col)s = excluded.%(col)s" % {'col': colname} for colname in colnames
    ])
    trews_columns = ",".join(colnames)

    ctxt.log.info(submit_trews % {'job': job_id, 'set_columns': trews_set_columns, 'columns': trews_columns} )
    await conn.execute(submit_trews % {'job': job_id, 'set_columns': trews_set_columns, 'columns': trews_columns} )
    ctxt.log.info("{}: results submitted".format(job_id))
    ctxt.log.info("submit completed")
    return job_id





async def workspace_to_criteria_meas(ctxt, job_id):
  # insert all results to the measurement table
  upsert_meas_sql = \
  """INSERT INTO criteria_meas (pat_id, tsp, fid, value, update_date)
                  select pat_id, tsp::timestamptz, fid, last(fs.value), last(NOW() )
                  from workspace.%(job)s_flowsheets_transformed fs
                  where tsp <> 'NaT' and tsp::timestamptz < now()
                  group by pat_id, tsp, fid
      ON CONFLICT (pat_id, tsp, fid)
          DO UPDATE SET value = EXCLUDED.value, update_date = NOW();
      INSERT INTO criteria_meas (pat_id, tsp, fid, value, update_date)
                  select pat_id, tsp::timestamptz, fid, last(lr.value), last(NOW() )
                  from workspace.%(job)s_lab_results_transformed lr
                  where tsp <> 'NaT' and tsp::timestamptz < now()
                  group by pat_id, tsp, fid
      ON CONFLICT (pat_id, tsp, fid)
          DO UPDATE SET value = EXCLUDED.value, update_date = NOW();
      INSERT INTO criteria_meas (pat_id, tsp, fid, value, update_date)
                  select pat_id, tsp::timestamptz, fid, last(lo.status), last(NOW() )
                  from workspace.%(job)s_lab_orders_transformed lo
                  where tsp <> 'NaT' and tsp::timestamptz < now()
                  group by pat_id, tsp, fid
      ON CONFLICT (pat_id, tsp, fid)
          DO UPDATE SET value = EXCLUDED.value, update_date = NOW();
      INSERT INTO criteria_meas (pat_id, tsp, fid, value, update_date)
                  select pat_id, tsp::timestamptz, fid, last(lo.status), last(NOW() )
                  from workspace.%(job)s_active_procedures_transformed lo
                  where tsp <> 'NaT' and tsp::timestamptz < now()
                  group by pat_id, tsp, fid
      ON CONFLICT (pat_id, tsp, fid)
          DO UPDATE SET value = EXCLUDED.value, update_date = NOW();
      INSERT INTO criteria_meas (pat_id, tsp, fid, value, update_date)
                  select pat_id, tsp::timestamptz, fid, last(mar.dose_value), last(NOW() )
                  from workspace.%(job)s_med_admin_transformed mar
                  where tsp <> 'NaT' and tsp::timestamptz < now()
                  group by pat_id, tsp, fid
      ON CONFLICT (pat_id, tsp, fid)
          DO UPDATE SET value = EXCLUDED.value, update_date = NOW();
      INSERT INTO criteria_meas (pat_id, tsp, fid, value, update_date)
                  select pat_id, tsp::timestamptz, fid, last(mo.dose), last(NOW() )
                  from workspace.%(job)s_med_orders_transformed mo
                  where tsp <> 'NaT' and tsp::timestamptz < now()
                  group by pat_id, tsp, fid
      ON CONFLICT (pat_id, tsp, fid)
          DO UPDATE SET value = EXCLUDED.value, update_date = NOW();
      delete from criteria_meas where value = '';
  """ % {'job': job_id}
  async with ctxt.db_pool.acquire() as conn:
    await conn.execute(upsert_meas_sql)
    return job_id





async def drop_tables(ctxt, job_id, days_offset=2):
  async with ctxt.db_pool.acquire() as conn:
    day = (dt.datetime.now() - dt.timedelta(days=days_offset)).strftime('%m%d')
    ctxt.log.info("cleaning data in workspace for day:%s" % day)
    await conn.execute("select drop_tables_pattern('workspace', '%%_%s');" % day)
    ctxt.log.info("cleaned data in workspace for day:%s" % day)




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
    description = dictionary[fid]['description'].lower()
    if description in features_description:
      features_description[description].append(fid)
    else:
      features_description[description] = [fid]

  report = "All features: %s \n Total number of features:%s\n" % (features, num)
  for desc in features_description:
    report += desc + ":\n" + " ".join(features_description[desc]) + "\n"
  return report




class Epic2OpLoader:
  def __init__(self):
    self.job_id = "job_etl_{}".format(dt.datetime.now().strftime('%m%d%H%M%S')).lower()



  def get_tasks(self, db_data_task, db_raw_data_task, mode, archive, sqlalchemy_str, deps=[]):
    job_id = self.job_id
    all_tasks = []
    if archive == 1:
      all_tasks.append(Task(
        name = 'epic_2_workspace_archive',
        deps = [db_raw_data_task],
        fn   = epic_2_workspace,
        args = [sqlalchemy_str, job_id, 'unicode'],
      ))
    if 'test' in mode:
      all_tasks += Task(
        name = 'test_data_2_workspace',
        fn   = test_data_2_workspace,
        args = [mode, job_id],
      )
    all_tasks += [
      Task(name = 'epic_2_workspace',
           deps = [db_data_task],
           fn   = epic_2_workspace,
           args = [sqlalchemy_str, job_id, None]),
      Task(name = 'workspace_to_cdm',
           deps = ['epic_2_workspace'],
           coro = workspace_to_cdm),
      Task(name = 'load_online_prediction_parameters', 
           deps = ['workspace_to_cdm'],
           coro = load_online_prediction_parameters),
      Task(name = 'workspace_fillin', 
           deps = ['load_online_prediction_parameters', 'workspace_to_cdm'],
           coro = workspace_fillin),
      Task(name = 'workspace_derive', 
           deps = ['load_online_prediction_parameters', 'workspace_fillin'],
           coro = workspace_derive),
      Task(name = 'workspace_predict', 
           deps = ['load_online_prediction_parameters', 'workspace_derive'],
           coro = workspace_predict),
      Task(name = 'workspace_submit', 
           deps = ['workspace_predict'],
           coro = workspace_submit),
      Task(name = 'workspace_to_criteria_meas', 
           deps = ['workspace_submit'],
           coro = workspace_to_criteria_meas),
      Task(name = 'drop_tables',
           deps = ['workspace_to_criteria_meas', 2],
           coro = drop_tables),
      Task(name = 'get_notifications_for_epic', 
           deps = ['drop_tables'],
           coro = get_notifications_for_epic),
    ]
    return all_tasks
