import etl.load.primitives.tbl.load_table as primitives
import datetime as dt
import asyncpg
import asyncio
from sqlalchemy import create_engine
from etl.load.pipelines.derive_main import derive_feature, get_derive_seq
from etl.load.pipelines.fillin import fillin_pipeline

import os

class Epic2OpLoader:
  def __init__(self, config):
    self.config = config
    self.log = self.config.log
    self.pool = None
    current_time = dt.datetime.now().strftime('%m%d%H%M%S')
    self.job_id = "job_etl_{}".format(current_time).lower()

  async def async_init(self):
    self.pool = await asyncpg.create_pool(
      database = self.config.db_name,
      user     = self.config.db_user,
      password = self.config.db_pass,
      host     = self.config.db_host,
      port     = self.config.db_port
    )

  def run_loop(self, db_data, db_raw_data):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(self.run(db_data, db_raw_data))

  async def run(self, db_data, db_raw_data):
    if self.pool is None:
      await self.async_init()
    async with self.pool.acquire() as conn:
      if self.archive == 1:
        self.epic_2_workspace(db_raw_data, dtypes='unicode')
      self.epic_2_workspace(db_data)
      await self.workspace_to_cdm(conn)
      await self.load_online_prediction_parameters(conn)
      await self.workspace_fillin(conn)
      await self.workspace_derive(conn)
      await self.workspace_predict(conn)
      await self.workspace_submit(conn)
      await self.workspace_to_criteria_meas(conn)
      await self.drop_tables(conn)


  async def get_cdm_feature_dict(self, conn):
    sql = "select * from cdm_feature"
    cdm_feature = await conn.fetch(sql)
    cdm_feature_dict = {f['fid']:f for f in cdm_feature}
    return cdm_feature_dict

  def get_features_with_intermediates(self, features, dictionary):
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

  def _get_feature_description_report(self, features, dictionary):
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

  async def load_online_prediction_parameters(self, conn):
    self.log.info("load online_prediction_features")
    self.feature_weights = {}
    # load features weights from database
    trews_feature_weights = await conn.fetch("select * from trews_feature_weights")
    for weight in trews_feature_weights:
        self.feature_weights[weight['fid']] = weight['weight']
        self.log.info("feature: %s\t weight: %s" % (weight['fid'], weight['weight']))
    trews_parameters = await conn.fetch("select * from trews_parameters")
    for parameter in trews_parameters:
        if parameter['name'] == 'max_score':
            self.max_score = parameter['value']
        if parameter['name'] == 'min_score':
            self.min_score = parameter['value']
    self.log.info('set max_score to %s and min_score to %s' % (self.max_score, self.min_score))
    self.cdm_feature_dict = await self.get_cdm_feature_dict(conn)
    # list the measured features for online prediction
    features_with_intermediates = self.get_features_with_intermediates(self.feature_weights.keys(), self.cdm_feature_dict)
    measured_features = [fid for fid in features_with_intermediates if\
        self.cdm_feature_dict[fid]["is_measured"] ]
    self.log.info("The measured features in online prediction: %s" \
        % self._get_feature_description_report(measured_features, self.cdm_feature_dict))

    self.fillin_features = [fid for fid in features_with_intermediates if\
        self.cdm_feature_dict[fid]["is_measured"] and \
        self.cdm_feature_dict[fid]["category"] == "TWF"]
    self.log.info("The fillin features in online prediction: %s" % self.fillin_features)

    # list the derive features for online prediction
    self.derive_features = [fid for fid in features_with_intermediates if\
        not self.cdm_feature_dict[fid]["is_measured"]]
    self.log.info("The derive features in online prediction: %s" % self.derive_features)

  async def workspace_fillin(self, conn):
    self.log.info("start fillin pipeline")
    fillin_table = 'workspace.%s_cdm_twf' % self.job_id
    for fid in self.fillin_features:
      feature = self.cdm_feature_dict[fid]
      if feature['category'] == 'TWF' and feature['is_measured']:
        await fillin_pipeline(self.log, conn, feature, recalculate_popmean=False, table=fillin_table)
    self.log.info("fillin completed")

  async def workspace_derive(self, conn):
    self.log.info("derive start")
    # get derive order based on the input derive_features
    derive_feature_dict = {fid: self.cdm_feature_dict[fid] for fid in self.derive_features}
    derive_feature_order = get_derive_seq(derive_feature_dict)
    # derive the features sequentially
    twf_table = 'workspace.%s_cdm_twf' % self.job_id
    for fid in derive_feature_order:
        self.log.info("deriving fid %s" % fid)
        await derive_feature(self.log, self.cdm_feature_dict[fid], conn, twf_table=twf_table)
    self.log.info("derive completed")

  async def workspace_predict(self, conn):
    self.log.info("predict start")
    num_feature = len(self.feature_weights)
    twf_features = [fid for fid in self.feature_weights \
        if self.cdm_feature_dict[fid]['category'] == 'TWF']
    s_features = [fid for fid in self.feature_weights \
        if self.cdm_feature_dict[fid]['category'] == 'S']
    twf_features_times_weights = ['coalesce(((%s::numeric - (select coalesce(max(mean), 0) from trews_scaler where fid = E\'%s\')) / (select coalesce(max(case when scale = 0 then 1 else scale end), 1) from trews_scaler where fid = E\'%s\')) * %s, 0)' % (fid, fid, fid, self.feature_weights[fid]) \
        for fid in twf_features if self.cdm_feature_dict[fid]['data_type'] != 'Boolean' ] + \
         ['coalesce(((%s::int - (select coalesce(max(mean), 0) from trews_scaler where fid = E\'%s\') ) / ( select coalesce(max(case when scale = 0 then 1 else scale end), 1) from trews_scaler where fid = E\'%s\')) * %s, 0)' % (fid,fid, fid, self.feature_weights[fid]) \
        for fid in twf_features if self.cdm_feature_dict[fid]['data_type'] == 'Boolean']
    s_features_times_weights = ['coalesce((( coalesce(%s.value::numeric,0) - (select coalesce(max(mean), 0) from trews_scaler where fid = E\'%s\') ) / (select coalesce(max(case when scale = 0 then 1 else scale end), 1) from trews_scaler where fid = E\'%s\')) * %s, 0)' % (fid,fid,fid, self.feature_weights[fid]) \
        for fid in s_features if self.cdm_feature_dict[fid]['data_type'] != 'Boolean'] + \
        ['coalesce(((  coalesce(%s.value::bool::int,0) - (select coalesce(max(mean), 0) from trews_scaler where fid = E\'%s\') ) / (select coalesce(max(case when scale = 0 then 1 else scale end), 1) from trews_scaler where fid = E\'%s\')) * %s, 0)' % (fid, fid, fid, self.feature_weights[fid]) \
        for fid in s_features if self.cdm_feature_dict[fid]['data_type'] == 'Boolean']
    feature_weight_colnames = \
        [fid for fid in twf_features if self.cdm_feature_dict[fid]['data_type'] != 'Boolean' ] + \
        [fid for fid in twf_features if self.cdm_feature_dict[fid]['data_type'] == 'Boolean'] + \
        [fid for fid in s_features if self.cdm_feature_dict[fid]['data_type'] != 'Boolean'] + \
        [fid for fid in s_features if self.cdm_feature_dict[fid]['data_type'] == 'Boolean']
    feature_weight_values = [ "(%s - %s)/%s" % (val, self.min_score/num_feature, self.max_score - self.min_score)\
        for val in (twf_features_times_weights + s_features_times_weights)]
    select_clause = ",".join(["%s %s" % (v,k) \
        for k,v in zip(feature_weight_colnames, feature_weight_values)])
    # select feature values
    # calculate the trewscores
    weight_sum = "+".join(['coalesce(%s,0)'  % col for col in feature_weight_colnames])
    target = 'workspace.' + self.job_id
    table = target + '_trews'
    twf_table = target + '_cdm_twf'
    sql = \
    '''
    drop table if exists %(table)s;
    CREATE TABLE %(table)s AS
    select %(twf_table)s.enc_id, tsp, %(cols)s, null::numeric trewscore
    from %(twf_table)s
    ''' % {'cols':select_clause, 'table': table, 'twf_table': twf_table}
    for f in s_features:
        sql += """ left outer join cdm_s %(fid)s
                  ON %(twf_table)s.enc_id = %(fid)s.enc_id
                  AND %(fid)s.fid = '%(fid)s'
               """ % {'fid': f, 'twf_table': twf_table}
    update_sql = \
    """
    ;update %(table)s set trewscore = %(sum)s;
    """ % {'sum':weight_sum, 'table':table, 'twf_table': twf_table}
    sql += update_sql
    self.log.info("predict trewscore:" + sql)
    await conn.execute(sql)
    self.log.info("predict completed")


  async def workspace_submit(self, conn):
    # submit to cdm_twf
    # submit to trews
    self.log.info("submit start")
    self.log.info("%s: submitting results ..." % self.job_id)
    select_all_colnames = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = '%(table)s';
    """

    submit_twf = """
    insert into cdm_twf
    (
        select * from workspace.%(job)s_cdm_twf
        )
    on conflict (enc_id, tsp)
        do update set %(set_columns)s;
    SELECT drop_tables('workspace', '%(job)s_cdm_twf');
    """
    submit_trews = """
    create table if not exists trews (like workspace.%(job)s_trews,
        unique (enc_id, tsp)
        );
    insert into trews (enc_id, tsp, %(columns)s) (
        select enc_id, tsp, %(columns)s from workspace.%(job)s_trews
        )
    on conflict (enc_id, tsp)
        do update set %(set_columns)s;
    SELECT drop_tables('workspace', '%(job)s_trews');
    """
    records = await conn.fetch(select_all_colnames % {'table': 'cdm_twf'})
    colnames = [row[0] for row in records if row[0] != 'enc_id' and row[0] != 'tsp']
    twf_set_columns = ",".join([
        "%(col)s = excluded.%(col)s" % {'col': colname} for colname in colnames
    ])
    self.log.info(submit_twf % {'job': self.job_id, 'set_columns': twf_set_columns} )
    await conn.execute(submit_twf % {'job': self.job_id, 'set_columns': twf_set_columns} )
    records = await conn.fetch(select_all_colnames % {'table': '%s_trews' % self.job_id})
    colnames = [row[0] for row in records if row[0] != 'enc_id' and row[0] != 'tsp']
    trews_set_columns = ",".join([
        "%(col)s = excluded.%(col)s" % {'col': colname} for colname in colnames
    ])
    trews_columns = ",".join(colnames)

    self.log.info(submit_trews % {'job': self.job_id, 'set_columns': trews_set_columns, 'columns': trews_columns} )
    await conn.execute(submit_trews % {'job': self.job_id, 'set_columns': trews_set_columns, 'columns': trews_columns} )
    self.log.info("%s: results submitted" % self.job_id)
    self.log.info("submit completed")

  async def drop_tables(self, conn, days_offset=2):
    day = (dt.datetime.now() - dt.timedelta(days=days_offset)).strftime('%m%d')
    self.log.info("cleaning data in workspace for day:%s" % day)
    await conn.execute("select drop_tables_pattern('workspace', '%%_%s');" % day)
    self.log.info("cleaned data in workspace for day:%s" % day)

  async def get_notifications_for_epic(self):
    async with self.pool.acquire() as conn:
      self.log.info("getting notifications to push to epic")
      return await conn.fetch("""
        select * from get_notifications_for_epic(null)
        """)

  async def workspace_to_criteria_meas(self, conn):
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
    """ % {'job': self.job_id}
    await conn.execute(upsert_meas_sql)

  def epic_2_workspace(self, db_data, dtypes=None):
    engine = create_engine(self.config.get_db_conn_string_sqlalchemy())
    for df_name, df in db_data.items():
      primitives.data_2_workspace(engine, self.job_id, df_name, df, dtypes=dtypes)

  async def workspace_to_cdm(self, conn):
    await primitives.insert_new_patients(conn, self.job_id)
    await primitives.create_job_cdm_twf_table(conn, self.job_id)
    await primitives.workspace_bedded_patients_2_cdm_s(conn, self.job_id)
    await primitives.workspace_flowsheets_2_cdm_t(conn, self.job_id)
    await primitives.workspace_lab_results_2_cdm_t(conn, self.job_id)
    await primitives.workspace_location_history_2_cdm_t(conn, self.job_id)
    await primitives.workspace_medication_administration_2_cdm_t(conn, self.job_id)
    await primitives.workspace_flowsheets_2_cdm_twf(conn, self.job_id)
    await primitives.workspace_lab_results_2_cdm_twf(conn, self.job_id)
    await primitives.workspace_lab_results_2_cdm_twf(conn, self.job_id)
