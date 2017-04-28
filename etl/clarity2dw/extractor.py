import pandas as pd
import numpy as np
from etl.transforms.primitives.row import transform
from etl.load.primitives.row import load_row
from etl.load.pipelines.fillin import fillin_pipeline
from etl.load.pipelines.derive_main import derive_main
import etl.load.primitives.tbl.load_table as load_table
import timeit
import importlib
import concurrent.futures
import asyncio
import asyncpg
import logging
from etl.core.config import Config
import sys
from multiprocessing import Pool
import os
import functools
import time

recalculate_popmean = False # if False, then remember to import cdm_g before extraction



async def run_transform_tasks(executor, feature_mapping, job, log):
  # Configure logging to show the id of the process
  # where the log message originates.
  log.info("start running transform tasks")
  transform_tasks = [
    asyncio.get_event_loop().run_in_executor(executor, transform_feature_mapping_task, mapping_row.to_dict(), job) for i, mapping_row in feature_mapping.iterrows()
  ]
  log.info("waiting for executor tasks")
  completed, pending = await asyncio.wait(transform_tasks)
  results = [t.result() for t in completed]
  log.info("results: {!r}".format(results))
  log.info("exit transform")

def transform_feature_mapping_task(mapping_row, job):
  '''
  The task to ETL one mapping row using a standalone process
  '''
  async def _run_(job, mapping_row):
    config = Config(**job['config'])
    config.log.info("start subprocess for fid = {}".format(mapping_row['fid(s)']))
    async with asyncpg.create_pool(database=config.db_name, user=config.db_user, password=config.db_pass, host=config.db_host, port=config.db_port) as pool:
      config.log.info("access database pool")
      extractor = Extractor(pool, config)
      async with pool.acquire() as conn:
        await extractor.transform_feature_mapping_row(conn, mapping_row)
    config.log.info("completed current task in this subprocess")

  loop = asyncio.new_event_loop()
  asyncio.set_event_loop(loop)
  loop.run_until_complete(_run_(job, mapping_row))


class Extractor:
  def __init__(self, config, pool=None):
    self.config = config
    self.pool = pool
    self.log = self.config.log

  def task(f):
    @functools.wraps(f)
    def wrapper(*args, **kwds):
      print('Calling decorated task')
      return f(*args, **kwds)
    return wrapper

  @task
  def example(self):
    print('example task')
    sleep(2)


  async def run(self, job):
    self.job = job
    self.log.info("start to run clarity ETL")
    async with self.pool.acquire() as conn:
      self.cdm_feature_dict = await self.get_cdm_feature_dict(conn)
      if job.get("reset_dataset", False):
        await self.reset_dataset(conn, job['reset_dataset'])
      if job.get("transform", False):
        await self.transform(conn, job['transform'])
      if job.get("fillin", False):
        await self.run_fillin(conn, job['fillin'])
      if job.get("derive", False):
        await self.derive(conn, job['derive'])
      if job.get("offline_criteria_processing", False):
        await self.offline_criteria_processing(conn, job['offline_criteria_processing'])
    self.log.info("completed clarity ETL")


  async def transform(self, conn, transform_job):
    self.log.info("Transform Job:")
    self.log.info(transform_job)
    if transform_job.get('populate_patients', False):
      max_num_pats = None
      if isinstance(transform_job.get('populate_patients'), dict):
        max_num_pats = transform_job.get('populate_patients').get('max_num_pats', None)
      await self.populate_patients(conn, limit=max_num_pats)
    if transform_job.get('populate_measured_features', False):
      self.plan = False
      populate_measured_features_job = transform_job.get('populate_measured_features')
      if populate_measured_features_job.get('plan', False):
        self.plan = transform_job['populate_measured_features']['plan']
      fid = None
      if populate_measured_features_job.get('fid', False):
        fid = transform_job['populate_measured_features']['fid']
      nproc = populate_measured_features_job.get('nproc', None)
      await self.populate_measured_features(conn, fid, nproc=nproc)

  async def run_fillin(self, conn, job):
    self.log.info("start fillin pipeline")
    # NOTE: we could optimize fillin in one run, e.g., update set all columns
    for fid in self.cdm_feature_dict:
      feature = self.cdm_feature_dict[fid]
      if 'recalculate_popmean' in job:
        recalculate_popmean = job['recalculate_popmean']
      if feature['category'] == 'TWF' and feature['is_measured']:
        await fillin_pipeline(self.log, conn, feature, self.config.dataset_id, recalculate_popmean)
    self.log.info("fillin completed")

  async def derive(self, conn, job):
    self.log.info("start derive pipeline")
    fid = None
    mode = None
    if 'fid' in job:
      fid = job['fid']
    await derive_main(self.log, conn, self.cdm_feature_dict, dataset_id = self.config.dataset_id, fid = fid, mode=mode)
    self.log.info("derive completed")

  async def offline_criteria_processing(self, conn, job):
    if job.get('load_cdm_to_criteria_meas', False):
      await load_table.load_cdm_to_criteria_meas(conn, self.config.dataset_id)
    if job.get('calculate_historical_criteria', False):
      await load_table.calculate_historical_criteria(conn)


  async def reset_dataset(self, conn, job):
    self.log.warn("reset_dataset")
    reset_sql = ''
    if job.get('remove_data', False):
      reset_sql += '''
      delete from cdm_s where dataset_id = %(dataset_id)s;
      delete from cdm_t where dataset_id = %(dataset_id)s;
      delete from cdm_twf where dataset_id = %(dataset_id)s;
      ''' % {'dataset_id': self.config.dataset_id}
    if job.get('remove_pat_enc', False):
      reset_sql += '''
      delete from pat_enc where dataset_id = %(dataset_id)s;
      ''' % {'dataset_id': self.config.dataset_id}
    if 'start_enc_id' in job:
      reset_sql += "select setval('pat_enc_enc_id_seq', %s);" % job['start_enc_id']
    self.log.debug("ETL init sql: " + reset_sql)
    result = await conn.execute(reset_sql)
    self.log.info("ETL Init: " + result)

##################
# transform pipeline
##################
  async def populate_patients(self, conn, limit=None):
    sql = '''
    insert into pat_enc (dataset_id, visit_id, pat_id)
    SELECT %(dataset_id)s, demo."CSN_ID" visit_id, demo."pat_id"
    FROM "Demographics" demo left join pat_enc pe on demo."CSN_ID"::text = pe.visit_id::text
    where pe.visit_id is null %(limit)s
    ''' % {'dataset_id': self.config.dataset_id, 'limit': 'limit {}'.format(limit) if limit else ''}
    self.log.debug("ETL populate_patients sql: " + sql)
    result = await conn.execute(sql)
    self.log.info("ETL populate_patients: " + result)

  async def populate_measured_features(self, conn, fids_2_proc=None, nproc=None):
    self.log.info("Using Feature Mapping:")
    self.log.info("{}".format(self.config.FEATURE_MAPPING_CSV))
    self.feature_mapping = pd.read_csv(self.config.FEATURE_MAPPING_CSV)

    pat_mappings = await self.get_pat_mapping(conn)
    self.visit_id_to_enc_id = pat_mappings['visit_id_to_enc_id']
    self.pat_id_to_enc_ids = pat_mappings['pat_id_to_enc_ids']
    self.log.info("load feature mapping")

    if nproc is not None and nproc > 0:
      executor = concurrent.futures.ProcessPoolExecutor(max_workers=nproc)
      await run_transform_tasks(executor, self.feature_mapping, self.job, self.log)
    else:
      for row_idx, mapping_row in self.feature_mapping.iterrows():
        await self.transform_feature_mapping_row(conn, mapping_row, fids_2_proc=fids_2_proc)

  async def transform_feature_mapping_row(self, conn, mapping_row, fids_2_proc=None):
    self.log.debug(mapping_row)
    fids = mapping_row['fid(s)']
    if fids_2_proc:
      if isinstance(fids_2_proc, list):
        if fids not in fids_2_proc:
          return
      else:
        if fids != fids_2_proc:
          return
    fids = [fid.strip() for fid in fids.split(',')] if ',' in fids else [fids]
    for fid in fids:
      if not self.cdm_feature_dict.get(fid, False):
        self.log.error("feature %s is not in cdm_feature" % fid)
        raise(ValueError("feature %s is not in cdm_feature" % fid))
    # get transform function
    transform_func_id = mapping_row['transform_func_id']
    if "." in str(transform_func_id): # if custom function
      i = len(transform_func_id) - transform_func_id[::-1].index('.')
      package = transform_func_id[:(i-1)]
      transform_func_id = transform_func_id[i:]
      self.log.info("fid: %s using package: %s and transform_func_id: %s" % (",".join(fids), package, transform_func_id))
      module = importlib.import_module(package)
      func = getattr(module, transform_func_id)
      await func(conn,self.config.dataset_id,self.log,self.plan)
    else: #if standard function
      # For now, use the fact that only custom functions are many to one.
      for fid in fids:
        await self.populate_feature_to_cdm(mapping_row.copy().set_value('fid', fid), conn, self.cdm_feature_dict[fid])

  async def get_pat_mapping(self, conn):
    sql = "select * from pat_enc where dataset_id = %s" % self.config.dataset_id
    pats = await conn.fetch(sql)
    visit_id_to_enc_id = {}
    pat_id_to_enc_ids = {}
    for pat in pats:
      visit_id_to_enc_id[pat['visit_id']] = pat['enc_id']
      if pat['pat_id'] in pat_id_to_enc_ids:
        pat_id_to_enc_ids[pat['pat_id']].append(pat['enc_id'])
      else:
        pat_id_to_enc_ids[pat['pat_id']] = [pat['enc_id']]
    return {
      "visit_id_to_enc_id": visit_id_to_enc_id,
      "pat_id_to_enc_ids": pat_id_to_enc_ids
    }

  async def get_cdm_feature_dict(self, conn):
    sql = "select * from cdm_feature where dataset_id = %s" % self.config.dataset_id
    cdm_feature = await conn.fetch(sql)
    cdm_feature_dict = {f['fid']:f for f in cdm_feature}
    return cdm_feature_dict


  async def populate_feature_to_cdm(self, mapping, conn, cdm_feature_attributes):
    data_type = cdm_feature_attributes['data_type']
    self.log.debug(mapping)
    fid = mapping['fid']
    self.log.info('importing feature value fid %s' % fid)
    self.log.debug(mapping['transform_func_id'])
    transform_func_id = mapping['transform_func_id']
    if str(transform_func_id) == 'nan':
      transform_func_id = None
    if transform_func_id:
      self.log.info("transform func: %s" % transform_func_id)
    category = cdm_feature_attributes['category']
    is_no_add = bool(mapping['is_no_add'] == "yes")
    is_med_action = bool(mapping['is_med_action'] == "yes")
    self.log.info("is_no_add: %s, is_med_action: %s" \
      % (is_no_add, is_med_action))
    fid_info = {'fid':fid, 'category':category, 'is_no_add':is_no_add,
          'data_type':data_type}
    start = timeit.default_timer()
    if is_med_action and fid != 'vent':
      line = await self.populate_medaction_features(conn, mapping, fid, transform_func_id, data_type, fid_info)
    elif is_med_action and fid == 'vent':
      line = await self.populate_vent(conn, mapping, fid, transform_func_id, data_type, fid_info)
    else:
      line = await self.populate_non_medaction_features(conn, mapping, fid, transform_func_id, data_type, category, is_no_add)
    if not self.plan:
      duration = timeit.default_timer() - start
      if line == 0:
        self.log.warn(\
          'stats: Zero line found in dblink for fid %s %s s' \
            % (fid, duration))
      else:
        self.log.info(\
          'stats: %s valid lines found in dblink for fid %s %s s' \
            % (line, fid, duration))

  async def populate_medaction_features(self, conn, mapping, fid, transform_func_id, data_type, fid_info):
    # process medication action input for HC_EPIC
    # order by csn_id, medication id, and timeActionTaken
    line = 0
    orderby = '"CSN_ID", "MEDICATION_ID", "TimeActionTaken"'
    sql = self.get_feature_sql_query(mapping, orderby=orderby)
    if self.plan:
      async with conn.transaction():
        async for row in conn.cursor(sql):
          self.log.debug(row)
    else:
      cur_enc_id = None
      cur_med_id = None
      cur_med_events = []
      async with conn.transaction():
        async for row in conn.cursor(sql):
          if str(row['visit_id']) in self.visit_id_to_enc_id:
            enc_id = self.visit_id_to_enc_id[str(row['visit_id'])]
            med_id = row['MEDICATION_ID']
            # print "cur", row
            if cur_enc_id is None \
              or cur_enc_id != enc_id or med_id != cur_med_id:
              # new enc_id, med_id pair
              if cur_enc_id is not None and cur_med_id is not None:
                # process cur_med_events
                len_of_events = len(cur_med_events)
                if len_of_events > 0:
                  line += len_of_events
                  await self.process_med_events(cur_enc_id, \
                    cur_med_id, cur_med_events, fid_info, \
                    mapping, conn)

              cur_enc_id = enc_id
              cur_med_id = med_id
              cur_med_events = []
            # print "cur events temp:", cur_med_events
            cur_med_events.append(row)
            # print "cur events:", cur_med_events
        if cur_enc_id is not None and cur_med_id is not None:
          # process cur_med_events
          len_of_events = len(cur_med_events)
          if len_of_events > 0:
            line += len_of_events
            await self.process_med_events(cur_enc_id, cur_med_id,
                        cur_med_events, fid_info, mapping,
                        conn)
    return line

  async def populate_vent(self, conn, mapping, fid, transform_func_id, data_type, fid_info):
    line = 0
    orderby = " icustay_id, realtime"
    sql = self.get_feature_sql_query(mapping, orderby=orderby)
    if self.plan:
      async with conn.transaction():
        async for row in conn.cursor(sql):
          self.log.debug(row)
    else:
      cur_enc_id = None
      cur_vent_events = []
      async with conn.transaction():
        async for row in conn.cursor(sql):
          if str(row['visit_id']) in self.visit_id_to_enc_id:
            enc_id = self.visit_id_to_enc_id[str(row['visit_id'])]
            if enc_id:
              if cur_enc_id is None or cur_enc_id != enc_id:
                # new enc_id, med_id pair
                if cur_enc_id:
                  # process cur_med_events
                  len_of_events = len(cur_vent_events)
                  if len_of_events > 0:
                    line += len_of_events
                    self.process_vent_events(cur_enc_id, \
                      cur_vent_events, fid_info, mapping, conn)
                cur_enc_id = enc_id
                cur_vent_events = []
              # print "cur events temp:", cur_med_events
              cur_vent_events.append(row)
              # print "cur events:", cur_med_events
        if cur_enc_id and len_of_events > 0:
          line += len_of_events
        self.process_vent_events(cur_enc_id, cur_vent_events, \
          fid_info, mapping, cdm)
    return line

  async def populate_non_medaction_features(self, conn, mapping, fid, transform_func_id, data_type, category, is_no_add):
    line = 0
    # process features unrelated to med actions
    sql = self.get_feature_sql_query(mapping)
    if self.plan:
      async with conn.transaction():
        async for row in conn.cursor(sql):
          self.log.debug(row)
    else:
      line = 0
      pat_id_based = 'pat_id' in mapping['select_cols']
      async with conn.transaction():
        async for row in conn.cursor(sql):
          if pat_id_based:
            if str(row['pat_id']) in self.pat_id_to_enc_ids:
              enc_ids = self.pat_id_to_enc_ids[str(row['pat_id'])]
              for enc_id in enc_ids:
                result = transform.transform(fid, \
                  transform_func_id, row, data_type, self.log)
                if result is not None:
                  line += 1
                  await self.save_result_to_cdm(fid, category, enc_id, \
                    row, result, conn, is_no_add)
          elif str(row['visit_id']) in self.visit_id_to_enc_id:
            enc_id = self.visit_id_to_enc_id[str(row['visit_id'])]
            if enc_id:
              # transform return a result containing both value and
              # confidence flag
              result = transform.transform(fid, transform_func_id, \
                row, data_type, self.log)
              # print row, result
              if result is not None:
                line += 1
                await self.save_result_to_cdm(fid, category, enc_id, \
                  row, result, conn, is_no_add)
          if line > 0 and line % 10000 == 0:
            self.log.info('import rows %s', line)
    return line

  def get_feature_sql_query(self, mapping, orderby=None):
    if 'subject_id' in mapping['select_cols'] \
      and 'pat_id' not in mapping['select_cols']:
      # hist features in mimic
      select_clause = \
        mapping['select_cols'].replace("subject_id", "subject_id pat_id")
    elif 'visit_id' in mapping['select_cols'] \
      or 'pat_id' in mapping['select_cols']:
      select_clause = mapping['select_cols']
    else:
      select_clause = '"CSN_ID" visit_id,' + mapping['select_cols']
    dbtable = mapping['dbtable']
    where_clause = str(mapping['where_conditions'])
    if where_clause == 'nan':
      where_clause = ''
    sql = "SELECT %s FROM %s %s" % (select_clause, dbtable, where_clause)
    if orderby:
      sql += " order by " + orderby
    if self.plan:
      sql += " limit 100"
    self.log.info("sql: %s" % sql)
    return sql

  async def _transform_med_action(self, fid, mapping, event, fid_info, conn, enc_id):
    dose_entry = transform.transform(fid, mapping['transform_func_id'], \
      event, fid_info['data_type'], self.log)
    if dose_entry:
      self.log.debug(dose_entry)
      tsp = dose_entry[0]
      volume = str(dose_entry[1])
      confidence = dose_entry[2]
      if fid_info['is_no_add']:
        await load_row.upsert_t(conn, [enc_id, tsp, fid, volume, confidence], dataset_id = self.config.dataset_id)
      else:
        await load_row.add_t(conn, [enc_id, tsp, fid, volume, confidence], dataset_id = self.config.dataset_id)



  async def process_med_events(self, enc_id, med_id, med_events,
               fid_info, mapping, conn):
    med_route = med_events[0]['MedRoute']
    med_name = med_events[0]['display_name']
    self.log.debug("\nentries from med_id %s, med_route %s:" \
      % (med_id, med_route))
    for row in med_events:
      self.log.debug(row)
    fid = fid_info['fid']
    self.log.debug("transformed entries:")
    self.log.debug("transform function: %s" % mapping['transform_func_id'])
    dose_intakes = transform.transform(fid, mapping['transform_func_id'], \
      med_events, fid_info['data_type'], self.log)
    if dose_intakes is not None and len(dose_intakes) > 0:
      for intake in dose_intakes:
        if intake is None:
          self.log.warn("transform function returns None!")
        else:
          self.log.debug(intake)
          # dose intake are T category
          tsp = intake[0]
          volume = str(intake[1])
          confidence = intake[2]

          if fid_info['is_no_add']:
            await load_row.upsert_t(conn, [enc_id, tsp, str(fid), str(volume), confidence], dataset_id = self.config.dataset_id)
          else:
            await load_row.add_t(conn, [enc_id, tsp, str(fid), str(volume), confidence], dataset_id = self.config.dataset_id)

  async def process_vent_events(self, enc_id, vent_events, fid_info, mapping, conn):
    self.log.debug("\nentries from enc_id %s:" % enc_id)
    for row in vent_events:
      self.log.debug(row)
    fid = fid_info['fid']
    self.log.debug("transformed entries:")
    self.log.debug("transform function: %s" % mapping['transform_func_id'])
    vent_results = transform.transform(fid, mapping['transform_func_id'], \
      vent_events, fid_info['data_type'], self.log)
    if vent_results is not None and len(vent_results) > 0:
      for result in vent_results:
        if result is None:
          self.log.warn("transform function returns None!")
        else:
          self.log.debug(result)
          # dose result are T category
          tsp = result[0]
          on_off = str(result[1])
          confidence = result[2]

          if fid_info['is_no_add']:
            await load_row.upsert_t(conn, [enc_id, tsp, fid, on_off, confidence], dataset_id = self.config.dataset_id)
          else:
            await load_row.add_t(conn, [enc_id, tsp, fid, on_off, confidence], dataset_id = self.config.dataset_id)

  async def save_result_to_cdm(self, fid, category, enc_id, row, results, conn, \
    is_no_add):
    if not isinstance(results[0], list):
      results = [results]
    for result in results:
      tsp = None
      if len(result) == 3:
        # contain tsp, value, and confidence
        tsp = result[0]
        result.pop(0)
      value = result[0]
      confidence = result[1]
      if category == 'S':
        if is_no_add:
          await load_row.upsert_s(conn, [enc_id, fid, str(value), confidence], dataset_id = self.config.dataset_id)
        else:
          await load_row.add_s(conn, [enc_id, fid, str(value), confidence], dataset_id = self.config.dataset_id)
      elif category == 'T':
        if tsp is None:
          if len(row) >= 4:
            tsp = row[2]
          else:
            tsp = row[1]
        if tsp is not None:
          if is_no_add:
            await load_row.upsert_t(conn, [enc_id, tsp, str(fid), str(value), confidence], dataset_id = self.config.dataset_id)
          else:
            await load_row.add_t(conn, [enc_id, tsp, str(fid), str(value), confidence], dataset_id = self.config.dataset_id)
      elif category == 'TWF':
        if tsp is None:
          if len(row) >= 4:
            tsp = row[2]
          else:
            tsp = row[1]
        if is_no_add:
          await load_row.upsert_twf(conn, [enc_id, tsp, fid, value, confidence], dataset_id = self.config.dataset_id)
        else:
          await load_row.add_twf(conn, [enc_id, tsp, fid, value, confidence], dataset_id = self.config.dataset_id)


