import functools
import time
import os
import numpy as np
import pandas as pd
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
import random

TRANSACTION_RETRY = 10
PSQL_WAIT_IN_SECS = 5

def log_time(log, name, start, extracted, loaded):
  duration = timeit.default_timer() - start
  if extracted == 0:
    log.warn(\
      'STATS: Zero row extraced for %s %s s' \
        % (name, duration))
  else:
    log.info(\
      'STATS: %s valid rows extraced for %s %s s' \
        % (extracted, name, duration))
  if loaded == 0:
    log.warn(\
      'STATS: Zero row loaded in CDM for %s %s s' \
        % (name, duration))
  else:
    log.info(\
      'STATS: %s valid rows loaded in CDM for %s %s s' \
        % (loaded, name, duration))

class Extractor:
  def __init__(self, job):
    self.job = job
    self.dataset_id = job['extractor'].get('dataset_id')
    self.plan = job['plan']
    CONF = os.path.dirname(os.path.abspath(__file__))
    CONF = os.path.join(CONF, 'conf')
    feature_mapping_csv = os.path.join(CONF, 'feature_mapping.csv')
    self.feature_mapping = pd.read_csv(feature_mapping_csv)


  # def extract(f):
  #   @functools.wraps(f)
  #   def wrapper(*args, **kwds):
  #     print('Calling decorated task')
  #     return f(*args, **kwds)
  #   return wrapper

  # @extract
  # def example(self, *args, **kwds):
  #   print('example task')
  #   time.sleep(2)

  async def extract_init(self, ctxt):
    async with ctxt.db_pool.acquire() as conn:
      await self.reset_dataset(conn, ctxt)
      return None

  async def query_cdm_feature_dict(self, conn):
    sql = "select * from cdm_feature where dataset_id = %s" % self.dataset_id
    cdm_feature = await conn.fetch(sql)
    self.cdm_feature_dict = {f['fid']:f for f in cdm_feature}
    return None

  async def reset_dataset(self, conn, ctxt):
    ctxt.log.info("reset_dataset")
    reset_job = self.job.get('reset_dataset', {})
    reset_sql = ''
    if reset_job.get('remove_data', False):
      reset_sql += '''
      delete from cdm_s where dataset_id = %(dataset_id)s;
      delete from cdm_t where dataset_id = %(dataset_id)s;
      delete from cdm_twf where dataset_id = %(dataset_id)s;
      delete from criteria_meas where dataset_id = %(dataset_id)s;
      ''' % {'dataset_id': self.dataset_id}
    if reset_job.get('remove_pat_enc', False):
      reset_sql += '''
      delete from pat_enc where dataset_id = %(dataset_id)s;
      ''' % {'dataset_id': self.dataset_id}
    if 'start_enc_id' in reset_job:
      reset_sql += "select setval('pat_enc_enc_id_seq', %s);" % reset_job['start_enc_id']
    if reset_sql:
      ctxt.log.info("ETL init sql: " + reset_sql)
      result = await conn.execute(reset_sql)
      ctxt.log.info("ETL Init: " + result)
    return None

  async def populate_patients(self, ctxt, _):
    if self.job.get('transform', False):
      if self.job.get('transform').get('populate_patients', False):
        async with ctxt.db_pool.acquire() as conn:
          populate_patients_job = self.job['transform']['populate_patients']
          limit = populate_patients_job.get('limit', None)
          sql = '''
          insert into pat_enc (dataset_id, visit_id, pat_id)
          SELECT %(dataset_id)s, demo."CSN_ID" visit_id, demo."pat_id"
          FROM "Demographics" demo left join pat_enc pe on demo."CSN_ID"::text = pe.visit_id::text and pe.dataset_id = %(dataset_id)s
          where pe.visit_id is null %(limit)s
          ''' % {'dataset_id': self.dataset_id, 'limit': 'limit {}'.format(limit) if limit else ''}
          ctxt.log.debug("ETL populate_patients sql: " + sql)
          result = await conn.execute(sql)
          ctxt.log.info("ETL populate_patients: " + result)
          return result
      else:
        ctxt.log.info("populate_patients skipped")
    else:
      ctxt.log.info("populate_patients skipped")

  async def transform_init(self, ctxt, _):
    if self.job.get('transform', False):
      if self.job.get('transform').get('populate_measured_features', False):
        async with ctxt.db_pool.acquire() as conn:
          ctxt.log.info("Using Feature Mapping:")
          pat_mappings = await self.get_pat_mapping(conn)
          self.visit_id_to_enc_id = pat_mappings['visit_id_to_enc_id']
          self.pat_id_to_enc_ids = pat_mappings['pat_id_to_enc_ids']
          ctxt.log.info("loaded feature and pat mapping")
          return pat_mappings
    else:
      ctxt.log.info("transform_init skipped")

  async def get_pat_mapping(self, conn):
    sql = "select * from pat_enc where dataset_id = %s" % self.dataset_id
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

  def get_transform_tasks(self):
    mapping_list = self.feature_mapping.to_dict('records')
    nprocs = 1
    if self.job.get('transform', False):
      nprocs = int(self.job.get('transform').get('nprocs', nprocs))
    transform_tasks = self.partition(mapping_list,  nprocs)
    return transform_tasks

  def partition(self, lst, n, random_shuffle=True):
    if random_shuffle:
      random.shuffle(lst)
    division = len(lst) // n
    return [lst[division * i:division * (i + 1)] for i in range(n)]

    # # TEST CASE B
    # lst = lst[:40]
    # division = len(lst) // n
    # return [lst[division * i:division * (i + 1)] for i in range(n)]

    # # TEST CASE A
    # lst_vent = None
    # lst_med = None
    # lst_bands = None
    # for item in lst:
    #   if item['fid(s)'] == 'vent':
    #     lst_vent = item
    #   if item['fid(s)'] == 'aclidinium_dose':
    #     lst_med = item
    #   if item['fid(s)'] == 'bands':
    #     lst_bands = item
    #   if item['fid(s)'] == 'heart_rate':
    #     lst_heart_rate = item
    #   if item['fid(s)'] == 'co2':
    #     lst_co2 = item
    #   if item['fid(s)'] == 'spo2':
    #     lst_spo2 = item
    #   if item['fid(s)'] == 'uti_approx':
    #     lst_approx = item
    #   if item['fid(s)'] == 'hematocrit':
    #     lst_hematocrit = item
    # return [ [lst[0], lst[2], lst_hematocrit, lst_med, lst_bands, lst_spo2, lst_heart_rate], [lst[1], lst[3], lst_approx, lst_vent, lst_co2]]
    # # return [ [lst[0]] ]#, lst[2], lst_med, lst_bands]]

  async def run_transform_task(self, ctxt, pat_mappings, task):
    if self.job.get('transform', False):
      if self.job.get('transform').get('populate_measured_features', False):
        self.visit_id_to_enc_id = pat_mappings['visit_id_to_enc_id']
        self.pat_id_to_enc_ids = pat_mappings['pat_id_to_enc_ids']
        self.min_tsp = self.job.get('transform').get('populate_measured_features').get('min_tsp')
        futures = []
        async with ctxt.db_pool.acquire() as conn:
            await self.query_cdm_feature_dict(conn)
        for mapping_row in task:
          futures.extend(self.run_feature_mapping_row(ctxt, mapping_row))
        ctxt.log.info("run_transform_task futures: {}".format(', '.join([m['fid(s)'] for m in task])))
        done, _ = await asyncio.wait(futures)
        for future in done:
          ctxt.log.info("run_transform_task completed: {}".format(future.result()))
    else:
      ctxt.log.info("transform task skipped")
    return None

  def run_feature_mapping_row(self, ctxt, mapping_row):
    log = ctxt.log
    fids = mapping_row['fid(s)']
    fids = [fid.strip() for fid in fids.split(',')] if ',' in fids else [fids]
    # fid validation check
    for fid in fids:
      if not self.cdm_feature_dict.get(fid, False):
        log.error("feature %s is not in cdm_feature" % fid)
        raise(ValueError("feature %s is not in cdm_feature" % fid))
    # get transform function
    transform_func_id = mapping_row['transform_func_id']
    futures = []
    if "." in str(transform_func_id):
      # if it is custom function
      i = len(transform_func_id) - transform_func_id[::-1].index('.')
      package = transform_func_id[:(i-1)]
      transform_func_id = transform_func_id[i:]
      log.info("fid: %s using package: %s and transform_func_id: %s" % (",".join(fids), package, transform_func_id))
      futures.append(self.run_custom_func(package, transform_func_id, ctxt, self.dataset_id, log,self.plan))
    else:
      # if it is standard function
      # For now, use the fact that only custom functions are many to one.
      for fid in fids:
        mapping_row.update({'fid': fid})
        futures.extend(self.populate_raw_feature_to_cdm(ctxt, mapping_row, self.cdm_feature_dict[fid]))
    return futures

  async def run_custom_func(self, package, transform_func_id, ctxt, dataset_id, log, plan):
    module = importlib.import_module(package)
    func = getattr(module, transform_func_id)
    # attempts = 0
    # while attempts < TRANSACTION_RETRY:
    #     async with ctxt.db_pool.acquire() as conn:
    #       try:
    #         await func(conn, dataset_id, log, plan)
    #         break
    #       except Exception as e:
    #         attempts += 1
    #         log.warn("PSQL Error %s %s" % (transform_func_id, e))
    #         log.info("Transaction retry attempts: {} {}".format(attempts, transform_func_id))
    #         continue
    # if attempts == TRANSACTION_RETRY:
    #   log.error("Transaction retry failed")
    async with ctxt.db_pool.acquire() as conn:
      await func(conn, dataset_id, log, plan)

  def populate_raw_feature_to_cdm(self, ctxt, mapping, cdm_feature_attributes):
    futures = []
    data_type = cdm_feature_attributes['data_type']
    log = ctxt.log
    fid = mapping['fid']
    transform_func_id = mapping['transform_func_id']
    category = cdm_feature_attributes['category']
    is_no_add = bool(mapping['is_no_add'] == "yes")
    is_med_action = bool(mapping['is_med_action'] == "yes")
    log.info('loading feature value fid %s, transform func: %s, is_no_add: %s, is_med_action: %s' \
      % (fid, transform_func_id, is_no_add, is_med_action))
    if str(transform_func_id) == 'nan':
      transform_func_id = None
    fid_info = {'fid':fid, 'category':category, 'is_no_add':is_no_add,
          'data_type':data_type}
    if is_med_action and fid != 'vent':
      futures.append(self.populate_medaction_features(ctxt, mapping, fid, transform_func_id, data_type, fid_info))
    elif is_med_action and fid == 'vent':
      futures.append(self.populate_vent(ctxt, mapping, fid, transform_func_id, data_type, fid_info))
    else:
      futures.append(self.populate_stateless_features(ctxt, mapping, fid, transform_func_id, data_type, fid_info, is_no_add))
    return futures

  def get_feature_sql_query(self, log, mapping, fid_info, orderby=None):
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
    if 'T' in fid_info['category'] and self.min_tsp is not None:
      where_clause = self.add_min_tsp(where_clause, select_clause)
    sql = "SELECT %s FROM %s %s" % (select_clause, dbtable, where_clause)
    if orderby:
      sql += " order by " + orderby
    if self.plan:
      sql += " limit 100"
    log.info("sql: %s" % sql)
    return sql

  def add_min_tsp(self, where_clause, select_clause):
    tsp=self.get_tsp_name(select_clause)
    min_tsp_sql = ''' {conjunctive} "{tsp}"::timestamptz > '{min_tsp}'::timestamptz'''.format(\
      conjunctive='and' if 'where' in where_clause.lower() else 'where', tsp=tsp
      , min_tsp=self.min_tsp) if tsp else ''
    if 'where' in where_clause.lower():
      where_clause = 'WHERE (' + where_clause.strip()[5:] + ')'
    return where_clause + min_tsp_sql

  def get_tsp_name(self, select_clause):
    if 'TimeActionTaken' in select_clause:
      return 'TimeActionTaken'
    elif 'RESULT_TIME' in select_clause:
      return 'RESULT_TIME'
    elif 'TimeTaken' in select_clause:
      return 'TimeTaken'
    elif 'effective_time' in select_clause:
      return 'effective_time'
    elif 'ORDER_TIME' in select_clause:
      return 'ORDER_TIME'
    elif 'HOSP_DISCH_TIME' in select_clause:
      return 'HOSP_DISCH_TIME'
    elif 'PLACEMENT_INSTANT' in select_clause:
      return 'PLACEMENT_INSTANT'
    elif 'firstdocumented' in select_clause:
      return 'firstdocumented'
    elif 'SPEC_NOTE_TIME_DTTM' in select_clause:
      return 'SPEC_NOTE_TIME_DTTM'
    elif 'PROC_START_TIME' in select_clause:
      return 'PROC_START_TIME'


  async def populate_medaction_features(self, ctxt, mapping, fid, transform_func_id, data_type, fid_info):
    # process medication action input for HC_EPIC
    # order by csn_id, medication id, and timeActionTaken
    log = ctxt.log
    orderby = '"CSN_ID", "MEDICATION_ID", "TimeActionTaken"'
    sql = self.get_feature_sql_query(log, mapping, fid_info, orderby=orderby)
    if self.plan:
      log.info("run plan query: {}".format(sql))
      await self.run_plan_query(ctxt, sql, fid)
    else:
      extracted_rows = 0
      loaded_rows = 0
      start = timeit.default_timer()
      log.info("run extract query: {}".format(sql))
      cur_enc_id = None
      cur_med_id = None
      cur_med_events = []
      async with ctxt.db_pool.acquire() as conn:
        async with conn.transaction():
          async for row in conn.cursor(sql):
            extracted_rows += 1
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
                    loaded_rows += await self.process_med_events(log, cur_enc_id, \
                      cur_med_id, cur_med_events, fid_info, \
                      mapping, conn)
                cur_enc_id = enc_id
                cur_med_id = med_id
                cur_med_events = []
              # print "cur events temp:", cur_med_events
              cur_med_events.append(row)
              # print("cur events:", cur_med_events)
        if cur_enc_id is not None and cur_med_id is not None:
          # process  last cur_med_events
          len_of_events = len(cur_med_events)
          if len_of_events > 0:
            loaded_rows += await self.process_med_events(log, cur_enc_id, cur_med_id, cur_med_events, fid_info, mapping, conn)
            if loaded_rows > 0 and loaded_rows % 5000 == 0:
              log.info('loaded {rows} rows for fid {fid}'.format(rows=loaded_rows, fid=fid))
      log_time(log, fid, start, extracted_rows, loaded_rows)
      return mapping


  async def process_med_events(self, log, enc_id, med_id, med_events,
               fid_info, mapping, conn):
    nrows = 0
    med_route = med_events[0]['MedRoute']
    med_name = med_events[0]['display_name']
    # log.debug("\nentries from med_id %s, med_route %s:" \
    #   % (med_id, med_route))
    # for row in med_events:
    #   log.debug(row)
    fid = fid_info['fid']
    # log.debug("transformed entries:")
    # log.debug("transform function: %s" % mapping['transform_func_id'])
    dose_intakes = transform.transform(fid, mapping['transform_func_id'], \
      med_events, fid_info['data_type'], log)
    if dose_intakes is not None and len(dose_intakes) > 0:
      for intake in dose_intakes:
        if intake is None:
          log.warn("transform function returns None!")
        else:
          # log.debug(intake)
          # dose intake are T category
          tsp = intake[0]
          volume = str(intake[1])
          confidence = intake[2]
          if fid_info['is_no_add']:
            await load_row.upsert_t(conn, [enc_id, tsp, str(fid), str(volume), confidence], dataset_id = self.dataset_id)
          else:
            await load_row.add_t(conn, [enc_id, tsp, str(fid), str(volume), confidence], dataset_id = self.dataset_id)
          nrows += 1
    return nrows

  async def run_plan_query(self, ctxt, sql, fid):
    rows = 0
    start = timeit.default_timer()
    async with ctxt.db_pool.acquire() as conn:
      async with conn.transaction():
        async for row in conn.cursor(sql):
          rows += 1
    log_time(ctxt.log, fid, start, rows)



  async def populate_stateless_features(self, ctxt, mapping, fid, transform_func_id, data_type, fid_info, is_no_add, num_fetch=10000):
    # process features unrelated to med actions
    log = ctxt.log
    category = fid_info['category']
    sql = self.get_feature_sql_query(log, mapping, fid_info)
    if self.plan:
      log.info("run plan query: {}".format(sql))
      await self.run_plan_query(ctxt, sql, fid)
    else:
      extracted_rows = 0
      loaded_rows = 0
      start = timeit.default_timer()
      log.info("run extract query: {}".format(sql))
      pat_id_based = 'pat_id' in mapping['select_cols']
      async with ctxt.db_pool.acquire() as conn:
        async with conn.transaction():
          async for row in conn.cursor(sql):
            extracted_rows += 1
            if extracted_rows % num_fetch == 0:
              log.info('extracted {rows} rows for fid {fid}'.format(rows=extracted_rows, fid=fid))
            # transform & loading
            enc_ids = None
            if pat_id_based:
                if str(row['pat_id']) in self.pat_id_to_enc_ids:
                  enc_ids = self.pat_id_to_enc_ids[str(row['pat_id'])]
            elif str(row['visit_id']) in self.visit_id_to_enc_id:
                enc_ids = [self.visit_id_to_enc_id[str(row['visit_id'])]]
            if enc_ids:
              for enc_id in enc_ids:
                transformed = transform.transform(fid, transform_func_id, row, data_type, log)
                # if transformed is not None:
                #   loading_list.append((enc_id, row, transformed))
                # loading
                # await cdm_conn.execute("LOCK cdm_twf IN EXCLUSIVE MODE")
                # for loading in loading_list:
                if transformed is not None:
                  loaded_rows += await self.load_cdm(fid, category, enc_id, row, transformed, conn, is_no_add)
                  if loaded_rows > 0 and loaded_rows % num_fetch == 0:
                    log.info('loaded {rows} rows for fid {fid}'.format(rows=loaded_rows, fid=fid))
            else:
              log.warn("populate_stateless_features {}: Unknown patient information".format(fid))
            # attempts = 0
            # while True:
            #   try:
            #     async with ctxt.db_pool.acquire() as cdm_conn:
            #       async with cdm_conn.transaction():
            #         # await cdm_conn.execute("LOCK cdm_twf IN EXCLUSIVE MODE")
            #         for loading in loading_list:
            #           loaded_rows += await self.load_cdm(fid, category, loading[0], loading[1], loading[2], cdm_conn, is_no_add)
            #         if loaded_rows > 0 and loaded_rows % num_fetch == 0:
            #           log.info('loaded {rows} rows for fid {fid}'.format(rows=loaded_rows, fid=fid))
            #     break
            #   except Exception as e:
            #     attempts += 1
            #     log.warn("PSQL Error %s %s" % (fid, e))
            #     log.info("Transaction retry attempts: {} times for {}".format(attempts, fid))
            #     time.sleep(PSQL_WAIT_IN_SECS * (attempts if attempts < TRANSACTION_RETRY else TRANSACTION_RETRY))
            #     if attempts == TRANSACTION_RETRY:
            #       log.error("Transaction retry failed {}".format(fid))
            #     continue
            # rows = await cur.fetch(num_fetch)
      log_time(log, fid, start, extracted_rows, loaded_rows)
      return mapping

  async def populate_vent(self, ctxt, mapping, fid, transform_func_id, data_type, fid_info, num_fetch=1000):
    orderby = " icustay_id, realtime"
    log = ctxt.log
    sql = self.get_feature_sql_query(log, mapping, fid_info, orderby=orderby)
    if self.plan:
      log.info("run plan query: {}".format(sql))
      await self.run_plan_query(ctxt, sql, fid)
    else:
      extracted_rows = 0
      loaded_rows = 0
      start = timeit.default_timer()
      log.info("run extract query: {}".format(sql))
      cur_enc_id = None
      cur_vent_events = []
      futures = []
      async with ctxt.db_pool.acquire() as conn:
        async with conn.transaction():
          async for row in conn.cursor(sql):
            extracted_rows += 1
            if str(row['visit_id']) in self.visit_id_to_enc_id:
              enc_id = self.visit_id_to_enc_id[str(row['visit_id'])]
              if enc_id:
                if cur_enc_id is None or cur_enc_id != enc_id:
                  # new enc_id, med_id pair
                  if cur_enc_id:
                    # process cur_med_events
                    len_of_events = len(cur_vent_events)
                    if len_of_events > 0:
                      loaded_rows += await self.process_vent_events(cur_enc_id, \
                        cur_vent_events, fid_info, mapping, conn)
                  cur_enc_id = enc_id
                  cur_vent_events = []
                # print "cur events temp:", cur_med_events
                cur_vent_events.append(row)
                # print "cur events:", cur_med_events
            if cur_enc_id and len_of_events > 0:
              loaded_rows += await self.process_vent_events(cur_enc_id, cur_vent_events, \
              fid_info, mapping, cdm)
            if loaded_rows > 0 and loaded_rows % num_fetch == 0:
              log.info('loaded {rows} rows for fid {fid}'.format(rows=loaded_rows, fid=fid))
      log_time(log, fid, start, extracted_rows, loaded_rows)
      return mapping

  async def process_vent_events(self, log, enc_id, vent_events, fid_info, mapping, conn):
    log.debug("\nentries from enc_id %s:" % enc_id)
    row_cnt = 0
    for row in vent_events:
      log.debug(row)
    fid = fid_info['fid']
    log.debug("transformed entries:")
    log.debug("transform function: %s" % mapping['transform_func_id'])
    vent_results = transform.transform(fid, mapping['transform_func_id'], \
      vent_events, fid_info['data_type'], log)
    futures = []
    if vent_results is not None and len(vent_results) > 0:
      for result in vent_results:
        if result is None:
          log.warn("transform function returns None!")
        else:
          log.debug(result)
          # dose result are T category
          tsp = result[0]
          on_off = str(result[1])
          confidence = result[2]

          if fid_info['is_no_add']:
            await load_row.upsert_t(conn, [enc_id, tsp, fid, on_off, confidence], dataset_id = self.dataset_id)
          else:
            await load_row.add_t(conn, [enc_id, tsp, fid, on_off, confidence], dataset_id = self.dataset_id)
          row_cnt += 1
    return row_cnt

  async def load_cdm(self, fid, category, enc_id, row, results, conn, is_no_add):
    rows = 0
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
          await load_row.upsert_s(conn, [enc_id, fid, str(value), confidence], dataset_id = self.dataset_id)
        else:
          await load_row.add_s(conn, [enc_id, fid, str(value), confidence], dataset_id = self.dataset_id)
      elif category == 'T' or category == 'TWF':
        if tsp is None:
          if len(row) >= 4:
            tsp = row[2]
          else:
            tsp = row[1]
        if tsp is not None:
          if is_no_add:
            await load_row.upsert_t(conn, [enc_id, tsp, str(fid), str(value), confidence], dataset_id = self.dataset_id)
          else:
            await load_row.add_t(conn, [enc_id, tsp, str(fid), str(value), confidence], dataset_id = self.dataset_id)
      # elif category == 'TWF':
      #   if tsp is None:
      #     if len(row) >= 4:
      #       tsp = row[2]
      #     else:
      #       tsp = row[1]
      #   if is_no_add:
      #     await load_row.upsert_twf(conn, [enc_id, tsp, fid, value, confidence], dataset_id = self.dataset_id)
      #   else:
      #     await load_row.add_twf(conn, [enc_id, tsp, fid, value, confidence], dataset_id = self.dataset_id)
      rows += 1
    return rows


  async def run_fillin(self, ctxt, *args):
    if self.job.get('fillin', False):
      log = ctxt.log
      log.info("start fillin pipeline")
      # we run the optimized fillin in one run, e.g., update set all columns
      fillin_sql = '''
      WITH twf_fids as (
        select array_agg(fid)::text[] as arr from cdm_feature where dataset_id = {dataset_id} and is_measured and category = 'TWF'
      )
      SELECT * from load_cdm_twf_from_cdm_t(
            (select arr from twf_fids),
            'cdm_twf'::text, {dataset_id}
      )
      ;
      WITH twf_fids as (
        select array_agg(fid)::text[] as arr from cdm_feature where dataset_id = {dataset_id} and is_measured and category = 'TWF'
      )
      SELECT * from last_value_in_window(
        (select arr from twf_fids), 'cdm_twf'::text, {dataset_id})
      '''.format(dataset_id=self.dataset_id)
      log.info("fillin sql: {}".format(fillin_sql))
      async with ctxt.db_pool.acquire() as conn:
        result = await conn.execute(fillin_sql)
        log.info(result)
      log.info("fillin completed")
    else:
      log.info("fillin skipped")

  async def run_derive(self, ctxt):
    if self.job.get('derive', False):
      fid = self.job.get('derive').get('fid', None)
      mode = self.job.get('derive').get('mode', None)
      async with ctxt.db_pool.acquire() as conn:
        await self.query_cdm_feature_dict(conn)
        await derive_main(log, conn, self.cdm_feature_dict, dataset_id = self.dataset_id, fid = fid, mode=mode)
      self.log.info("derive completed")
    else:
      self.log.info("derive skipped")