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
import time
import importlib
import concurrent.futures
import asyncio
import asyncpg
import logging
from etl.core.config import Config
import sys
import random
import uvloop


TRANSACTION_RETRY = 10
PSQL_WAIT_IN_SECS = 5

def log_time(log, name, start, extracted, loaded):
  duration = time.time() - start
  if extracted == 0:
    msg = '%s STATS: Zero row extraced, ' % name
  else:
    msg = '%s STATS: %s valid rows extracted, ' \
        % (name, extracted)
  if loaded == 0:
    msg += 'Zero row loaded in CDM, duration %s s' % duration
  else:
    msg += '%s valid rows loaded in CDM, duration %s s' % (loaded, duration)
  log.info(msg)

class Extractor:
  def __init__(self, job):
    self.job = job
    self.dataset_id = job['extractor'].get('dataset_id')
    self.plan = job['plan']
    CONF = os.path.dirname(os.path.abspath(__file__))
    CONF = os.path.join(CONF, 'conf')
    feature_mapping_csv = os.path.join(CONF, 'feature_mapping.csv')
    self.feature_mapping = pd.read_csv(feature_mapping_csv)


  async def extract_init(self, ctxt):
    ctxt.log.info("start extract_init task")
    async with ctxt.db_pool.acquire() as conn:
      await self.reset_dataset(conn, ctxt)
      ctxt.log.info("completed extract_init task")
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

  async def vacuum_analyze_dataset(self, ctxt, *args):
    if self.job.get('fillin', False) and self.job.get('fillin').get('vacuum', False):
      ctxt.log.info("start vacuum_analyze task")
      async with ctxt.db_pool.acquire() as conn:
        vacuum_sql = [
          'vacuum analyze cdm_s;',
          'vacuum analyze cdm_t;',
          'vacuum analyze cdm_twf;',
          'vacuum analyze criteria_meas;',
        ]
        for sql in vacuum_sql:
          ctxt.log.info(sql)
          result = await conn.execute(sql)
          ctxt.log.info(result)
        ctxt.log.info("completed vacuum_analyze task")
    else:
      ctxt.log.info("skipped vacuum")
      return None

  async def populate_patients(self, ctxt, _):
    if self.job.get('transform', False):
      self.min_tsp = self.job.get('transform').get('min_tsp')
      if self.job.get('transform').get('populate_patients', False):
        async with ctxt.db_pool.acquire() as conn:
          populate_patients_job = self.job['transform']['populate_patients']
          limit = populate_patients_job.get('limit', None)
          sql = '''
          insert into pat_enc (dataset_id, visit_id, pat_id)
          SELECT %(dataset_id)s, demo."CSN_ID" visit_id, demo."pat_id"
          FROM "Demographics" demo left join pat_enc pe on demo."CSN_ID"::text = pe.visit_id::text and pe.dataset_id = %(dataset_id)s
          where pe.visit_id is null %(min_tsp)s %(limit)s
          ''' % {'dataset_id': self.dataset_id,
                  'min_tsp': ''' and "HOSP_ADMSN_TIME" >= '{}'::timestamptz'''.format(self.min_tsp) if self.min_tsp else '',
                  'limit': 'limit {}'.format(limit) if limit else ''}
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
    shuffle = False
    if self.job.get('transform', False):
      nprocs = int(self.job.get('transform').get('populate_measured_features').get('nprocs', nprocs))
      shuffle = self.job.get('transform').get('populate_measured_features').get('shuffle', False)
    transform_tasks = self.partition(mapping_list,  nprocs, random_shuffle=shuffle)
    return transform_tasks

  def partition(self, lst, n, random_shuffle=False):
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
    #   if item['fid(s)'] == 'cms_antibiotics, crystalloid_fluid, vasopressors_dose':
    #     lst_cus = item
    # return [ [ lst_cus, lst[0], lst[2], lst_hematocrit, lst_med, lst_bands, lst_spo2, lst_heart_rate], [lst[1], lst[3], lst_approx, lst_vent, lst_co2]]
    # # return [ [lst[0], lst[2]], [lst_med, lst_bands]]

  async def run_transform_task(self, ctxt, pat_mappings, task):
    if self.job.get('transform', False):
      if self.job.get('transform').get('populate_measured_features', False):
        self.visit_id_to_enc_id = pat_mappings['visit_id_to_enc_id']
        self.pat_id_to_enc_ids = pat_mappings['pat_id_to_enc_ids']
        self.min_tsp = self.job.get('transform').get('min_tsp')
        futures = []
        async with ctxt.db_pool.acquire() as conn:
            await self.query_cdm_feature_dict(conn)
        for mapping_row in task:
          futures.extend(self.run_feature_mapping_row(ctxt, mapping_row))
        ctxt.log.info("run_transform_task futures: {}".format(', '.join([m['fid(s)'] for m in task])))
        # done, _ = await asyncio.wait(futures)
        # for future in done:
        #   ctxt.log.info("run_transform_task completed: {}".format(future.result()))
        while len(futures) > 0:
          done, pending = await asyncio.wait(futures, return_when = asyncio.FIRST_COMPLETED)
          for future in done:
            ctxt.log.info("run_transform_task completed: {}".format(future.result()))
          futures = pending
          ctxt.log.info("remaining transform_tasks: {}".format(len(futures)))
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
        print(self.cdm_feature_dict)
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
    try:
      conn_acquired = False
      async with ctxt.db_pool.acquire() as conn:
        conn_acquired = True
        await func(conn, dataset_id, log, plan)
      if not conn_acquired:
        log.error("Error: connection is not acquired for {}".format(transform_func_id))
    except Exception as e:
      log.warn("Error: custom function error %s %s" % (transform_func_id, e))

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
    category = fid_info['category']
    is_no_add = fid_info['is_no_add']
    sql = self.get_feature_sql_query(log, mapping, fid_info, orderby=orderby)
    if self.plan:
      log.info("run plan query: {}".format(sql))
      await self.run_plan_query(ctxt, sql, fid)
    else:
      conn_acquired = False
      async with ctxt.db_pool.acquire() as conn:
        start = time.time()
        extracted_rows = 0
        loaded_rows = 0
        log.info("run extract query: {}".format(sql))
        cur_enc_id = None
        cur_med_id = None
        cur_med_events = []
        conn_acquired = True
        rows_to_load = []
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
                    rows_to_load += self.process_med_events(log, cur_enc_id, \
                      cur_med_id, cur_med_events, fid_info, \
                      mapping)
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
              rows_to_load += self.process_med_events(log, cur_enc_id, cur_med_id, cur_med_events, fid_info, mapping)
        if rows_to_load:
          log.info("{} rows are going to load".format(len(rows_to_load)))
          await self.load_cdm(category, rows_to_load, conn, is_no_add, log=log)
          loaded_rows = len(rows_to_load)
        log_time(log, fid, start, extracted_rows, loaded_rows)
      if not conn_acquired:
        log.error("Error: connection is not acquireed {}".format(fid))
      return mapping


  def process_med_events(self, log, enc_id, med_id, med_events,
               fid_info, mapping):
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
    rows_to_load = []
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
          row_to_load = [enc_id, tsp, fid, volume, confidence]
          rows_to_load.append(row_to_load)
    return rows_to_load

  async def run_plan_query(self, ctxt, sql, fid):
    rows = 0
    start = time.time()
    async with ctxt.db_pool.acquire() as conn:
      async with conn.transaction():
        async for row in conn.cursor(sql):
          rows += 1
    log_time(ctxt.log, fid, start, rows)



  async def populate_stateless_features(self, ctxt, mapping, fid, transform_func_id, data_type, fid_info, is_no_add, num_fetch=1000):
    # process features unrelated to med actions
    log = ctxt.log
    category = fid_info['category']
    sql = self.get_feature_sql_query(log, mapping, fid_info)
    if self.plan:
      log.info("run plan query: {}".format(sql))
      await self.run_plan_query(ctxt, sql, fid)
    else:
      conn_acquired = False
      async with ctxt.db_pool.acquire() as conn:
        extracted_rows = 0
        loaded_rows = 0
        log.info("run extract query: {}".format(sql))
        pat_id_based = 'pat_id' in mapping['select_cols']
        start = time.time()
        conn_acquired = True
        rows_to_load = []
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
                transformed_list = transform.transform(fid, transform_func_id, row, data_type, log)
                if transformed_list:
                  if not isinstance(transformed_list[0], list):
                    transformed_list = [transformed_list]
                  for transformed in transformed_list:
                    if category == 'S':
                      row_to_load = [enc_id, fid, transformed[-2], transformed[-1]]
                    elif category == 'T' or category == 'TWF':
                      if len(transformed) == 3:
                        tsp = transformed[0]
                      else:
                        tsp = row[2] if len(row) >= 4 else row[1]
                      if tsp is not None:
                        try:
                          row_to_load = [enc_id, tsp, fid, str(transformed[-2]), transformed[-1]]
                        except Exception as e:
                          log.error(transformed)
                          log.error(row)
                          raise(e)
                    rows_to_load.append(row_to_load)
          if rows_to_load:
              log.info("{} rows are going to load".format(len(rows_to_load)))
              await self.load_cdm(category, rows_to_load, conn, is_no_add, log=log)
          loaded_rows = len(rows_to_load)
        log_time(log, fid, start, extracted_rows, loaded_rows)
      if not conn_acquired:
        log.error("Error: connection is not acquired for {}".format(fid))
      return mapping

  async def populate_vent(self, ctxt, mapping, fid, transform_func_id, data_type, fid_info, num_fetch=1000):
    orderby = " icustay_id, realtime"
    log = ctxt.log
    sql = self.get_feature_sql_query(log, mapping, fid_info, orderby=orderby)
    if self.plan:
      log.info("run plan query: {}".format(sql))
      await self.run_plan_query(ctxt, sql, fid)
    else:
      conn_acquired = False
      async with ctxt.db_pool.acquire() as conn:
        conn_acquired = True
        extracted_rows = 0
        loaded_rows = 0
        log.info("run extract query: {}".format(sql))
        cur_enc_id = None
        cur_vent_events = []
        futures = []
        start = time.time()
        rows_to_load = []
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
                      rows_to_load += self.process_vent_events(cur_enc_id, \
                        cur_vent_events, fid_info, mapping)
                  cur_enc_id = enc_id
                  cur_vent_events = []
                # print "cur events temp:", cur_med_events
                cur_vent_events.append(row)
                # print "cur events:", cur_med_events
            if cur_enc_id and len_of_events > 0:
              rows_to_load += self.process_vent_events(cur_enc_id, cur_vent_events, \
              fid_info, mapping)
        if rows_to_load:
          await self.load_cdm(category, rows_to_load, conn, is_no_add, log=log)
          loaded_rows = len(rows_to_load)
        log_time(log, fid, start, extracted_rows, loaded_rows)
      if not conn_acquired:
        log.error("Error: connection is not acquired for {}".format(fid))
      return mapping

  def process_vent_events(self, log, enc_id, vent_events, fid_info, mapping):
    log.debug("\nentries from enc_id %s:" % enc_id)
    for row in vent_events:
      log.debug(row)
    fid = fid_info['fid']
    log.debug("transformed entries:")
    log.debug("transform function: %s" % mapping['transform_func_id'])
    vent_results = transform.transform(fid, mapping['transform_func_id'], \
      vent_events, fid_info['data_type'], log)
    rows_to_load = []
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
          row_to_load = [enc_id, tsp, fid, on_off, confidence]
          rows_to_load.append(row_to_load)
    return rows_to_load

  async def load_cdm(self, category, rows, conn, is_no_add, log=None):
    if category == 'S':
      if is_no_add:
        await load_row.upsert_s(conn, rows, dataset_id = self.dataset_id, many=True, log=log)
      else:
        await load_row.add_s(conn, rows, dataset_id = self.dataset_id, many=True, log=log)
    elif category == 'T' or category == 'TWF':
      if is_no_add:
        await load_row.upsert_t(conn, rows, dataset_id = self.dataset_id, many=True, log=log)
      else:
        await load_row.add_t(conn, rows, dataset_id = self.dataset_id, many=True, log=log)


  async def run_fillin(self, ctxt, *args):
    log = ctxt.log
    if self.job.get('fillin', False):
      log.info("start fillin pipeline")
      # we run the optimized fillin in one run, e.g., update set all columns
      fillin_sql = '''
      WITH twf_fids as (
        select array_agg(fid)::text[] as arr from cdm_feature where dataset_id = {dataset_id} and is_measured and category = 'TWF'
      )
      SELECT * from load_cdm_twf_from_cdm_t(
            (select arr from twf_fids),
            'cdm_twf'::text, {dataset_id}
      );
      WITH twf_fids as (
        select array_agg(fid)::text[] as arr from cdm_feature where dataset_id = {dataset_id} and is_measured and category = 'TWF'
      )
      SELECT * from last_value_in_window(
        (select arr from twf_fids), 'cdm_twf'::text, {dataset_id});
      '''.format(dataset_id=self.dataset_id)
      async with ctxt.db_pool.acquire() as conn:
        log.info("start fillin: {}".format(fillin_sql))
        result = await conn.execute(fillin_sql)
        log.info(result)
        log.info("fillin completed")
    else:
      log.info("fillin skipped")


  async def derive_init(self, ctxt, derive_feature_addr, dataset_id):
    ctxt.log('start derive_init')
    temp_table_groups = {}
    twf_table = None
    for fid in derive_feature_addr:
      twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
      if twf_table is None and derive_feature_addr[fid]['twf_table'] is not None:
        twf_table = derive_feature_addr[fid]['twf_table']
      if twf_table_temp in temp_table_groups:
        temp_table_groups[twf_table_temp].append(fid)
      else:
        temp_table_groups[twf_table_temp] = [fid]
    create_temp_table = '''
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE UNLOGGED {table_name}
    AS {query}
    WITH NO DATA;
    '''
    async with ctxt.db_pool.acquire() as conn:
      for table_name in temp_table_groups:
        query = 'select {dataset_id} enc_id, tsp, {cols} from {twf_table} limit 1'.format(
            dataset_id='{},'.format(dataset_id) if dataset_id else '',
            cols=', '.join(['{}, {}_c'.format(fid) in temp_table_groups[table_name]]),
            twf_table=twf_table
          )
        sql = create_temp_table.format(table_name=table_name, query=query)
        ctxt.log.info("create temp table: " + sql)
        await conn.execute(sql)
    ctxt.log('derive_init completed')

  async def derive_join(self, ctxt, derive_feature_addr, dataset_id):
    ctxt.log.info("start derive_join")
    join_sql = '''
    INSERT INTO {twf_table} ({dataset_id_key} enc_id, tsp, {cols})
    SELECT cdm_twf.dataset_id, cdm_twf.enc_id, cdm_twf.tsp, {cols}
    FORM {twf_table} cdm_twf {inner_joins}
    ON CONFLICT ({dataset_id_key} enc_id, tsp) DO UPDATE SET
    {set_cols};
    '''
    temp_table_groups = {}
    twf_table = None
    dataset_id_key='{},'.format(dataset_id) if dataset_id else ''
    for fid in derive_feature_addr:
      twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
      if twf_table is None and derive_feature_addr[fid]['twf_table'] is not None:
        twf_table = derive_feature_addr[fid]['twf_table']
      if twf_table_temp in temp_table_groups:
        temp_table_groups[twf_table_temp].append(fid)
      else:
        temp_table_groups[twf_table_temp] = [fid]
    cols = ', '.join(['{}, {}_c' for fid in derive_feature_addr])
    set_cols = ', '.join(['{} = excluded.{}, {}_c = excluded.{}_c' for fid in derive_feature_addr])
    inner_joins = ['inner join {tbl} on {dataset_match} cdm_twf.enc_id = {tbl}.enc_id and cdm_twf.tsp = {tbl}.tsp'.format(tbl=table, dataset_match='cdm_twf.dataset_id = {tbl}.dataset_id'.format(tbl=table) if dataset_id is not None else '') for table in temp_table_groups]
    join_sql = join_sql.format(
        twf_table=twf_table,
        dataset_id_key=dataset_id_key,
        cols=cols,
        inner_joins=inner_joins,
        set_cols=set_cols
      )
    for table_name in temp_table_groups:
      join_sql += 'DROP TABLE {};'.format(table_name)
    ctxt.log.info(join_sql)
    ctxt.log.info("completed derive_join")

  async def run_derive(self, ctxt, fid=None, derive_feature_addr=None):
    log = ctxt.log
    if self.job.get('derive', False):
      if fid is None:
        fid = self.job.get('derive').get('fid', None)
        mode = self.job.get('derive').get('mode', None)
      async with ctxt.db_pool.acquire() as conn:
        await self.query_cdm_feature_dict(conn)
        await derive_main(log, conn, self.cdm_feature_dict, dataset_id = self.dataset_id, fid = fid, mode=mode, derive_feature_addr=derive_feature_addr)
      log.info("derive completed")
    else:
      log.info("derive skipped")

  async def offline_criteria_processing(self, ctxt, _):
    if self.job.get('load_cdm_to_criteria_meas', False):
      async with ctxt.db_pool.acquire() as conn:
        await load_table.load_cdm_to_criteria_meas(conn, self.dataset_id)
    if self.job.get('calculate_historical_criteria', False):
      async with ctxt.db_pool.acquire() as conn:
        await load_table.calculate_historical_criteria(conn)
