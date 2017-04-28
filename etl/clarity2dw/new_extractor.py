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
    ctxt.log.info(reset_sql)
    ctxt.log.debug("ETL init sql: " + reset_sql)
    result = await conn.execute(reset_sql)
    ctxt.log.info("ETL Init: " + result)
    return None

  async def populate_patients(self, ctxt, _):
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

  async def transform_init(self, ctxt, _):
    async with ctxt.db_pool.acquire() as conn:
      ctxt.log.info("Using Feature Mapping:")
      pat_mappings = await self.get_pat_mapping(conn)
      self.visit_id_to_enc_id = pat_mappings['visit_id_to_enc_id']
      self.pat_id_to_enc_ids = pat_mappings['pat_id_to_enc_ids']
      ctxt.log.info("loaded feature and pat mapping")
      return pat_mappings

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
    l = len(mapping_list)
    # Try divide by half
    transform_tasks = self.partition(mapping_list, 2)
    return transform_tasks

  def partition(self, lst, n, random_shuffle=False):
    # if random_shuffle:
    #   random.shuffle(lst)
    # division = len(lst) / n
    # return [lst[round(division * i):round(division * (i + 1))] for i in range(n)]
    return [ [lst[0]]]

  async def run_transform_task(self, ctxt, pat_mappings, task):
    self.visit_id_to_enc_id = pat_mappings['visit_id_to_enc_id']
    self.pat_id_to_enc_ids = pat_mappings['pat_id_to_enc_ids']
    futures = []
    for mapping_row in task:
      async with ctxt.db_pool.acquire() as conn:
        await self.query_cdm_feature_dict(conn)
      futures.extend(self.transform_feature_mapping_row(ctxt, conn, mapping_row))
      ctxt.log.info("run_transform_task futures:")
    ctxt.log.info(futures)
    done, _ = await asyncio.wait(futures)
    for future in done:
      ctxt.log.info("run_transform_task completed: {}".format(future.result()))
    return None

  def transform_feature_mapping_row(self, ctxt, conn, mapping_row):
    log = ctxt.log
    log.info("run transform {}".format(mapping_row['fid(s)']))
    log.debug(mapping_row)
    fids = mapping_row['fid(s)']
    fids = [fid.strip() for fid in fids.split(',')] if ',' in fids else [fids]
    for fid in fids:
      if not self.cdm_feature_dict.get(fid, False):
        log.error("feature %s is not in cdm_feature" % fid)
        raise(ValueError("feature %s is not in cdm_feature" % fid))
    # get transform function
    transform_func_id = mapping_row['transform_func_id']
    futures = []
    if "." in str(transform_func_id): # if custom function
      i = len(transform_func_id) - transform_func_id[::-1].index('.')
      package = transform_func_id[:(i-1)]
      transform_func_id = transform_func_id[i:]
      log.info("fid: %s using package: %s and transform_func_id: %s" % (",".join(fids), package, transform_func_id))
      module = importlib.import_module(package)
      func = getattr(module, transform_func_id)
      futures.append(func(conn, self.dataset_id, log,self.plan))
    else: #if standard function
      # For now, use the fact that only custom functions are many to one.
      for fid in fids:
        mapping_row.update({'fid': fid})
        futures.extend(self.populate_feature_to_cdm(ctxt, mapping_row, conn, self.cdm_feature_dict[fid]))
    log.info("print futures")
    log.info(futures)
    return futures

  def populate_feature_to_cdm(self, ctxt, mapping, conn, cdm_feature_attributes):
    data_type = cdm_feature_attributes['data_type']
    log = ctxt.log
    log.debug(mapping)
    fid = mapping['fid']
    log.info('importing feature value fid %s' % fid)
    log.debug(mapping['transform_func_id'])
    transform_func_id = mapping['transform_func_id']
    if str(transform_func_id) == 'nan':
      transform_func_id = None
    if transform_func_id:
      log.info("transform func: %s" % transform_func_id)
    category = cdm_feature_attributes['category']
    is_no_add = bool(mapping['is_no_add'] == "yes")
    is_med_action = bool(mapping['is_med_action'] == "yes")
    log.info("is_no_add: %s, is_med_action: %s" \
      % (is_no_add, is_med_action))
    fid_info = {'fid':fid, 'category':category, 'is_no_add':is_no_add,
          'data_type':data_type}
    if is_med_action and fid != 'vent':
      return self.populate_medaction_features(ctxt, conn, mapping, fid, transform_func_id, data_type, fid_info)
    elif is_med_action and fid == 'vent':
      return self.populate_vent(ctxt, conn, mapping, fid, transform_func_id, data_type, fid_info)
    else:
      return self.populate_non_medaction_features(ctxt, conn, mapping, fid, transform_func_id, data_type, category, is_no_add)

  def get_feature_sql_query(self, log, mapping, orderby=None):
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
    log.info("sql: %s" % sql)
    return sql

  async def populate_medaction_features(self, log, conn, mapping, fid, transform_func_id, data_type, fid_info):
    # process medication action input for HC_EPIC
    # order by csn_id, medication id, and timeActionTaken
    orderby = '"CSN_ID", "MEDICATION_ID", "TimeActionTaken"'
    sql = self.get_feature_sql_query(log, mapping, orderby=orderby)
    if self.plan:
      async with conn.transaction():
        async for row in conn.cursor(sql):
          log.debug(row)
    else:
      cur_enc_id = None
      cur_med_id = None
      cur_med_events = []
      futures = []
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
                  futures += self.process_med_events(cur_enc_id, \
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
            futures += self.process_med_events(ctxt.log, cur_enc_id, cur_med_id,
                        cur_med_events, fid_info, mapping,
                        conn)
      await asyncio.wait(futures)

  async def process_med_events(self, log, enc_id, med_id, med_events,
               fid_info, mapping, conn):
    med_route = med_events[0]['MedRoute']
    med_name = med_events[0]['display_name']
    log.debug("\nentries from med_id %s, med_route %s:" \
      % (med_id, med_route))
    for row in med_events:
      log.debug(row)
    fid = fid_info['fid']
    log.debug("transformed entries:")
    log.debug("transform function: %s" % mapping['transform_func_id'])
    dose_intakes = transform.transform(fid, mapping['transform_func_id'], \
      med_events, fid_info['data_type'], log)
    futures = []
    if dose_intakes is not None and len(dose_intakes) > 0:
      for intake in dose_intakes:
        if intake is None:
          log.warn("transform function returns None!")
        else:
          log.debug(intake)
          # dose intake are T category
          tsp = intake[0]
          volume = str(intake[1])
          confidence = intake[2]

          if fid_info['is_no_add']:
            futures.append(load_row.upsert_t(conn, [enc_id, tsp, str(fid), str(volume), confidence], dataset_id = self.dataset_id))
          else:
            futures.append(load_row.add_t(conn, [enc_id, tsp, str(fid), str(volume), confidence], dataset_id = self.dataset_id))
    return futures

  async def populate_vent(self, log, conn, mapping, fid, transform_func_id, data_type, fid_info):
    line = 0
    orderby = " icustay_id, realtime"
    sql = self.get_feature_sql_query(log, mapping, orderby=orderby)
    if self.plan:
      async with conn.transaction():
        async for row in conn.cursor(sql):
          log.debug(row)
    else:
      cur_enc_id = None
      cur_vent_events = []
      futures = []
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
                    futures += self.process_vent_events(cur_enc_id, \
                      cur_vent_events, fid_info, mapping, conn)
                cur_enc_id = enc_id
                cur_vent_events = []
              # print "cur events temp:", cur_med_events
              cur_vent_events.append(row)
              # print "cur events:", cur_med_events
        if cur_enc_id and len_of_events > 0:
          line += len_of_events
        futures += self.process_vent_events(cur_enc_id, cur_vent_events, \
          fid_info, mapping, cdm)
      await asyncio.wait(futures)
    return line

  async def run_plan_query(self, ctxt, conn, sql, fid):
    rows = 0
    start = timeit.default_timer()
    async with ctxt.db_pool.acquire() as conn:
      async with conn.transaction():
        async for row in conn.cursor(sql):
          rows += 1
    self.log_time(ctxt.log, fid, start, rows)

  def log_time(self, log, fid, start, rows):
    log.info('performed {} rows(s)'.format(rows))
    duration = timeit.default_timer() - start
    if rows == 0:
      log.warn(\
        'STATS: Zero row found in dblink for fid %s %s s' \
          % (fid, duration))
    else:
      log.info(\
        'STATS: %s valid rows found in dblink for fid %s %s s' \
          % (rows, fid, duration))

  def populate_non_medaction_features(self, ctxt, conn, mapping, fid, transform_func_id, data_type, category, is_no_add):
    # process features unrelated to med actions
    async def _run_etl(ctxt, conn, mapping, fid, transform_func_id, data_type, category, is_no_add):
      rows = 0
      start = timeit.default_timer()
      pat_id_based = 'pat_id' in mapping['select_cols']
      async with conn.transaction():
        async for row in conn.cursor(sql):
          if pat_id_based:
            if str(row['pat_id']) in self.pat_id_to_enc_ids:
              enc_ids = self.pat_id_to_enc_ids[str(row['pat_id'])]
              for enc_id in enc_ids:
                result = transform.transform(fid, \
                  transform_func_id, row, data_type, log)
                if result is not None:
                  rows += 1
                  await self.save_result_to_cdm(fid, category, enc_id, \
                    row, result, conn, is_no_add)
          elif str(row['visit_id']) in self.visit_id_to_enc_id:
            enc_id = self.visit_id_to_enc_id[str(row['visit_id'])]
            if enc_id:
              # transform return a result containing both value and
              # confidence flag
              result = transform.transform(fid, transform_func_id, \
                row, data_type, log)
              # print row, result
              if result is not None:
                rows += 1
                await self.save_result_to_cdm(fid, category, enc_id, \
                  row, result, conn, is_no_add)
        self.log_time(log, fid, start, rows)

    log = ctxt.log
    sql = self.get_feature_sql_query(log, mapping)
    futures = []
    if self.plan:
      futures.append(self.run_plan_query(ctxt, conn, sql, fid))
    else:
      futures.append(_run_etl(ctxt, conn, mapping, fid, transform_func_id, data_type, category, is_no_add))
    return futures

  async def process_vent_events(self, log, enc_id, vent_events, fid_info, mapping, conn):
    log.debug("\nentries from enc_id %s:" % enc_id)
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
            futures.append(load_row.upsert_t(conn, [enc_id, tsp, fid, on_off, confidence], dataset_id = self.dataset_id))
          else:
            futures.append(load_row.add_t(conn, [enc_id, tsp, fid, on_off, confidence], dataset_id = self.dataset_id))
      await asyncio.wait(futures)

  async def save_result_to_cdm(self, fid, category, enc_id, row, results, conn, \
    is_no_add):
    if not isinstance(results[0], list):
      results = [results]
    futures = []
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
      elif category == 'T':
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
      elif category == 'TWF':
        if tsp is None:
          if len(row) >= 4:
            tsp = row[2]
          else:
            tsp = row[1]
        if is_no_add:
          await load_row.upsert_twf(conn, [enc_id, tsp, fid, value, confidence], dataset_id = self.dataset_id)
        else:
          await load_row.add_twf(conn, [enc_id, tsp, fid, value, confidence], dataset_id = self.dataset_id)
