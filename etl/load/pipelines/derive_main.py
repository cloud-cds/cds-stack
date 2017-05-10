# derive pipeline for cdm_twf
import etl.load.primitives.tbl.derive as derive_func
import etl.load.primitives.tbl.clean_tbl as clean_tbl
from etl.load.primitives.tbl.derive import with_ds
from etl.load.primitives.tbl.derive_helper import *

async def derive_main(log, conn, cdm_feature_dict, mode=None, fid=None, dataset_id=None, derive_feature_addr=None):
  '''
  mode: "append", run derive functions beginning with @fid sequentially
  mode: "dependent", run derive functions for @fid and other features depends on @fid
  mode: None, run derive functions sequentially for all derive features
  '''
  # generate a sequence to derive
  if mode == 'append':
    append = fid
    log.info("starts from feature %s" % append)
    derive_feature_order = get_derive_seq(cdm_feature_dict)
    log.debug("derive feautre order: " + ", ".join(derive_feature_order))
    idx = derive_feature_order.index(append)
    for i in range(idx, len(derive_feature_order)):
      fid = derive_feature_order[i]
      await derive_feature(log, fid, cdm_feature_dict, conn, dataset_id=dataset_id, derive_feature_addr=derive_feature_addr)
  elif mode == 'dependent':
    dependent = fid
    if not cdm_feature_dict[fid]['is_measured']:
      log.info("update feature %s and its dependents" % dependent)
      await derive_feature(log, fid, cdm_feature_dict, conn, dataset_id=dataset_id, derive_feature_addr=derive_feature_addr)
    else:
      log.info("update feature %s's dependents" % dependent)
    derive_feature_order = get_dependent_features([dependent], cdm_feature_dict)
    for fid in derive_feature_order:
      await derive_feature(log, fid, cdm_feature_dict, conn, dataset_id=dataset_id, derive_feature_addr=derive_feature_addr)
  elif mode is None:
    if fid is None:
      log.info("derive features one by one")
      derive_feature_order = get_derive_seq(cdm_feature_dict)
      log.debug("derive feautre order: " + ", ".join(derive_feature_order))
      for fid in derive_feature_order:
        await derive_feature(log, fid, cdm_feature_dict, conn, dataset_id=dataset_id, derive_feature_addr=derive_feature_addr)
    else:
      log.info("derive feature: %s" % fid)
      await derive_feature(log, fid, cdm_feature_dict, conn, dataset_id=dataset_id, derive_feature_addr=derive_feature_addr)
  else:
    log.error("Unknown mode!")

def get_derive_seq(features=None, input_map=None):
  """
  features is a list of dictionaries
  """
  def rm_measured_dependencies(row, df_list):
    lst = map(str.lstrip, map(str.strip, row.split(',')))
    return [x for x in lst if x in df_list]

  def reduce_dependencies(dependency_lst):
    return [x for x in dependency_lst if x not in order]

  order = []

  if input_map:
    d_map = input_map
  else:
    # create the dependency map
    d_map = {f:features[f]['derive_func_input'] for f in features if not features[f]['is_measured'] and not features[f]['is_deprecated']}
  derived_features = d_map.keys()
  # clear out dependencies on measured features, they should be in CDM already
  d_map = dict((k,rm_measured_dependencies(v, derived_features)) \
    for (k,v) in d_map.items())

  while (len(d_map) != 0):
    ind =  [k for k in d_map if len(d_map[k]) == 0]
    order.extend(ind)
    d_map = dict((k,v) for (k,v) in d_map.items() if k not in order)
    d_map = dict((k, reduce_dependencies(v)) for (k, v) in d_map.items())
  return order

def get_dependent_features(feature_list, cdm_feature_dict):
  # create the dependency map
  d_map = dict((fid, cdm_feature_dict[fid]['derive_func_input']) \
      for fid in cdm_feature_dict if ((not cdm_feature_dict[fid]['is_measured']) \
      and (not cdm_feature_dict[fid]['is_deprecated'])))
  derived_features = d_map.keys()
  get_dependents = feature_list
  dependency_list = []
  first = True

  while len(get_dependents) != 0:
    if first:
      first = False
    else:
      dependency_list.append(get_dependents)
    get_dependents = [fid for fid in derived_features if \
      any(map(lambda x: x in [item.strip() for item \
        in d_map[fid].split(",")], get_dependents))]
    # get_dependents = [fid in derived_features if \
    #     any(map(lambda x: x in d_map[fid], get_dependents))]

  dependent_features = [fid for lst in dependency_list for fid in lst]
  if len(dependent_features) ==  0:
    return dependent_features
  else:
    dic = dict((fid, cdm_feature_dict[fid]['derive_func_input']) \
        for fid in cdm_feature_dict if fid in dependent_features)
    return get_derive_seq(input_map=dic)

async def derive_feature(log, fid, cdm_feature_dict, conn, dataset_id=None, derive_feature_addr=None):
  feature = cdm_feature_dict[fid]
  derive_func_id = feature['derive_func_id']
  derive_func_input = feature['derive_func_input']
  fid_category = feature['category']
  log.info("derive feature %s, function %s, inputs (%s) %s" \
  % (fid, derive_func_id, derive_func_input, 'dataset_id %s' % dataset_id if dataset_id is not None else ''))
  if fid in query_config:
    config_entry = query_config[fid]
    fid_input_items = [item.strip() for item in derive_func_input.split(',')]
    clean_sql = ''
    sql = ''
    if fid_input_items == config_entry['fid_input_items']:
      if fid_category == 'TWF':
        twf_table = derive_feature_addr[fid]['twf_table']
        twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
        if 'clean' in config_entry:
          clean_args = config_entry['clean']
          clean_sql = clean_tbl.cdm_twf_clean(fid, twf_table = twf_table_temp, dataset_id = dataset_id, **clean_args)
        if config_entry['derive_type'] == 'simple':
          sql = gen_simple_twf_query(config_entry, fid, dataset_id, derive_feature_addr, cdm_feature_dict)
        elif config_entry['derive_type'] == 'subquery':
          sql = gen_subquery_upsert_query(config_entry, fid, dataset_id, derive_feature_addr, cdm_feature_dict)
        log.debug(clean_sql + sql)
        await conn.execute(clean_sql + sql)
      elif fid_category == 'T':
        # Note they do not touch TWF table
        sql = gen_cdm_t_delete_and_insert_query(config_entry, fid, dataset_id)
        log.debug(clean_sql + sql)
        await conn.execute(clean_sql + sql)
      else:
        log.error("Invalid derive fid category: {}".format(fid_category))
    else:
      log.error("fid_input dismatch")
  else:
    log.info("Derive function is not implemented in driver, so we use legacy derive function")
    await derive_func.derive(fid, derive_func_id, derive_func_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict)
  log.info("derive feature %s end." % fid)



def gen_simple_twf_query(config_entry, fid, dataset_id, derive_feature_addr, cdm_feature_dict):
  twf_table = derive_feature_addr[fid]['twf_table']
  twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
  fid_input_items = config_entry['fid_input_items']
  select_table_joins = get_select_table_joins(fid_input_items, derive_feature_addr, cdm_feature_dict, dataset_id)
  update_expr = config_entry['fid_update_expr']
  update_expr_params = {}
  if '%(twf_table)s' in update_expr:
    update_expr_params['twf_table'] = twf_table
  if '%(with_ds_s)s' in update_expr:
    update_expr_params['with_ds_s'] = with_ds(dataset_id, table_name='cdm_s')
  if '%(with_ds_t)s' in update_expr:
    update_expr_params['with_ds_t'] = with_ds(dataset_id, table_name='cdm_t')
  if '%(with_ds_twf)s' in update_expr:
    update_expr_params['with_ds_twf'] = with_ds(dataset_id, table_name='cdm_twf')
  if update_expr_params:
    update_expr = update_expr % update_expr_params
  c_update_expr = config_entry['fid_c_update_expr']
  if '%(twf_table)s' in c_update_expr:
    c_update_expr = c_update_expr % {'twf_table': twf_table}
  sql = """
  INSERT INTO %(twf_table_temp)s (%(dataset_id_key)s enc_id, tsp, %(insert_cols)s)
  SELECT %(dataset_id_key)s enc_id, tsp, %(select_cols)s FROM
  (%(select_table_joins)s) source
  ON CONFLICT (%(dataset_id_key)s enc_id, tsp) DO UPDATE SET
  %(update_cols)s
  """ % {
    'twf_table_temp'     : twf_table_temp,
    'dataset_id_key'     : dataset_id_key(None, dataset_id),
    'select_table_joins' : select_table_joins,
    'select_cols'        : '({update_expr}) as {fid}, ({c_update_expr}) as {fid}_c'.format(\
                                  update_expr=update_expr, fid=fid, c_update_expr=c_update_expr),
    'update_cols'        : '{fid} = excluded.{fid}, {fid}_c = excluded.{fid}_c'.format(fid=fid),
    'insert_cols'        : '{fid}, {fid}_c'.format(fid=fid)
  }
  return sql

def gen_subquery_upsert_query(config_entry, fid, dataset_id, derive_feature_addr, cdm_feature_dict):
  twf_table = derive_feature_addr[fid]['twf_table']
  twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
  subquery_params = {}
  subquery_params['fid'] = fid
  fid_input_items = config_entry['fid_input_items']
  # generate twf_table from selection
  subquery_params['twf_table_join'] = '(' + \
    get_select_table_joins(fid_input_items, derive_feature_addr, cdm_feature_dict, dataset_id) \
    + ')'
  subquery_params['twf_table'] = twf_table
  subquery_params['dataset_id'] = dataset_id
  subquery_params['with_ds_twf'] = with_ds(dataset_id, table_name='cdm_twf', conjunctive=False)
  subquery_params['and_with_ds_twf'] = with_ds(dataset_id, table_name='cdm_twf', conjunctive=True)
  subquery_params['with_ds_t'] = with_ds(dataset_id, table_name='cdm_t', conjunctive=True)
  subquery_params['with_ds_ttwf'] = (' AND cdm_t.dataset_id = cdm_twf.dataset_id' if dataset_id else '') + with_ds(dataset_id, table_name='cdm_twf', conjunctive=False)
  subquery_params['dataset_id_key'] = dataset_id_key('cdm_twf', dataset_id)
  subquery = config_entry['subquery'](subquery_params)
  upsert_clause = '''
  INSERT INTO %(twf_table_temp)s (%(dataset_id_key)s enc_id, tsp,%(fid)s, %(fid)s_c)
  (
    %(subquery)s
  )
  ON CONFLICT (%(dataset_id_key)s enc_id, tsp) DO UPDATE SET
  %(fid)s = excluded.%(fid)s,
  %(fid)s_c = excluded.%(fid)s_c
  ''' % {
    'fid':fid,
    'twf_table_temp': twf_table_temp,
    'subquery': subquery,
    'dataset_id_key': 'dataset_id, ' if dataset_id is not None else ''
  }
  return upsert_clause

def gen_cdm_t_delete_and_insert_query(config_entry, fid, dataset_id):
  fid_select_expr = config_entry['fid_select_expr'] % {
    'dataset_col_block': 'dataset_id,' if dataset_id is not None else '',
    'dataset_where_block': (' and dataset_id = %s' % dataset_id) if dataset_id is not None else ''
  }
  insert_clause = """
  DELETE FROM cdm_t where fid = '%(fid)s' %(dataset_where_block)s;
  INSERT INTO cdm_t (%(dataset_col_block)s enc_id,tsp,fid,value,confidence) (%(select_expr)s);
  """ % {
    'fid':fid,
    'select_expr': fid_select_expr,
    'dataset_col_block': 'dataset_id,' if dataset_id is not None else '',
    'dataset_where_block': (' and dataset_id = %s' % dataset_id) if dataset_id is not None else ''
  }
  return insert_clause




query_config = {
  # 'bun_to_cr': {
  #   'fid_input_items'   : ['bun', 'creatinine'],
  #   'derive_type'       : 'simple',
  #   'fid_update_expr'   : 'bun/creatinine',
  #   'fid_c_update_expr' : 'creatinine_c | bun_c',
  # },
  'pao2_to_fio2': {
    'fid_input_items':  ['pao2', 'fio2'],
    'derive_type': 'simple',
    'fid_update_expr': 'pao2/fio2*100',
    'fid_c_update_expr': 'pao2_c | fio2_c'
  },
  'metabolic_acidosis': {
    'fid_input_items': ['arterial_ph', 'bicarbonate'],
    'derive_type': 'simple',
    'fid_update_expr': '(case when arterial_ph < 7.35 and bicarbonate < 22 then 1 else 0 end)',
    'fid_c_update_expr': 'arterial_ph_c | bicarbonate_c'
  },
  'mi': {
    # TODO add chest pain variable
    'fid_input_items': ['troponin'],
    'derive_type': 'simple',
    'fid_update_expr': '(case when troponin > 0.01 and troponin_c < 8 then 1 else 0 end)',
    'fid_c_update_expr': 'troponin_c',
  },
  'acute_liver_failure': {
    'fid_input_items': ['inr', 'gcs', 'liver_disease_hist'],
    'derive_type': 'simple',
    'fid_update_expr': '''(case when not enc_id in (select enc_id from cdm_s where fid='liver_disease_hist' and value::boolean=TRUE)
      and inr >= 1.5 and gcs <= 13 and inr_c < 24 and gcs_c < 24
      then 1
      else 0 end)''',
    'fid_c_update_expr': 'inr_c | gcs_c',
  },
  'hepatic_sofa': {
    'fid_input_items': ['bilirubin'],
    'derive_type': 'simple',
    'fid_update_expr': '''(CASE
                      WHEN (bilirubin > 12.0) THEN 4
                      WHEN (bilirubin >= 6.0) THEN 3
                      WHEN (bilirubin >= 2.0) THEN 2
                      WHEN (bilirubin >= 1.2) THEN 1
                      ELSE 0
                    END)''',
    'fid_c_update_expr': 'bilirubin_c',
  },
  'qsofa': {
    'fid_input_items': ['resp_rate', 'sbpm', 'gcs'],
    'derive_type': 'simple',
    'fid_update_expr': ''' (resp_rate >= 22 and based_on_popmean(resp_rate_c) = 0)::int
                    + (sbpm <= 100 and based_on_popmean(sbpm_c) = 0)::int
                    + (gcs < 13 and based_on_popmean(gcs_c) = 0)::int''',
    'fid_c_update_expr': 'coalesce(resp_rate_c,0) | coalesce(sbpm_c,0) | coalesce(gcs_c,0)',
  },
  'neurologic_sofa': {
    'fid_input_items': ['gcs'],
    'derive_type': 'simple',
    'fid_update_expr': '''(CASE
                      WHEN (gcs < 6) THEN 4
                      WHEN (gcs < 10) THEN 3
                      WHEN (gcs < 13) THEN 2
                      WHEN (gcs < 15) THEN 1
                      ELSE 0
                    END)''',
    'fid_c_update_expr': 'gcs_c',
  },
  'hematologic_sofa': {
    'fid_input_items': ['platelets'],
    'derive_type': 'simple',
    'fid_update_expr': '''(CASE
                      WHEN (platelets < 20) THEN 4
                      WHEN (platelets < 50) THEN 3
                      WHEN (platelets < 100) THEN 2
                      WHEN (platelets < 150) THEN 1
                      ELSE 0
                    END)''',
    'fid_c_update_expr': 'platelets_c',
  },
  'nbp_mean': {
    'fid_input_items': ['nbp_dias', 'nbp_sys'],
    'derive_type': 'simple',
    'fid_update_expr': 'nbp_sys/3 + nbp_dias/3*2',
    'fid_c_update_expr': 'nbp_sys_c | nbp_dias_c',
  },
  'sirs_wbc_oor': {
    'fid_input_items': ['wbc'],
    'derive_type': 'simple',
    'fid_update_expr': '(wbc < 4 OR wbc > 12)',
    'fid_c_update_expr': 'wbc_c',
  },
  'obstructive_pe_shock': {
    'fid_input_items': ['lactate', 'ddimer', 'spo2', 'heart_rate'],
    'derive_type': 'simple',
    'fid_update_expr': '''(case when (lactate > 2
                        and ddimer > 0.5
                        and spo2 < 95
                        and heart_rate > 100) then 1
                    else 0 end
                    )
                   ''',
    'fid_c_update_expr': 'lactate_c | ddimer_c | spo2_c | heart_rate_c',
  },
  'sirs_hr_oor': {
    'fid_input_items': ['heart_rate'],
    'derive_type': 'simple',
    'fid_update_expr': 'heart_rate > 90',
    'fid_c_update_expr': 'heart_rate_c',
  },
  'sirs_resp_oor': {
    'fid_input_items': ['resp_rate', 'paco2'],
    'derive_type': 'simple',
    'fid_update_expr': '(resp_rate > 20 OR paco2 <= 32)',
    'fid_c_update_expr': '''(case
                      when resp_rate > 20 and cast(1-based_on_popmean(resp_rate_c) as bool) then resp_rate_c
                      when paco2 <= 32 and cast(1-based_on_popmean(paco2_c) as bool) then paco2_c
                      ELSE
                      resp_rate_c | paco2_c
                      end)''',
  },
  'sirs_temperature_oor': {
    'fid_input_items': ['temperature'],
    'derive_type': 'simple',
    'fid_update_expr': 'temperature < 96.8 or temperature > 100.4',
    'fid_c_update_expr': 'temperature_c',
  },
  'shock_idx': {
    'fid_input_items': ['heart_rate', 'sbpm'],
    'derive_type': 'simple',
    'fid_update_expr': 'heart_rate/sbpm',
    'fid_c_update_expr': 'heart_rate_c | sbpm_c',
  },
  'acute_pancreatitis': {
    'fid_input_items': ['lipase', 'amylase'],
    'derive_type': 'simple',
    'fid_update_expr': '(lipase > 400 and amylase > 450)::int',
    'fid_c_update_expr': 'lipase_c | amylase_c',
  },
  'hypotension_raw': {
    'fid_input_items': ['sbpm', 'mapm'],
    'derive_type': 'simple',
    'fid_update_expr': '(sbpm < 90 and based_on_popmean(sbpm_c) != 1) OR (mapm < 65 and based_on_popmean(mapm_c) != 1)',
    'fid_c_update_expr': 'coalesce(sbpm_c,0) | coalesce(mapm_c,0)',
  },
  'sepsis_related_organ_failure': {
    'fid_input_items': ['inr', 'platelets', 'creatinine', 'bilirubin', 'urine_output_24hr', 'lactate', 'pao2_to_fio2', 'hypotension_intp'],
    'derive_type': 'simple',
    'fid_update_expr': '''
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
                    OR (urine_output_24hr < 500 AND
                        tsp - (SELECT min(cdm_twf_2.tsp) FROM %(twf_table)s cdm_twf_2
                           WHERE cdm_twf_2.enc_id = cdm_twf.enc_id)
                        >= interval '24 hours')
                      ''',
    'fid_c_update_expr': '''
                      coalesce(inr_c,0)
                      | coalesce(platelets_c,0) | coalesce(creatinine_c,0)
                      | coalesce(bilirubin_c,0)
                      | coalesce(lactate_c,0) | coalesce(pao2_to_fio2_c,0)
                      | coalesce(hypotension_intp_c,0)
                      | coalesce(urine_output_24hr_c,0)
                     ''',
  },
  'acute_organ_failure': {
    'fid_input_items': ['inr', 'platelets', 'creatinine', 'chronic_kidney_hist', 'bilirubin', 'liver_disease_hist', 'urine_output_24hr', 'lactate', 'pao2_to_fio2', 'chronic_pulmonary_hist', 'hypotension_intp'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
      WITH S as (SELECT %(dataset_id_key)s cdm_twf.enc_id, min(cdm_twf.tsp) min_tsp FROM %(twf_table)s cdm_twf %(sub_dataset_id_equal)s
          group by %(dataset_id_key)s cdm_twf.enc_id)
      SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
      coalesce(
        (inr > 1.5 and based_on_popmean(inr_c) != 1)
        OR (platelets < 100 and based_on_popmean(platelets_c) != 1)
        OR (lactate > 4.0 and based_on_popmean(lactate_c) != 1)
        OR hypotension_intp is TRUE
        OR (creatinine > 2 and based_on_popmean(creatinine_c) != 1 and
          coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'chronic_kidney_hist' %(with_ds_s)s limit 1)
          , False) = False)
        OR (bilirubin > 2 and based_on_popmean(bilirubin_c) != 1 and
          coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'liver_disease_hist' %(with_ds_s)s limit 1)
          , False) = False)
        OR (pao2_to_fio2 < 100 and based_on_popmean(pao2_to_fio2_c) != 1 and
          coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'chronic_pulmonary_hist' %(with_ds_s)s limit 1)
          , False) = False)
        OR (
        urine_output_24hr < 500
        AND
          tsp - S.min_tsp
          >= interval '24 hours'
        AND
          coalesce((select cast(value as boolean) from cdm_s
          where cdm_s.enc_id = cdm_twf.enc_id
            and cdm_s.fid = 'chronic_kidney_hist' %(with_ds_s)s limit 1)
          , False) = False
        ), false) as acute_organ_failure,
      coalesce(inr_c,0)
      | coalesce(platelets_c,0) | coalesce(creatinine_c,0)
      | coalesce(bilirubin_c,0)
      | coalesce(lactate_c,0) | coalesce(pao2_to_fio2_c,0)
      | coalesce(hypotension_intp_c,0)
      | coalesce(urine_output_24hr_c,0) as acute_organ_failure_c
      FROM %(twf_table_join)s cdm_twf inner join S on %(dataset_id_match)s
        and cdm_twf.enc_id = S.enc_id
        %(dataset_id_equal)s
    ''' % {
      'twf_table'           : para.get("twf_table"),
      'twf_table_join'      : para.get("twf_table_join"),
      'dataset_id_key'      : para.get("dataset_id_key"),
      'dataset_id_match'    : dataset_id_match(" ", "cdm_twf", "S", para.get("dataset_id")),
      'dataset_id_equal'    : dataset_id_equal(" WHERE ", "cdm_twf", para.get("dataset_id")),
      'sub_dataset_id_equal': dataset_id_equal("WHERE ", "cdm_twf", para.get("dataset_id")),
      'with_ds_s'           : dataset_id_equal(" and ", "cdm_s", para.get("dataset_id"))
    },
    'clean': {'value': False, 'confidence': 0},
  },
  'severe_sepsis': {
    'fid_input_items': ['sirs_intp', 'acute_organ_failure', 'sepsis_note','infections_angus_diag', 'infections_angus_hist'],
    'derive_type': 'simple',
    'fid_update_expr': '''
                    sirs_intp is true
                    AND acute_organ_failure is true
                    AND enc_id in
                    (
                    SELECT distinct enc_id FROM cdm_s WHERE
                    (fid = 'infections_angus_diag' AND value like 'True')
                    OR (fid = 'infections_angus_hist' AND value like 'True')
                    OR (fid = 'sepsis_note' AND value like 'True')
                    )
                    ''',
    'fid_c_update_expr': 'coalesce(acute_organ_failure_c,0) | coalesce(sirs_intp_c, 0)',
  },
  'worst_sofa': {
    'fid_input_items': ['resp_sofa', 'hepatic_sofa', 'hematologic_sofa', 'cardio_sofa', 'neurologic_sofa', 'renal_sofa'],
    'derive_type': 'simple',
    'fid_update_expr': '''GREATEST(resp_sofa * (1-based_on_popmean(resp_sofa_c)),
                              hepatic_sofa * (1-based_on_popmean(hepatic_sofa_c)),
                              hematologic_sofa * (1-based_on_popmean(hematologic_sofa_c)),
                              cardio_sofa * (1-based_on_popmean(cardio_sofa_c)),
                              neurologic_sofa * (1-based_on_popmean(neurologic_sofa_c)),
                              renal_sofa * (1-based_on_popmean(renal_sofa_c)))''',
    'fid_c_update_expr': '''
                      coalesce(resp_sofa_c, 0) |
                      coalesce(hepatic_sofa_c, 0) |
                      coalesce(hematologic_sofa_c, 0) |
                      coalesce(cardio_sofa_c, 0) |
                      coalesce(neurologic_sofa_c, 0) |
                      coalesce(renal_sofa_c, 0)
                      ''',
  },
  'any_organ_failure': {
    'fid_input_items': ['worst_sofa'],
    'derive_type': 'simple',
    'fid_update_expr': '(worst_sofa = 4)',
    'fid_c_update_expr': 'worst_sofa_c',
    },
  'sirs_raw': {
    'fid_input_items': ['sirs_temperature_oor', 'sirs_hr_oor', 'sirs_resp_oor', 'sirs_wbc_oor'],
    'derive_type': 'simple',
    'fid_update_expr': '''(cast(sirs_temperature_oor as int)
                      * (1-based_on_popmean(sirs_temperature_oor_c))
                    + cast(sirs_hr_oor as int) * (1-based_on_popmean(sirs_hr_oor_c))
                    + cast(sirs_resp_oor as int) * (1-based_on_popmean(sirs_resp_oor_c))
                    + cast(sirs_wbc_oor as int) * (1-based_on_popmean(sirs_wbc_oor_c))
                    >= 2)''',
    'fid_c_update_expr': 'sirs_temperature_oor_c | sirs_hr_oor_c | sirs_resp_oor_c | sirs_wbc_oor_c',
  },
  'mapm': {
    'fid_input_items': ['abp_mean','nbp_mean'],
    'derive_type': 'simple',
    'fid_update_expr': '''
                   case
                    when based_on_popmean(abp_mean_c) = 0 and abp_mean is not null
                    then abp_mean
                    when based_on_popmean(nbp_mean_c) = 0 and nbp_mean is not null
                    then nbp_mean
                    else coalesce(abp_mean, nbp_mean)
                  end
                   ''',
    'fid_c_update_expr': '''
                      case
                      when based_on_popmean(abp_mean_c) = 0 and abp_mean is not null
                        then abp_mean_c
                      when based_on_popmean(nbp_mean_c) = 0 and nbp_mean is not null
                        then nbp_mean_c
                      else coalesce(abp_mean_c, 0) | coalesce(nbp_mean_c, 0)
                      end
                     ''',
  },
  'dbpm': {
    'fid_input_items': ['abp_dias', 'nbp_dias'],
    'derive_type': 'simple',
    'fid_update_expr': '''
                    (CASE
                    WHEN based_on_popmean(abp_dias_c) = 1 or abp_dias is null THEN nbp_dias
                    WHEN (abp_dias > 0.15 * nbp_dias)
                      and based_on_popmean(abp_dias_c) = 0
                      and based_on_popmean(nbp_dias_c) = 0 THEN nbp_dias
                    ELSE abp_dias
                    END)
                   ''',
    'fid_c_update_expr': '''
                      (CASE
                      WHEN based_on_popmean(abp_dias_c) = 1 or abp_dias is null THEN nbp_dias_c
                      WHEN (abp_dias > 0.15 * nbp_dias)
                        and based_on_popmean(abp_dias_c) = 0
                        and based_on_popmean(nbp_dias_c) = 0 THEN nbp_dias_c | abp_dias_c
                      ELSE abp_dias_c
                      END)
                     ''',
  },
  'sbpm': {
    'fid_input_items': ['abp_sys', 'nbp_sys'],
    'derive_type': 'simple',
    'fid_update_expr': '''
                    (CASE
                    WHEN based_on_popmean(abp_sys_c) = 1 or abp_sys is null THEN nbp_sys
                    WHEN (abp_sys < 0.15 * nbp_sys)
                      and based_on_popmean(abp_sys_c) = 0
                      and based_on_popmean(nbp_sys_c) = 0 THEN nbp_sys
                    ELSE abp_sys
                    END)
                   ''',
    'fid_c_update_expr': '''
                      (CASE
                      WHEN based_on_popmean(abp_sys_c) = 1 or abp_sys is null THEN nbp_sys_c
                      WHEN (abp_sys < 0.15 * nbp_sys)
                        and based_on_popmean(abp_sys_c) = 0
                        and based_on_popmean(nbp_sys_c) = 0 THEN nbp_sys_c | abp_sys_c
                      ELSE abp_sys_c
                      END)
                     ''',
  },
  'fluid_resuscitation': {
    'fid_input_items': ['fluids_intake_3hr', 'weight'],
    'derive_type': 'simple',
    'fid_update_expr': '''(CASE
                    WHEN fluids_intake_3hr/weight >= 30
                    THEN True
                    ELSE False
                    END)''',
    'fid_c_update_expr': '''
                      (CASE
                      WHEN fluids_intake_3hr/weight >= 30
                        THEN fluids_intake_3hr_c | weight_c
                      ELSE 0
                      END)
                     ''',
  },
  'cmi': {
    'fid_input_items': ['severe_sepsis', 'fluid_resuscitation', 'vasopressor_resuscitation','fluids_intake_1hr'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
        WITH subquery as (select %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp from %(twf_table_join)s cdm_twf
           where cdm_twf.severe_sepsis
           %(and_with_ds_twf)s)
        SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
          bool_or(CASE
          WHEN
            (
            (fluid_resuscitation is TRUE
              or vasopressor_resuscitation is true)
            OR fluids_intake_1hr > 250
            )
            AND
            (
              subquery.tsp <= cdm_twf.tsp
              and cdm_twf.tsp - subquery.tsp < interval '6 hours'
            )
            THEN TRUE
          ELSE FALSE
          END) as cmi,
          max(coalesce(severe_sepsis_c,0)
              | coalesce(fluid_resuscitation_c,0)
              | coalesce(vasopressor_resuscitation_c,0)
              | coalesce(fluids_intake_1hr_c,0)) as cmi_c
        FROM %(twf_table_join)s cdm_twf inner join subquery
        on cdm_twf.enc_id = subquery.enc_id
        %(dataset_id_match)s
        GROUP BY %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
        ''' % {
          'twf_table_join': para['twf_table_join'],
          'dataset_id_key': para['dataset_id_key'],
          'and_with_ds_twf': dataset_id_equal(" and ", "cdm_twf", para['dataset_id']),
          'dataset_id_match': dataset_id_match(" and ", "cdm_twf", "subquery", para['dataset_id'])
        },
  },
  'minutes_since_any_organ_fail': {
    'fid_input_items': ['any_organ_failure'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
    WITH subquery as (
      select  %(dataset_id_key)s cdm_twf.enc_id, min(cdm_twf.tsp) tsp, max(cdm_twf.any_organ_failure_c) c
        FROM %(twf_table_join)s cdm_twf
        where cdm_twf.any_organ_failure %(dataset_id_equal)s
      group by %(dataset_id_key)s cdm_twf.enc_id
    )
    SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
    (case when cdm_twf.tsp > subquery.tsp
        then EXTRACT(EPOCH FROM (cdm_twf.tsp - subquery.tsp))/60
        else 0
     end) as minutes_since_any_organ_fail,
     subquery.c as minutes_since_any_organ_fail_c
    FROM %(twf_table_join)s cdm_twf
    inner join subquery on cdm_twf.enc_id = subquery.enc_id
    %(dataset_id_match)s
    %(dataset_id_equal_w)s
    ''' % {
      'twf_table_join': para.get("twf_table_join"),
      'dataset_id_key': para.get("dataset_id_key"),
      'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id")),
      'dataset_id_equal_w': dataset_id_equal(" where ", "cdm_twf", para.get("dataset_id")),
      'dataset_id_match': dataset_id_match(" and ", "cdm_twf", "subquery", para.get("dataset_id"))
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  'minutes_to_shock_onset': {
    'fid_input_items': ['septic_shock'],
    'derive_type': 'subquery',
    'subquery': lambda para:
    '''
    WITH subquery as (
      select %(dataset_id_key)s enc_id, min(tsp) tsp, max(septic_shock_c) c from
        %(twf_table_join)s cdm_twf
        where septic_shock %(dataset_id_equal)s
      group by %(dataset_id_key)s enc_id
    )
    SELECT %(dataset_id_key)s enc_id, tsp,
    (case when cdm_twf.tsp > subquery.tsp
        then EXTRACT(EPOCH FROM (subquery.tsp - cdm_twf.tsp))/60
        else 0
     end) minutes_to_shock_onset, subquery.c minutes_to_shock_onset_c
    FROM %(twf_table_join)s cdm_twf inner join subquery on
    cdm_twf.enc_id = subquery.enc_id %(dataset_id_match)s
    ''' % {
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table_join': para.get('twf_table_join'),
      'dataset_id_equal': dataset_id_equal(' and ', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_match': dataset_id_match(' and ', 'cdm_twf', 'subquery', para.get("dataset_id"))
    }
  },
  'minutes_since_any_antibiotics': {
    'fid_input_items': ['any_antibiotics'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
    WITH subquery as (
      select %(dataset_id_key_t)s cdm_t.enc_id, min(tsp) tsp, max(confidence)::int c
        from cdm_t
        where cdm_t.fid = 'any_antibiotics' and cdm_t.value::boolean %(dataset_id_equal_t)s
      group by %(dataset_id_key_t)s enc_id
    )
    SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
     (case when cdm_twf.tsp > subquery.tsp
        then EXTRACT(EPOCH FROM (cdm_twf.tsp - subquery.tsp))/60
        else 0
     end) as minutes_since_any_antibiotics,
     subquery.c as minutes_since_any_antibiotics_c
    FROM %(twf_table_join)s cdm_twf inner join subquery on cdm_twf.enc_id = subquery.enc_id
    and cdm_twf.tsp >= subquery.tsp
    %(dataset_id_match)s
    %(dataset_id_equal)s
    ''' % {
      'dataset_id_key': para.get("dataset_id_key"),
      'dataset_id_key_t': dataset_id_key('cdm_t', para.get('dataset_id')),
      'dataset_id_equal_t': dataset_id_equal('and ', 'cdm_t', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(' and ', 'cdm_twf', para.get("dataset_id")),
      'twf_table_join': para.get('twf_table_join'),
      'dataset_id_match': dataset_id_match(" and ", "cdm_twf", "subquery", para.get("dataset_id"))
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  'treatment_within_6_hours': {
    'fid_input_items': ['any_antibiotics',
                                'any_anticoagulant',
                                'any_beta_blocker',
                                'any_glucocorticoid',
                                'any_inotrope',
                                'any_pressor',
                                'vent',],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
    WITH subquery as (
        select %(dataset_id_key)s enc_id,tsp, confidence::int c from cdm_t
        where
          (fid = 'any_antibiotics'
          or fid = 'any_anticoagulant'
          or fid = 'any_beta_blocker'
          or fid = 'any_glucocorticoid'
          or fid = 'any_inotrope'
          or fid = 'any_pressor'
          or fid = 'vent' and value = 'True')
          %(dataset_id_equal_t)s
    )
    SELECT %(dataset_id_key)s enc_id, tsp
     (case when cdm_twf.treatment_within_6_hours then true
        when not cdm_twf.treatment_within_6_hours
          and cdm_twf.tsp > subquery.tsp
          and cdm_twf.tsp - subquery.tsp < interval '6 hours'
        then true
        else false
     end) as treatment_within_6_hours,
    coalesce(cdm_twf.treatment_within_6_hours_c, 0) | coalesce(subquery.c, 0) as treatment_within_6_hours_c
    FROM %(twf_table_join)s cdm_twf
    inner join subquery on cdm_twf.enc_id = subquery.enc_id %(dataset_id_match)s
    %(dataset_id_equal)s
    ''' % {
      'dataset_id_key': para.get('dataset_id_key'),
      'dataset_id_equal_t': dataset_id_equal(' and ', 'cdm_t', para.get('dataset_id')),
      'twf_table_join': para.get('twf_table_join'),
      'dataset_id_match': dataset_id_match(' and ', 'cdm_twf', 'subquery', para.get('dataset_id')),
      'dataset_id_equal': dataset_id_equal(' and ', 'cdm_twf', para.get('dataset_id'))
    }
  },
  'fluids_intake_3hr': {
    'fid_input_items': ['fluids_intake'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
          SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
          sum(coalesce(cdm_t.value::float,0)) fluids_intake_3hr,
          max(coalesce(cdm_t.confidence, 0)) fluids_intake_3hr_c
          from %(twf_table_join)s cdm_twf
          inner join cdm_t
          on cdm_t.enc_id = cdm_twf.enc_id and cdm_t.tsp <= cdm_twf.tsp
          and cdm_t.tsp > cdm_twf.tsp - interval '3 hours' %(dataset_id_match)s
          where fid = 'fluids_intake' %(dataset_id_equal)s
          group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table_join': para.get('twf_table_join'),
      'with_ds_ttwf': para.get('with_ds_ttwf'),
      'dataset_id_match': dataset_id_match(' and ','cdm_t', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id"))
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  'renal_sofa': {
    'fid_input_items': ['creatinine', 'urine_output_24hr'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
          WITH S as (SELECT %(dataset_id_key)s cdm_twf.enc_id, min(cdm_twf.tsp) min_tsp
          FROM %(twf_table)s cdm_twf %(where_dataset_id_equal)s
          group by %(dataset_id_key)s cdm_twf.enc_id)
          SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
          (
            case when creatinine > 5.0 or (urine_output_24hr < 200 and tsp - min_tsp >= '24 hours'::interval) then 4
            when creatinine >= 3.5 or (urine_output_24hr < 500 and tsp - min_tsp >= '24 hours'::interval) then 3
            when creatinine >= 2.0 then 2
            when creatinine >= 1.2 then 1
            else 0 end
          ) as renal_sofa,
          (
            creatinine_c | urine_output_24hr_c
          ) as renal_sofa_c
          FROM %(twf_table_join)s cdm_twf
          inner join S on cdm_twf.enc_id = S.enc_id
          %(dataset_id_match)s %(dataset_id_equal)s
    ''' % {
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table': para.get('twf_table'),
      'twf_table_join': para.get('twf_table_join'),
      'dataset_id_match': dataset_id_match(' and ','S', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id")),
      'where_dataset_id_equal': dataset_id_equal(" where ", "cdm_twf", para.get("dataset_id"))
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  'fluids_intake_1hr': {
    'fid_input_items': ['fluids_intake'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
          SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
          sum(coalesce(cdm_t.value::float,0)) fluids_intake_1hr,
          max(coalesce(cdm_t.confidence, 0)) fluids_intake_1hr_c
          from %(twf_table_join)s cdm_twf
          inner join cdm_t
          on cdm_t.enc_id = cdm_twf.enc_id and cdm_t.tsp <= cdm_twf.tsp
          and cdm_t.tsp > cdm_twf.tsp - interval '1 hours' %(dataset_id_match)s
          where fid = 'fluids_intake' %(dataset_id_equal)s
          group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table_join': para.get('twf_table_join'),
      'with_ds_ttwf': para.get('with_ds_ttwf'),
      'dataset_id_match': dataset_id_match(' and ','cdm_t', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id"))
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  'fluids_intake_24hr': {
    'fid_input_items': ['fluids_intake'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
          SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
          sum(coalesce(cdm_t.value::float,0)) fluids_intake_24hr,
          max(coalesce(cdm_t.confidence, 0)) fluids_intake_24hr_c
          from %(twf_table_join)s cdm_twf
          inner join cdm_t
          on cdm_t.enc_id = cdm_twf.enc_id and cdm_t.tsp <= cdm_twf.tsp
          and cdm_t.tsp > cdm_twf.tsp - interval '24 hours' %(dataset_id_match)s
          where fid = 'fluids_intake' %(dataset_id_equal)s
          group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table_join': para.get('twf_table_join'),
      'with_ds_ttwf': para.get('with_ds_ttwf'),
      'dataset_id_match': dataset_id_match(' and ','cdm_t', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id"))
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  'urine_output_6hr': {
    'fid_input_items': ['urine_output'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
          SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
          sum(coalesce(cdm_t.value::float, 0)) urine_output_6hr,
          max(coalesce(cdm_t.confidence, 0)) urine_output_6hr_c
          from %(twf_table_join)s cdm_twf
          inner join cdm_t
          on cdm_t.enc_id = cdm_twf.enc_id and cdm_t.tsp <= cdm_twf.tsp
          and cdm_t.tsp > cdm_twf.tsp - interval '6 hours' %(dataset_id_match)s
          where fid = 'urine_output' %(dataset_id_equal)s
          group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table_join': para.get('twf_table_join'),
      'with_ds_ttwf': para.get('with_ds_ttwf'),
      'dataset_id_match': dataset_id_match(' and ','cdm_t', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id"))
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  'urine_output_24hr': {
    'fid_input_items': ['urine_output'],
    'derive_type': 'subquery',
    'subquery': lambda para: '''
          SELECT %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp,
          sum(coalesce(cdm_t.value::float, 0)) urine_output_24hr,
          max(coalesce(cdm_t.confidence, 0)) urine_output_24hr_c
          from %(twf_table_join)s cdm_twf
          left join cdm_t
          on cdm_t.enc_id = cdm_twf.enc_id and cdm_t.tsp <= cdm_twf.tsp
          and cdm_t.tsp > cdm_twf.tsp - interval '24 hours' %(dataset_id_match)s
          where fid = 'urine_output' %(dataset_id_equal)s
          group by %(dataset_id_key)s cdm_twf.enc_id, cdm_twf.tsp
    ''' % {
      'dataset_id_key': para.get("dataset_id_key"),
      'twf_table_join': para.get('twf_table_join'),
      'with_ds_ttwf': para.get('with_ds_ttwf'),
      'dataset_id_match': dataset_id_match(' and ','cdm_t', 'cdm_twf', para.get("dataset_id")),
      'dataset_id_equal': dataset_id_equal(" and ", "cdm_twf", para.get("dataset_id"))
    },
    'clean': {'value': 0, 'confidence': 0},
  },
  # TBD
  # 'acute_kidney_failure':
  # {
  #   'fid_input_items': ['acute_kidney_failure_inhosp', 'creatinine', 'urine_output_24hr', 'dialysis'],
  #   'derive_type': 'subquery',
  #   'subquery': lambda para: '''
  #   WITH A as(
  #     SELECT distinct enc_id, tsp FROM cdm_t
  #       WHERE  fid = 'acute_kidney_failure_inhosp' %(dataset_id_equal_t)s
  #       ORDER BY enc_id,  tsp
  #   ),
  #   min_tsp as (
  #     select cdm_twf.enc_id, min(cdm_twf.tsp) from %(twf_table)s cdm_twf
  #     %(dataset_id_equal)s
  #     group by cdm_twf.enc_id
  #   )
  #   select %(dataset_id)s
  #   FROM A left join %(twf_table) B on A.enc_id = B.enc_id and B.tsp >= A.tsp
  #     and A.tsp <= B.tsp + interval '24 hours'
  #   left join %(twf_table) C on A.enc_id = C.enc_id and C.tsp >= A.tsp
  #     and A.tsp <= C.tsp + interval '24 hours'
  #   inner join min_tsp on C.enc_id = min_tsp.enc_id and C.tsp - min_tsp.min >= interval '24 hours'
  #   left join cdm_t D on D.enc_id = A.enc_id and D.tsp >= A.tsp and A.tsp <= D.tsp + interval '24 hours'
  #   WHERE B.creatinine > 5 and C.urine_output_24hr < 500 and D.fid = 'dialysis'
  #   %(dataset_id_equal_b)s %(dataset_id_equal_c)s %(dataset_id_equal_d)s
  #   ''' % {
  #     'twf_table': para.get('twf_table')
  #     'dataset_id_equal': dataset_id_equal(" where ", 'cdm_twf', para.get("dataset_id"))
  #     'dataset_id_equal_t': dataset_id_equal(" and ", 'cdm_t', para.get("dataset_id")),
  #     'dataset_id_equal_b': dataset_id_equal(" and ", 'B', para.get("dataset_id")),
  #     'dataset_id_equal_c': dataset_id_equal(" and ", 'C', para.get("dataset_id")),
  #     'dataset_id_equal_d': dataset_id_equal(" and ", 'D', para.get("dataset_id")),
  #   }
  # }
  'any_anticoagulant': {
    'fid_input_items': ['apixaban_dose', 'dabigatran_dose', 'rivaroxaban_dose', 'warfarin_dose', 'heparin_dose'],
    'derive_type': 'simple',
    'fid_select_expr': '''
                                SELECT distinct %(dataset_col_block)s enc_id, tsp, 'any_anticoagulant', 'True', max(confidence) confidence FROM cdm_t
                                WHERE fid ~ 'apixaban_dose|dabigatran_dose|rivaroxaban_dose|warfarin_dose|heparin_dose' AND cast(value::json->>'dose' as numeric) > 0 %(dataset_where_block)s
                                group by %(dataset_col_block)s enc_id, tsp''',
  },
  'any_beta_blocker': {
    'fid_input_items': ['acebutolol_dose', 'atenolol_dose', 'bisoprolol_dose', 'metoprolol_dose', 'nadolol_dose', 'propanolol_dose'],
    'derive_type': 'simple',
    'fid_select_expr': '''
                                SELECT distinct %(dataset_col_block)s enc_id, tsp, 'any_beta_blocker', 'True', max(confidence) confidence FROM cdm_t
                                WHERE fid ~ 'acebutolol_dose|atenolol_dose|bisoprolol_dose|metoprolol_dose|nadolol_dose|propanolol_dose' AND cast(value::json->>'dose' as numeric) > 0 %(dataset_where_block)s
                                group by %(dataset_col_block)s enc_id, tsp''',
  },
  'any_glucocorticoid': {
    'fid_input_items': ['hydrocortisone_dose', 'prednisone_dose', 'prednisolone_dose', 'methylprednisolone_dose', 'dexamethasone_dose', 'betamethasone_dose', 'fludrocortisone_dose'],
    'derive_type': 'simple',
    'fid_select_expr': '''
                                SELECT distinct %(dataset_col_block)s enc_id, tsp, 'any_glucocorticoid', 'True', max(confidence) confidence FROM cdm_t
                                WHERE fid ~ 'hydrocortisone_dose|prednisone_dose|prednisolone_dose|methylprednisolone_dose|dexamethasone_dose|betamethasone_dose|fludrocortisone_dose' AND cast(value::json->>'dose' as numeric) > 0 %(dataset_where_block)s
                                group by %(dataset_col_block)s enc_id, tsp''',
  },
  'any_antibiotics': {
    'fid_input_items': ['ampicillin_dose', 'clindamycin_dose', 'erythromycin_dose' , 'gentamicin_dose' , 'oxacillin_dose' , 'tobramycin_dose' , 'vancomycin_dose' , 'ceftazidime_dose' , 'cefazolin_dose' , 'penicillin_g_dose' , 'meropenem_dose' , 'penicillin_dose' , 'amoxicillin_dose' , 'piperacillin_tazbac_dose', 'rifampin_dose', 'meropenem_dose', 'rapamycin_dose'],
    'derive_type': 'simple',
    'fid_select_expr': '''
                                SELECT distinct %(dataset_col_block)s enc_id, tsp, 'any_antibiotics', 'True', max(confidence) confidence FROM cdm_t
                                WHERE fid ~ 'ampicillin_dose|clindamycin_dose|erythromycin_dose|gentamicin_dose|oxacillin_dose|tobramycin_dose|vancomycin_dose|ceftazidime_dose|cefazolin_dose|penicillin_g_dose|meropenem_dose|penicillin_dose|amoxicillin_dose|piperacillin_tazbac_dose|rifampin_dose|meropenem_dose|rapamycin_dose' AND cast(value::json->>'dose' as numeric) > 0 %(dataset_where_block)s
                                group by %(dataset_col_block)s enc_id, tsp''',
  },
}

