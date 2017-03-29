# derive pipeline for cdm_twf
import etl.load.primitives.tbl.derive as derive_func

async def derive_main(log, conn, cdm_feature_dict, mode=None, fid=None, dataset_id=None, table="cdm_twf"):
  '''
  mode: "append", run derive functions beginning with @fid sequentially
  mode: "dependent", run derive functions for @fid and other features depends on @fid
  mode: None, run derive functions sequentially for all derive features
  '''
  # generate a sequence to derive
  derive_feature_order = get_derive_seq(cdm_feature_dict)
  log.debug("derive feautre order: " + ", ".join(derive_feature_order))
  if mode == 'append':
    append = fid
    log.info("starts from feature %s" % append)
    idx = derive_feature_order.index(append)
    for i in range(idx, len(derive_feature_order)):
      fid = derive_feature_order[i]
      await derive_feature(log, cdm_feature_dict[fid], conn, dataset_id=dataset_id, twf_table=table)
  elif mode == 'dependent':
    dependent = fid
    if cdm_feature_dict[fid]['is_measured'] == 'no':
      log.info("update feature %s and its dependents" % dependent)
      await derive_feature(log, cdm_feature_dict[fid], conn, dataset_id=dataset_id, twf_table=table)
    else:
      log.info("update feature %s's dependents" % dependent)
    derive_feature_order = get_dependent_features([dependent], cdm_feature_dict)
    for fid in derive_feature_order:
      await derive_feature(log, cdm_feature_dict[fid], conn, dataset_id=dataset_id, twf_table=table)
  elif mode is None and fid is None:
    log.info("derive features one by one")
    for fid in derive_feature_order:
      await derive_feature(log, cdm_feature_dict[fid], conn, dataset_id=dataset_id, twf_table=table)
  elif mode is None and fid is not None:
    log.info("derive feature: %s" % fid)
    await derive_feature(log, cdm_feature_dict[fid], conn, dataset_id=dataset_id, twf_table=table)
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
  print(derived_features)
  # clear out dependencies on measured features, they should be in CDM already
  d_map = dict((k,rm_measured_dependencies(v, derived_features)) \
    for (k,v) in d_map.items())

  while (len(d_map) != 0):
    ind =  [k for k in d_map if len(d_map[k]) == 0]
    order.extend(ind)
    d_map = dict((k,v) for (k,v) in d_map.items() if k not in order)
    d_map = dict((k, reduce_dependencies(v)) for (k, v) in d_map.items())
  return order

def get_dependent_features(feature_list, features):
  # create the dependency map
  d_map = dict((fid, features[fid]['derive_func_input']) \
      for fid in features if ((not features[fid]['is_measured']) \
      and (not features[fid]['is_deprecated'])))
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

  dependent_features = [item for lst in dependency_list for item in lst]
  if len(dependent_features) ==  0:
    return dependent_features
  else:
    dic = dict((feature['fid'], feature['derive_func_input']) \
        for feature in features if feature['fid'] in dependent_features)
    return get_derive_seq(input_map=dic)

async def derive_feature(log, feature, conn, dataset_id=None, twf_table='cdm_twf'):
  fid = feature['fid']
  derive_func_id = feature['derive_func_id']
  derive_func_input = feature['derive_func_input']
  fid_category = feature['category']
  log.info("derive feature %s, function %s, inputs (%s) %s" \
  % (fid, derive_func_id, derive_func_input, 'dataset_id %s' % dataset_id if dataset_id is not None else ''))
  await derive_func_driver(fid, fid_category, derive_func_id, derive_func_input, conn, log, dataset_id, twf_table)
  log.info("derive feature %s end." % fid)



async def derive_func_driver(fid, fid_category, derive_func_id, derive_func_input, conn, log, dataset_id, twf_table):
  if fid in derive_config:
    config_entry = derive_config[fid]
    fid_input_items = [item.strip() for item in derive_func_input.split(',')]

    if fid_input_items == config_entry['fid_input_items']:
      update_clause = ''
      if fid_category == 'TWF':
        update_expr = config_entry['fid_update_expr']
        if '%(twf_table)s' in update_expr:
          update_expr = update_expr % {'twf_table': twf_table}
        c_update_expr = config_entry['fid_c_update_expr']
        if '%(twf_table)s' in c_update_expr:
          c_update_expr = c_update_expr % {'twf_table': twf_table}
        update_clause = """
        UPDATE %(twf_table)s SET %(fid)s = %(update_expr)s,
          %(fid)s_c = %(c_update_expr)s
        """ % {
          'fid':fid,
          'update_expr': update_expr,
          'c_update_expr': c_update_expr,
          'twf_table': twf_table
        }
        if config_entry['derive_type'] == 'simple':
          update_clause +=  '' if dataset_id is None else ' WHERE dataset_id = %s' % dataset_id
        elif config_entry['derive_type'] == 'subquery':
          update_from = (" FROM " + config_entry['fid_update_from']) % {'twf_table': twf_table}
          update_where = " WHERE " + config_entry['fid_update_where'] % {'twf_table': twf_table}
          update_where += ('' if dataset_id is None else ' and dataset_id = %s' % dataset_id)
          update_clause += update_from + update_where
        sql = update_clause
      elif fid_category == 'T':
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
        sql = insert_clause
      log.debug(sql)
      await conn.execute(sql)

    else:
      log.error("fid_input dismatch")
  else:
    log.info("Derive function is not implemented in driver, so we use legacy derive function")
    await derive_func.derive(fid, derive_func_id, derive_func_input, conn, log, dataset_id, twf_table)



derive_config = {
  'bun_to_cr': {
    'fid_input_items'   : ['bun', 'creatinine'],
    'derive_type'       : 'simple',
    'fid_update_expr'   : 'bun/creatinine',
    'fid_c_update_expr' : 'creatinine_c | bun_c',
  },
  'pao2_to_fio2': {
    'fid_input_items':  ['pao2', 'fio2'],
    'derive_type': 'simple',
    'fid_update_expr': 'pao2/fio2',
    'fid_c_update_expr': 'pao2_c | fio2_c'
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
    'derive_type': 'simple',
    'fid_update_expr': '''
                    (inr > 1.5 and based_on_popmean(inr_c) != 1)
                    OR (platelets < 100 and based_on_popmean(platelets_c) != 1)
                    OR (lactate > 4.0 and based_on_popmean(lactate_c) != 1)
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
                      OR (
                      urine_output_24hr < 500
                      AND
                        tsp - (SELECT min(cdm_twf_2.tsp) FROM %(twf_table)s cdm_twf_2
                           WHERE cdm_twf_2.enc_id = cdm_twf.enc_id)
                        >= interval '24 hours'
                      AND
                        coalesce((select cast(value as boolean) from cdm_s
                        where cdm_s.enc_id = cdm_twf.enc_id
                          and cdm_s.fid = 'chronic_kidney_hist' limit 1)
                        , False) = False
                      )
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
  'severe_sepsis': {
    'fid_input_items': ['sirs_intp', 'acute_organ_failure', 'sepsis_note','infections_angus_diag', 'infections_angus_hist'],
    'derive_type': 'simple',
    'fid_update_expr': '''
                    sirs_raw is true
                    AND acute_organ_failure is true
                    AND enc_id in
                    (
                    SELECT distinct enc_id FROM cdm_s WHERE
                    (fid = 'infections_angus_diag' AND value like 'True')
                    OR (fid = 'infections_angus_hist' AND value like 'True')
                    OR (fid = 'sepsis_note' AND value like 'True')
                    )
                    ''',
    'fid_c_update_expr': 'coalesce(acute_organ_failure_c,0) | coalesce(sirs_raw_c, 0)',
  },
  'cmi': {
    'fid_input_items': ['severe_sepsis', 'fluid_resuscitation', 'vasopressor_resuscitation','fluids_intake_1hr'],
    'derive_type': 'simple',
    'fid_update_expr': '''
                    (CASE
                    WHEN
                      (
                      (fluid_resuscitation is TRUE
                        or vasopressor_resuscitation is true)
                      OR fluids_intake_1hr > 250
                      )
                      AND
                      (select bool_or(severe_sepsis) from %(twf_table)s twf
                         where twf.enc_id = cdm_twf.enc_id
                         and twf.tsp <= cdm_twf.tsp
                         and cdm_twf.tsp - twf.tsp < interval '6 hours'
                      )
                      THEN TRUE
                    ELSE FALSE
                    END)
                   ''',
    'fid_c_update_expr': '''cmi_c | coalesce(severe_sepsis_c,0)
                      | coalesce(fluid_resuscitation_c,0)
                      | coalesce(vasopressor_resuscitation_c,0)
                      | coalesce(fluids_intake_1hr_c,0)''',
  },
  'worst_sofa': {
    'fid_input_items': ['resp_sofa', 'hepatic_sofa', 'hematologic_sofa', 'cardio_sofa', 'neurologic_sofa', 'renal_sofa'],
    'derive_type': 'simple',
    'fid_update_expr': '''GREATEST(resp_sofa * (1-based_on_popmean(resp_sofa_c)),
                              hepatic_sofa* (1-based_on_popmean(hepatic_sofa_c)),
                              hematologic_sofa* (1-based_on_popmean(hematologic_sofa_c)),
                              cardio_sofa* (1-based_on_popmean(cardio_sofa_c)),
                              neurologic_sofa* (1-based_on_popmean(neurologic_sofa_c)),
                              renal_sofa* (1-based_on_popmean(renal_sofa_c)))''',
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
  'minutes_since_any_organ_fail': {
    'fid_input_items': ['any_organ_failure'],
    'derive_type': 'subquery',
    'fid_update_from': '''
                        (
                          select enc_id, first(tsp) tsp, first(any_organ_failure_c) c from(
                            select enc_id, tsp, any_organ_failure_c from %(twf_table)s
                            where any_organ_failure
                            order by tsp
                            ) as ordered
                          group by ordered.enc_id
                        ) as subquery
                      ''',
    'fid_update_expr': '''
                                 (case when %(twf_table)s.tsp > subquery.tsp
                                    then EXTRACT(EPOCH FROM (%(twf_table)s.tsp - subquery.tsp))/60
                                    else 0
                                 end)
                                 ''',
    'fid_c_update_expr': 'subquery.c',
    'fid_update_where': '%(twf_table)s.enc_id = subquery.enc_id'
  },
  'minutes_to_shock_onset': {
    'fid_input_items': ['septic_shock'],
    'derive_type': 'subquery',
    'fid_update_from': '''
                        (
                          select enc_id, first(tsp) tsp, first(septic_shock_c) c from(
                            select enc_id, tsp, septic_shock_c from %(twf_table)s
                            where septic_shock
                            order by tsp
                            ) as ordered
                          group by ordered.enc_id
                        ) as subquery
                      ''',
    'fid_update_expr': '''
                                 (case when %(twf_table)s.tsp > subquery.tsp
                                    then EXTRACT(EPOCH FROM (subquery.tsp - %(twf_table)s.tsp))/60
                                    else 0
                                 end)
                                 ''',
    'fid_c_update_expr': 'subquery.c',
    'fid_update_where': '%(twf_table)s.enc_id = subquery.enc_id'
  },
  'minutes_since_any_antibiotics': {
    'fid_input_items': ['any_antibiotics'],
    'derive_type': 'subquery',
    'fid_update_from': '''
                        (
                          select enc_id, first(tsp) tsp, first(confidence)::int c from(
                            select enc_id, tsp, confidence from cdm_t
                            where fid = 'any_antibiotics' and value::boolean
                            order by tsp
                            ) as ordered
                          group by ordered.enc_id
                        ) as subquery
                      ''',
    'fid_update_expr': '''
                                 (case when %(twf_table)s.tsp > subquery.tsp
                                    then EXTRACT(EPOCH FROM (%(twf_table)s.tsp - subquery.tsp))/60
                                    else 0
                                 end)
                                 ''',
    'fid_c_update_expr': 'subquery.c',
    'fid_update_where': '%(twf_table)s.enc_id = subquery.enc_id'
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
    'fid_update_from': '''
                        (
                            select enc_id,tsp, confidence::int c from cdm_t
                            where
                              fid = 'any_antibiotics'
                              or fid = 'any_anticoagulant'
                              or fid = 'any_beta_blocker'
                              or fid = 'any_glucocorticoid'
                              or fid = 'any_inotrope'
                              or fid = 'any_pressor'
                              or fid = 'vent' and value = 'True'
                        ) as subquery
                      ''',
    'fid_update_expr': '''
                                 (case when cdm_twf.treatment_within_6_hours then true
                                    when not cdm_twf.treatment_within_6_hours
                                      and cdm_twf.tsp > subquery.tsp
                                      and cdm_twf.tsp - subquery.tsp < interval '6 hours'
                                    then true
                                    else false
                                 end)
                                 ''',
    'fid_c_update_expr': 'coalesce(cdm_twf.treatment_within_6_hours_c, 0) | coalesce(subquery.c, 0)',
    'fid_update_where': '%(twf_table)s.enc_id = subquery.enc_id'
  },
  'fluids_intake_3hr': {
    'fid_input_items': ['fluids_intake'],
    'derive_type': 'subquery',
    'fid_update_from': '''
                        (
                            select twf.enc_id, twf.tsp,
                            sum(t.value::float) sum_v, max(t.confidence) max_c
                            from %(twf_table)s twf
                            inner join cdm_t t
                            on t.enc_id = twf.enc_id and t.tsp <= twf.tsp
                            and t.tsp > twf.tsp - interval '3 hours'
                            and fid = 'fluids_intake'
                            group by twf.enc_id, twf.tsp
                        ) as subquery
                      ''',
    'fid_update_expr': 'subquery.sum_v',
    'fid_c_update_expr': 'subquery.max_c',
    'fid_update_where': '%(twf_table)s.enc_id = subquery.enc_id and %(twf_table)s.tsp = subquery.tsp'
  },
  'fluids_intake_1hr': {
    'fid_input_items': ['fluids_intake'],
    'derive_type': 'subquery',
    'fid_update_from': '''
                        (
                            select twf.enc_id, twf.tsp,
                            sum(t.value::float) sum_v, max(t.confidence) max_c
                            from %(twf_table)s twf
                            inner join cdm_t t
                            on t.enc_id = twf.enc_id and t.tsp <= twf.tsp
                            and t.tsp > twf.tsp - interval '1 hours'
                            and fid = 'fluids_intake'
                            group by twf.enc_id, twf.tsp
                        ) as subquery
                      ''',
    'fid_update_expr': 'subquery.sum_v',
    'fid_c_update_expr': 'subquery.max_c',
    'fid_update_where': '%(twf_table)s.enc_id = subquery.enc_id and %(twf_table)s.tsp = subquery.tsp'
  },
  'fluids_intake_24hr': {
    'fid_input_items': ['fluids_intake'],
    'derive_type': 'subquery',
    'fid_update_from': '''
                        (
                            select twf.enc_id, twf.tsp,
                            sum(t.value::float) sum_v, max(t.confidence) max_c
                            from %(twf_table)s twf
                            inner join cdm_t t
                            on t.enc_id = twf.enc_id and t.tsp <= twf.tsp
                            and t.tsp > twf.tsp - interval '24 hours'
                            and fid = 'fluids_intake'
                            group by twf.enc_id, twf.tsp
                        ) as subquery
                      ''',
    'fid_update_expr': 'subquery.sum_v',
    'fid_c_update_expr': 'subquery.max_c',
    'fid_update_where': '%(twf_table)s.enc_id = subquery.enc_id and %(twf_table)s.tsp = subquery.tsp'
  },
  'urine_output_6hr': {
    'fid_input_items': ['urine_output'],
    'derive_type': 'subquery',
    'fid_update_from': '''
                        (
                            select twf.enc_id, twf.tsp,
                            sum(t.value::float) sum_v, max(t.confidence) max_c
                            from %(twf_table)s twf
                            inner join cdm_t t
                            on t.enc_id = twf.enc_id and t.tsp <= twf.tsp
                            and t.tsp > twf.tsp - interval '6 hours'
                            and fid = 'urine_output'
                            group by twf.enc_id, twf.tsp
                        ) as subquery
                      ''',
    'fid_update_expr': 'subquery.sum_v',
    'fid_c_update_expr': 'subquery.max_c',
    'fid_update_where': '%(twf_table)s.enc_id = subquery.enc_id and %(twf_table)s.tsp = subquery.tsp'
  },
  'urine_output_24hr': {
    'fid_input_items': ['urine_output'],
    'derive_type': 'subquery',
    'fid_update_from': '''
                        (
                            select twf.enc_id, twf.tsp,
                            sum(t.value::float) sum_v, max(t.confidence) max_c
                            from %(twf_table)s twf
                            inner join cdm_t t
                            on t.enc_id = twf.enc_id and t.tsp <= twf.tsp
                            and t.tsp > twf.tsp - interval '24 hours'
                            and fid = 'urine_output'
                            group by twf.enc_id, twf.tsp
                        ) as subquery
                      ''',
    'fid_update_expr': 'subquery.sum_v',
    'fid_c_update_expr': 'subquery.max_c',
    'fid_update_where': '%(twf_table)s.enc_id = subquery.enc_id and %(twf_table)s.tsp = subquery.tsp'
  },
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

