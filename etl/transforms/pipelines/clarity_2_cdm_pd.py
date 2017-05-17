from etl.transforms.primitives.df import derive
from etl.transforms.primitives.df import format_data
from etl.transforms.primitives.df import restructure
from etl.transforms.primitives.df import translate
from etl.transforms.primitives.df.pandas_utils import async_read_df
from etl.mappings import lab_procedures as lp_config
from etl.load.primitives.row import load_row
import etl.transforms.primitives.df.filter_rows as filter_rows
from etl.clarity2dw.extractor import log_time
import time
import os
import asyncio


def get_min_tsp(tsp_name, tsp_with_quotes=True):
  if 'min_tsp' in os.environ:
      min_tsp = os.environ['min_tsp']
      if tsp_with_quotes:
        return ''' and "{tsp}"::timestamptz > '{min_tsp}'::timestamptz'''.format(tsp=tsp_name, min_tsp=min_tsp)
      else:
        return ''' and {tsp}::timestamptz > '{min_tsp}'::timestamptz'''.format(tsp=tsp_name, min_tsp=min_tsp)
  else:
    return ''

#============================
# Utilities
#============================
async def pull_med_orders(connection, dataset_id, fids, log, is_plan, clarity_workspace):
  start = time.time()
  log.info('Entered Med Orders Extraction')
  sql = """select pe.enc_id, mo."ORDER_INST", mo."display_name", mo."MedUnit",mo."Dose"
           from {ws}."OrderMed" mo
           inner join
           pat_enc pe
           on mo."CSN_ID"::text=pe.visit_id and pe.dataset_id = {dataset_id} {min_tsp}
          ;""".format(dataset_id=dataset_id, min_tsp=get_min_tsp("ORDER_INST"), ws=clarity_workspace)
  log.info(sql)
  mo = await async_read_df(sql, connection)
  if mo is None:
      return
  extracted = mo.shape[0]

  mo = restructure.select_columns(mo, {'enc_id': 'enc_id',
                                       'ORDER_INST':'tsp',
                                       'display_name': 'full_name',
                                       'MedUnit':'dose_unit',
                                       'Dose': 'dose'})

  mo = mo.dropna(subset=['full_name'])
  mo = translate.translate_med_name_to_fid(mo)
  mo = filter_rows.filter_medications(mo)
  mo = format_data.clean_units(mo, 'fid', 'dose_unit')
  mo = format_data.clean_values(mo, 'fid', 'dose')
  mo = translate.convert_units(mo, fid_col='fid',
                               fids=['piperacillin_tazbac_dose', 'vancomycin_dose',
                                        'cefazolin_dose', 'cefepime_dose', 'ceftriaxone_dose',
                                        'ampicillin_dose'],
                               unit_col='dose_unit', from_unit='g', to_unit='mg',
                               value_col='dose', convert_func=translate.g_to_mg)


  mo = derive.combine(mo, 'vasopressors_dose',
                      ['vasopressin_dose', 'neosynephrine_dose', 'levophed_infusion_dose',
                             'lactated_ringers', 'epinephrine_dose', 'dopamine_dose',
                             'dobutamine_dose'],keep_originals=False)

  mo = derive.combine(mo, 'crystalloid_fluid',
                      ['lactated_ringers', 'sodium_chloride'],keep_originals=False)

  mo = derive.combine(mo, 'cms_antibiotics',
                      ['cefepime_dose', 'ceftriaxone_dose', 'piperacillin_tazbac_dose',
                             'levofloxacin_dose', 'moxifloxacin_dose', 'vancomycin_dose',
                             'metronidazole_dose', 'aztronam_dose', 'ciprofloxacin_dose',
                             'gentamicin_dose', 'azithromycin_dose'],keep_originals=False)

  mo = format_data.threshold_values(mo, 'dose')
  mo = mo.loc[mo['fid'].apply(lambda x: x in ['cms_antibiotics', 'crystalloid_fluid', 'vasopressors_dose'])]
  mo['fid'] += '_order'
  mo['confidence'] = 2
  if not is_plan:
    for idx, row in mo.iterrows():
      await load_row.upsert_t(connection,
                              [row['enc_id'], row['tsp'], row['fid'], str(row['dose']), row['confidence']],
                              dataset_id=dataset_id)
    log.info('Medication Administration Write complete')
  else:
    log.info('Medication Administration Upsert skipped, due to plan mode')
  loaded = mo.shape[0]
  log_time(log, 'pull_med_orders', start, extracted, loaded)
  return 'pull_med_orders'

async def pull_medication_admin(connection, dataset_id, fids, log, is_plan, clarity_workspace):
  start = time.time()
  log.info('Entering Medication Administration Processing')
  sql = """select pe.enc_id, ma.display_name,
                  ma."Dose", ma."MedUnit",
                  ma."INFUSION_RATE", ma."MAR_INF_RATE_UNIT",
                  ma."TimeActionTaken", ma."ActionTaken"
          from
          {ws}."MedicationAdministration"  ma
          inner join
          pat_enc pe
          on ma."CSN_ID"::text=pe.visit_id and pe.dataset_id = {dataset_id} {min_tsp}
        """.format(dataset_id=dataset_id, min_tsp=get_min_tsp("TimeActionTaken"), ws=clarity_workspace)
  log.info(sql)
  ma = await async_read_df(sql,connection)

  if ma is None:
    return
  extracted = ma.shape[0]
  ma = restructure.select_columns(ma, {'enc_id'           : 'enc_id',
                                      'display_name'      : 'full_name',
                                      'Dose'              : 'dose_value',
                                      'MedUnit'           : 'dose_unit',
                                      'INFUSION_RATE'     : 'rate_value',
                                      'MAR_INF_RATE_UNIT' : 'rate_unit',
                                      'TimeActionTaken'   : 'tsp',
                                      'ActionTaken'       : 'action'})

  cms_antibiotics_fids = [
        'cefepime_dose',
        'ceftriaxone_dose',
        'piperacillin_tazbac_dose',
        'levofloxacin_dose',
        'moxifloxacin_dose',
        'vancomycin_dose',
        'metronidazole_dose',
        'aztronam_dose',
        'ciprofloxacin_dose',
        'gentamicin_dose',
        'azithromycin_dose'
    ]

  crystalloid_fluid_fids = ['lactated_ringers', 'sodium_chloride']

  vasopressors_fids = [
        'vasopressin_dose',
        'neosynephrine_dose',
        'levophed_infusion_dose',
        'epinephrine_dose',
        'dopamine_dose',
        'milrinone_dose'
        'dobutamine_dose'
    ]

  log.info('pull_medication_admin translate_med_name_to_fid')
  ma = translate.translate_med_name_to_fid(ma)

  log.info('pull_medication_admin filter_medications')
  ma = filter_rows.filter_medications(ma)

  log.info('pull_medication_admin filter_stopped_medications')
  ma = filter_rows.filter_stopped_medications(ma)

  log.info('pull_medication_admin converting g to mg')
  ma = translate.convert_units(ma,
                               fid_col = 'fid',
                               fids = ['piperacillin_tazbac_dose', 'vancomycin_dose',
                                       'cefazolin_dose', 'cefepime_dose', 'ceftriaxone_dose',
                                       'ampicillin_dose'],
                               unit_col = 'dose_unit', from_unit = 'g', to_unit = 'mg',
                               value_col = 'dose_value', convert_func = translate.g_to_mg)

  log.info('pull_medication_admin converting ml/hr to ml')
  ma = translate.convert_units(ma,
                               fid_col = 'fid',
                               fids = crystalloid_fluid_fids,
                               unit_col = 'dose_unit', from_unit = 'mL/hr', to_unit = 'mL',
                               value_col = 'dose_value', convert_func = translate.ml_per_hr_to_ml_for_1hr)

  log.info('pull_medication_admin combining')

  ma = derive.combine(ma, 'vasopressors_dose', vasopressors_fids, keep_originals=False)
  ma = derive.combine(ma, 'crystalloid_fluid', crystalloid_fluid_fids, keep_originals=False)
  ma = derive.combine(ma, 'cms_antibiotics', cms_antibiotics_fids, keep_originals=False)

  ma = format_data.threshold_values(ma, 'dose_value')

  ma = ma.loc[ma['fid'].apply(lambda x: x in ['cms_antibiotics', 'crystalloid_fluid', 'vasopressors_dose'])]

  ma['confidence'] = 2

  log.info('pull_medication_admin loading')

  if not is_plan:
    for idx, row in ma.iterrows():
      await load_row.upsert_t(connection, [row['enc_id'], row['tsp'], row['fid'], str(row['dose_value']), row['confidence']], dataset_id=dataset_id)
    log.info('Medication Order Write complete')
  else:
    log.info('Medication Order Upsert skipped, due to plan mode')
  loaded = ma.shape[0]
  log_time(log, 'pull_medication_admin', start, extracted, loaded)
  return 'pull_medication_admin'

async def bands(connection, dataset_id, fids, log, is_plan, clarity_workspace):
  log.info("Entering bands Processing")
  start = time.time()
  sql = """select pe.enc_id, lb."NAME" ,
                  lb."ResultValue", lb."RESULT_TIME"
          from
            {ws}."Labs_643"  lb
          inner join
            pat_enc pe
          on lb."CSN_ID"::text=pe.visit_id and pe.dataset_id = {dataset_id} {min_tsp}
          WHERE "NAME"='BANDS';
        """.format(dataset_id=dataset_id, min_tsp=get_min_tsp("RESULT_TIME"), ws=clarity_workspace)
  log.info(sql)
  labs = await async_read_df(sql,connection)
  if labs is None:
    return
  extracted = labs.shape[0]

  labs = restructure.select_columns(labs, {'enc_id': 'enc_id',
                                           'NAME': 'fid',
                                           'ResultValue': 'value',
                                           'RESULT_TIME': 'tsp'})

  labs['fid'] = labs['fid'].apply(lambda x: x.lower())

  labs = format_data.clean_values(labs, 'fid', 'value')

  labs['confidence'] = 2

  if not is_plan:
    for idx, row in labs.iterrows():
      await load_row.upsert_t(connection, [row['enc_id'], row['tsp'], 'bands', str(row['value']), row['confidence']], dataset_id=dataset_id)
    log.info('Bands Write complete')
  else:
    log.info('Bands write skipped, due to plan mode')
  loaded = labs.shape[0]
  log_time(log, 'bands', start, extracted, loaded)
  return 'bands'

async def pull_order_procs(connection, dataset_id, fids, log, is_plan, clarity_workspace):
  log.info("Entering order procs Processing")
  start = time.time()
  extracted = 0
  t_field = ''
  if 'lactate_order' in fids:
    t_field = 'op."RESULT_TIME"'
  elif 'blood_culture_order' in fids:
    t_field = 'op."SPECIMN_TAKEN_TIME"'
  else:
    log.error('Invalid fid for pull_order_procs: %s' % str(fids))
    return

  sql = """select pe.enc_id,
            op."CSN_ID",op."proc_name", {t_field} as "TSP", op."OrderStatus", op."LabStatus",
            op."PROC_START_TIME",op."PROC_ENDING_TIME"
            from
              {ws}."OrderProcs"  op
            inner join
              pat_enc pe
            on op."CSN_ID"::text=pe.visit_id and pe.dataset_id = {dataset_id} {min_tsp};
        """.format(dataset_id=dataset_id, min_tsp=get_min_tsp(t_field, tsp_with_quotes=False), ws=clarity_workspace, t_field=t_field)

  log.info(sql)
  op = await async_read_df(sql,connection)
  if op is None:
      return
  lp_map = await async_read_df("""SELECT * FROM {}.lab_proc_dict;""".format(clarity_workspace), connection)

  if lp_map is None:
    return

  log.info('pull_order_procs restructuring %s' % str(op.head(5)))
  op = restructure.select_columns(op, {'enc_id'           : 'enc_id',
                                       'proc_name'        : 'fid',
                                       'TSP'              : 'tsp',
                                       'OrderStatus'      : 'order_status',
                                       'LabStatus'        : 'proc_status',
                                       'PROC_START_TIME'  : 'proc_start_tsp',
                                       'PROC_ENDING_TIME' : 'proc_end_tsp'})

  log.info('pull_order_procs derive_lab_status_clarity %s' % str(op.head(5)))
  op = derive.derive_lab_status_clarity(op)

  log.info('pull_order_procs declaring get_fid_name_mapping')

  def get_fid_name_mapping(fids, lp_map):
    log.info('pull_order_procs called get_fid_name_mapping with %s %s' % (str(fids), str(lp_map)))
    fid_map = dict()
    for order_fid in fids:
      fid = order_fid[:-6]
      log.info('pull_order_procs get_fid_name_mapping for %s %s' % (order_fid, fid))

      found = False
      for pfid, codes in lp_config.procedure_ids:
        if fid == pfid:
          found = True
          nameList = list()
          for code in codes:
            rs = lp_map[lp_map['proc_id'] == code]['proc_name']
            if not rs.empty:
              nameList.append(rs.iloc[0])
          fid_map[fid] = nameList

      if not found:
        log.error('pull_order_procs could not find fid name mapping for %s' % fid)

    log.info('pull_order_procs get_fid_name_mapping result: %s' % str(fid_map))
    return fid_map

  log.info('pull_order_procs calling get_fid_name_mapping with fids: %s' % str(fids))
  fid_map = get_fid_name_mapping(fids, lp_map)
  for fid, names in fid_map.items():
    for name in names:
      op.loc[op['fid'] == name, 'fid'] = fid

  log.info('pull_order_procs creating op')

  op = op[[x in fid_map.keys() for x in op['fid']]]
  op = op.loc[op['fid'].apply(lambda x: x in ['blood_culture', 'lactate'])]
  op['fid'] += '_order'
  op['confidence']=2

  log.info('pull_order_procs loading: %s' % str(op))

  if not is_plan:
    for idx, row in op.iterrows():
      # TODO need to make sure the data is valid
      if row['tsp'] and not str(row['tsp']) == 'NaT':
        await load_row.upsert_t(connection, [row['enc_id'], row['tsp'], row['fid'], str(row['status']), row['confidence']], log=log, dataset_id=dataset_id)
    log.info('Order Procs Write complete')
  else:
    log.info('Order Procs write skipped, due to plan mode')
  loaded = op.shape[0]
  log_time(log, 'pull_order_procs', start, extracted, loaded)
  return 'pull_order_procs'

async def notes(connection, dataset_id, fids, log, is_plan, clarity_workspace):
  log.info("Entering Notes Processing")
  start = time.time()
  sql = '''
  insert into cdm_notes(dataset_id, pat_id, note_id, note_type, note_status, note_body, dates, providers)
  select  %(dataset_id)s as dataset_id,
          PE.pat_id as pat_id,
          "NOTE_ID" as note_id,
          "NoteType" as note_type,
          coalesce("NoteStatus", 'unknown') as note_status,
          string_agg("NOTE_TEXT", E'\n') as note_body,
          json_build_object('create_instant_dttm', "CREATE_INSTANT_DTTM",
                            'spec_note_time_dttm', json_agg(distinct "SPEC_NOTE_TIME_DTTM"),
                            'entry_instant_dttm', json_agg(distinct "ENTRY_ISTANT_DTTM")
                            ) as dates,
          json_build_object('AuthorType', json_agg(distinct "AuthorType")) as providers
  from %(ws)s."Notes" N
  inner join pat_enc PE
    on N."CSN_ID" = PE.visit_id and PE.dataset_id = %(dataset_id)s %(min_tsp)s
  group by PE.pat_id, "NOTE_ID", "NoteType", "NoteStatus", "CREATE_INSTANT_DTTM"
  on conflict (dataset_id, pat_id, note_id, note_type, note_status) do update
    set note_body = excluded.note_body,
        dates = excluded.dates,
        providers = excluded.providers
  ''' % {'dataset_id': dataset_id, 'min_tsp': get_min_tsp('CREATE_INSTANT_DTTM'), 'ws': clarity_workspace}

  log.info(sql)
  status = 'did-not-run'
  if not is_plan:
    status = await load_row.execute_load(connection, sql, log, timeout=None)
  else:
    log.info('Notes query skipped, due to plan mode')

  log_time(log, 'note', start, '[status: %s]' % status, '[status: %s]' % status)
  return 'note'
