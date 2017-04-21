from etl.transforms.primitives.df import derive
from etl.transforms.primitives.df import format_data
from etl.transforms.primitives.df import restructure
from etl.transforms.primitives.df import translate
from etl.transforms.primitives.df.pandas_utils import async_read_df
from etl.mappings import lab_procedures as lp_config
from etl.load.primitives.row import load_row
import etl.transforms.primitives.df.filter_rows as filter_rows



#============================
# Utilities
#============================
async def pull_med_orders(connection, dataset_id, log, is_plan):

    log.info('Entered Med Orders Extraction')

    mo = await async_read_df("""select pe.enc_id, mo."ORDER_INST", mo."display_name", mo."MedUnit",mo."Dose"
                                 from "OrderMed" mo
                                 inner join
                                 pat_enc pe
                                 on mo."CSN_ID"::text=pe.visit_id
                                ;""", connection)
    if mo is None:
        return

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

    return mo

async def pull_medication_admin(connection, dataset_id, log, is_plan):

  log.info('Entering Medication Administrtaion Processing')

  ma = await async_read_df("""select pe.enc_id, ma.display_name,
                                  ma."Dose", ma."MedUnit",
                                  ma."INFUSION_RATE", ma."MAR_INF_RATE_UNIT",
                                  ma."TimeActionTaken"
                              from
                              "MedicationAdministration"  ma
                              inner join
                              pat_enc pe
                              on ma."CSN_ID"::text=pe.visit_id""",connection)

  if ma is None:
    return

  ma = restructure.select_columns(ma, {'enc_id': 'enc_id',
                                      'display_name':'full_name',
                                      'Dose':'dose_value',
                                      'MedUnit':'dose_unit',
                                      'INFUSION_RATE':'rate_value',
                                      'MAR_INF_RATE_UNIT':'rate_unit',
                                      'TimeActionTaken':'tsp'})

  ma = translate.translate_med_name_to_fid(ma)
  ma = filter_rows.filter_medications(ma)
  ma = translate.convert_units(ma,
                               fid_col = 'fid',
                               fids = ['piperacillin_tazbac_dose', 'vancomycin_dose',
                          'cefazolin_dose', 'cefepime_dose', 'ceftriaxone_dose',
                          'ampicillin_dose'],
                               unit_col = 'dose_unit', from_unit = 'g', to_unit = 'mg',
                               value_col = 'dose_value', convert_func = translate.g_to_mg)

  ma = derive.combine(ma, 'vasopressors_dose',
                      ['vasopressin_dose', 'neosynephrine_dose', 'levophed_infusion_dose',
                      'epinephrine_dose', 'dopamine_dose', 'milrinone_dose'
                                                           'dobutamine_dose'],keep_originals=False)
  ma = derive.combine(ma, 'crystalloid_fluid',
                      ['lactated_ringers', 'sodium_chloride'],keep_originals=False)

  ma = derive.combine(ma, 'cms_antibiotics',
                      ['cefepime_dose', 'ceftriaxone_dose', 'piperacillin_tazbac_dose',
                      'levofloxacin_dose', 'moxifloxacin_dose', 'vancomycin_dose',
                      'metronidazole_dose', 'aztronam_dose', 'ciprofloxacin_dose',
                      'gentamicin_dose', 'azithromycin_dose'],keep_originals=False)

  ma = format_data.threshold_values(ma, 'dose_value')

  ma = ma.loc[ma['fid'].apply(lambda x: x in ['cms_antibiotics', 'crystalloid_fluid', 'vasopressors_dose'])]

  ma['confidence'] = 2

  if not is_plan:
    for idx, row in ma.iterrows():
      await load_row.upsert_t(connection, [row['enc_id'], row['tsp'], row['fid'], str(row['dose_value']), row['confidence']], dataset_id=dataset_id)
    log.info('Medication Order Write complete')
  else:
    log.info('Medication Order Upsert skipped, due to plan mode')

  return ma

async def bands(connection, dataset_id, log, is_plan):
  log.info("Entering bands Processing")

  labs = await async_read_df("""select pe.enc_id, lb."NAME" ,
                                lb."ResultValue", lb."RESULT_TIME"
                                from
                                  "Labs_643"  lb
                                inner join
                                  pat_enc pe
                                on lb."CSN_ID"::text=pe.visit_id
                                WHERE "NAME"='BANDS';""",connection)
  if labs is None:
    return

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

async def pull_order_procs(connection, dataset_id, log, is_plan):
  log.info("Entering order procs Processing")

  op = await async_read_df("""select pe.enc_id,
                              op."CSN_ID",op."proc_name", op."ORDER_TIME", op."OrderStatus", op."LabStatus",
                              op."PROC_START_TIME",op."PROC_ENDING_TIME"
                              from
                                "OrderProcs"  op
                              inner join
                                pat_enc pe
                              on op."CSN_ID"::text=pe.visit_id;""",connection)
  if op is None:
      return
  lp_map = await async_read_df("""SELECT * FROM lab_proc_dict;""", connection)

  if lp_map is None:
    return

  op = restructure.select_columns(op, {'enc_id': 'enc_id',
                                       'proc_name': 'fid',
                                       'ORDER_TIME': 'tsp',
                                       'OrderStatus': 'order_status',
                                       'LabStatus': 'proc_status',
                                       'PROC_START_TIME': 'proc_start_tsp',
                                       'PROC_ENDING_TIME': 'proc_end_tsp'})

  op = derive.derive_lab_status_clarity(op)

  def get_fid_name_mapping(lp_map):
    fid_map = dict()
    for fid, codes in lp_config.procedure_ids:
      nameList = list()
      for code in codes:
        rs = lp_map[lp_map['proc_id'] == code]['proc_name']
        if not rs.empty:
          nameList.append(rs.iloc[0])
      fid_map[fid] = nameList
    return fid_map

  fid_map = get_fid_name_mapping(lp_map)
  for fid, names in fid_map.items():
    for name in names:
      op.loc[op['fid'] == name, 'fid'] = fid

  op = op[[x in fid_map.keys() for x in op['fid']]]

  op = op.loc[op['fid'].apply(lambda x: x in ['blood_culture', 'lactate'])]

  op['fid'] += '_order'

  op['confidence']=2

  if not is_plan:
    for idx, row in op.iterrows():
      await load_row.upsert_t(connection, [row['enc_id'], row['tsp'], row['fid'], str(row['status']), row['confidence']], dataset_id=dataset_id)
    log.info('Order Procs Write complete')
  else:
    log.info('Order Procs write skipped, due to plan mode')

  pass
