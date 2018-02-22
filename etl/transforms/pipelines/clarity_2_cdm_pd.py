from etl.transforms.primitives.df import derive
from etl.transforms.primitives.df import format_data
from etl.transforms.primitives.df import restructure
from etl.transforms.primitives.df import translate
from etl.transforms.primitives.df.pandas_utils import async_read_df
from etl.mappings import lab_procedures_clarity
from etl.load.primitives.row import load_row
import etl.transforms.primitives.df.filter_rows as filter_rows
from etl.clarity2dw.extractor import log_time
import time
import os
import re
import pandas as pd
import json
import asyncio
from etl.mappings.med_regex import med_regex
import json


def get_min_tsp(tsp_name, tsp_with_quotes=True):
  if 'min_tsp' in os.environ:
    min_tsp = os.environ['min_tsp']
    if tsp_with_quotes:
      return ''' and "{tsp}"::timestamptz > '{min_tsp}'::timestamptz'''.format(tsp=tsp_name, min_tsp=min_tsp)
    else:
      return ''' and {tsp}::timestamptz > '{min_tsp}'::timestamptz'''.format(tsp=tsp_name, min_tsp=min_tsp)
  else:
    return ''


# ============================
# Utilities
# ============================
async def pull_med_orders(connection, dataset_id, fids, log, is_plan, clarity_workspace):
  """
  :param connection: database connection
  :param dataset_id: dataset_id
  :param fids: fids that are going to be written by this code
  :param log: instantited logging object
  :param is_plan: If is_plan is True, don't write, otherwise write
  :param clarity_workspace: database schema to write to within a database
  """

  start = time.time()
  log.info('Entered Med Orders Extraction')
  cms_antibiotics_fids = [med['fid'] for med in med_regex if 'part_of' in med and 'cms_antibiotics' in med['part_of']]

  crystalloid_fluid_fids = [med['fid'] for med in med_regex if
                'part_of' in med and 'crystalloid_fluid' in med['part_of']]

  vasopressors_fids = [med['fid'] for med in med_regex if 'part_of' in med and 'vasopressors_dose' in med['part_of']]
  all_fids = cms_antibiotics_fids + crystalloid_fluid_fids + vasopressors_fids
  sql = """select pe.enc_id, mo."ORDER_INST", mo."display_name", mo."MedUnit",mo."Dose"
       from {ws}."OrderMed" mo
       inner join
       pat_enc pe
       on mo."CSN_ID"::text=pe.visit_id and pe.dataset_id = {dataset_id} {min_tsp}
      ;""".format(dataset_id=dataset_id, min_tsp=get_min_tsp("ORDER_INST"), ws=clarity_workspace)
  log.info(sql)
  mo = await async_read_df(sql, connection)
  if mo is None:
    return 0
  extracted = mo.shape[0]

  mo = restructure.select_columns(mo, {'enc_id': 'enc_id',
                                       'ORDER_INST': 'tsp',
                                       'display_name': 'full_name',
                                       'MedUnit': 'dose_unit',
                                       'Dose': 'dose'})
  mo = mo.dropna(subset=['full_name'])
  mo = translate.translate_med_name_to_fid(mo)
  mo = filter_rows.filter_medications(mo)
  # mo = format_data.clean_units(mo, 'fid', 'dose_unit')
  # mo = format_data.clean_values(mo, 'fid', 'dose')
  mo = translate.convert_units(mo, fid_col='fid',
                               fids=['piperacillin_tazobac_dose', 'vancomycin_dose',
                                        'cefazolin_dose', 'cefepime_dose', 'ceftriaxone_dose',
                                        'ampicillin_dose'],
                               unit_col='dose_unit', from_unit='g', to_unit='mg',
                               value_col='dose', convert_func=translate.g_to_mg)

  mo = derive.combine(mo, 'crystalloid_fluid',
                      crystalloid_fluid_fids, keep_originals=False)

  mo = derive.combine(mo, 'cms_antibiotics',
                      cms_antibiotics_fids, keep_originals=False)

  mo = derive.combine(mo, 'vasopressors_dose',
                      vasopressors_fids, keep_originals=False)

  # mo = format_data.threshold_values(mo, 'dose')
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
  return log_time(log, 'pull_med_orders', start, extracted, loaded)

async def pull_medication_admin(connection, dataset_id, fids, log, is_plan, clarity_workspace):
  start = time.time()
  log.info('Entering Medication Administration Processing')
  cms_antibiotics_fids = [med['fid'] for med in med_regex if 'part_of' in med and 'cms_antibiotics' in med['part_of']]

  crystalloid_fluid_fids = [med['fid'] for med in med_regex if
                'part_of' in med and 'crystalloid_fluid' in med['part_of']]

  vasopressors_fids = [med['fid'] for med in med_regex if 'part_of' in med and 'vasopressors_dose' in med['part_of']]
  all_fids = cms_antibiotics_fids + crystalloid_fluid_fids + vasopressors_fids
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
  ma = await async_read_df(sql, connection)

  if ma is None:
    return 0
  extracted = ma.shape[0]
  ma = restructure.select_columns(ma, {'enc_id': 'enc_id',
                                       'display_name': 'full_name',
                                       'Dose': 'dose_value',
                                       'MedUnit': 'dose_unit',
                                       'INFUSION_RATE': 'rate_value',
                                       'MAR_INF_RATE_UNIT': 'rate_unit',
                                       'TimeActionTaken': 'tsp',
                                       'ActionTaken': 'action'})

  log.info('pull_medication_admin translate_med_name_to_fid')
  ma = translate.translate_med_name_to_fid(ma)

  log.info('pull_medication_admin filter_medications')
  ma = filter_rows.filter_medications(ma)

  log.info('pull_medication_admin filter_stopped_medications')
  ma = filter_rows.filter_stopped_medications(ma)

  log.info('pull_medication_admin converting g to mg')
  ma = translate.convert_units(ma,
                               fid_col='fid',
                               fids=['piperacillin_tazobac_dose', 'vancomycin_dose',
                                   'cefazolin_dose', 'cefepime_dose', 'ceftriaxone_dose',
                                   'ampicillin_dose'],
                               unit_col='dose_unit', from_unit='g', to_unit='mg',
                               value_col='dose_value', convert_func=translate.g_to_mg)

  log.info('pull_medication_admin converting ml/hr to ml')
  ma = translate.convert_units(ma,
                               fid_col='fid',
                               fids=crystalloid_fluid_fids,
                               unit_col='dose_unit', from_unit='mL/hr', to_unit='mL',
                               value_col='dose_value', convert_func=translate.ml_per_hr_to_ml_for_1hr)

  log.info('pull_medication_admin combining')

  ma = derive.combine(ma, 'crystalloid_fluid', crystalloid_fluid_fids, keep_originals=False)
  ma = derive.combine(ma, 'cms_antibiotics', cms_antibiotics_fids, keep_originals=False)
  ma = derive.combine(ma, 'vasopressors_dose',
                      vasopressors_fids, keep_originals=False)

  ma = format_data.threshold_values(ma, 'dose_value')

  ma = ma.loc[ma['fid'].apply(lambda x: x in ['cms_antibiotics', 'crystalloid_fluid', 'vasopressors_dose'])]

  ma['confidence'] = 2

  log.info('pull_medication_admin loading')

  if not is_plan:
    for idx, row in ma.iterrows():
      # NOTE: allow nan values here because some medication has no values, i.e., lactated ringers infusion
      await load_row.upsert_t(connection,
                              [row['enc_id'], row['tsp'], row['fid'], str(row['dose_value']), row['confidence']],
                  dataset_id=dataset_id)
    log.info('Medication Order Write complete')
  else:
    log.info('Medication Order Upsert skipped, due to plan mode')
  loaded = ma.shape[0]
  return log_time(log, 'pull_medication_admin', start, extracted, loaded)

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
  labs = await async_read_df(sql, connection)
  if labs is None:
    return 0
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
      await load_row.upsert_t(connection,
                              [row['enc_id'], row['tsp'], 'bands', str(row['value']), row['confidence']],
                              dataset_id=dataset_id)
    log.info('Bands Write complete')
  else:
    log.info('Bands write skipped, due to plan mode')
  loaded = labs.shape[0]
  return log_time(log, 'bands', start, extracted, loaded)

async def pull_lvef(connection, dataset_id, fids, log, is_plan, clarity_workspace):
    log.info("Entering LVEF Processing")
    start = time.time()

    def find_text(dt, pattern, index=None, start_adjust = 0, end_adjust = 50):
        for k in range(0, len(dt['narrative'])):
            imd = ' '.join(filter(None, dt[1][k: k + 2]))    # combine mutiple lines
            if re.search(pattern, imd, re.I):                # if combined line is not null
                match = re.finditer(pattern, imd, re.I)      # find all matched patterns
                for x in match:
                    start = x.start() + start_adjust         # adjust starting point
                    end = x.end() + end_adjust               # adjust ending point
                    string = imd[start: end]                 # extract target sentence
                    result = re.findall(r'(?=>)?\d\d\s*\.?\s*\d*(?=(?:%|percent))?', string)
                    if len(result) > 0:
                        result = "".join(result[0].split())  # taking care of split numbers
                        return [dt['enc_id'], dt['tsp'], dt['order_proc_id'], k + 1, imd, result, string, dt['order_name']]
        return False

    dict_adj = {"Mildly" : 40, "mildly" : 40, "Moderately" : 30, "moderately" : 30, "Severely" : 20, "severely" : 20,
                "Hyperdynamic" : 70, "hyperdynamic" : 70, "Normal" : 55, "normal" : 55, "Low normal" : 50, "low normal" : 50}

    def find_num(dt, pattern, index = None, start_adjust = 0, end_adjust = 50):
        for k in range(0, len(dt['narrative'])):
            imd = ' '.join(filter(None, dt[1][k: k + 2]))   # combine mutiple lines
            if re.search(pattern, imd, re.I):               # if combined line is not null
                match = re.finditer(pattern, imd, re.I)     # find all matched patterns
                for x in match:
                    start = x.start() + start_adjust        # adjust starting point
                    end = x.end() + end_adjust              # adjust ending point
                    string = imd[start : end]               # extract target sentence
                    result = re.findall(r'(?=>)?\d\d\s*\.?\s*\d*(?=(?:%|percent))?', string)
                    if len(result) > 0:
                        result = "".join(result[0].split()) # taking care of split numbers
                        return [dt['enc_id'], dt['tsp'], dt['order_proc_id'], k + 1, imd, result, index, dt['order_name']]
        return False

    def lvef_find(dt):
        text = ' '.join(filter(None, dt['narrative']))
        nuclear_stress_test = re.search(r'perfusion\s*imag.*', text, re.I)
        if nuclear_stress_test: return [dt['enc_id'], dt['tsp'], dt['order_proc_id'], None, None, None, None, dt['order_name']]
        # Find EF first by number
        result = find_num(dt, r'LV EF \(biplane\):|EF(\s*\w+\s*)?estimated|EF(\w*\s*)?estimated', 'da')
        if result:
            return result
        result = find_num(dt, r' EF |LVEF:?| LV(\s*\w+\s*)?EF ', 'di')
        if result:
            return result
        result = find_num(dt, r' ejection fraction.*%*', 'la')
        if result:
            return result
        # EF is not explicitly written down, search for systolic function
        result = find_text(dt, r'left\s*ventricular\s*(ejection|systolic)?\s*(fraction|function)', 'ta', 20, 20)
        if result:
            return result
        result = find_text(dt, r'left\s+(ventricle|ventricular)(\s+\w*){0,5}\s+function(\s+is)?', 'tb', 20, 35)
        if result:
            return result
        return [dt['enc_id'], dt['tsp'], dt['order_proc_id'], None, None, None, None, dt['order_name']]

    lvef_sql = """with ids as
    (select distinct order_proc_id from {ws}.order_narrative where narrative ilike '%%echo%%'
    and order_proc_id in (select distinct order_proc_id from {ws}.order_narrative
    where (narrative ilike '%%ventricular%%function%%' or narrative ilike '%%ventricular%%fraction%%'
    or narrative ilike '%%ejection%%fraction%%' or narrative ilike '%%lvef%%'
    or narrative ilike '%%right%%ventricular%%function%%'))),
    data as
    (select * from {ws}.order_narrative where order_proc_id in (select distinct order_proc_id from ids) order by order_proc_id, line)
    select csn_id, enc_id, order_display_name, order_proc_id, line, narrative, contact_date from data inner join pat_enc on data.csn_id = pat_enc.visit_id
    where pat_enc.dataset_id = {dataset} order by order_proc_id, line"""
    lvef_df = await async_read_df(lvef_sql.format(ws=clarity_workspace, dataset = dataset_id), connection)
    extracted = lvef_df.shape[0]
    order_proc_id = lvef_df.loc[0]['order_proc_id']
    date = lvef_df.loc[0]['contact_date']
    enc_id = lvef_df.loc[0]['enc_id']
    order_name = lvef_df.loc[0]['order_display_name']
    para = []
    count = 0
    lvef_final = pd.DataFrame(columns=['order_proc_id', 'narrative', 'enc_id', 'tsp', 'order_name'])
    for i, j in lvef_df.iterrows():
        if j[3] == order_proc_id:
            para.append(j[5])
        else:
            lvef_final.loc[count] = [order_proc_id, para, enc_id, str(date), order_name]
            # log.info(type(lvef_final.loc[count]['tsp']))
            count += 1
            order_proc_id = j[3]
            para = [j[5]]
            date = j[6]
            enc_id = j[1]
            order_name = j[2]

    lvef_dt = lvef_final.apply(lvef_find, axis=1)

    if not is_plan:
        for i in lvef_dt:
            json_dump = json.dumps(
                {'order_proc_id': i[2], 'line_number': i[3], 'text': i[4], 'value': i[5], 'index': i[6],
                 'proc_name': i[7]})
            await load_row.upsert_t(connection,
                                    [i[0], pd.to_datetime(i[1]), 'lvef', json_dump, 2],
                                    dataset_id=dataset_id, log=log)
        log.info('LVEF write complete')
    else:
        log.info('LVEF write skipped, due to plan mode')
    loaded = len(lvef_dt)
    return log_time(log, 'lvef', start, extracted, loaded)


async def pull_rvf(connection, dataset_id, fids, log, is_plan, clarity_workspace):
    log.info("Entering RVF Processing")
    start = time.time()

    def find_text(dt, pattern, index=None, start_adjust=10, end_adjust=50):
        for k in range(0, len(dt['narrative'])):
            imd = ' '.join(filter(None, dt[1][k - 1: k + 4]))  # combine mutiple lines
            if imd and re.search(r'([,\.]?RV[,\.]?|right ventri[a-z]*) .* function', imd,
                                re.I):  # if combined line is not null
                match = re.finditer(pattern, imd, re.I)  # find all matched patterns
                for x in match:
                    start = (x.start() - start_adjust) if (x.start() - start_adjust) >= 0 else 0  # adjust starting point
                    end = x.end() + end_adjust  # adjust ending point
                    string = imd[start: end]  # extract target sentence
                    results = re.findall(r'(mildly|severely|moderately)?(?:\s)*(hyperdynamic|reduced|diminished|decreased|depressed|low normal|normal)', string, re.I)
                    if len(results) > 0:
                        result = " ".join(results[0]).lower().strip()
                        return [dt['enc_id'], dt['tsp'], dt['order_proc_id'], k + 1, imd, result, string, dt['order_name'], mapping_fun.get(result)]
        return None


    mapping_fun = {"hyperdynamic" : 0, "normal" : 1, "low normal" : 2,
                "mildly reduced" : 3, "mildly diminished" : 3, "mildly decreased" : 3,
                "reduced" : 4, "depressed" : 4, "decreased" : 4,
                "moderately decreased" : 5,"moderately diminished" : 5, "moderately reduced" : 5,
                "severely decreased" : 5,"severely diminished" : 5, "severely reduced" : 5}

    def rvf_find(dt):
        # Exclude nuclear stress test
        text = ' '.join(filter(None, dt['narrative']))
        nuclear_stress_test = re.search(r'perfusion\s*imag.*', text, re.I)
        if nuclear_stress_test:
            return [dt['enc_id'], dt['tsp'], dt['order_proc_id'], None, None, None, None, dt['order_name'], None]

        # Check one pattern by one pattern
        result = find_text(dt, r'(\w+\s+){1,2}right\s*ventricular\s*(global)?\s*systolic\s*function', 'RV', 0, 20)
        if result:
            return result
        result = find_text(dt, r'([,\.]?RV[,\.]?|right\s+(ventricle|ventricular))(\s+\w*){0,5}\s+function(\s+is)?', 'RV is', 10, 25)
        if result:
            return result
        return [dt['enc_id'], dt['tsp'], dt['order_proc_id'], None, None, None, None, dt['order_name'], None]

    rvf_sql = """with ids as
    (select distinct order_proc_id from {ws}.order_narrative where narrative ilike '%%echo%%'
    and order_proc_id in (select distinct order_proc_id from {ws}.order_narrative
    where (narrative ilike '%%ventricular%%function%%' or narrative ilike '%%ventricular%%fraction%%'
    or narrative ilike '%%ejection%%fraction%%' or narrative ilike '%%lvef%%'
    or narrative ilike '%%right%%ventricular%%function%%'))),
    data as
    (select * from {ws}.order_narrative where order_proc_id in (select distinct order_proc_id from ids) order by order_proc_id, line)
    select csn_id, enc_id, order_display_name, order_proc_id, line, narrative, contact_date from data inner join pat_enc on data.csn_id = pat_enc.visit_id
    where pat_enc.dataset_id = {dataset} order by order_proc_id, line"""
    rvf_df = await async_read_df(rvf_sql.format(ws=clarity_workspace, dataset = dataset_id), connection)
    log.info("Finished reading RVF data from clarity")
    extracted = rvf_df.shape[0]
    order_proc_id = rvf_df.loc[0]['order_proc_id']
    date = rvf_df.loc[0]['contact_date']
    enc_id = rvf_df.loc[0]['enc_id']
    order_name = rvf_df.loc[0]['order_display_name']
    para = []
    count = 0
    rvf_final = pd.DataFrame(columns=['order_proc_id', 'narrative', 'enc_id', 'tsp', 'order_name'])
    for i, j in rvf_df.iterrows():
        if j[3] == order_proc_id:
            para.append(j[5])
        else:
            rvf_final.loc[count] = [order_proc_id, para, enc_id, str(date), order_name]
            count += 1
            order_proc_id = j[3]
            para = [j[5]]
            date = j[6]
            enc_id = j[1]
            order_name = j[2]
    log.info("Finished preprocessing RVF data")

    rvf_dt = rvf_final.apply(rvf_find, axis=1)
    if not is_plan:
        log.info("Finished processing RVF data, dumping into JSON")
        for i in rvf_dt:
            json_dump = json.dumps({'order_proc_id': i[2], 'line_number': i[3], 'text': i[4], 'textvalue': i[5], 'proc_name': i[7], 'value': i[8]})
            await load_row.upsert_t(connection,[i[0], pd.to_datetime(i[1]), 'rvf', json_dump, 2], dataset_id = dataset_id, log=log)
        log.info('RVF write complete')
    else:
        log.info('RVF write skipped, due to plan mode')
    loaded = len(rvf_dt)
    return log_time(log, 'rvf', start, extracted, loaded)


async def pull_order_procs(connection, dataset_id, fids, log, is_plan, clarity_workspace):
  log.info("Entering order procs Processing")
  start = time.time()
  extracted = 0
  t_field = 'ParentStartTime'
  sql = """select pe.enc_id, op.*
            from
              {ws}."OrderProcs"  op
            inner join
              pat_enc pe
            on op."CSN_ID"::text=pe.visit_id and pe.dataset_id = {dataset_id} {min_tsp};
        """.format(dataset_id=dataset_id, min_tsp=get_min_tsp(t_field, tsp_with_quotes=True), ws=clarity_workspace, t_field=t_field)

  log.info(sql)
  op = await async_read_df(sql, connection)
  if op is None:
    return 0
  lp_map = await async_read_df("""SELECT * FROM {}.lab_proc_dict;""".format(clarity_workspace), connection)

  if lp_map is None:
    return 0

  log.info('pull_order_procs restructuring %s' % str(op.head(5)))
  op = restructure.select_columns(op, {'enc_id'             : 'enc_id',
                                       'proc_name'          : 'fid',
                                       'ParentStartTime'    : 'tsp',
                                       'OrderStatus'        : 'order_status',
                                       'LabStatus'          : 'proc_status',
                                       'ORDER_TIME'         : 'release_tsp',
                                       'RESULT_TIME'        : 'result_tsp',
                                       'SPECIMN_TAKEN_TIME' : 'collect_tsp',
                                       })

  log.info('pull_order_procs derive_lab_status_clarity %s' % str(op.head(5)))
  op = derive.derive_lab_status_clarity(op)
  log.info('pull_order_procs op after derive_lab_status_clarity: %s' % str(op))

  log.info('pull_order_procs declaring get_fid_name_mapping')

  def get_fid_name_mapping(fids, lp_map):
    log.info('pull_order_procs called get_fid_name_mapping with %s %s' % (str(fids), str(lp_map)))
    fid_map = dict()
    for order_fid in fids:
      fid = order_fid[:-6]
      log.info('pull_order_procs get_fid_name_mapping for %s %s' % (order_fid, fid))

      found = False
      for pfid, codes in lab_procedures_clarity.procedure_ids:
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
  op['name'] = op.fid
  for fid, names in fid_map.items():
    for name in names:
      op.loc[op['fid'] == name, 'fid'] = fid

  log.info('pull_order_procs creating op')

  op = op[[x in fid_map.keys() for x in op['fid']]]
  log.info('op debug: {}'.format(op))
  # op = op.loc[op['fid'].apply(lambda x: x in fids)]
  op['fid'] += '_order'
  op['confidence'] = 2

  log.info('pull_order_procs loading: %s' % str(op))

  if not is_plan:
    for idx, row in op.iterrows():
      if row['tsp'] and not str(row['tsp']) == 'NaT':
        value = {
          'name'          : row['name'],
          'status'        : row['status'],
          'order_tsp'     : str(row['tsp']),
          'release_tsp'   : str(row['release_tsp']),
          'result_tsp'    : str(row['result_tsp']),
          'collect_tsp'   : str(row['collect_tsp'])
        }
        await load_row.upsert_t(connection, [row['enc_id'], row['tsp'], row['fid'], json.dumps(value), row['confidence']], log=log, dataset_id=dataset_id)
    log.info('Order Procs Write complete')
  else:
    log.info('Order Procs write skipped, due to plan mode')
  loaded = op.shape[0]
  return log_time(log, 'pull_order_procs', start, extracted, loaded)

async def notes(connection, dataset_id, fids, log, is_plan, clarity_workspace):
  log.info("Entering Notes Processing")
  start = time.time()
  sql = '''
  insert into cdm_notes(dataset_id, enc_id, note_id, note_type, note_status, note_body, dates, providers)
  select  %(dataset_id)s as dataset_id,
      PE.enc_id as enc_id,
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
  group by PE.enc_id, "NOTE_ID", "NoteType", "NoteStatus", "CREATE_INSTANT_DTTM"
  on conflict (dataset_id, enc_id, note_id, note_type, note_status) do update
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

  return log_time(log, 'note', start, '[status: %s]' % status, '[status: %s]' % status)

async def split_vytorin(connection, dataset_id, fids, log, is_plan, clarity_workspace):
    log.info("Entering Vytorin Split Processing")

    start = time.time()
    query = """select * from {ws}."MedicationAdministration" inner join pat_enc on {ws}."MedicationAdministration"."CSN_ID" = pat_enc.visit_id
    WHERE "display_name" ilike '%%vytorin%%'
    AND NOT "display_name" ILIKE ANY(array['%%gel%%','%%vaginal%%','%%cream%%','%%ophthalmic%%','%%ointment%%','%%nebulizer%%','%%drop%%'])
    AND pat_enc.dataset_id = {dataset}
    """.format(ws=clarity_workspace, dataset = dataset_id)

    sql_dt = await async_read_df(query, connection)
    log.info("Finished reading Vytorin data")
    results = []

    for ix, dt in sql_dt.iterrows():
        fid_list = re.findall(r'ezetimibe|simvastatin|pravastatin', dt['display_name'], re.I)
        dosage_list = re.findall(r'(\d+\.?\d{0,2})', dt['display_name'], re.I)
        for j in [0, 1]:
            fid = "{}_dose".format(fid_list[j]).lower()
            dose = dosage_list[j].lower()
            results.append([dataset_id, dt['enc_id'], pd.to_datetime(dt['TimeActionTaken']),
             fid, str(dt['ORDER_INST']), dt['ActionTaken'], dose])
    log.info("Finished processing Vytorin data")

    if not is_plan:
        log.info("Start writing into databaset")
        for i in results:
            json_dump = json.dumps({'order_tsp' : i[4], 'action' : i[5], 'dose' : i[6]})
            await load_row.upsert_t(connection, [i[1], i[2], i[3], json_dump, 2], dataset_id = dataset_id, log = log)
        log.info('Vytorin write in complete')
    else:
        log.info('Vytorin write in skipped, due to plan mode')
    extracted = len(sql_dt)
    loaded = len(results)
    return log_time(log, 'votorin', start, extracted, loaded)

async def split_hydrochlorothiazide(connection, dataset_id, fids, log, is_plan, clarity_workspace):
    log.info("Entering hydrochlorothiazide Split Processing")
    start = time.time()

    query = """select * from {ws}."MedicationAdministration"
    inner join pat_enc on {ws}."MedicationAdministration"."CSN_ID" = pat_enc.visit_id
    WHERE "display_name" ilike 'hydrochlorothiazide%%'
    AND NOT "display_name" ILIKE ANY(array['%%gel%%','%%vaginal%%','%%cream%%','%%ophthalmic%%','%%ointment%%','%%nebulizer%%','%%drop%%'])
    AND pat_enc.dataset_id = {dataset}
    """.format(ws=clarity_workspace, dataset = dataset_id)

    sql_dt = await async_read_df(query, connection)
    log.info("Finished reading Hydrochlorothiazide data")
    results = []

    for ix, dt in sql_dt.iterrows():
        fid_list = re.findall(r'hydrodiuril|MICROZIDE', dt['display_name'], re.I)
        dosage_list = re.findall(r'(\d+\.?\d{0,2})', dt['display_name'], re.I)

        for j in [0]:
            fid = "{}_dose".format(fid_list[j]).lower()
            dose = dosage_list[j].lower()
            results.append([dataset_id, dt['enc_id'], pd.to_datetime(dt['TimeActionTaken']),
             fid, str(dt['ORDER_INST']), dt['ActionTaken'], dose])
    log.info("Finished processing Hydrochlorothiazide data")

    if not is_plan:
        log.info("Start writing into databaset")
        for i in results:
            json_dump = json.dumps({'order_tsp' : i[4], 'action' : i[5], 'dose' : i[6]})
            await load_row.upsert_t(connection, [i[1], i[2], i[3], json_dump, 2], dataset_id = dataset_id, log = log)
        log.info('Hydrochlorothiazide write in complete')
    else:
        log.info('Hydrochlorothiazide write in skipped, due to plan mode')

    extracted = len(sql_dt)
    loaded = len(results)
    return log_time(log, 'hydrochlorothiazide', start, extracted, loaded)


async def split_amlodipine_valsartan(connection, dataset_id, fids, log, is_plan, clarity_workspace):
    log.info("Entering amlodipine_valsartan Split Processing")
    start = time.time()

    query = """select * from {ws}."MedicationAdministration"
    inner join pat_enc on {ws}."MedicationAdministration"."CSN_ID" = pat_enc.visit_id
    WHERE "display_name" ilike 'amlodipine%%valsartan%%EXFORGE%%'
    AND NOT "display_name" ILIKE ANY(array['%%gel%%','%%vaginal%%','%%cream%%','%%ophthalmic%%','%%ointment%%','%%nebulizer%%','%%drop%%'])
    AND pat_enc.dataset_id = {dataset}
    """.format(ws=clarity_workspace, dataset = dataset_id)

    sql_dt = await async_read_df(query, connection)
    log.info("Finished reading amlodipine_valsartan data")
    results = []

    for ix, dt in sql_dt.iterrows():
        fid_list = re.findall(r'amlodipine|valsartan', dt['display_name'], re.I)
        dosage_list = re.findall(r'(\d+\.?\d{0,2})', dt['display_name'], re.I)
        for j in [0, 1]:
            fid = "{}_dose".format(fid_list[j]).lower()
            dose = dosage_list[j].lower()
            results.append([dataset_id, dt['enc_id'], pd.to_datetime(dt['TimeActionTaken']),
                fid, str(dt['ORDER_INST']), dt['ActionTaken'], dose])
    log.info("Finished processing amlodipine_valsartan data")

    if not is_plan:
        log.info("Start writing into databaset")
        for i in results:
            json_dump = json.dumps({'order_tsp' : i[4], 'action' : i[5], 'dose' : i[6]})
            await load_row.upsert_t(connection, [i[1], i[2], i[3], json_dump, 2], dataset_id = dataset_id, log = log)
        log.info('amlodipine_valsartan write in complete')
    else:
        log.info('amlodipine_valsartan write in skipped, due to plan mode')

    extracted = len(sql_dt)
    loaded = len(results)
    return log_time(log, 'amlodipine_valsartan', start, extracted, loaded)


async def split_valsartan_hydrochlorothiazide(connection, dataset_id, fids, log, is_plan, clarity_workspace):
    log.info("Entering valsartan_hydrochlorothiazide Split Processing")
    start = time.time()

    query = """select * from {ws}."MedicationAdministration"
    inner join pat_enc on {ws}."MedicationAdministration"."CSN_ID" = pat_enc.visit_id
    WHERE "display_name" ilike 'valsartan%%hydrochlorothiazide%%'
    AND NOT "display_name" ILIKE ANY(array['%%gel%%','%%vaginal%%','%%cream%%','%%ophthalmic%%','%%ointment%%','%%nebulizer%%','%%drop%%'])
    AND pat_enc.dataset_id = {dataset}
    """.format(ws=clarity_workspace, dataset = dataset_id)

    sql_dt = await async_read_df(query, connection)
    log.info("Finished reading valsartan_hydrochlorothiazide data")
    results = []

    for ix, dt in sql_dt.iterrows():
        fid_list = re.findall(r'hydrochlorothiazide|valsartan', dt['display_name'], re.I)
        dosage_list = re.findall(r'(\d+\.?\d{0,2})', dt['display_name'], re.I)
        for j in [0, 1]:
            fid = "{}_dose".format(fid_list[j]).lower()
            dose = dosage_list[j]
            results.append([dataset_id, dt['enc_id'], pd.to_datetime(dt['TimeActionTaken']),
             fid, str(dt['ORDER_INST']), dt['ActionTaken'], dose])
    log.info("Finished processing valsartan_hydrochlorothiazide data")

    if not is_plan:
        log.info("Start writing into databaset")
        for i in results:
            json_dump = json.dumps({'order_tsp' : i[4], 'action' : i[5], 'dose' : i[6]})
            await load_row.upsert_t(connection, [i[1], i[2], i[3], json_dump, 2], dataset_id = dataset_id, log = log)
        log.info('valsartan_hydrochlorothiazide write in complete')
    else:
        log.info('valsartan_hydrochlorothiazide write in skipped, due to plan mode')

    extracted = len(sql_dt)
    loaded = len(results)
    return log_time(log, 'valsartan_hydrochlorothiazide', start, extracted, loaded)



async def split_candesartan_hydrochlorothiazide(connection, dataset_id, fids, log, is_plan, clarity_workspace):
    log.info("Entering candesartan_hydrochlorothiazide Split Processing")
    start = time.time()

    query = """select * from {ws}."MedicationAdministration"
    inner join pat_enc on {ws}."MedicationAdministration"."CSN_ID" = pat_enc.visit_id
    WHERE "display_name" ilike 'candesartan%%hydrochlorothiazide%%'
    AND NOT "display_name" ILIKE ANY(array['%%gel%%','%%vaginal%%','%%cream%%','%%ophthalmic%%','%%ointment%%','%%nebulizer%%','%%drop%%'])
    AND pat_enc.dataset_id = {dataset}
    """.format(ws=clarity_workspace, dataset = dataset_id)

    sql_dt = await async_read_df(query, connection)
    log.info("Finished reading candesartan_hydrochlorothiazide data")
    results = []

    for ix, dt in sql_dt.iterrows():
        fid_list = re.findall(r'hydrochlorothiazide|candesartan', dt['display_name'], re.I)
        dosage_list = re.findall(r'(\d+\.?\d{0,2})', dt['display_name'], re.I)
        for j in [0, 1]:
            fid = "{}_dose".format(fid_list[j]).lower()
            dose = dosage_list[j]
            results.append([dataset_id, dt['enc_id'], pd.to_datetime(dt['TimeActionTaken']),
             fid, str(dt['ORDER_INST']), dt['ActionTaken'], dose])
    log.info("Finished processing candesartan_hydrochlorothiazide data")

    if not is_plan:
        log.info("Start writing into databaset")
        for i in results:
            json_dump = json.dumps({'order_tsp' : i[4], 'action' : i[5], 'dose' : i[6]})
            await load_row.upsert_t(connection, [i[1], i[2], i[3], json_dump, 2], dataset_id = dataset_id, log = log)
        log.info('candesartan_hydrochlorothiazide write in complete')
    else:
        log.info('candesartan_hydrochlorothiazide write in skipped, due to plan mode')

    extracted = len(sql_dt)
    loaded = len(results)
    return log_time(log, 'candesartan_hydrochlorothiazide', start, extracted, loaded)


async def split_irbesartan_hydrochlorothiazide(connection, dataset_id, fids, log, is_plan, clarity_workspace):
    log.info("Entering irbesartan_hydrochlorothiazide Split Processing")
    start = time.time()

    query = """select * from {ws}."MedicationAdministration"
    inner join pat_enc on {ws}."MedicationAdministration"."CSN_ID" = pat_enc.visit_id
    WHERE "display_name" ilike 'irbesartan%%hydrochlorothiazide%%'
    AND NOT "display_name" ILIKE ANY(array['%%gel%%','%%vaginal%%','%%cream%%','%%ophthalmic%%','%%ointment%%','%%nebulizer%%','%%drop%%'])
    AND pat_enc.dataset_id = {dataset}
    """.format(ws=clarity_workspace, dataset = dataset_id)

    sql_dt = await async_read_df(query, connection)
    log.info("Finished reading irbesartan_hydrochlorothiazide data")
    results = []

    for ix, dt in sql_dt.iterrows():
        fid_list = re.findall(r'hydrochlorothiazide|irbesartan', dt['display_name'], re.I)
        dosage_list = re.findall(r'(\d+\.?\d{0,2})', dt['display_name'], re.I)
        for j in [0, 1]:
            fid = "{}_dose".format(fid_list[j]).lower()
            dose = dosage_list[j]
            results.append([dataset_id, dt['enc_id'], pd.to_datetime(dt['TimeActionTaken']),
             fid, str(dt['ORDER_INST']), dt['ActionTaken'], dose])

    log.info("Finished processing irbesartan_hydrochlorothiazide data")
    if not is_plan:
        log.info("Start writing into databaset")
        for i in results:
            json_dump = json.dumps({'order_tsp' : i[4], 'action' : i[5], 'dose' : i[6]})
            await load_row.upsert_t(connection, [i[1], i[2], i[3], json_dump, 2], dataset_id = dataset_id, log = log)
        log.info('irbesartan_hydrochlorothiazide write in complete')
    else:
        log.info('irbesartan_hydrochlorothiazide write in skipped, due to plan mode')

    extracted = len(sql_dt)
    loaded = len(results)
    return log_time(log, 'irbesartan_hydrochlorothiazide', start, extracted, loaded)


async def split_losartan_hydrochlorothiazide(connection, dataset_id, fids, log, is_plan, clarity_workspace):
    log.info("Entering losartan_hydrochlorothiazide Split Processing")
    start = time.time()

    query = """select * from {ws}."MedicationAdministration"
    inner join pat_enc on {ws}."MedicationAdministration"."CSN_ID" = pat_enc.visit_id
    WHERE "display_name" ilike 'losartan%%hydrochlorothiazide%%'
    AND NOT "display_name" ILIKE ANY(array['%%gel%%','%%vaginal%%','%%cream%%','%%ophthalmic%%','%%ointment%%','%%nebulizer%%','%%drop%%'])
    AND pat_enc.dataset_id = {dataset}
    """.format(ws=clarity_workspace, dataset = dataset_id)

    sql_dt = await async_read_df(query, connection)
    log.info("Finished reading losartan_hydrochlorothiazide data")
    results = []

    for ix, dt in sql_dt.iterrows():
        fid_list = re.findall(r'hydrochlorothiazide|losartan', dt['display_name'], re.I)
        dosage_list = re.findall(r'(\d+\.?\d{0,2})', dt['display_name'], re.I)
        for j in [0, 1]:
            fid = "{}_dose".format(fid_list[j]).lower()
            dose = dosage_list[j]
            results.append([dataset_id, dt['enc_id'], pd.to_datetime(dt['TimeActionTaken']),
             fid, str(dt['ORDER_INST']), dt['ActionTaken'], dose])

    log.info("Finished processing losartan_hydrochlorothiazide data")

    if not is_plan:
        log.info("Start writing into databaset")
        for i in results:
            json_dump = json.dumps({'order_tsp' : i[4], 'action' : i[5], 'dose' : i[6]})
            await load_row.upsert_t(connection, [i[1], i[2], i[3], json_dump, 2], dataset_id = dataset_id, log = log)
        log.info('losartan_hydrochlorothiazide write in complete')
    else:
        log.info('losartan_hydrochlorothiazide write in skipped, due to plan mode')
    extracted = len(sql_dt)
    loaded = len(results)
    return log_time(log, 'losartan_hydrochlorothiazide', start, extracted, loaded)


async def split_telmisartan_amlodipine(connection, dataset_id, fids, log, is_plan, clarity_workspace):
    log.info("Entering telmisartan_amlodipine Split Processing")

    start = time.time()
    query = """select * from {ws}."MedicationAdministration"
    inner join pat_enc on {ws}."MedicationAdministration"."CSN_ID" = pat_enc.visit_id
    WHERE "display_name" ilike 'telmisartan%%amlodipine%%'
    AND NOT "display_name" ILIKE ANY(array['%%gel%%','%%vaginal%%','%%cream%%','%%ophthalmic%%','%%ointment%%','%%nebulizer%%','%%drop%%'])
    AND pat_enc.dataset_id = {dataset}
    """.format(ws=clarity_workspace, dataset = dataset_id)

    sql_dt = await async_read_df(query, connection)
    log.info("Finished reading telmisartan_amlodipine data")
    results = []

    for ix, dt in sql_dt.iterrows():
        fid_list = re.findall(r'telmisartan|amlodipine', dt['display_name'], re.I)
        dosage_list = re.findall(r'(\d+\.?\d{0,2})', dt['display_name'], re.I)
        for j in [0, 1]:
            fid = "{}_dose".format(fid_list[j]).lower()
            dose = dosage_list[j]
            results.append([dataset_id, dt['enc_id'], pd.to_datetime(dt['TimeActionTaken']),
             fid, str(dt['ORDER_INST']), dt['ActionTaken'], dose])

    log.info("Finished processing telmisartan_amlodipine data")

    if not is_plan:
        log.info("Start writing into databaset")
        for i in results:
            json_dump = json.dumps({'order_tsp' : i[4], 'action' : i[5], 'dose' : i[6]})
            await load_row.upsert_t(connection, [i[1], i[2], i[3], json_dump, 2], dataset_id = dataset_id, log = log)
        log.info('telmisartan_amlodipine write in complete')
    else:
        log.info('telmisartan_amlodipine write in skipped, due to plan mode')

    extracted = len(sql_dt)
    loaded = len(results)
    return log_time(log, 'telmisartan_amlodipine', start, extracted, loaded)


async def split_telmisartan_hydrochlorothiazide(connection, dataset_id, fids, log, is_plan, clarity_workspace):
    log.info("Entering telmisartan_hydrochlorothiazide Split Processing")
    start = time.time()

    query = """select * from {ws}."MedicationAdministration"
    inner join pat_enc on {ws}."MedicationAdministration"."CSN_ID" = pat_enc.visit_id
    WHERE "display_name" ilike 'telmisartan%%hydrochlorothiazide%%'
    AND NOT "display_name" ILIKE ANY(array['%%gel%%','%%vaginal%%','%%cream%%','%%ophthalmic%%','%%ointment%%','%%nebulizer%%','%%drop%%'])
    AND pat_enc.dataset_id = {dataset}
    """.format(ws=clarity_workspace, dataset = dataset_id)

    sql_dt = await async_read_df(query, connection)
    log.info("Finished reading telmisartan_hydrochlorothiazide data")
    results = []

    for ix, dt in sql_dt.iterrows():
        fid_list = re.findall(r'telmisartan|hydrochlorothiazide', dt['display_name'], re.I)
        dosage_list = re.findall(r'(\d+\.?\d{0,2})', dt['display_name'], re.I)
        for j in [0, 1]:
            fid = "{}_dose".format(fid_list[j]).lower()
            dose = dosage_list[j]
            results.append([dataset_id, dt['enc_id'], pd.to_datetime(dt['TimeActionTaken']),
             fid, str(dt['ORDER_INST']), dt['ActionTaken'], dose])

    log.info("Finished processing telmisartan_hydrochlorothiazide data")

    if not is_plan:
        log.info("Start writing into databaset")
        for i in results:
            json_dump = json.dumps({'order_tsp' : i[4], 'action' : i[5], 'dose' : i[6]})
            await load_row.upsert_t(connection, [i[1], i[2], i[3], json_dump, 2], dataset_id = dataset_id, log = log)
        log.info('telmisartan_hydrochlorothiazide write in complete')
    else:
        log.info('telmisartan_hydrochlorothiazide write in skipped, due to plan mode')

    extracted = len(sql_dt)
    loaded = len(results)

    return log_time(log, 'telmisartan_hydrochlorothiazide', start, extracted, loaded)

