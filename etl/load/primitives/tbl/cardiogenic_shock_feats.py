import asyncio
from datetime import timedelta

import etl.load.primitives.tbl.clean_tbl as clean_tbl
from etl.load.primitives.tbl.derive_helper import *
import json
import pandas as pd


async def calculate_major_blood_loss(output_fid, input_fid_string, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):
  """
  fid_input should be name of the feature for which change is to be computed
  fid should be <fid of old feather>_change
  """
  # ------------------------------------------------
  # Input Cleaning / Setup
  # ------------------------------------------------

  # Make sure the fid is correct (fid_input can be anything)
  assert output_fid == 'major_blood_loss', 'wrong output fid %s' % output_fid
  assert set([item.strip() for item in input_fid_string.split(',')]) == set(['hemoglobin_change', 'hemoglobin_minutes_since_measurement', 'transfuse_rbc']), 'wrong inputs'


  destination_tbl = derive_feature_addr[output_fid]['twf_table_temp']

  # ------------------------------------------------
  # Remove Existing Output
  # ------------------------------------------------
  await conn.execute(clean_tbl.cdm_t_clean(output_fid, dataset_id=dataset_id, incremental=incremental))

  # ------------------------------------------------
  # Handle Update from cdm_t
  # ------------------------------------------------

  source_cdm_t_tbl = 'cdm_t'

  parameters = json.loads(cdm_feature_dict[output_fid]['function_arguments'])['derive']

  cdm_t_update = """
  with n_ago_transfusions as (
    select  {n_ago_select} enc_id, tsp, value, confidence,
      extract(epoch from (tsp - (lag(tsp,{lag_num}) OVER (PARTITION BY {n_ago_partition} enc_id ORDER BY tsp))) )/(60*60) hours_n_ag0
    from
      {source_cdm_t}
    where fid = 'transfuse_rbc' {n_ago_where} )
  INSERT INTO cdm_t ({insert_into} enc_id,tsp,fid,value, confidence) 
  select {insert_select} enc_id, tsp, 'major_blood_loss', True, confidence
  from n_ago_transfusions
  where hours_n_ag0 <= {lookback_hours};
  """.format(n_ago_select = ' dataset_id, ' if dataset_id is not None else '',
             n_ago_partition = ' dataset_id, ' if dataset_id is not None else '',
             n_ago_where = ' and dataset_id = {} '.format(dataset_id) if dataset_id is not None else '',
             source_cdm_t = source_cdm_t_tbl,
             insert_into = 'dataset_id, ' if dataset_id is not None else '',
             insert_select = 'dataset_id,' if dataset_id is not None else '',
             lookback_hours = parameters['lookback_hours'],
             lag_num = int(parameters['num_doses'])-1)

  log.info("major_blood_loss:%s" % cdm_t_update)

  await conn.execute(cdm_t_update)

  # ------------------------------------------------
  # Handle Update from cdm_twf
  # ------------------------------------------------

  select_table_joins = get_select_table_joins(['hemoglobin_change', 'hemoglobin_minutes_since_measurement'], derive_feature_addr, cdm_feature_dict, dataset_id, incremental)

  twf_update = """
  with cdm_twf_proc as (
    {cdm_twf_proc}
  ),
  hemoglobin_loss as (
    select 
      {hemoglobin_loss_select}
      enc_id, 
      tsp, 
      hemoglobin_change, 
      hemoglobin_minutes_since_measurement/60  as last_meas_time,
      hemoglobin_change_c
    from 
      cdm_twf_proc 
    where hemoglobin_change <= -{hem_loss} and hemoglobin_change_c <= 8 and hemoglobin_minutes_since_measurement/60 <= {lookback_hours} )
  INSERT INTO cdm_t ({insert_into} enc_id,tsp,fid,value, confidence) 
  select {insert_select} enc_id, tsp, 'major_blood_loss', True, hemoglobin_change_c
  from hemoglobin_loss
  on conflict do NOTHING
  """.format(hemoglobin_loss_select = ' dataset_id, ' if dataset_id is not None else '',
             cdm_twf_proc = select_table_joins,
             insert_into='dataset_id, ' if dataset_id is not None else '',
             insert_select='dataset_id,' if dataset_id is not None else '',
             lookback_hours=parameters['lookback_hours'],
             hem_loss=parameters['min_hemoglobin_loss']
             )

  log.info("major_blood_loss:%s" % twf_update)

  await conn.execute(twf_update)

  return output_fid

async def calc_num_administrations(output_fid, input_fid_string, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):

  name_prefix = input_fid_string[:-5] # removes _dose
  # input should looks like something _X_dose, and output should look like X_num_admin
  assert output_fid == '%s_num_admin' % name_prefix,\
    'input fid should be X_dose, and output FID should be X_num_admin {output_fid} {input_fid_string}'.\
      format(output_fid=output_fid, input_fid_string=input_fid_string)


  # ------------------------------------------------
  # Remove Existing Output
  # ------------------------------------------------
  await conn.execute(clean_tbl.cdm_t_clean(output_fid, dataset_id=dataset_id, incremental=incremental))

  parameters = json.loads(cdm_feature_dict[output_fid]['function_arguments'])['derive']

  sql = """
  with 
    administrations as (
    select enc_id, tsp, confidence
    from cdm_t
    where  {dataset_id_where} fid = '{input_fid}' and value::json #>>'{{action}}'='Given'
    ),
    num_administrations as (
      select lh.enc_id, lh.tsp, first(lh.confidence) as confidence, count(distinct rh.tsp) as num_administrations
      from
        administrations lh
        left join
        administrations rh
        on lh.enc_id = rh.enc_id and rh.tsp between lh.tsp - '{lookback}'::interval and lh.tsp
      group by lh.enc_id, lh.tsp 
    )
  INSERT INTO cdm_t ({dataset_id_block} enc_id, tsp,   fid,          value, confidence) 
  select              {insert_const}    enc_id, tsp, '{output_fid}', num_administrations, confidence
  from num_administrations 
  """.format(input_fid = input_fid_string,
             lookback = str(parameters['lookback_hours']) + ' hours',
             output_fid = output_fid,
             dataset_id_block = 'dataset_id,' if dataset_id is not None else '',
             dataset_id_where = 'dataset_id = {} and '.format(dataset_id) if  dataset_id is not None else '',
             insert_const = '{},'.format(dataset_id) if  dataset_id is not None else '',
             )

  log.info("fid {fid}:{sql}".format(fid=output_fid,sql=sql))

  await conn.execute(sql)


  return output_fid


async def calc_acute_heart_failure(output_fid, input_fid_string, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):

    assert output_fid == 'acute_heart_failure', 'output fid should be acute_heart_failure'

    input_fid = [item.strip() for item in input_fid_string.split(',')]

    assert input_fid[0] == 'acute_heart_failure_icd9_diag' and \
           input_fid[1] == 'acute_heart_failure_icd9_prob' and \
           input_fid[2] == 'furosemide_IV_num_admin' and \
           input_fid[3] == 'bumetanide_IV_num_admin'and \
           input_fid[4] == 'hosp_admit', \
           'wrong fid_input %s' % input_fid_string

    # ------------------------------------------------
    # Remove Existing Output
    # ------------------------------------------------
    await conn.execute(clean_tbl.cdm_s_clean(output_fid, dataset_id=dataset_id, incremental=incremental))

    select_sql = """with
    acute_df as
    (with acute_tbl as
    (select * from cdm_s where {dataset_id_where} (fid ilike 'acute_heart_failure_icd9_diag' or fid ilike 'acute_heart_failure_icd9_prob')
    and enc_id in (select enc_id from cdm_s where fid ilike 'hosp_admit'))
    select distinct enc_id from acute_tbl),
    furo_df as
    (select * from cdm_t where {dataset_id_where} (fid ilike 'furosemide_IV_num_admin' or fid ilike 'bumetanide_IV_num_admin') and value::integer > 1),
    final as
    (select {furo_dataset} coalesce(acute_df.enc_id, furo_df.enc_id) as enc_id, furo_df.fid, furo_df.value, furo_df.confidence from furo_df
    inner join acute_df on furo_df.enc_id = acute_df.enc_id
    order by enc_id, tsp)
    select * from final;"""
    insert_sql = """insert into cdm_s ({dataset_id_block} enc_id, fid, value, confidence) values ({dataset_id} {enc_id}, '{fid}', '{value}', {confidence})"""

    select_df = await conn.fetch(select_sql.format(dataset_id_where = 'dataset_id = {} and '.format(dataset_id) if  dataset_id is not None else '',
                                                   furo_dataset='furo_df.dataset_id,' if dataset_id is not None else ''))

    curr_enc = -1
    for row in select_df:
        if curr_enc != row['enc_id']:
            await conn.execute(insert_sql.format(dataset_id='{},'.format(dataset_id) if dataset_id is not None else '',
                                                 dataset_id_block='dataset_id,' if dataset_id is not None else '',
                                                 enc_id=row['enc_id'],
                                                 fid=output_fid,
                                                 value="True",
                                                 confidence=row['confidence']))
        curr_enc = row['enc_id']

    log.info("fid {fid}:{sql}".format(fid=output_fid, sql=insert_sql))

    return output_fid


async def calc_cardiogenic_shock(output_fid, input_fid_string, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):

    assert output_fid == 'cardiogenic_shock', 'output fid should be cardiogenic_shock'

    input_fid = [item.strip() for item in input_fid_string.split(',')]

    assert input_fid[0] == 'sbpm' and input_fid[1] == 'any_inotrope' and input_fid[2] == 'acute_heart_failure' and \
           input_fid[3] == 'mech_cardiac_support_device', \
        'wrong fid_input %s' % input_fid_string

    # ------------------------------------------------
    # Remove Existing Output
    # ------------------------------------------------
    await conn.execute(clean_tbl.cdm_t_clean(output_fid, dataset_id=dataset_id, incremental=incremental))

    parameters = json.loads(cdm_feature_dict[output_fid]['function_arguments'])['derive']
    lookback = timedelta(int(parameters['lookback_hours'])/24.0)

    select_sql = """with
    ino_tbl as(select * from cdm_t where {dataset_id_where} (fid = 'any_inotrope' or fid = 'mech_cardiac_support_device') and value='True' and enc_id in (select distinct enc_id from cdm_s where fid ilike 'acute_heart_failure') and enc_id in (select distinct enc_id from cdm_twf where sbpm::float < 90)),
    sbp_tbl as(select enc_id, tsp, sbpm from cdm_twf where {dataset_id_where} sbpm::float < 90 and enc_id in (select enc_id from cdm_t where {dataset_id_where} (fid = 'any_inotrope' or fid = 'mech_cardiac_support_device') and value='True') and enc_id in (select distinct enc_id from cdm_s where fid ilike 'acute_heart_failure'))
    select COALESCE(ino_tbl.enc_id, sbp_tbl.enc_id) as enc_id, COALESCE(ino_tbl.tsp, sbp_tbl.tsp) as tsp, fid, sbpm, confidence
    from ino_tbl full join sbp_tbl on ino_tbl.enc_id = sbp_tbl.enc_id and ino_tbl.tsp = sbp_tbl.tsp
    order by enc_id, tsp;"""
    insert_sql = """insert into cdm_t ({dataset_id_block} enc_id, tsp, fid, value, confidence) values ({dataset_id} {enc_id}, '{tsp}', '{fid}', '{value}', {confidence})"""
    records = await conn.fetch(select_sql.format(dataset_id_where = 'dataset_id = {} and '.format(dataset_id) if  dataset_id is not None else ''))
    enc_dict = {}
    for rec in records:
        if rec['enc_id'] not in enc_dict and (rec['fid'] == input_fid[1] or rec['fid'] == input_fid[3]):
            enc_dict[int(rec['enc_id'])] = [(rec['tsp'], rec['confidence'])]
        elif rec['fid'] == input_fid[1] or rec['fid'] == input_fid[3]:
            enc_dict[int(rec['enc_id'])].append((rec['tsp'], rec['confidence']))
    for i in range(len(records)):
        enc_flag = 0
        if records[i]['fid'] == input_fid[1] or records[i]['fid'] == input_fid[3]:
            temp_fid = records[i]['fid']
            temp_pos = i
            while temp_fid == input_fid[1] or temp_fid == input_fid[3]:
                if records[temp_pos]['enc_id'] == records[i]['enc_id']:
                    temp_pos -= 1
                    temp_fid = records[temp_pos]['fid']
                else:
                    enc_flag = 1
                    break
            if enc_flag != 1:
                if abs(records[i]['tsp'] - records[temp_pos]['tsp']) < lookback:
                    await conn.execute(
                        insert_sql.format(dataset_id='{},'.format(dataset_id) if dataset_id is not None else '',
                                          dataset_id_block='dataset_id,' if dataset_id is not None else '',
                                          enc_id=records[i]['enc_id'],
                                          tsp=records[i]['tsp'],
                                          fid=output_fid, value="True",
                                          confidence=records[i]['confidence']))
        else:
            for rec in enc_dict[records[i]['enc_id']]:
                if records[i]['tsp'] > rec[0] and (records[i]['tsp'] - rec[0]) < lookback:
                    await conn.execute(
                        insert_sql.format(dataset_id='{},'.format(dataset_id) if dataset_id is not None else '',
                                          dataset_id_block='dataset_id,' if dataset_id is not None else '',
                                          enc_id=records[i]['enc_id'],
                                          tsp=records[i]['tsp'],
                                          fid=output_fid, value="True",
                                          confidence=rec[1]))
                    break

    log.info("fid {fid}:{sql}".format(fid=output_fid, sql=insert_sql))

    return output_fid


async def code_doc_note_update(output_fid, input_fid_string, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):

    assert output_fid == 'code_doc_note', 'output fid should be code_doc_note'

    input_fid = [item.strip() for item in input_fid_string.split(',')]

    assert input_fid[0] == 'hosp_admsn_time' and input_fid[1] == 'discharge', \
        'wrong fid_input %s' % input_fid_string

    # ------------------------------------------------
    # Remove Existing Output
    # ------------------------------------------------
    await conn.execute(clean_tbl.cdm_t_clean(output_fid, dataset_id=dataset_id, incremental=incremental))

    select_sql = """with tbl1 as (select pat_id, note_id, note_type, note_status, (dates::json #>>'{{create_instant_dttm}}')::timestamptz as create_tsp from cdm_notes where {dataset_id_where} note_type ilike 'code documentation' order by pat_id, note_id),
    tbl2 as (select pat_id, enc_id from pat_enc where {dataset_id_where} pat_id in (select distinct pat_id from cdm_notes where {dataset_id_where} note_type ilike 'code documentation')),
    tbl3 as (select enc_id, value::timestamptz as admsn_time from cdm_s where {dataset_id_where} fid ilike 'hosp_admsn_time' and enc_id in (select enc_id from pat_enc where {dataset_id_where} pat_id in (select distinct pat_id from cdm_notes where note_type ilike 'code documentation'))),
    tbl4 as (select enc_id, tsp as discharge_time from cdm_t where {dataset_id_where} fid ilike 'discharge' and enc_id in (select enc_id from pat_enc where {dataset_id_where} pat_id in (select distinct pat_id from cdm_notes where note_type ilike 'code documentation'))),
    tbl5 as (select coalesce(tbl3.enc_id, tbl4.enc_id) as enc_id, admsn_time, discharge_time from tbl3 join tbl4 on tbl3.enc_id = tbl4.enc_id),
    tbl6 as (select coalesce(tbl5.enc_id, tbl2.enc_id) as enc_id, pat_id, admsn_time, discharge_time from tbl5 join tbl2 on tbl5.enc_id = tbl2.enc_id)
    select coalesce(tbl1.pat_id, tbl6.pat_id) as pat_id, tbl6.enc_id, note_id, note_type, note_status, create_tsp, admsn_time, discharge_time from tbl1 full join tbl6 on tbl1.pat_id = tbl6.pat_id where tbl1.create_tsp between tbl6.admsn_time and tbl6.discharge_time order by note_id;"""
    select_df = await conn.fetch(select_sql.format(dataset_id_where = 'dataset_id = {} and '.format(dataset_id) if  dataset_id is not None else ''))
    insert_sql = """insert into cdm_t ({dataset_id_block} enc_id, tsp, fid, value, confidence) values ({dataset_id} {enc_id}, '{tsp}', '{fid}', '{value}', {confidence})"""
    distinct = {}
    for i in select_df:
        enc_id = i['enc_id']
        pat_id = i['pat_id']
        note_type = i['note_type']
        note_status = i['note_status']
        tsp = i['create_tsp']
        note_id = i['note_id']
        if enc_id not in distinct:
            distinct[enc_id] = [tsp]
            value = json.dumps({'pat_id': pat_id, 'note_id': note_id, 'note_type': note_type, 'note_status': note_status})
            await conn.execute(insert_sql.format(dataset_id='{},'.format(dataset_id) if dataset_id is not None else '',
                                  dataset_id_block='dataset_id,' if dataset_id is not None else '',
                                  enc_id=enc_id,
                                  tsp=tsp,
                                  fid=output_fid, value=value,
                                  confidence=1))
        else:
            if tsp not in distinct[enc_id]:
                distinct[enc_id].append(tsp)
                value = json.dumps(
                    {'pat_id': pat_id, 'note_id': note_id, 'note_type': note_type, 'note_status': note_status})
                await conn.execute(insert_sql.format(dataset_id='{},'.format(dataset_id) if dataset_id is not None else '',
                                      dataset_id_block='dataset_id,' if dataset_id is not None else '',
                                      enc_id=enc_id,
                                      tsp=tsp,
                                      fid=output_fid, value=value,
                                      confidence=1))

    log.info("fid {fid}:{sql}".format(fid=output_fid, sql=insert_sql))

    return output_fid


async def hosp_admit_update(output_fid, input_fid_string, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):
    assert output_fid == 'hosp_admit', 'output fid should be hosp_admit'

    input_fid = [item.strip() for item in input_fid_string.split(',')]

    assert input_fid[0] == 'discharge', \
        'wrong fid_input %s' % input_fid_string

    # ------------------------------------------------
    # Remove Existing Output
    # ------------------------------------------------
    await conn.execute(clean_tbl.cdm_s_clean(output_fid, dataset_id=dataset_id, incremental=incremental))

    select_sql = """select distinct enc_id from cdm_t
    where {dataset_id_where} (fid ilike 'discharge' and value::json #>> '{{department}}' not ilike '%%emergency%%')
    order by enc_id;"""
    insert_sql = """insert into cdm_s ({dataset_id_block} enc_id, fid, value, confidence) values ({dataset_id} {enc_id}, '{fid}', '{value}', {confidence})"""
    result = await conn.fetch(select_sql.format(dataset_id_where = 'dataset_id = {} and '.format(dataset_id) if  dataset_id is not None else ''))
    for row in result:
        await conn.execute(insert_sql.format(dataset_id='{},'.format(dataset_id) if dataset_id is not None else '',
                                             dataset_id_block='dataset_id,' if dataset_id is not None else '',
                                             enc_id=row['enc_id'],
                                             fid=output_fid,
                                             value="True",
                                             confidence=1))

    log.info("fid {fid}:{sql}".format(fid=output_fid, sql=insert_sql))

    return output_fid
