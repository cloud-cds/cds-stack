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
             lookback = parameters['lookback_hours'] + ' hours',
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

    assert input_fid[0] == 'acute_heart_failure_icd9_diag' and input_fid[1] == 'acute_heart_failure_icd9_hist' and \
           input_fid[2] == 'acute_heart_failure_icd9_prob' and input_fid[3] == 'furosemide_IV_num_admin', \
        'wrong fid_input %s' % input_fid_string

    acute_sql = """with acute_tbl as (select * from cdm_s where fid ilike '{fid1}' or fid ilike '{fid2}' or fid ilike '{fid3}') select distinct enc_id from acute_tbl""".format(fid1=input_fid[0],
                                                                                                                                                                          fid2=input_fid[1],
                                                                                                                                                                          fid3=input_fid[2])
    furo_sql = """select * from cdm_t where enc_id = {enc_id} and fid ilike '{fid}' and value::integer > 1;"""
    insert_sql = """insert into cdm_s ({dataset_id_block}, enc_id, fid, value, confidence) values ({dataset_id}, {enc_id}, '{fid}', '{value}', {confidence})"""

    acute_df = await conn.fetch(acute_sql)

    for row in acute_df:
        furo_df = await conn.fetch(furo_sql.format(enc_id=row['enc_id'], fid=input_fid[3]))
        if len(furo_df) != 0:
            await conn.execute(insert_sql.format(dataset_id='{}'.format(dataset_id) if dataset_id is not None else '',
                                                 dataset_id_block='dataset_id' if dataset_id is not None else '',
                                                 enc_id=row['enc_id'],
                                                 fid=output_fid,
                                                 value="True",
                                                 confidence=1))

    log.info("fid {fid}:{sql}".format(fid=output_fid, sql=insert_sql))

    return output_fid


# async def calc_cardiogenic_shock2(output_fid, input_fid_string, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):
#
#     assert output_fid == 'cardiogenic_shock', 'output fid should be cardiogenic_shock'
#
#     input_fid = [item.strip() for item in input_fid_string.split(',')]
#
#     assert input_fid[0] == 'sbpm' and input_fid[1] == 'any_inotrope' and \
#            input_fid[2] == 'mech_cardiac_support_device', \
#         'wrong fid_input %s' % input_fid_string
#
#     parameters = json.loads(cdm_feature_dict[output_fid]['function_arguments'])['derive']
#     lookback = timedelta(int(parameters['lookback_hours'])/24.0)
#
#     blood_pressure_sql = """select enc_id, tsp from cdm_twf where {sbpm}::float < 90 order by enc_id, tsp;""".format(sbpm=input_fid[0])
#     ino_mech_sql = """select * from cdm_t where enc_id = {enc_id} and (fid = '{fid1}' or fid = '{fid2}') and (tsp between '{lookback}' and '{lookahead}') and value = 'True' order by tsp;"""
#     insert_sql = """insert into temp2 ({dataset_id_block}, enc_id, tsp, fid, value, confidence) values ({dataset_id}, {enc_id}, '{tsp}', '{fid}', '{value}', {confidence})"""
#     records = await conn.fetch(blood_pressure_sql)
#     prev_pat = records[0]['enc_id']
#     prev_time = records[0]['tsp']
#     for rec in records:
#         shock_check_df = await conn.fetch(ino_mech_sql.format_map({'enc_id': rec['enc_id'], 'lookahead': rec['tsp'] + lookback, 'lookback': rec['tsp'] - lookback, 'fid1': input_fid[1],'fid2': input_fid[2]}))
#         if not len(shock_check_df) == 0:
#             temp_tsp = min(rec['tsp'], shock_check_df[0]['tsp'])
#             if rec['enc_id'] == prev_pat:
#                 if temp_tsp != prev_time:
#                     await conn.execute(
#                         insert_sql.format(dataset_id='{}'.format(dataset_id) if dataset_id is not None else '',
#                                           dataset_id_block='dataset_id' if dataset_id is not None else '',
#                                           enc_id=rec['enc_id'],
#                                           tsp=min(rec['tsp'], shock_check_df[0]['tsp']),
#                                           fid=output_fid, value="True",
#                                           confidence=1))
#                     prev_time = temp_tsp
#             else:
#                 await conn.execute(
#                     insert_sql.format(dataset_id='{}'.format(dataset_id) if dataset_id is not None else '',
#                                       dataset_id_block='dataset_id' if dataset_id is not None else '',
#                                       enc_id=rec['enc_id'],
#                                       tsp=min(rec['tsp'], shock_check_df[0]['tsp']),
#                                       fid=output_fid, value="True",
#                                       confidence=1))
#                 prev_pat = rec['enc_id']
#                 prev_time = temp_tsp
#
#     log.info("fid {fid}:{sql}".format(fid=output_fid, sql=insert_sql))
#
#     return output_fid

async def calc_cardiogenic_shock(output_fid, input_fid_string, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):

    assert output_fid == 'cardiogenic_shock', 'output fid should be cardiogenic_shock'

    input_fid = [item.strip() for item in input_fid_string.split(',')]

    assert input_fid[0] == 'sbpm' and input_fid[1] == 'any_inotrope' and \
           input_fid[2] == 'mech_cardiac_support_device', \
        'wrong fid_input %s' % input_fid_string

    parameters = json.loads(cdm_feature_dict[output_fid]['function_arguments'])['derive']
    lookback = timedelta(int(parameters['lookback_hours'])/24.0)

    select_sql = """with
    ino_tbl as(select * from cdm_t where (fid = '{fid1}' or fid = '{fid2}') and value='True'),
    sbp_tbl as(select enc_id, tsp, sbpm from cdm_twf where {fid0}::float < 90 and enc_id in (select enc_id from cdm_t where (fid = '{fid1}' or fid = '{fid2}')and value='True'))
    select COALESCE(ino_tbl.enc_id, sbp_tbl.enc_id) as enc_id, COALESCE(ino_tbl.tsp, sbp_tbl.tsp) as tsp, fid, sbpm, confidence
    from ino_tbl full join sbp_tbl on ino_tbl.enc_id = sbp_tbl.enc_id and ino_tbl.tsp = sbp_tbl.tsp
    order by enc_id, tsp;"""
    insert_sql = """insert into cdm_t ({dataset_id_block}, enc_id, tsp, fid, value, confidence) values ({dataset_id}, {enc_id}, '{tsp}', '{fid}', '{value}', {confidence})"""
    records = await conn.fetch(select_sql.format(fid0=input_fid[0], fid1=input_fid[1], fid2=input_fid[2]))
    enc_dict = {}
    for rec in records:
        if rec['enc_id'] not in enc_dict and (rec['fid'] == input_fid[1] or rec['fid'] == input_fid[2]):
            enc_dict[int(rec['enc_id'])] = [(rec['tsp'], rec['confidence'])]
        elif rec['fid'] == input_fid[1] or rec['fid'] == input_fid[2]:
            enc_dict[int(rec['enc_id'])].append((rec['tsp'], rec['confidence']))
    for i in range(len(records)):
        enc_flag = 0
        if records[i]['fid'] == input_fid[1] or records[i]['fid'] == input_fid[2]:
            temp_fid = records[i]['fid']
            temp_pos = i
            while temp_fid == input_fid[1] or temp_fid == input_fid[2]:
                if records[temp_pos]['enc_id'] == records[i]['enc_id']:
                    temp_pos -= 1
                    temp_fid = records[temp_pos]['fid']
                else:
                    enc_flag = 1
                    break
            if enc_flag != 1:
                if abs(records[i]['tsp'] - records[temp_pos]['tsp']) < lookback:
                    await conn.execute(
                        insert_sql.format(dataset_id='{}'.format(dataset_id) if dataset_id is not None else '',
                                          dataset_id_block='dataset_id' if dataset_id is not None else '',
                                          enc_id=records[i]['enc_id'],
                                          tsp=records[i]['tsp'],
                                          fid=output_fid, value="True",
                                          confidence=records[i]['confidence']))
        else:
            for rec in enc_dict[records[i]['enc_id']]:
                if records[i]['tsp'] > rec[0] and (records[i]['tsp'] - rec[0]) < lookback:
                    await conn.execute(
                        insert_sql.format(dataset_id='{}'.format(dataset_id) if dataset_id is not None else '',
                                          dataset_id_block='dataset_id' if dataset_id is not None else '',
                                          enc_id=records[i]['enc_id'],
                                          tsp=records[i]['tsp'],
                                          fid=output_fid, value="True",
                                          confidence=rec[1]))
                    break

    log.info("fid {fid}:{sql}".format(fid=output_fid, sql=insert_sql))

    return output_fid
