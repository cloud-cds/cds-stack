import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl
from etl.load.primitives.tbl.derive_helper import *
import json


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