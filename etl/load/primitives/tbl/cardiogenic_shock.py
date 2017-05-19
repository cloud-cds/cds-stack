import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl
from etl.load.primitives.tbl.derive_helper import *


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

  cdm_t_update = """
  with n_ago_transfusions as (
    select  {n_ago_select} enc_id, tsp, value, confidence,
      extract(epoch from (tsp - (lag(tsp,1) OVER (PARTITION BY {n_ago_partition} enc_id ORDER BY tsp))) )/(60*60) hours_n_ag0
    from
      {source_cdm_t}
    where fid = 'transfuse_rbc' {n_ago_where} )
  INSERT INTO cdm_t ({insert_into} enc_id,tsp,fid,value, confidence) 
  select {insert_select} enc_id, tsp, 'major_blood_loss', True, confidence
  from n_ago_transfusions
  where hours_n_ag0 <= 24;
  """.format(n_ago_select = ' dataset_id, ' if dataset_id is not None else '',
             n_ago_partition = ' dataset_id, ' if dataset_id is not None else '',
             n_ago_where = ' and dataset_id = 1000 ' if dataset_id is not None else '',
             source_cdm_t = source_cdm_t_tbl,
             insert_into = 'dataset_id, ' if dataset_id is not None else '',
             insert_select = 'dataset_id,' if dataset_id is not None else '')

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
    where hemoglobin_change <= -3 and hemoglobin_change_c <= 8 and hemoglobin_minutes_since_measurement/60 <= 24 )
  INSERT INTO cdm_t ({insert_into} enc_id,tsp,fid,value, confidence) 
  select {insert_select} enc_id, tsp, 'major_blood_loss', True, hemoglobin_change_c
  from hemoglobin_loss
  """.format(hemoglobin_loss_select = ' dataset_id, ' if dataset_id is not None else '',
             cdm_twf_proc = select_table_joins,
             insert_into='dataset_id, ' if dataset_id is not None else '',
             insert_select='dataset_id,' if dataset_id is not None else ''
             )

  log.info("major_blood_loss:%s" % twf_update)

  await conn.execute(twf_update)

  return output_fid