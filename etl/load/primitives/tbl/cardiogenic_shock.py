import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl
from etl.load.primitives.tbl.derive_helper import *


async def calculate_major_blood_loss(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):
  """
  fid_input should be name of the feature for which change is to be computed
  fid should be <fid of old feather>_change
  """
  # Make sure the fid is correct (fid_input can be anything)
  assert fid == '%s_change' % fid_input, 'wrong fid %s' % fid_input
  destination_tbl = derive_feature_addr[fid]['twf_table_temp']
  source_tbl = derive_feature_addr[fid_input][
    'twf_table_temp'] if fid_input in derive_feature_addr else get_src_twf_table(derive_feature_addr)
  await conn.execute(clean_tbl.cdm_twf_clean(fid, twf_table=destination_tbl, dataset_id=dataset_id))

  sql = ''

  log.info(
    """
            ***************                                                
          *             *                                                
          *             *                                                
          *             *                                                
          *             *                                                
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
          *                                                              
  ***********************         
  """)

  log.info("time_since_last_measured:%s" % sql)
  await conn.execute(sql)