import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl
from etl.load.primitives.tbl.derive_helper import *

async def septic_shock_iii_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental, cdm_t_target, cdm_t_lookbackhours):
  # UPDATE 8/19/2016
  assert fid == 'septic_shock_iii', 'wrong fid %s' % fid
  twf_table_temp = derive_feature_addr[fid]['twf_table_temp']

  await conn.execute(clean_tbl.cdm_twf_clean(fid, value=0, confidence=0, twf_table=twf_table_temp, dataset_id=dataset_id))

  fid_input_items = ['qsofa', 'worst_sofa', 'lactate', 'fluid_resuscitation', 'vasopressor_resuscitation', 'hypotension_intp']
  twf_table_join = get_select_table_joins(fid_input_items, derive_feature_addr, cdm_feature_dict, dataset_id, incremental)
  update_clause = """
  update %(twf_table)s set septic_shock_iii=value from
  (select c1.enc_id, c1.tsp, 1 as value from
      (%(twf_table_join)s) c1
  join (select * from %(cdm_t)s as cdm_t where fid='suspicion_of_infection'
   %(dataset_block)s %(incremental_cdm_t)s) c2
  on c1.enc_id=c2.enc_id and c2.tsp <=c1.tsp and c2.tsp >= c1.tsp - interval '6 hours') c3
  where %(twf_table)s.enc_id=c3.enc_id and %(twf_table)s.tsp=c3.tsp %(dataset_block)s;
  """ % {'twf_table': twf_table_temp,
         'dataset_block': ' and dataset_id = %s' % dataset_id if dataset_id is not None else '',
         'dataset_where_block': ' where dataset_id = %s' % dataset_id if dataset_id is not None else '',
         'twf_table_join': twf_table_join,
         'incremental_cdm_t': incremental_enc_id_in(' and ', 'cdm_t', dataset_id,incremental),
         'cdm_t': cdm_t_target}

  log.info("septic_shock_iii_update step one:%s" % update_clause)
  await conn.execute(update_clause)
  twf_table_join_c1 = twf_table_join
  twf_table_join_c2 = twf_table_join

  update_clause = """
  update %(twf_table)s set septic_shock_iii=value  from (select c1.enc_id, c1.tsp, 2 as value , c2.lactate_c|c2.hypotension_intp_c confidence from (
          %(twf_table_join_c1)s
      ) c1
  join (select enc_id, tsp, qsofa, worst_sofa, lactate, fluid_resuscitation, vasopressor_resuscitation, hypotension_intp, lactate_c, hypotension_intp_c from (
        %(twf_table_join_c2)s) source
    ) c2
  on c1.enc_id=c2.enc_id and c2.tsp <=c1.tsp and c2.tsp >= c1.tsp - interval '6 hours'
  where c2.qsofa >= 2 and c2.worst_sofa >= 2 and c2.lactate >2 and c2.lactate_c < 24
  and ((c1.hypotension_intp::integer=1 and c1.hypotension_intp_c < 24 and c1.fluid_resuscitation::integer =1) or c1.vasopressor_resuscitation::integer =1)) c3
  where %(twf_table)s.enc_id=c3.enc_id and %(twf_table)s.tsp=c3.tsp and %(twf_table)s.septic_shock_iii =1 %(dataset_block)s;
  """ % {'twf_table': twf_table_temp,
         'dataset_block': ' and %s.dataset_id = %s' % (twf_table_temp, dataset_id) if dataset_id is not None else '',
         'dataset_where_block': ' where dataset_id = %s' % dataset_id if dataset_id is not None else '',
         'twf_table_join_c1': twf_table_join_c1,
         'twf_table_join_c2': twf_table_join_c2}

  log.info("septic_shock_iii_update step two:%s" % update_clause)
  await conn.execute(update_clause)
