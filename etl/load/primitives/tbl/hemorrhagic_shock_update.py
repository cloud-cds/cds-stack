import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl
from etl.load.primitives.tbl.derive_helper import *
'''
  Created: 09/01/2016
  Author: Katie Henry
  Purpose: Define hemorrhagic shock
  Comments: Currently implements definition of traumatic hemorrhagic shock
  See arch file for details
'''
async def hemorrhagic_shock_update(fid, fid_input, conn, log, dataset_id, derive_feature_addr, cdm_feature_dict, incremental):
    assert fid == 'hemorrhagic_shock', 'wrong fid %s' % fid
    fid_input_items = [item.strip() for item in fid_input.split(',')]
    assert fid_input_items[0] == 'transfuse_rbc' \
        and fid_input_items[1] == 'lactate' \
        and fid_input_items[2] == 'sbpm', \
        'wrong fid_input %s' % fid_input
    twf_table_src = get_src_twf_table(derive_feature_addr)
    twf_table_temp = derive_feature_addr[fid]['twf_table_temp']
    await conn.execute(clean_tbl.cdm_twf_clean(fid, value=0, confidence=0, twf_table=twf_table_temp, dataset_id=dataset_id))
    update_clause = """
      update %(twf_table)s SET hemorrhagic_shock = (num_transfusions >=4)::int, hemorrhagic_shock_c=confidence
      from
         (select c1.enc_id, c1.tsp, lactate, sbpm, lactate_c|sbpm_c confidence, count (*) num_transfusions from
        (select enc_id, tsp, lactate, lactate_c, sbpm, sbpm_c from %(twf_table)s
        where sbpm <= 90 and lactate > 2 %(dataset_block)s %(incremental_twf_table)s
        ) c1 join
        (select * from cdm_t where fid='transfuse_rbc' %(dataset_block)s %(incremental_cdm_t)s ) c2
        on c1.enc_id=c2.enc_id and c2.tsp >= c1.tsp and c2.tsp <= c1.tsp + interval '6 hours'
        group by c1.enc_id, c1.tsp, lactate_c, sbpm_c, lactate, sbpm ) c4
      where %(twf_table)s.enc_id=c4.enc_id and %(twf_table)s.tsp=c4.tsp %(dataset_block)s;
    """ % {'twf_table': twf_table_src,
           'twf_table_temp': twf_table_temp,
           'dataset_block': ' and dataset_id = %s' % dataset_id \
              if dataset_id is not None else '',
           'incremental_twf_table': incremental_enc_id_in(' and ', twf_table_src, dataset_id,incremental),
           'incremental_cdm_t': incremental_enc_id_in(' and ', 'cdm_t', dataset_id,incremental)
           }
    log.info("hemorrhagic_shock_update:%s" % update_clause)
    await conn.execute(update_clause)