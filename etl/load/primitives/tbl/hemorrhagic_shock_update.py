import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl

'''
  Created: 09/01/2016
  Author: Katie Henry
  Purpose: Define hemorrhagic shock
  Comments: Currently implements definition of traumatic hemorrhagic shock
  See arch file for details
'''
async def hemorrhagic_shock_update(fid, fid_input, conn, log, dataset_id=None,  twf_table='cdm_twf'):
    assert fid == 'hemorrhagic_shock', 'wrong fid %s' % fid
    fid_input_items = [item.strip() for item in fid_input.split(',')]
    assert fid_input_items[0] == 'transfuse_rbc' \
        and fid_input_items[1] == 'lactate' \
        and fid_input_items[2] == 'sbpm', \
        'wrong fid_input %s' % fid_input
    await clean_tbl.cdm_twf_clean(conn, fid,  twf_table=twf_table, dataset_id=dataset_id)
    update_clause = """
      update %(twf_table)s SET hemorrhagic_shock = (num_transfusions >=4)::int, hemorrhagic_shock_c=confidence
      from
         (select c1.enc_id, c1.tsp, lactate, sbpm, lactate_c|sbpm_c confidence, count (*) num_transfusions from
        (select enc_id, tsp, lactate, lactate_c, sbpm, sbpm_c from %(twf_table)s
        where sbpm <= 90 and lactate > 2 %(dataset_block)s
        ) c1 join
        (select * from cdm_t where fid='transfuse_rbc' %(dataset_block)s ) c2
        on c1.enc_id=c2.enc_id and c2.tsp >= c1.tsp and c2.tsp <= c1.tsp + interval '6 hours'
        group by c1.enc_id, c1.tsp, lactate_c, sbpm_c, lactate, sbpm ) c4
      where %(twf_table)s.enc_id=c4.enc_id and %(twf_table)s.tsp=c4.tsp %(dataset_block)s;
    """ % {'twf_table': twf_table, 'dataset_block': ' and dataset_id = %s' % dataset_id if dataset_id is not None else ''}
    log.info("hemorrhagic_shock_update:%s" % update_clause)
    await conn.execute(update_clause)