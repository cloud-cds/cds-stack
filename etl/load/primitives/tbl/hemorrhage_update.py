import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl
import etl.load.primitives.row.load_row as load_row
from etl.load.primitives.tbl.derive_helper import *
'''
    Created: 09/01/2016
    Author: Katie Henry
    Purpose: Insert hemorrhage events into cdm_t based on definition in arch file
    Comments: Need to implement transfuse_rbc feature before this function can be run
'''
async def hemorrhage_update(fid, fid_input, conn, log , dataset_id, derive_feature_addr, cdm_feature_dict, incremental):
    assert fid == 'hemorrhage', 'wrong fid %s' % fid
    fid_input_items = [item.strip() for item in fid_input.split(',')]
    assert fid_input_items[0] == 'transfuse_rbc', \
        'wrong fid_input %s' % fid_input
    await conn.execute(clean_tbl.cdm_t_clean(fid, dataset_id))
    select_sql = """
        select rbc_2.enc_id, rbc_2.tsp, 1 confidence,
            prior_events, future_events
            from (select distinct c1.enc_id, c1.tsp, count (*) prior_events
            from cdm_t c1 join cdm_t c2
            on c1.enc_id=c2.enc_id and c2.tsp <= c1.tsp and c2.tsp > c1.tsp - interval '24 hours'
            where c1.fid='transfuse_rbc' and c2.fid='transfuse_rbc' %(dataset_block)s
            %(incremental_c1)s
            group by c1.enc_id, c1.tsp
            order by prior_events, c1.enc_id, c1.tsp) rbc_1
            right join
            (select c1.enc_id, c1.tsp, count (*) future_events
            from cdm_t c1 join cdm_t c2
            on c1.enc_id=c2.enc_id and c2.tsp >= c1.tsp and c2.tsp <= c1.tsp + interval '24 hours'
            where  c1.enc_id = c2.enc_id and
            c1.fid='transfuse_rbc' and c2.fid='transfuse_rbc' %(dataset_block)s
            %(incremental_c2)s
            group by c1.enc_id, c1.tsp
            order by  c1.enc_id, c1.tsp) rbc_2
            on rbc_1.enc_id=rbc_2.enc_id and rbc_1.tsp=rbc_2.tsp
            where prior_events =1 and future_events >= 3
            order by rbc_2.enc_id, rbc_2.tsp;
    """ % {'dataset_block': ' and c1.dataset_id = %s and c2.dataset_id = %s' \
                % (dataset_id, dataset_id) if dataset_id is not None else '',
           'incremental_c1': incremental_enc_id_in(' and ', 'c1', dataset_id,incremental),
           'incremental_c2': incremental_enc_id_in(' and ', 'c2', dataset_id,incremental),
           }
    # log.info(select_sql)
    rows = await conn.fetch(select_sql)
    for row in rows:
        values = [row['enc_id'], row['tsp'], fid, "True",
                  row['confidence']]
        load_row.upsert_t(conn, values, dataset_id)

