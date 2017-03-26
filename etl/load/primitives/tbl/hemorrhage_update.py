'''
    Created: 09/01/2016
    Author: Katie Henry
    Purpose: Insert hemorrhage events into cdm_t based on definition in arch file
    Comments: Need to implement transfuse_rbc feature before this function can be run
'''
def hemorrhage_update(fid, fid_input, cdm, twf_table='cdm_twf'):
    assert fid == 'hemorrhage', 'wrong fid %s' % fid 
    fid_input_items = [item.strip() for item in fid_input.split(',')]
    assert fid_input_items[0] == 'transfuse_rbc', \
        'wrong fid_input %s' % fid_input
    cdm.clean_t(fid)
    select_sql = """
        select rbc_2.enc_id, rbc_2.tsp, 1 confidence, prior_events, future_events from (select distinct c1.enc_id, c1.tsp, count (*) prior_events 
            from cdm_t c1 join cdm_t c2 
            on c1.enc_id=c2.enc_id and c2.tsp <= c1.tsp and c2.tsp > c1.tsp - interval '24 hours' 
            where c1.fid='transfuse_rbc' and c2.fid='transfuse_rbc'
            group by c1.enc_id, c1.tsp 
            order by prior_events, c1.enc_id, c1.tsp) rbc_1
            right join 
            (select c1.enc_id, c1.tsp, count (*) future_events 
            from cdm_t c1 join cdm_t c2 
            on c1.enc_id=c2.enc_id and c2.tsp >= c1.tsp and c2.tsp <= c1.tsp + interval '24 hours' 
            where  c1.enc_id = c2.enc_id and
            c1.fid='transfuse_rbc' and c2.fid='transfuse_rbc' 
            group by c1.enc_id, c1.tsp 
            order by  c1.enc_id, c1.tsp) rbc_2
            on rbc_1.enc_id=rbc_2.enc_id and rbc_1.tsp=rbc_2.tsp
            where prior_events =1 and future_events >= 3 
            order by rbc_2.enc_id, rbc_2.tsp;
    """
    server_cursor = cdm.select_with_sql(select_sql)
    rows = server_cursor.fetchall()
    server_cursor.close()
    for row in rows:
        values = [row['enc_id'], row['tsp'], fid, "True", 
                  row['confidence']]
        cdm.insert_t(values)
   
