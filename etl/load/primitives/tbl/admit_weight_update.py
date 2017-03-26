def admit_weight_update(fid, fid_input, cdm, twf_table='cdm_twf'):
    assert fid == 'admit_weight' and fid_input == "weight", \
        'wrong fid %s and fid_input %s' % (fid, fid_input)
    cdm.clean_s(fid)
    # insert the first weight measurement as admit_weight for each enc_id
    select_sql = """
    select key.enc_id, %(twf_table)s.weight, %(twf_table)s.weight_c from
    (select distinct enc_id, min(tsp) tsp from %(twf_table)s
        where based_on_popmean(weight_c) = 0
        group by enc_id
        order by enc_id) as key
    inner join %(twf_table)s on
    key.enc_id = %(twf_table)s.enc_id and key.tsp = %(twf_table)s.tsp;
   
    """ % {'twf_table': twf_table}
    server_cursor = cdm.select_with_sql(select_sql)
    records = server_cursor.fetchall()
    server_cursor.close()
    for record in records:
        enc_id = record['enc_id']
        weight = record['weight']
        weight_c = record['weight_c']
        cdm.insert_s([enc_id, fid, weight, weight_c])

    select_enc_id_without_admit_weight = """
    select pat_enc.enc_id from pat_enc 
    left join cdm_s on pat_enc.enc_id = cdm_s.enc_id 
        and cdm_s.fid = 'admit_weight'
    where value is null
    """
    server_cursor = cdm.select_with_sql(select_enc_id_without_admit_weight)
    records = server_cursor.fetchall()
    server_cursor.close()
    calculate_popmean_sql = """
    select avg(cast(value as real)) from cdm_s 
    where fid = 'admit_weight' and confidence < 8 
    """
    server_cursor = cdm.select_with_sql(calculate_popmean_sql)
    popmean = server_cursor.fetchone()[0]
    server_cursor.close()
    # import sys
    # sys.path.append("")
    import confidence
    for record in records:
        enc_id = record['enc_id']
        if popmean:
            weight = str(popmean)
        else:
            weight = None
        weight_c = confidence.POPMEAN + confidence.FILLEDIN
        cdm.upsert_s([enc_id, fid, weight, weight_c])        