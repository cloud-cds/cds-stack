async def admit_weight_update(fid, fid_input, conn, log, twf_table='cdm_twf'):
    assert fid == 'admit_weight' and fid_input == "weight", \
        'wrong fid %s and fid_input %s' % (fid, fid_input)
    clean_tbl.cdm_s_clean(conn, fid)
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
    records = conn.fetch(select_sql)
    for record in records:
        enc_id = record['enc_id']
        weight = record['weight']
        weight_c = record['weight_c']
        load_row.upsert_s(conn, [enc_id, fid, weight, weight_c])

    select_enc_id_without_admit_weight = """
    select pat_enc.enc_id from pat_enc
    left join cdm_s on pat_enc.enc_id = cdm_s.enc_id
        and cdm_s.fid = 'admit_weight'
    where value is null
    """
    records = await conn.fetch(select_enc_id_without_admit_weight)
    calculate_popmean_sql = """
    select avg(cast(value as real)) from cdm_s
    where fid = 'admit_weight' and confidence < 8
    """
    popmean = await conn.fetchrow(calculate_popmean_sql)
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
        load_row.upsert_s([enc_id, fid, weight, weight_c])