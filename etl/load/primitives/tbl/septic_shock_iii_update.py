def septic_shock_iii_update(fid, fid_input, conn, log,  twf_table='cdm_twf'):
    # UPDATE 8/19/2016
    assert fid == 'septic_shock_iii', 'wrong fid %s' % fid

    clean_tbl.cdm_twf_clean(conn, fid, value=0, confidence=0, twf_table=twf_table)


    update_clause = """
    update %(twf_table)s set septic_shock_iii=value from
    (select c1.enc_id, c1.tsp, 1 as value from
    (select enc_id, tsp, qsofa, worst_sofa, lactate, fluid_resuscitation, vasopressor_resuscitation, hypotension_intp, lactate_c, hypotension_intp_c
    from %(twf_table)s ) c1
    join (select * from cdm_t where fid='suspicion_of_infection') c2
    on c1.enc_id=c2.enc_id and c2.tsp <=c1.tsp and c2.tsp >= c1.tsp - interval '6 hours') c3
    where %(twf_table)s.enc_id=c3.enc_id and %(twf_table)s.tsp=c3.tsp;
    """ % {'twf_table': twf_table}

    log.info("septic_shock_iii_update step one:%s" % update_clause)
    await conn.execute(update_clause)

    update_clause = """
    update %(twf_table)s set septic_shock_iii=value  from (select c1.enc_id, c1.tsp, 2 as value , c2.lactate_c|c2.hypotension_intp_c confidence from (select enc_id, tsp, qsofa, worst_sofa, lactate, fluid_resuscitation, vasopressor_resuscitation, hypotension_intp, lactate_c, hypotension_intp_c from %(twf_table)s ) c1
    join (select enc_id, tsp, qsofa, worst_sofa, lactate, fluid_resuscitation, vasopressor_resuscitation, hypotension_intp, lactate_c, hypotension_intp_c from %(twf_table)s ) c2
    on c1.enc_id=c2.enc_id and c2.tsp <=c1.tsp and c2.tsp >= c1.tsp - interval '6 hours'
    where c2.qsofa >= 2 and c2.worst_sofa >= 2 and c2.lactate >2 and c2.lactate_c < 24
    and ((c1.hypotension_intp::integer=1 and c1.hypotension_intp_c < 24 and c1.fluid_resuscitation::integer =1) or c1.vasopressor_resuscitation::integer =1)) c3
    where %(twf_table)s.enc_id=c3.enc_id and %(twf_table)s.tsp=c3.tsp and %(twf_table)s.septic_shock_iii =1;
    """ % {'twf_table': twf_table}

    log.info("septic_shock_iii_update step two:%s" % update_clause)
    await conn.execute(update_clause)
