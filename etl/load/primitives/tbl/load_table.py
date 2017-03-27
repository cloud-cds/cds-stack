import etl.core.config
import logging


# TODO: make async / use COPY
def data_2_workspace(engine, job_id, df_name, df):
    nrows = df.shape[0]
    table_name = "workspace.{}_{}".format(job_id, df_name)
    logging.info("saving data frame to %s: nrows = %s" % (table_name, nrows))
    df.to_sql(table_name, engine, if_exists='replace', index=False, schema='public')
    '''
    buf = StringIO()
    # saving a data frame to a buffer (same as with a regular file):
    df.to_csv(buf, index=False, sep='\t', header=False, \
        quoting=csv.QUOTE_NONE, date_format="ISO", escapechar=" ")
    cols = ",".join([ "{} text".format(col) for col in df.columns.values.tolist()])
    buf.seek(0)
    prepare_table = \
    """DROP table if exists %(tab)s;
    create table %(tab)s (
        %(cols)s
    );
    """ % {'tab': table_name, 'cols': cols}
    logging.info("create table: {}".format(prepare_table))
    await conn.execute(prepare_table)

    if nrows > 0 and not df.empty:
        cur.copy_from(buf, table_name)
        logging.info(table_name + " saved: nrows = {}".format(nrows))
    else:
        logging.warn("zero rows in the data frame:{}".format(table_name))
    '''



async def insert_new_patients(conn, job_id):
    # insert new bedded_patients
    # for new patients insert into pat_enc
    insert_new_patients_sql = """
    insert into pat_enc (pat_id, visit_id)
    select bp.pat_id, bp.visit_id
    from "workspace.%s_bedded_patients_transformed" bp
    left join pat_enc pe on bp.visit_id = pe.visit_id
    where pe.enc_id is null;
    """ % job_id
    logging.info("%s: insert new bedded_patients: %s" % (job_id, insert_new_patients_sql))
    await conn.execute(insert_new_patients_sql)



async def create_job_cdm_twf_table(conn, job_id):
    # create snapshot cdm_twf tables for this job, i.e., all bedded patients
    create_job_cdm_twf_table = """
    DROP TABLE IF EXISTS "workspace.%(job)s_cdm_twf" CASCADE;
    create table "workspace.%(job)s_cdm_twf" as
    select cdm_twf.* from cdm_twf
    inner join pat_enc on pat_enc.enc_id = cdm_twf.enc_id
    inner join "workspace.%(job)s_bedded_patients_transformed" bp
        on bp.visit_id = pat_enc.visit_id;
    alter table "workspace.%(job)s_cdm_twf" add primary key (enc_id, tsp);
    """ % {'job':job_id}
    logging.info("%s: create job cdm_twf table: %s" % (job_id, create_job_cdm_twf_table))
    await conn.execute(create_job_cdm_twf_table)



async def workspace_bedded_patients_2_cdm_s(conn, job_id):
    import_raw_features = """
    -- age
    INSERT INTO cdm_s (enc_id, fid, value, confidence)
    select pe.enc_id, 'age', bp.age, 1 as c from "workspace.%(job)s_bedded_patients_transformed" bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    ON CONFLICT (enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)
    import_raw_features = """
    -- gender
    INSERT INTO cdm_s (enc_id, fid, value, confidence)
    select pe.enc_id, 'gender', bp.gender, 1 as c from "workspace.%(job)s_bedded_patients_transformed" bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    ON CONFLICT (enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)
    import_raw_features = """
    -- diagnosis
    INSERT INTO cdm_s (enc_id, fid, value, confidence)
    select pe.enc_id, json_object_keys(diagnosis::json), 'True', 1
    from "workspace.%(job)s_bedded_patients_transformed" bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    ON CONFLICT (enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)
    import_raw_features = """
    -- problem
    INSERT INTO cdm_s (enc_id, fid, value, confidence)
    select pe.enc_id, json_object_keys(problem::json), 'True', 1
    from "workspace.%(job)s_bedded_patients_transformed" bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    ON CONFLICT (enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)
    import_raw_features = """
    -- history
    INSERT INTO cdm_s (enc_id, fid, value, confidence)
    select pe.enc_id, json_object_keys(history::json), 'True', 1
    from "workspace.%(job)s_bedded_patients_transformed" bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    ON CONFLICT (enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)
    import_raw_features = """
    -- hospital
    INSERT INTO cdm_s (enc_id, fid, value, confidence)
    select pe.enc_id, 'hospital', hospital, 1
    from "workspace.%(job)s_bedded_patients_transformed" bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    ON CONFLICT (enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)
    import_raw_features = """
    -- admittime
    INSERT INTO cdm_s (enc_id, fid, value, confidence)
    select pe.enc_id, 'admittime', admittime, 1
    from "workspace.%(job)s_bedded_patients_transformed" bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    ON CONFLICT (enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)



async def workspace_flowsheets_2_cdm_t(conn, job_id):
    import_raw_features = \
    """INSERT INTO cdm_t (enc_id, tsp, fid, value, confidence)
            select pat_enc.enc_id, fs.tsp::timestamptz, fs.fid, last(fs.value), 0 from "workspace.%(job)s_flowsheets_transformed" fs
                inner join pat_enc on pat_enc.visit_id = fs.visit_id
                inner join cdm_feature on fs.fid = cdm_feature.fid and cdm_feature.category = 'T'
            where fs.tsp <> 'NaT' and fs.tsp::timestamptz < now()
            group by pat_enc.enc_id, tsp, fs.fid
        ON CONFLICT (enc_id, tsp, fid)
            DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)



async def workspace_lab_results_2_cdm_t(conn, job_id):
    import_raw_features = \
    """INSERT INTO cdm_t (enc_id, tsp, fid, value, confidence)
        select pat_enc.enc_id, lr.tsp::timestamptz, lr.fid, lr.value, 0 from "workspace.%(job)s_lab_results_transformed" lr
            inner join pat_enc on pat_enc.visit_id = lr.visit_id
            inner join cdm_feature on lr.fid = cdm_feature.fid and cdm_feature.category = 'T'
        where lr.tsp <> 'NaT' and lr.tsp::timestamptz < now()
    ON CONFLICT (enc_id, tsp, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)



async def workspace_location_history_2_cdm_t(conn, job_id):
    import_raw_features = \
    """INSERT INTO cdm_t (enc_id, tsp, fid, value, confidence)
        select pat_enc.enc_id, lr.tsp::timestamptz, lr.fid, lr.value, 0 from "workspace.%(job)s_location_history_transformed" lr
            inner join pat_enc on pat_enc.visit_id = lr.visit_id
            inner join cdm_feature on lr.fid = cdm_feature.fid and cdm_feature.category = 'T'
        where lr.tsp <> 'NaT' and lr.tsp::timestamptz < now()
        ON CONFLICT (enc_id, tsp, fid)
           DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)



async def workspace_medication_administration_2_cdm_t(conn, job_id):
    import_raw_features = \
    """INSERT INTO cdm_t (enc_id, tsp, fid, value, confidence)
        select pat_enc.enc_id, mar.tsp::timestamptz, mar.fid,
            json_build_object('dose',SUM(mar.dose_value::numeric),'action',last(mar.action))
            , 0
        from "workspace.%(job)s_med_admin_transformed" mar
            inner join pat_enc on pat_enc.visit_id = mar.visit_id
            inner join cdm_feature on cdm_feature.fid = mar.fid
        where isnumeric(mar.dose_value) and mar.tsp <> 'NaT' and mar.tsp::timestamptz < now() and mar.fid ~ 'dose'
        group by pat_enc.enc_id, tsp, mar.fid
    ON CONFLICT (enc_id, tsp, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    -- fluids and others
    INSERT INTO cdm_t (enc_id, tsp, fid, value, confidence)
        select pat_enc.enc_id, mar.tsp::timestamptz, mar.fid,
            max(mar.dose_value::numeric), 0
        from "workspace.%(job)s_med_admin_transformed" mar
            inner join pat_enc on pat_enc.visit_id = mar.visit_id
            inner join cdm_feature on cdm_feature.fid = mar.fid
        where isnumeric(mar.dose_value) and mar.tsp <> 'NaT' and mar.tsp::timestamptz < now() and mar.fid not ilike 'dose'
        group by pat_enc.enc_id, tsp, mar.fid
    ON CONFLICT (enc_id, tsp, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)



async def workspace_flowsheets_2_cdm_twf(conn, job_id):
    select_twf_features = """
    select distinct c.fid, c.data_type from cdm_feature c
    inner join "workspace.%(job)s_%(tab)s_transformed" fs on fs.fid = c.fid
    where c.category = 'TWF'
    """
    twf_features = await conn.fetch(select_twf_features % {'job': job_id, 'tab': 'flowsheets'})
    upsert_twf_features = """
    insert into "workspace.%(job)s_cdm_twf" (enc_id, tsp, %(fid)s, %(fid)s_c)
        select pat_enc.enc_id, raw.tsp::timestamptz, last(raw.value::%(dt)s), 0
        from "workspace.%(job)s_%(tab)s_transformed" raw
        inner join pat_enc on pat_enc.visit_id = raw.visit_id
        where raw.fid = '%(fid)s' and isnumeric(raw.value) and raw.tsp <> 'NaT' and raw.tsp::timestamptz < now()
        group by pat_enc.enc_id, tsp
    ON CONFLICT (enc_id, tsp)
        DO UPDATE SET %(fid)s = EXCLUDED.%(fid)s, %(fid)s_c = EXCLUDED.%(fid)s_c;
    """
    for feature in twf_features:
        update_twf_feature = upsert_twf_features % {'fid': feature['fid'],
                                                    'dt':  feature['data_type'],
                                                    'job': job_id,
                                                    'tab': 'flowsheets'}
        logging.info("%s: update_twf_feature: %s" % (job_id, update_twf_feature))
        await conn.execute(update_twf_feature)



async def workspace_lab_results_2_cdm_twf(conn, job_id):
    select_twf_features = """
    select distinct c.fid, c.data_type from cdm_feature c
    inner join "workspace.%(job)s_%(tab)s_transformed" fs on fs.fid = c.fid
    where c.category = 'TWF'
    """
    twf_features = await conn.fetch(select_twf_features % {'job': job_id, 'tab': 'lab_results'})
    upsert_twf_features = """
    insert into "workspace.%(job)s_cdm_twf" (enc_id, tsp, %(fid)s, %(fid)s_c)
        select pat_enc.enc_id, raw.tsp::timestamptz, last(raw.value::%(dt)s), 0
        from "workspace.%(job)s_%(tab)s_transformed" raw
        inner join pat_enc on pat_enc.visit_id = raw.visit_id
        where raw.fid = '%(fid)s' and isnumeric(raw.value) and raw.tsp <> 'NaT' and raw.tsp::timestamptz < now()
        group by pat_enc.enc_id, tsp
    ON CONFLICT (enc_id, tsp)
        DO UPDATE SET %(fid)s = EXCLUDED.%(fid)s, %(fid)s_c = EXCLUDED.%(fid)s_c;
    """
    for feature in twf_features:
        update_twf_feature = upsert_twf_features % {'fid': feature['fid'],
                                                    'dt':  feature['data_type'],
                                                    'job': job_id,
                                                    'tab': 'lab_results'}
        logging.info("%s: update_twf_feature: %s" % (job_id, update_twf_feature))
        await conn.execute(update_twf_feature)
    logging.info("load raw data to cdm completed")



async def workspace_lab_results_2_cdm_twf(conn, job_id):
    # NOTE{zad}: usually no twf features in MAR but for safty we cover them in MAR
    select_twf_features = """
    select distinct c.fid, c.data_type from cdm_feature c
    inner join "workspace.%(job)s_%(tab)s_transformed" fs on fs.fid = c.fid
    where c.category = 'TWF'
    """
    twf_features = await conn.fetch(select_twf_features % {'job': job_id, 'tab': 'med_admin'})
    upsert_twf_features = """
    insert into "workspace.%(job)s_cdm_twf" (enc_id, tsp, %(fid)s, %(fid)s_c)
        select pat_enc.enc_id, raw.tsp::timestamptz, last(raw.dose_value::%(dt)s), 0
        from "workspace.%(job)s_%(tab)s_transformed" raw
        inner join pat_enc on pat_enc.visit_id = raw.visit_id
        where raw.fid = '%(fid)s' and isnumeric(raw.dose_value) and raw.tsp <> 'NaT' and raw.tsp::timestamptz < now()
        group by pat_enc.enc_id, tsp
    ON CONFLICT (enc_id, tsp)
        DO UPDATE SET %(fid)s = EXCLUDED.%(fid)s, %(fid)s_c = EXCLUDED.%(fid)s_c;
    """
    for feature in twf_features:
        update_twf_feature = upsert_twf_features % {'fid': feature['fid'],
                                                    'dt':  feature['data_type'],
                                                    'job': job_id,
                                                    'tab': 'med_admin'}
        logging.info("%s: update_twf_feature: %s" % (job_id, update_twf_feature))
        await conn.execute(update_twf_feature)
    logging.info("load raw data to cdm completed")
