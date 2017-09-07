import etl.core.config
import logging
import asyncpg

async def data_2_workspace(logger, conn, job_id, df_name, df, dtypes=None, if_exists='replace'):
    if df is None or df.empty or len(df) == 0:
        logger.error('Failed to load table {} (invalid dataframe)'.format(df_name))
        return

    nrows = len(df)
    table_name = "{}_{}".format(job_id, df_name)

    cols = ",".join([ "{} text".format(col) for col in df.columns.values.tolist()])
    prepare_table = \
    """DROP table if exists workspace.%(tab)s;
    create table workspace.%(tab)s (
        %(cols)s
    );
    """ % {'tab': table_name, 'cols': cols}
    logger.info("create table: {}".format(prepare_table))
    await conn.execute(prepare_table)
    tuples = [tuple([str(y) for y in x]) for x in df.values]
    await conn.copy_records_to_table(table_name, records=tuples, schema_name="workspace")
    logger.info(table_name + " saved: nrows = {}".format(nrows))



async def insert_new_patients(conn, job_id):
    # insert new bedded_patients
    # for new patients insert into pat_enc
    insert_new_patients_sql = """
    insert into pat_enc (pat_id, visit_id)
    select bp.pat_id, bp.visit_id
    from workspace.%s_bedded_patients_transformed bp
    left join pat_enc pe on bp.visit_id = pe.visit_id
    where pe.enc_id is null;
    """ % job_id
    logging.info("%s: insert new bedded_patients: %s" % (job_id, insert_new_patients_sql))
    await conn.execute(insert_new_patients_sql)



async def create_job_cdm_twf_table(conn, job_id):
    # create snapshot cdm_twf tables for this job, i.e., all bedded patients
    create_job_cdm_twf_table = """
    DROP TABLE IF EXISTS workspace.%(job)s_cdm_twf CASCADE;
    create table workspace.%(job)s_cdm_twf as
    select cdm_twf.* from cdm_twf
    inner join pat_enc on pat_enc.enc_id = cdm_twf.enc_id
    inner join workspace.%(job)s_bedded_patients_transformed bp
        on bp.visit_id = pat_enc.visit_id
    where now() - tsp < (select value::interval from parameters where name = 'etl_workspace_lookbackhours');
    alter table workspace.%(job)s_cdm_twf add primary key (enc_id, tsp);
    """ % {'job':job_id}
    logging.info("%s: create job cdm_twf table: %s" % (job_id, create_job_cdm_twf_table))
    await conn.execute(create_job_cdm_twf_table)

async def create_job_cdm_t_table(conn, job_id):
    # create snapshot cdm_t tables for this job, i.e., all bedded patients
    create_job_cdm_t_table = """
    DROP TABLE IF EXISTS workspace.%(job)s_cdm_t CASCADE;
    create table workspace.%(job)s_cdm_t as
    select cdm_t.* from cdm_t
    inner join pat_enc on pat_enc.enc_id = cdm_t.enc_id
    inner join workspace.%(job)s_bedded_patients_transformed bp
        on bp.visit_id = pat_enc.visit_id
    where now() - tsp < (select value::interval from parameters where name = 'etl_workspace_lookbackhours');
    alter table workspace.%(job)s_cdm_t add primary key (enc_id, tsp, fid);
    """ % {'job':job_id}
    logging.info("%s: create job cdm_t table: %s" % (job_id, create_job_cdm_t_table))
    await conn.execute(create_job_cdm_t_table)


async def workspace_bedded_patients_2_cdm_s(conn, job_id):
    import_raw_features = """
    -- age
    INSERT INTO cdm_s (enc_id, fid, value, confidence)
    select pe.enc_id, 'age', bp.age, 1 as c from workspace.%(job)s_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    ON CONFLICT (enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)
    import_raw_features = """
    -- gender
    INSERT INTO cdm_s (enc_id, fid, value, confidence)
    select pe.enc_id, 'gender', bp.gender::numeric::int, 1 as c from workspace.%(job)s_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
        where isnumeric(bp.gender) and lower(bp.gender) <> 'nan'
    ON CONFLICT (enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)
    import_raw_features = """
    -- diagnosis
    INSERT INTO cdm_s (enc_id, fid, value, confidence)
    select pe.enc_id, json_object_keys(diagnosis::json), 'True', 1
    from workspace.%(job)s_bedded_patients_transformed bp
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
    from workspace.%(job)s_bedded_patients_transformed bp
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
    from workspace.%(job)s_bedded_patients_transformed bp
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
    from workspace.%(job)s_bedded_patients_transformed bp
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
    from workspace.%(job)s_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    ON CONFLICT (enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)
    import_raw_features = """
    -- patient class
    INSERT INTO cdm_s (enc_id, fid, value, confidence)
    select pe.enc_id, 'patient_class', patient_class, 1
    from workspace.%(job)s_bedded_patients_transformed bp
        inner join pat_enc pe on pe.visit_id = bp.visit_id
    ON CONFLICT (enc_id, fid)
    DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)



async def workspace_flowsheets_2_cdm_t(conn, job_id):
    import_raw_features = \
    """INSERT INTO workspace.%(job)s_cdm_t (enc_id, tsp, fid, value, confidence)
            select pat_enc.enc_id, fs.tsp::timestamptz, fs.fid, last(fs.value), 0 from workspace.%(job)s_flowsheets_transformed fs
                inner join pat_enc on pat_enc.visit_id = fs.visit_id
                inner join cdm_feature on fs.fid = cdm_feature.fid and cdm_feature.category = 'T'
            where fs.tsp <> 'NaT' and fs.tsp::timestamptz < now()
                and fs.fid <> 'fluids_intake'
            group by pat_enc.enc_id, tsp, fs.fid
        ON CONFLICT (enc_id, tsp, fid)
            DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)



async def workspace_lab_results_2_cdm_t(conn, job_id):
    import_raw_features = \
    """INSERT INTO workspace.%(job)s_cdm_t (enc_id, tsp, fid, value, confidence)
        select pat_enc.enc_id, lr.tsp::timestamptz, lr.fid, first(lr.value), 0 from workspace.%(job)s_lab_results_transformed lr
            inner join pat_enc on pat_enc.visit_id = lr.visit_id
            inner join cdm_feature on lr.fid = cdm_feature.fid and cdm_feature.category = 'T'
        where lr.tsp <> 'NaT' and lr.tsp::timestamptz < now()
        group by pat_enc.enc_id, lr.tsp, lr.fid
    ON CONFLICT (enc_id, tsp, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;

    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)



async def workspace_location_history_2_cdm_t(conn, job_id):
    import_raw_features = \
    """INSERT INTO workspace.%(job)s_cdm_t (enc_id, tsp, fid, value, confidence)
        select pat_enc.enc_id, lr.tsp::timestamptz, lr.fid, lr.value, 0 from workspace.%(job)s_location_history_transformed lr
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
    """INSERT INTO workspace.%(job)s_cdm_t (enc_id, tsp, fid, value, confidence)
        select pat_enc.enc_id, mar.tsp::timestamptz, mar.fid,
            json_build_object('dose',SUM(mar.dose_value::numeric),'action',last(mar.action))
            , 0
        from workspace.%(job)s_med_admin_transformed mar
            inner join pat_enc on pat_enc.visit_id = mar.visit_id
            inner join cdm_feature on cdm_feature.fid = mar.fid
        where isnumeric(mar.dose_value) and mar.tsp <> 'NaT' and mar.tsp::timestamptz < now() and mar.fid ~ 'dose'
        group by pat_enc.enc_id, tsp, mar.fid
    ON CONFLICT (enc_id, tsp, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    -- others excluded fluids
    INSERT INTO workspace.%(job)s_cdm_t (enc_id, tsp, fid, value, confidence)
        select pat_enc.enc_id, mar.tsp::timestamptz, mar.fid,
            max(mar.dose_value::numeric), 0
        from workspace.%(job)s_med_admin_transformed mar
            inner join pat_enc on pat_enc.visit_id = mar.visit_id
            inner join cdm_feature on cdm_feature.fid = mar.fid
        where isnumeric(mar.dose_value) and mar.tsp <> 'NaT' and mar.tsp::timestamptz < now() and mar.fid not ilike 'dose' and mar.fid <> 'fluids_intake'
        group by pat_enc.enc_id, tsp, mar.fid
    ON CONFLICT (enc_id, tsp, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)

async def workspace_fluids_intake_2_cdm_t(conn, job_id):
    import_raw_features = \
    """
    with u as (
        select pat_enc.enc_id, mar.tsp::timestamptz, mar.fid, mar.dose_value as value
            from workspace.%(job)s_med_admin_transformed mar
                inner join pat_enc on pat_enc.visit_id = mar.visit_id
                inner join cdm_feature on cdm_feature.fid = mar.fid
            where isnumeric(mar.dose_value) and mar.tsp <> 'NaT' and mar.tsp::timestamptz < now() and mar.fid = 'fluids_intake'
                        and mar.dose_value::numeric > 0
            UNION
            select pat_enc.enc_id, fs.tsp::timestamptz, fs.fid, fs.value::text
            from workspace.%(job)s_flowsheets_transformed fs
                inner join pat_enc on pat_enc.visit_id = fs.visit_id
                inner join cdm_feature on fs.fid = cdm_feature.fid and cdm_feature.category = 'T'
                where fs.tsp <> 'NaT' and fs.tsp::timestamptz < now()
                and fs.fid = 'fluids_intake'
                and fs.value::numeric > 0
    )
    INSERT INTO workspace.%(job)s_cdm_t (enc_id, tsp, fid, value, confidence)
        select u.enc_id, u.tsp, u.fid,
                sum(u.value::numeric), 0
        from u
        group by u.enc_id, u.tsp, u.fid
    ON CONFLICT (enc_id, tsp, fid)
        DO UPDATE SET value = EXCLUDED.value, confidence = EXCLUDED.confidence;
    """ % {'job': job_id}
    logging.info("%s: import_raw_features: %s" % (job_id, import_raw_features))
    await conn.execute(import_raw_features)

async def workspace_flowsheets_2_cdm_twf(conn, job_id):
    select_twf_features = """
    select distinct c.fid, c.data_type from cdm_feature c
    inner join workspace.%(job)s_%(tab)s_transformed fs on fs.fid = c.fid
    where c.category = 'TWF'
    """
    twf_features = await conn.fetch(select_twf_features % {'job': job_id, 'tab': 'flowsheets'})
    upsert_twf_features = """
    insert into workspace.%(job)s_cdm_twf (enc_id, tsp, %(fid)s, %(fid)s_c)
        select pat_enc.enc_id, raw.tsp::timestamptz, last(raw.value::%(dt)s), 0
        from workspace.%(job)s_%(tab)s_transformed raw
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
    inner join workspace.%(job)s_%(tab)s_transformed fs on fs.fid = c.fid
    where c.category = 'TWF'
    """
    twf_features = await conn.fetch(select_twf_features % {'job': job_id, 'tab': 'lab_results'})
    upsert_twf_features = """
    insert into workspace.%(job)s_cdm_twf (enc_id, tsp, %(fid)s, %(fid)s_c)
        select pat_enc.enc_id, raw.tsp::timestamptz, last(raw.value::%(dt)s), 0
        from workspace.%(job)s_%(tab)s_transformed raw
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



async def workspace_medication_administration_2_cdm_twf(conn, job_id):
    # NOTE{zad}: usually no twf features in MAR but for safty we cover them in MAR
    select_twf_features = """
    select distinct c.fid, c.data_type from cdm_feature c
    inner join workspace.%(job)s_%(tab)s_transformed fs on fs.fid = c.fid
    where c.category = 'TWF'
    """
    twf_features = await conn.fetch(select_twf_features % {'job': job_id, 'tab': 'med_admin'})
    upsert_twf_features = """
    insert into workspace.%(job)s_cdm_twf (enc_id, tsp, %(fid)s, %(fid)s_c)
        select pat_enc.enc_id, raw.tsp::timestamptz, last(raw.dose_value::%(dt)s), 0
        from workspace.%(job)s_%(tab)s_transformed raw
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


async def load_cdm_to_criteria_meas(conn, dataset_id, incremental):
    sql = """select * from load_cdm_to_criteria_meas(
    {dataset_id}, {incremental});
    """.format(dataset_id=dataset_id, incremental=incremental)
    await conn.execute(sql)

async def calculate_historical_criteria(conn):
    sql = 'select * from calculate_historical_criteria(NULL);'
    await conn.execute(sql)

async def gen_label_and_report(conn, dataset_id):
    sql = '''select * from run_cdm_label_and_report(
        {dataset_id}, {label_des}, {server}, {nprocs})
    '''.format(dataset_id=dataset_id,
        label_des='labels clarity daily on D7', server='dev_dw', nprocs=12)
    logging.info("gen_label_and_report: {}".format(sql))
    await conn.execute(sql)

async def workspace_notes_2_cdm_notes(conn, job_id):
    test_tables_sql = \
    '''
    select (
        select count(*) from information_schema.tables
        where table_schema = 'workspace'
        and table_name in ('%(job)s_notes_transformed', '%(job)s_note_texts_transformed')
    ) = 2;
    '''
    tbl_count = await conn.fetchval(test_tables_sql)
    if tbl_count:
        load_notes_sql = \
        '''
        insert into cdm_notes
            select N.pat_id, N.note_id, N.note_type, N.note_status, NT.note_body, N.dates::json, N.providers::json
            from workspace.%(job)s_notes_transformed N
            left join workspace.%(job)s_note_texts_transformed NT on N.note_id = NT.note_id
        on conflict (pat_id, note_id, note_type, note_status) do update
            set note_body = excluded.note_body,
                dates = excluded.dates,
                providers = excluded.providers;
        ''' % {'job': job_id}
        await conn.execute(load_notes_sql)