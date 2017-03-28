import asyncio
import etl.load.primitives.tbl.clean_tbl as clean_tbl

async def change_since_last_measured(fid, fid_input, conn, log, dataset_id = None, twf_table='cdm_twf'):
    """
    fid_input should be name of the feature for which change is to be computed
    fid should be <fid of old feather>_change
    """
    # Make sure the fid is correct (fid_input can be anything)
    assert fid == '%s_change' % fid_input, 'wrong fid %s' % fid_input
    await clean_tbl.cdm_twf_clean(conn, fid,  twf_table=twf_table, dataset_id=dataset_id)

    sql = """
    create temp table change as select enc_id, tsp, %(fid)s, %(fid)s_c,
        lag(enc_id, 1) over (order by enc_id, tsp) enc_id_last,
        lag(tsp, 1) over (order by enc_id, tsp) tsp_last,
        lag(%(fid)s, 1) over (order by enc_id, tsp) %(fid)s_last,
        lag(%(fid)s_c, 1) over (order by enc_id, tsp) %(fid)s_c_last,
        %(fid)s - lag(%(fid)s, 1) over (order by enc_id, tsp) diff,
        lag(enc_id, -1) over (order by enc_id, tsp) enc_id_next,
        lag(tsp, -1) over (order by enc_id, tsp) tsp_next,
        lag(%(fid)s, -1) over (order by enc_id, tsp) %(fid)s_next,
        lag(%(fid)s_c, -1) over (order by enc_id, tsp) %(fid)s_c_next
    from %(twf_table)s
    where %(fid)s_c < 8 %(dataset_block)s
    order by enc_id, tsp;

    delete from change where enc_id <> enc_id_last;


    update %(twf_table)s set %(fid_input)s = subquery.diff,
        %(fid_input)s_c = subquery.%(fid)s_c | subquery.%(fid)s_c_last
    from (
        select * from change where change.enc_id = change.enc_id_next and change.enc_id_last is not null
    ) as subquery
    where %(twf_table)s.enc_id = subquery.enc_id
        and %(twf_table)s.tsp >= subquery.tsp
        and %(twf_table)s.tsp <  subquery.tsp_next
        %(dataset_block)s;


    update %(twf_table)s set %(fid_input)s = subquery.diff,
        %(fid_input)s_c = subquery.%(fid)s_c | subquery.%(fid)s_c_last
    from (
        select * from change where enc_id <> enc_id_next and enc_id_last is not null
    ) as subquery
    where %(twf_table)s.enc_id = subquery.enc_id
        and %(twf_table)s.tsp >= subquery.tsp %(dataset_block)s;
    """ % {'fid': fid, 'fid_input': fid_input,  'twf_table':'cdm_twf', 'dataset_block': ' and dataset_id = %s' % dataset_id if dataset_id is not None else ''}

    log.info("change_since_last_measured:%s" % sql)
    await conn.execute(sql)

# def change_since_last_measured(fid, fid_input, cdm):
#     """
#     fid_input should be name of the feature for which change is to be computed
#     fid should be <fid of old feather>_change
#     """
#     # Make sure the fid is correct (fid_input can be anything)
#     assert fid == '%s_change' % fid_input, 'wrong fid %s' % fid_input

#     cdm.clean_twf('%s_change' % fid_input)

#     # Get all of the fid_input items ordered by enc_id then tsp
#     select_sql = """
#     SELECT enc_id, tsp, %(fid_input)s, %(fid_input)s_c FROM %(twf_table)s
#     ORDER BY enc_id, tsp;
#     """ % {'fid_input': fid_input}
#     server_cursor = cdm.select_with_sql(select_sql)
#     records = server_cursor.fetchall()
#     server_cursor.close()

#     # Initialize variables before loop
#     temp_data = []
#     prev_enc_id = -1


#     # Go through all records and fill the temporary python list with the correct data
#     for record in records:
#         cur_enc_id = record['enc_id']
#         conf_value = record['%s_c' % fid_input]
#         # New encounter --> reset the values
#         if prev_enc_id != cur_enc_id:
#             diff = 0
#             diff_conf_value = 0
#             prev_enc_id = cur_enc_id
#             first_measurement_occurred = False
#         # Only calculate the diff if:
#         #   1) It's an original measurement (fid_c < 8)
#         #   AND
#         #   2) It's not the first measurement
#         if conf_value < 8:
#             if first_measurement_occurred:
#                 new_measurement = float(record['%s' % fid_input])
#                 diff = new_measurement - prev_measurement
#                 diff_conf_value = conf_value
#                 prev_measurement = new_measurement
#             else:
#                 first_measurement_occurred = True
#                 prev_measurement = float(record['%s' % fid_input])
#         temp_data.append([cur_enc_id, record['tsp'], fid, diff, diff_conf_value])

#     ##### Manage db connection without Andong's API #####
#     conn = psycopg2.connect("dbname=hcgh_1608 user=zad")
#     cursor_name = 'ews_server_cursor'
#     cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
#     # Fill in the %(twf_table)s with the data stored in temp_table_data
#     for value in temp_data:
#         update_cols = "\"%s\",\"%s_c\"" % (value[2], value[2])
#         update_values = "%s, %s" % (value[3], value[4])
#         where_conditions = "enc_id=%s AND tsp='%s'" % (value[0], value[1])
#         update_sql = """
#         UPDATE %(twf_table)s
#         SET (%s) = (%s)
#         WHERE %s
#         """ % (update_cols, update_values, where_conditions)
#         cur.execute(update_sql)

#     conn.commit()
#     cur.close()
#     conn.close()
