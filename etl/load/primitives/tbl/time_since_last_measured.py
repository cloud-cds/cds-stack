# DEPRECATED
import asyncio
from datetime import datetime
import etl.load.primitives.tbl.clean_tbl as clean_tbl

########################################
########################################
# Mike's CSV version
########################################
########################################

async def time_since_last_measured(fid, fid_input, conn, log, twf_table='cdm_twf'):
    # this function is not ready
    """
    fid_input should be name of the feature for which change is to be computed
    fid should be <fid of old feather>_minutes_since_measurement
    """
    # Make sure the fid is correct (fid_input can be anything)
    assert fid == '%s_minutes_since_measurement' % fid_input, 'wrong fid %s' % fid_input
    await conn(clean_tbl.cdm_twf_clean(fid, twf_table=twf_table))

    # Get all of the fid_input items ordered by enc_id then tsp
    records = await conn.fetch(\
        "SELECT enc_id, tsp, %(fid_input)s_c FROM %(twf_table)s ORDER BY enc_id, tsp;" \
            % {'fid_input': fid_input, 'twf_table': twf_table})
    # Initialize variables before loop
    prev_enc_id = -1

    # Go through all records and fill the temporary python list with the correct data
    for record in records:
        # New encounter --> reset the values
        if prev_enc_id != record['enc_id']:
            a_measurement_occurred = False
            time_diff_minutes = -1
            time_diff_conf_value = 0
            prev_enc_id = record['enc_id']
        # A measurement is happening --> Set start time
        if record['%s_c' % fid_input] < 8 and record['%s_c' % fid_input] != 0:
            measurement_time = record['tsp']
            a_measurement_occurred = True
        # Set the data
        if a_measurement_occurred:
            time_diff_minutes = (record['tsp'] - measurement_time).seconds//60
            time_diff_conf_value = record['%s_c' % fid_input]
        else:
            time_diff_minutes = -1
            time_diff_conf_value = 0
        # Insert the data
        record['%s' % fid] = time_diff_minutes
        record['%s_c' % fid] = time_diff_conf_value

    # TODO write csv to a StringIO instead and copy it into the database
    # Create a csv file with the python data
    with open('temp_file.csv', 'w') as f:
        writer = csv.writer(f, delimiter=',')
        for row in records:
            writer.writerow(row)

    # Fill in a temporary table with the csv file
    with psycopg2.connect("dbname=hcgh_1608 user=katie") as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            with open('temp_data.csv', 'r') as f:
                cur.copy_from(f, 'temp_table', sep=',', null='null')

    # Move the data from temp_table to %(twf_table)s
    sql = """
    UPDATE %(twf_table)s SET
        %(fid)s = subquery.%(fid)s,
        %(fid)s_c = subquery.%(fid)s_c
    from (
        SELECT * FROM temp_table
    ) AS subquery
    WHERE %(twf_table)s.enc_id = subquery.enc_id and %(twf_table)s.tsp = subquery.tsp;
    """ % {'fid': fid, 'twf_table': twf_table}

    cdm.log.info("time_since_last_measured:%s" % sql)
    cdm.query_with_sql(sql)


########################################
########################################
# Andong's procedural language version (need debug)
########################################
########################################

# def time_since_last_measured(fid, fid_input, cdm):
#     """
#     fid_input should be name of the feature for which change is to be computed
#     fid should be <fid of old feather>_minutes_since_measurement
#     """
#     # Make sure the fid is correct (fid_input can be anything)
#     assert fid == '%s_minutes_since_measurement' % fid_input, 'wrong fid %s' % fid_input
#
#     cdm.clean_twf(fid, value=-1)
#     sql = """
#     create temp table change as select enc_id, tsp, %(fid)s, %(fid)s_c,
#         lag(enc_id, 1) over (order by enc_id, tsp) enc_id_last,
#         lag(tsp, 1) over (order by enc_id, tsp) tsp_last,
#         lag(%(fid)s, 1) over (order by enc_id, tsp) %(fid)s_last,
#         lag(%(fid)s_c, 1) over (order by enc_id, tsp) %(fid)s_c_last
#     from cdm_twf
#     where %(fid)s_c < 8
#     order by enc_id, tsp;
#
#     delete from change where enc_id <> enc_id_last;
#
#
#     update cdm_twf set %(fid_input)s = EXTRACT(EPOCH FROM (cdm_twf.tsp - subquery.tsp_last))/60,
#         %(fid_input)s_c = subquery.%(fid)s_c_last
#     from (
#         select * from change where change.enc_id_last is not null
#     ) as subquery
#     where cdm_twf.enc_id = subquery.enc_id
#         and cdm_twf.tsp >= subquery.tsp_last
#         and cdm_twf.tsp <  subquery.tsp ;
#     """ % {'fid': fid, 'fid_input': fid_input}
#
#     cdm.log.info("time_since_last_measured:%s" % sql)
#     cdm.query_with_sql(sql)


# def time_since_last_measured(fid, fid_input, cdm):
#     """
#     fid_input should be name of the feature for which change is to be computed
#     fid should be <fid of old feather>_minutes_since_measurement
#     """
#     # Make sure the fid is correct (fid_input can be anything)
#     assert fid == '%s_minutes_since_measurement' % fid_input, 'wrong fid %s' % fid_input

#     cdm.clean_twf(fid, value=-1)

#     # Get all of the fid_input items ordered by enc_id then tsp
#     select_sql = """
#     SELECT enc_id, tsp, %(fid_input)s_c FROM cdm_twf
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
#             tsp_diff_minutes = -1
#             tsp_diff_conf_value = 0
#             prev_enc_id = cur_enc_id
#             a_measurement_occured = False
#         # Only calculate the diff if it's an original measurement (fid_c < 8)
#         if conf_value < 8 and conf_value != 0:
#             if a_measurement_occured:
#                 new_measurement_tsp = record['tsp']
#                 tsp_diff = new_measurement_tsp - prev_measurement_tsp
#                 tsp_diff_minutes = tsp_diff.seconds//60
#                 diff_conf_value = conf_value
#                 prev_measurement_tsp = new_measurement_tsp
#             else:
#                 a_measurement_occured = True
#                 prev_measurement_tsp = record['tsp']
#         temp_data.append([cur_enc_id, record['tsp'], fid, tsp_diff_minutes, tsp_diff_conf_value])

#     # Fill in the cdm_twf with the data stored in temp_table_data
#     for value in temp_data:
#         cdm.update_twf(value)
