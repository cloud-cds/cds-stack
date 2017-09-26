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

