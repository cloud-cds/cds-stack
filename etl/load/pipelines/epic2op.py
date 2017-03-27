import etl.load.primitives.tbl.load_table as primitives
import datetime as dt
import asyncpg
import asyncio
from sqlalchemy import create_engine

class Epic2OpLoader:
    def __init__(self, config):
        self.config = config
        self.pool = None
        current_time = dt.datetime.now().strftime('%m%d%H%M%S')
        self.job_id = "job_etl_{}".format(current_time).lower()

    async def async_init(self):
        self.pool = await asyncpg.create_pool(
            database = self.config.db_name,
            user     = self.config.db_user,
            password = self.config.db_pass,
            host     = self.config.db_host,
            port     = self.config.db_port
        )

    def run_loop(self, db_data):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run(db_data))

    async def run(self, db_data):
        self.epic_2_workspace(db_data)
        await self.workspace_to_cdm()

    def epic_2_workspace(self, db_data):
        engine = create_engine(self.config.get_db_conn_string_sqlalchemy())
        for df_name, df in db_data.items():
            primitives.data_2_workspace(engine, self.job_id, df_name, df)

    async def workspace_to_cdm(self):
        if self.pool is None:
            await self.async_init()
        async with self.pool.acquire() as conn:
            await primitives.insert_new_patients(conn, self.job_id)
            await primitives.create_job_cdm_twf_table(conn, self.job_id)
            await primitives.workspace_bedded_patients_2_cdm_s(conn, self.job_id)
            await primitives.workspace_flowsheets_2_cdm_t(conn, self.job_id)
            await primitives.workspace_lab_results_2_cdm_t(conn, self.job_id)
            await primitives.workspace_location_history_2_cdm_t(conn, self.job_id)
            await primitives.workspace_medication_administration_2_cdm_t(conn, self.job_id)
            await primitives.workspace_flowsheets_2_cdm_twf(conn, self.job_id)
            await primitives.workspace_lab_results_2_cdm_twf(conn, self.job_id)
            await primitives.workspace_lab_results_2_cdm_twf(conn, self.job_id)
