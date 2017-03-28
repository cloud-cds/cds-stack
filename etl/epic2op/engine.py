from etl.core.exceptions import TransformError
from etl.core.config import est_tsp_fmt, Config
from etl.epic2op.extractor import Extractor
from etl.transforms.pipelines import jhapi
from etl.load.pipelines.epic2op import Epic2OpLoader
import os, sys, traceback
import pandas as pd
import datetime as dt
import dateparser
import logging
import asyncpg
import ujson as json

class Engine():
    def __init__(self, hospital=None, lookback_hours=None):
        self.config = Config(debug=True)
        self.loader = Epic2OpLoader(self.config)
        self.extractor = Extractor(
            hospital =       hospital or os.environ['TREWS_ETL_HOSPITAL'],
            lookback_hours = lookback_hours or os.environ['TREWS_ETL_HOURS'],
            jhapi_server =   'prod' or os.environ['TREWS_ETL_SERVER'],
            jhapi_id =       os.environ['jhapi_client_id'],
            jhapi_secret =   os.environ['jhapi_client_secret'],
        )
        self.extract_time = dt.timedelta(0)
        self.transform_time = dt.timedelta(0)

    def skip_none(self, df, transform_function):
        if df is None or df.empty:
            return None
        try:
            start = dt.datetime.now()
            df = transform_function(df)
            logging.info("function time: {}".format(dt.datetime.now() - start))
            return df
        except TransformError as e:
            logging.error("== EXCEPTION CAUGHT ==")
            logging.error("error location:   " + e.func_name)
            logging.error("reason for error: " + e.reason)
            logging.error(e.context)
            traceback.print_exc()


    def transform(self, df, transform_list, df_name):
        logging.info("Transforming {}".format(df_name))
        start = dt.datetime.now()
        if df is None:
            return None
        if type(df) == list:
            df = pd.concat(df)
        for transform_fn in transform_list:
            df = self.skip_none(df, transform_fn)
        self.transform_time += (dt.datetime.now() - start)
        return df


    def extract(self, extract_func, extract_name, extract_func_args=[]):
        logging.info("Extracting {}".format(extract_name))
        start = dt.datetime.now()
        df = extract_func(*extract_func_args)
        self.extract_time += (dt.datetime.now() - start)
        return df


    def main(self):
        driver_start = dt.datetime.now()

        pats = self.extract(self.extractor.extract_bedded_patients, "bedded_patients")
        pats_t = self.transform(pats, jhapi.bedded_patients_transforms, "bedded_patients")
        pats_t = pats_t.assign(hospital = self.extractor.hospital)

        flowsheets = self.extract(self.extractor.extract_flowsheets, "flowsheets", [pats_t])
        flowsheets_t = self.transform(flowsheets, jhapi.flowsheet_transforms, "flowsheets")

        lab_orders = self.extract(self.extractor.extract_lab_orders, "lab_orders", [pats_t])
        lab_orders_t = self.transform(lab_orders, jhapi.lab_orders_transforms, "lab_orders")

        lab_procedures = self.extract(self.extractor.extract_lab_procedures, "lab_procedures", [pats_t])
        lab_procedures_t = self.transform(lab_procedures, jhapi.lab_procedures_transforms, "lab_procedures")

        lab_orders_t = lab_orders_t.append(lab_procedures_t) if lab_orders_t is not None else lab_procedures_t

        lab_results = self.extract(self.extractor.extract_lab_results, "lab_results", [pats_t])
        lab_results_t = self.transform(lab_results, jhapi.lab_results_transforms, "lab_results")

        loc_history = self.extract(self.extractor.extract_loc_history, "loc_history", [pats_t])
        loc_history_t = self.transform(loc_history, jhapi.loc_history_transforms, "loc_history")

        med_orders = self.extract(self.extractor.extract_med_orders, "med_orders", [pats_t])
        med_orders_t = self.transform(med_orders, jhapi.med_orders_transforms, "med_orders")
        med_orders_t['fid'] += '_order'

        request_data = med_orders_t[['pat_id', 'visit_id', 'ids']]\
            .groupby(['pat_id', 'visit_id'])['ids']\
            .apply(list)\
            .reset_index()
        ma_start = dt.datetime.now()
        med_admin = self.extract(self.extractor.extract_med_admin, "med_admin", [request_data])
        ma_total = dt.datetime.now() - ma_start
        med_admin_t = self.transform(med_admin, jhapi.med_admin_transforms, "med_admin")

        # Timezone hack
        def tz_hack(tsp):
            return (dateparser.parse(tsp) - dt.timedelta(hours=5)).strftime(est_tsp_fmt)
        flowsheets_t['tsp'] = flowsheets_t['tsp'].apply(tz_hack)
        med_admin_t['tsp'] = med_admin_t['tsp'].apply(tz_hack)

        # Main finished
        logging.info("extract time: {}".format(self.extract_time))
        logging.info("transform time: {}".format(self.transform_time))
        logging.info("total time: {}".format(dt.datetime.now() - driver_start))

        # Create stats object
        cloudwatch_stats = {
            'total_time':    (dt.datetime.now() - driver_start).total_seconds(),
            'request_time':  self.extract_time.total_seconds(),
            'bedded_pats':   len(pats_t.index),
            'flowsheets':    len(flowsheets_t.index),
            'lab_orders':    len(lab_orders_t.index),
            'lab_results':   len(lab_results_t.index),
            'med_orders':    len(med_orders_t.index),
            'med_admin':     len(med_admin_t.index),
            'loc_history':   len(loc_history_t.index),
        }

        # Prepare for database
        pats_t.diagnosis = pats_t.diagnosis.apply(json.dumps)
        pats_t.history = pats_t.history.apply(json.dumps)
        pats_t.problem = pats_t.problem.apply(json.dumps)
        med_orders_t.ids = med_orders_t.ids.apply(json.dumps)
        db_data = {
            'bedded_patients_transformed': pats_t,
            'flowsheets_transformed': flowsheets_t,
            'lab_orders_transformed': lab_orders_t,
            'lab_results_transformed': lab_results_t,
            'med_orders_transformed': med_orders_t,
            'med_admin_transformed': med_admin_t,
            'location_history_transformed': loc_history_t,
        }

        # TODO: send stats to cloudwatch

        self.loader.run_loop(db_data)

        # TODO: fillin
        # TODO: derive



if __name__ == '__main__':
    pd.set_option('display.width', 200)
    pd.set_option('display.max_rows', 30)
    pd.set_option('display.max_columns', 1000)
    pd.set_option('display.max_colwidth', 40)
    pd.options.mode.chained_assignment = None
    logging.getLogger().setLevel(0)
    engine = Engine()
    results = engine.main()
