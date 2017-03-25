from etl.core.exceptions import TransformError
from etl.core.config import est_tsp_fmt
from etl.online.extractor import Extractor
from etl.transforms.pipelines import jhapi
import os, sys, traceback
import pandas as pd
import datetime as dt
import logging

class Engine():
    def __init__(self, extractor):
        self.extractor = extractor

    def skip_none(self, df, transform_function):
        if df is None or df.empty:
            return None
        try:
            start = dt.datetime.now()
            df = transform_function(df)
            print("function time: {}".format(dt.datetime.now() - start))
            return df
        except TransformError as e:
            print("== EXCEPTION CAUGHT ==")
            print("error location:   " + e.func_name)
            print("reason for error: " + e.reason)
            print(e.context)
            traceback.print_exc()


    def transform(self, df, transform_list, df_name):
        print("Transforming {}".format(df_name))
        if df is None:
            return None
        if type(df) == list:
            df = pd.concat(df)
        for transform_fn in transform_list:
            df = self.skip_none(df, transform_fn)
        return df


    def main(self):
        driver_start = dt.datetime.now()

        print("\n\nBEDDED PATIENTS")
        pats = self.extractor.extract_bedded_patients()
        pats_t = self.transform(pats, jhapi.bedded_patients_transforms, "bedded_patients")
        pats_t = pats_t.assign(hospital = self.extractor.hospital)
        print(pats_t)

        # print("\n\nFLOWSHEETS")
        # flowsheets = self.extractor.extract_flowsheets(pats_t)
        # flowsheets_t = self.transform(flowsheets, jhapi.flowsheet_transforms, "flowsheets")
        # print(flowsheets_t)
        #
        # print("\n\nLAB ORDERS")
        # lab_orders = self.extractor.extract_lab_orders(pats_t)
        # lab_orders_t = self.transform(lab_orders, jhapi.lab_orders_transforms, "lab_orders")
        # print(lab_orders_t)
        #
        # print("\n\nLAB PROCEDURES")
        # lab_procedures = self.extractor.extract_lab_procedures(pats_t)
        # lab_procedures_t = self.transform(lab_procedures, jhapi.lab_procedures_transforms, "lab_procedures")
        # print(lab_procedures_t)
        #
        # lab_orders_t = lab_orders_t.append(lab_procedures_t) if lab_orders_t is not None else lab_procedures_t
        #
        # print("\n\nLAB RESULTS")
        # lab_results = self.extractor.extract_lab_results(pats_t)
        # lab_results_t = self.transform(lab_results, jhapi.lab_results_transforms, "lab_results")
        # print(lab_results_t)
        #
        # print("\n\nLOCATION HISTORY")
        # loc_history = self.extractor.extract_loc_history(pats_t)
        # loc_history_t = self.transform(loc_history, jhapi.loc_history_transforms, "loc_history")
        # print(loc_history_t)

        print("\n\nMED ORDERS")
        med_orders = self.extractor.extract_med_orders(pats_t)
        med_orders_t = self.transform(med_orders, jhapi.med_orders_transforms, "med_orders")
        med_orders_t['fid'] += '_order'
        print(med_orders_t)

        # print("\n\nMED ADMIN")
        # request_data = med_orders_t[['pat_id', 'visit_id', 'ids']]\
        #     .groupby(['pat_id', 'visit_id'])['ids']\
        #     .apply(list)\
        #     .reset_index()
        # ma_start = dt.datetime.now()
        # med_admin = self.extractor.extract_med_admin(request_data)
        # ma_total = dt.datetime.now() - ma_start
        # med_admin_t = self.transform(med_admin, jhapi.med_admin_transforms, "med_admin")

        # Timezone hack
        def tz_hack(tsp):
            return (dateparser.parse(tsp) - dt.timedelta(hours=5)).strftime(est_tsp_fmt)
        flowsheets_t['tsp'] = flowsheets_t['tsp'].apply(tz_hack)
        med_admin_t['tsp'] = med_admin_t['tsp'].apply(tz_hack)

        # Main finished
        print("total time: ", str(dt.datetime.now() - driver_start))

        # Create stats object
        # stats = {
        #     'total_time':       (dt.datetime.now() - driver_start).total_seconds(),
        #     'request_time':     (api_request_total + ma_total).total_seconds(),
        #     'bedded_pats':      len(pats_t.index),
        #     'flowsheets':       len(flowsheets_t.index),
        #     'lab_orders':       len(lab_orders_t.index),
        #     'lab_results':      len(lab_results_t.index),
        #     'med_orders':       len(med_orders_t.index),
        #     'med_admin':        len(med_admin_t.index),
        #     'location_history': len(location_history_t.index),
        # }

        # return {
        #     'pats': pats, 'pats_t': pats_t,
        #     'flowsheets': pd.concat(flowsheets), 'flowsheets_t': flowsheets_t,
        #     'lab_orders': pd.concat(lab_orders), 'lab_orders_t': lab_orders_t,
        #     'lab_results': pd.concat(lab_results), 'lab_results_t': lab_results_t,
        #     'med_orders': pd.concat(med_orders), 'med_orders_t': med_orders_t,
        #     'med_admin': pd.concat(med_admin), 'med_admin_t': med_admin_t,
        #     'location_history': pd.concat(location_history), 'location_history_t': location_history_t,
        #     'stats': stats
        # }


if __name__ == '__main__':
    pd.set_option('display.width', 200)
    pd.set_option('display.max_rows', 30)
    pd.set_option('display.max_columns', 1000)
    pd.set_option('display.max_colwidth', 40)
    pd.options.mode.chained_assignment = None
    logging.getLogger().setLevel(0)
    ex = Extractor(
        hospital =       'HCGH',
        lookback_hours = 25,
        jhapi_server =   'prod',
        jhapi_id =       '09487db62cdc41d0a6fafa57a2cd30f5',
        jhapi_secret =   '7e415e173a7149029606B508289D4799'
    )
    engine = Engine(ex)
    results = engine.main()
