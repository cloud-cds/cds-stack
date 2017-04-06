# import pickle
# import os
# import pandas as pd
# from sqlalchemy import create_engine, text\
# import email
# import os
#
# DB_CONN_STR = 'postgresql://{}:{}@{}:{}/{}'
# user = os.environ['db_user']
# host = os.environ['db_host']
# db = 'opsdx_dw'
# port = os.environ['db_port']
# password = os.environ['db_password']
# DB_CONN_STR = DB_CONN_STR.format(user, password, host, port, db)
#
# engine = create_engine(DB_CONN_STR)
# conn = engine.connect()
#
# op = pd.read_sql_query(text("""select * from "OrderProcs";"""), con=conn)
# op.to_pickle("/home/ubuntu/peterm/dat/op.pkl")
#
# ma = pd.read_sql_query(text("""select "CSN_ID", "display_name",
#                                       "Dose", "MedUnit",
#                                       "INFUSION_RATE", "MAR_INF_RATE_UNIT",
#                                       "TimeActionTaken"
#                                       from "MedicationAdministration" """),con=conn)
# ma.to_pickle("/home/ubuntu/peterm/dat/ma.pkl")
#
# mo = pd.read_sql_query(text("""select "display_name", "MedUnit","Dose" from "OrderMed";"""), con=conn)
# mo.to_pickle("/home/ubuntu/peterm/dat/mo.pkl")
#
# labs = pd.read_sql_query(text("""SELECT "CSN_ID", "NAME" ,
#                               "ResultValue", "RESULT_TIME", "REFERENCE_UNIT"
#                               FROM "Labs_643";"""), con=conn)
# labs.to_pickle("/home/ubuntu/peterm/dat/labs.pkl")
#
# lp_map = pd.read_sql_query(text("""SELECT * FROM lab_proc_dict;"""), con=conn)
# lp_map.to_pickle("/home/ubuntu/peterm/dat/lp_map.pkl")
#
# conn.close()