from etl.transforms.primitives.df import restructure
import pandas as pd

from inpatient_updater.config import lab_procedures as lp_config

clarityData = "/Users/pmarian3/Desktop/clarityLoc"
clarityData+='/'

op = pd.read_csv(clarityData + 'op.csv')

op = restructure.select_columns(op, {'CSN_ID': 'csn_id',
                                    'proc_name':'fid',
                                    'ORDER_TIME': 'tsp',
                                    'OrderStatus':'order_status',
                                    'LabStatus':'proc_status',
                                    'PROC_START_TIME':'proc_start_tsp',
                                    'PROC_ENDING_TIME':'proc_end_tsp'})


def get_fid_name_mapping():
    lp_map = pd.read_csv(clarityData + 'lab_proc_dict.csv')
    fid_map = dict()
    for fid, codes in lp_config.procedure_ids:
        nameList = list()
        for code in codes:
            rs = lp_map[lp_map['proc_id'] == int(code)]['proc_name']
            if not rs.empty:
                nameList.append(rs.iloc[0])
        fid_map[fid] = nameList
    return fid_map

fid_map = get_fid_name_mapping()
for fid, names in fid_map.iteritems():
    for name in names:
        op.loc[op['fid']==name,'fid'] = fid

op = op[ [x in fid_map.keys() for x in op['fid']] ]


