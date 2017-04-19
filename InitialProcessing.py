
# ## 1. Download data frame and save a local copy
# ### import libraries and set global variables
# updated: 10/21/2016
# Note: introduce dashan_id with frame_id; separate diagnosis, medical history, and, problem list.
# 
# updated: 10/18/2016
# Note: minor chagnes: disable "convert gender to int" since gender is integer now; 
# 
# updated: 9/6/2016
# Note: minor changes: convert gender to int; add subtype approx features
# 
# updated: 5/26/2016
# Note: make full dataset include data after shock
# 
# updated: 4/17/2016
# Note: remove sirs_intp in the feature_list

from dashan_app_sepsis.DashanInput import InputParamFactory, inputParams
from dashan_core.src.ews_client.client import DataFrameFactory, DataFrame, Session
from dashan_app_sepsis import sepsis_functions as func
import numpy as np
import sys
import time

def GenerateDataFrame(inputPassedIn):
    inputFact = InputParamFactory()
    inputValues = inputFact.parseInput(inputPassedIn)

    print inputValues

    dashan_id = inputValues.dashan_id
    data_id = inputValues.data_id
    feature_list = inputValues.feature_list

    print 'Generating data for ' + data_id
    #==============================================================
    ## Add Mandatory Columns
    #==============================================================
    # 'cmi', 'septic_shock', 'severe_sepsis'
    if 'cmi' not in feature_list:
        feature_list.append('cmi')
    if 'septic_shock' not in feature_list:
        feature_list.append('septic_shock')
    if 'severe_sepsis' not in feature_list:
        feature_list.append('severe_sepsis')

    #==============================================================
    ## Download Data Frame
    #==============================================================
    print "selected features for sepsis:", feature_list

    session = Session(dashan_id)
    session.log.info('connect to the database')
    session.connect()
    session.log.info('dowloading ...')

    sql, feature_name_lst = session.build_sql_string(feature_list, nrows=inputValues.maxNumRows)
    data_frame = session.download_sql_string(sql, (['enc_id', 'tsp'] + feature_name_lst))

    data_frame.save(data_id)
    session.disconnect()
    session.log.info('disconnect to the database')
    #==============================================================
    ## Clean Data Frame
    #==============================================================
    if "minutes_since_any_antibiotics" in feature_list:
        none_idx = np.logical_or(
            data_frame[:, "minutes_since_any_antibiotics"] == np.array(None),
            np.less(data_frame[:, "minutes_since_any_antibiotics"],0))
        data_frame[none_idx, "minutes_since_any_antibiotics"] = 0

    # replace none values from minutes_since_organ_fail to -60*1000
    if "minutes_since_any_organ_fail" in feature_list:
        none_idx = np.logical_or(
            data_frame[:, "minutes_since_any_organ_fail"] == np.array(None),
            np.less(data_frame[:, "minutes_since_any_organ_fail"],0)) # negative means before anti-biotics adminsitration
        data_frame[none_idx, "minutes_since_any_organ_fail"] = 0

    if "fluids_intake_24hr" in feature_list:
        # replace none values from fluids_intake_24hrs to 0
        none_idx = data_frame[:, "fluids_intake_24hr"] == np.array(None)
        data_frame[none_idx, "fluids_intake_24hr"] = 0

    feature_list = [f for f in data_frame.get_feature_names() if f.endswith('_diag') or f.endswith('_hist') or f.endswith('_prob')]
    for feature in feature_list:
        data_frame.replace_none(feature, False)


    print "processed data frame:"
    print "shape:", data_frame.shape
    print "column_names:", data_frame.colnames()

    #==============================================================
    ## generate min_to_cmi, min_to_shock, min_to_severe_sepsis
    #==============================================================
    colnames = data_frame.colnames()
    data_frame_p_full = data_frame
    data_frame_p_full.save('%s_cleaned' % data_id)

def getAdverseEventName(inputValues):
    data_id = inputValues.data_id
    censoring_event = inputValues.censoring_event
    adverse_event = inputValues.adverse_event

    name = data_id + '.' + adverse_event
    return name

def adverseEventProcessing(inputValues):

    #@peter, you have to fix all of these

    data_id = inputValues.data_id
    name_string = getAdverseEventName(inputValues)
    # ===================================
    # Load Data
    # ===================================

    print time.strftime("%H:%M:%S") + " loading  dataset "
    factory = DataFrameFactory()
    data_frame_p_full = factory.load('%s_cleaned' % data_id)
    print time.strftime("%H:%M:%S") + " load complete"

    # ===================================
    # Get Adverse Event Information and filter by adverse event
    # ===================================

    min_to_censorEvent = func.minutes_to_feature(data_frame_p_full, inputValues.censoring_event)  # replacing min_to_cmi
    min_to_adverseEvent = func.minutes_to_feature(data_frame_p_full, inputValues.adverse_event,
                                                  allowNegative=True)  # replacing min to shock

    min_to_max_hosp_time = func.minutes_to_maximum_hospital_time(data_frame_p_full[:, :2])
    after_adverseEvent = min_to_adverseEvent < 0

    # ### remove samples after shock onset, and nans
    before_adverseEvent = ~ after_adverseEvent

    min_to_adverseEvent = min_to_adverseEvent[before_adverseEvent]
    min_to_censorEvent = min_to_censorEvent[before_adverseEvent]
    min_to_max_hosp_time = min_to_max_hosp_time[before_adverseEvent]
    min_to_cens = func.minutes_to_censor(min_to_censorEvent, min_to_adverseEvent)

    # ### save them to files
    np.savetxt('%s.min_to_censorEvent.txt' % name_string, min_to_censorEvent, delimiter='\n',
               fmt='%f')  # Used in evaluate and evaluate all
    np.savetxt('%s.min_to_adverseEvent.txt' % name_string, min_to_adverseEvent, delimiter='\n',
               fmt='%f')  # Used in evaluate and evaluate all

    data_frame_p = DataFrame(data_frame_p_full[before_adverseEvent], data_frame_p_full.colnames())
    data_frame_p.save('%s_processed' % name_string)

    # ==============================================================
    # ## Generate left and right edges
    # ==============================================================
    # ### calculate the left and right edges
    # |min_to_cmi   |    min_to_shock    |    left_edge    |    right_edge    |
    # |:-----------:|:------------------:|:---------------:|:----------------:|
    # |     x       |         y          |        x        |          y       |
    # |     y       |         x          |        x        |          x       |
    # |     x       |        nan         |        x        |         inf      |
    # |    nan      |         y          |        y        |          y       |
    # |    nan      |        nan         |       max       |         inf      |
    # Here, x < y.
    #
    # Right censored: right_edge is inf
    #
    # Interval censored: both left and right edge are not inf

    left_edge, right_edge = func.generate_edges(min_to_cens, min_to_adverseEvent,
                                                min_to_max_hosp_time)

    np.savetxt('%s.left_edge.txt' % name_string, left_edge, delimiter='\n', fmt='%f')
    np.savetxt('%s.right_edge.txt' % name_string, right_edge, delimiter='\n',
               fmt='%f')  # needed for both train and evaluate

if __name__ == "__main__":
    inputPassedIn = sys.argv[1]
    GenerateDataFrame(inputPassedIn)
    adverseEventProcessing(inputPassedIn)



