from datetime import datetime
import pandas as pd
import sqlalchemy
from datetime import datetime as dt
from datetime import timedelta
import numpy as np
from pytz import timezone
from collections import OrderedDict
import json

#---------------------------------
## Metric Classes
#---------------------------------
# Abstract base class metric

class metric(object):

  def __init__(self,connection, first_time_str, last_time_str):
    self.name = 'abstract metric'
    self.connection = connection
    self.first_time_str = first_time_str
    self.last_time_str = last_time_str
    self.data = []

  def calc(self):
    print("No calc defined for metric {}".format(self.name))
  def to_html(self):
    print("No to_html defined for metric {}".format(self.name))
  def to_cwm(self):
    print("No to_cwm defined for metric {}".format(self.name))


class report_introduction(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'Introduction'

  def to_html(self):
    html = '<p>'
    html += 'The following report covers times between {s} and {e}'.format(s=self.first_time_str, e=self.last_time_str)
    html += '</p>'
    return html

class ed_metrics(metric):

  def __init__(self, connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'Emergency Department Metrics'
    self.window = timedelta(days=2)
    #self.window = timedelta(days=7) ## For debugging only
    self.connection = connection

  ## Takes max_time to select for patients in a specific timeframe.
  def get_enc_ids(self, discharge_time):
    # Removes:
    # patients younger than 18.
    # patients from depts that deal with patients younger than 18 (e.g emergency pediatrics)
    # Remaining patients are either bedded (still in hospital) or have been discharged after the discharge_time. Currently code works for solely for HCGH.
      query = """with excluded_encids as (

                select distinct EXC.enc_id
                from cdm_t EXC
                inner join cdm_s on cdm_s.enc_id = EXC.enc_id and cdm_s.fid = 'age'
                inner join cdm_t on cdm_t.enc_id = EXC.enc_id and cdm_t.fid = 'care_unit'
                group by EXC.enc_id
                having count(*) filter (where cdm_s.value::numeric < 18) > 0
                or count(*) filter(where cdm_t.value in ('HCGH LABOR & DELIVERY', 'HCGH EMERGENCY-PEDS', 'HCGH 2C NICU', 'HCGH 1CX PEDIATRICS', 'HCGH 2N MCU')) > 0
            ),
            bedded as (

                select distinct BP.enc_id
                from get_latest_enc_ids('HCGH') BP
                where BP.enc_id not in(
                    select enc_id from excluded_encids
                )
            ),
            discharged as (
                select distinct enc_id from cdm_t
                where fid='discharge' and
                tsp > '{0}'
                and enc_id not in (
                    select enc_id from excluded_encids
                )
                and value::json ->> 'department' like '%HCGH%'
            )
            select enc_id
            from (
                (select d.enc_id from discharged d)
                union
                (select b.enc_id from bedded b)
            ) R1""".format(str(discharge_time))

      encids_df = pd.read_sql(sqlalchemy.text(query), self.connection)
      enc_ids = encids_df['enc_id'].as_matrix().astype(int)
      return enc_ids

  ## Modified to remove home medication cases.
  def get_cdmt_df(self, valid_enc_ids):

    ### read cdm_t to get min/max_tsp and build care_unit_df
    query = """select enc_id, tsp, fid, value from cdm_t where enc_id in ({0})
               order by enc_id, tsp""".format(', '.join([str(e) for e in valid_enc_ids]))
    cdmt_df = pd.read_sql(sqlalchemy.text(query), self.connection, columns=['enc_id', 'tsp', 'fid', 'value'])
    cdmt_df['tsp'] = pd.to_datetime(cdmt_df['tsp']).dt.tz_convert(timezone('utc'))
    return cdmt_df

  def get_criteria_events_df(self, valid_enc_ids, start_date):
    start_date = start_date.round('S') # Match that of the start_date.

    ## fetch criteria_events as base table. Keep all columns for now, but may truncate depending on usage.
    ## Currently using start_date (date that we deployed TREWS) as cutoff. Need to consider patients that may have been admitted earlier and have states that would be missing.
    query = """ select * from criteria_events where update_date > '{0}' and enc_id in ({1})""".format(str(start_date), ','.join([str(e) for e in valid_enc_ids]))
    criteria_events_df = pd.read_sql(sqlalchemy.text(query), self.connection, columns=['event_id', ' enc_id', 'name', 'is_met', 'measurement_time', 'override_time','override_user','override_value','value','update_date', 'flag','is_acute'])
    criteria_events_df['update_date'] = pd.to_datetime(criteria_events_df['update_date']).dt.tz_convert(timezone('utc'))
    return criteria_events_df


  ## Modified get_care_unit to remove home med.
  def get_care_unit_remove_homeMed(self, cdmt_df, start_tsp):

      ### Find hospital enter time for each patient min(care_unit, lactate/blood/abx_order, vitals)
      ## Find first care_unit admits
      first_CU_admits = cdmt_df.loc[cdmt_df['fid']=='care_unit'].groupby('enc_id', as_index=False)['tsp'].idxmin()
      first_CU_admits = cdmt_df.ix[first_CU_admits]
      first_CU_admits.rename(columns={'tsp':'1st_care_unit'}, inplace=True)

      ## Find first lactate/blood/abx_order, (does not include home medication orders)
      first_orders = cdmt_df.loc[cdmt_df['fid'].str.contains('(blood_culture|lactate)_order')].groupby('enc_id', as_index=False)['tsp'].idxmin()
      first_orders = cdmt_df.ix[first_orders]
      first_orders.rename(columns={'tsp':'1st_order'}, inplace=True)

      ## First vitals order
      first_vitals = cdmt_df.loc[cdmt_df['fid'].str.contains('heart_rate|temperature|co2')].groupby('enc_id', as_index=False)['tsp'].idxmin()
      first_vitals = cdmt_df.ix[first_vitals]
      first_vitals.rename(columns={'tsp':'1st_vitals'}, inplace=True)

      ## Combine and take min
      allMins = pd.merge(first_CU_admits[['enc_id','1st_care_unit']], first_orders[['enc_id', '1st_order']], on='enc_id', how='left')
      allMins = pd.merge(allMins, first_vitals[['enc_id', '1st_vitals']], on='enc_id', how='left')
      allMins['min_enter_time'] = allMins.apply(lambda x: min(x['1st_care_unit'], x['1st_order'], x['1st_vitals']), axis=1)

      ## Change cdmt_df to remove home meds before returning
      cdmt_df_no_homeMed = pd.merge(cdmt_df, allMins[['enc_id', 'min_enter_time']], on='enc_id', how='left')
      cdmt_df_no_homeMed = cdmt_df_no_homeMed.loc[cdmt_df_no_homeMed['tsp'] >= cdmt_df_no_homeMed['min_enter_time']]

      ### Build care_unit_df
      care_unit_df = cdmt_df.loc[cdmt_df['fid']=='care_unit', ['enc_id', 'tsp', 'value']].copy()
      care_unit_df = care_unit_df.sort_values(by=['enc_id', 'tsp'])
      care_unit_df.rename(columns={'tsp':'enter_time', 'value':'care_unit'}, inplace=True)
      ## At this point in code, home medication patients have correct time.
      care_unit_df['leave_time'] = care_unit_df.groupby('enc_id')['enter_time'].shift(-1)
      care_unit_df = care_unit_df.loc[care_unit_df['care_unit']!='Discharge']

      ## Set the min enter time using allMins
      first_care_unit = care_unit_df.groupby('enc_id', as_index=False)['enter_time'].idxmin()
      updated_enter_time = pd.merge(care_unit_df.ix[first_care_unit], allMins[['enc_id', 'min_enter_time']], on='enc_id', how='inner').set_index(care_unit_df.ix[first_care_unit].index)
      updated_enter_time['enter_time'] = updated_enter_time['min_enter_time']
      updated_enter_time.drop('min_enter_time', axis=1, inplace=True)
      care_unit_df.update(updated_enter_time)

      ## For some reason, bug in pandas where join on enc_id returns enc_id as floats. Not problem for this application since numbers are small so no precision errors.
      care_unit_df['enc_id'] = care_unit_df['enc_id'].astype('int')

      ## Now populate last leave_time as last cdm_t entry
      with_care_unit_cdmt = cdmt_df.loc[cdmt_df['enc_id'].isin(care_unit_df['enc_id']), ['enc_id', 'tsp']]
      max_tsp_df = with_care_unit_cdmt.groupby('enc_id', as_index=False)['tsp'].agg({'max_tsp':max})

      care_unit_df = pd.merge(care_unit_df, max_tsp_df, how='left', on='enc_id')
      idx_max = care_unit_df.groupby('enc_id', as_index=False)['enter_time'].idxmax()
      care_unit_df.loc[idx_max, 'leave_time'] = care_unit_df.loc[idx_max, 'max_tsp'] + pd.to_timedelta('1min')
      care_unit_df.drop(['max_tsp'], axis=1, inplace=True)

      ## Keep ED care_units that have an additional ED time that was broken up.
      ED_leave_time = care_unit_df.loc[care_unit_df['care_unit'] == 'HCGH EMERGENCY-ADULTS']
      ED_leave_time = ED_leave_time.groupby('enc_id', as_index=False)['leave_time'].agg(max)
      ED_leave_time.rename(columns={'leave_time':'ED_leave_time'}, inplace=True)
      care_unit_df = pd.merge(care_unit_df, ED_leave_time, on='enc_id', how='left')

      ## Remove all ED care_units that don't have max time after leave time
      care_unit_df = care_unit_df.loc[~((care_unit_df['care_unit'] == 'HCGH EMERGENCY-ADULTS') & (care_unit_df['ED_leave_time'] < start_tsp))]

      ## Remove all remaining non-ED care_unit stays where patient leaves ED before start of window
      care_unit_df = care_unit_df.loc[~((care_unit_df['leave_time'] < start_tsp) & (care_unit_df['care_unit'] != 'HCGH EMERGENCY-ADULTS'))]

      ## If enter time is before the report start date, we truncate to the start_tsp to guarantee time window for report metrics.
      care_unit_df['enter_time'] = pd.to_datetime(care_unit_df['enter_time']).dt.tz_localize(timezone('utc'))
      care_unit_df['report_start_time'] = care_unit_df['enter_time'].apply( lambda x, start_tsp=start_tsp: start_tsp if x < start_tsp else x)
      care_unit_df['leave_time'] = pd.to_datetime(care_unit_df['leave_time']).dt.tz_localize(timezone('utc'))

      return(care_unit_df, cdmt_df_no_homeMed)

  ## Currently used to test merge care_unit. Should be same as previous code since care_units need to be fetched from cdm_t
  def get_care_unit(self, cdmt_df):

      care_unit_df = cdmt_df.loc[cdmt_df['fid']=='care_unit', ['enc_id', 'tsp', 'value']].copy()
      care_unit_df = care_unit_df.sort_values(by=['enc_id', 'tsp'])
      care_unit_df.rename(columns={'tsp':'enter_time', 'value':'care_unit'}, inplace=True)
      care_unit_df['leave_time'] = care_unit_df.groupby('enc_id')['enter_time'].shift(-1)
      care_unit_df = care_unit_df.loc[care_unit_df['care_unit']!='Discharge']

      with_care_unit_cdmt = cdmt_df.loc[cdmt_df['enc_id'].isin(care_unit_df['enc_id']), ['enc_id', 'tsp']]
      max_min_tsp_df = with_care_unit_cdmt.groupby('enc_id', as_index=False)['tsp'].agg({'max_tsp':max, 'min_tsp':min})

      care_unit_df = pd.merge(care_unit_df, max_min_tsp_df, how='left', on='enc_id')
      idx_max = care_unit_df.groupby('enc_id', as_index=False)['enter_time'].idxmax()
      idx_min = care_unit_df.groupby('enc_id', as_index=False)['enter_time'].idxmin()
      care_unit_df.loc[idx_min, 'enter_time'] = care_unit_df.loc[idx_min, 'min_tsp'] - pd.to_timedelta('1min')
      care_unit_df.loc[idx_max, 'leave_time'] = care_unit_df.loc[idx_max, 'max_tsp'] + pd.to_timedelta('1min')
      care_unit_df.drop(['min_tsp', 'max_tsp'], axis=1, inplace=True)

      care_unit_df['leave_time'] = pd.to_datetime(care_unit_df['leave_time']).dt.tz_localize(timezone('utc'))

      return care_unit_df

  def get_cdm_twf_df(self, valid_enc_ids, start_date):
    start_date = start_date.round('S')
    query = """select enc_id, tsp, sirs_resp_oor, sirs_hr_oor, sirs_wbc_oor, sirs_temperature_oor from cdm_twf
               where tsp > '{0}' and enc_id in ({1})""".format(str(start_date), ','.join([str(e) for e in valid_enc_ids]))
    cdm_twf_df = pd.read_sql(sqlalchemy.text(query), self.connection, columns = ['enc_id', 'tsp', 'resp', 'hr', 'wbc', 'temperature'])
    return cdm_twf_df

  def calc(self):
    # TREWS Deployment date. Cannot run metrics before this date.
    deploy_tsp = pd.to_datetime('2017-11-06 16:00:00+00:00').tz_localize(timezone('utc'))

    # Use timestamp of when script is run. Can potentially hardcode instead but should be okay if running as CRON job.
    end_tsp = pd.to_datetime(self.last_time_str).tz_localize(timezone('utc'))

    start_tsp = end_tsp - self.window

    # For generating HTML only
    self.report_start = start_tsp.strftime('%x %X %Z')
    self.report_end = end_tsp.strftime('%x %X %Z')

    ## get_valid_enc_ids. See function for exclusion details.
    valid_enc_ids = self.get_enc_ids(start_tsp)

    ## get flags from criteria_events. Currently just taking end_tsp but can do better in future
    criteria_events_df = self.get_criteria_events_df(valid_enc_ids, deploy_tsp)

    ## get cdm_t table to fetch care units
    cdmt_df = self.get_cdmt_df(valid_enc_ids)

    ## Fetch care units using cdmt_df
    care_unit_df, cdmt_df = self.get_care_unit_remove_homeMed(cdmt_df, start_tsp)

    ## Subset cdmt_df to only take entries after they are admitted to the hospital. Removes home medications such as cms_abx_alert
    def merge_with_care_unit(main_df, care_unit_df=care_unit_df):
      tmp_df = pd.merge(main_df, care_unit_df, how='left', on='enc_id')
      ind1 = tmp_df['update_date']>tmp_df['enter_time']
      ind2 = tmp_df['update_date']<tmp_df['leave_time']
      ind3 = tmp_df['leave_time'].isnull()
      tmp_df = tmp_df.loc[((ind1)&(ind2))|((ind1)&(ind3)), :]
      return tmp_df

    ## Merge criteria_evens with care_unit_df
    merged_df = merge_with_care_unit(criteria_events_df)

    ## Use merged_df from now on since almost all metrics are for patients that have some TREWS alert
    merged_df['flag'] = merged_df['flag'].apply(lambda x: x + 1000 if x < 0 else x) ## Want to see history

    ##### Compute metrics with merged_df #######
    # Consider better naming scheme for metrics

    ## Get rid of all entries not in ED
    merged_df_ED = merged_df.loc[merged_df['care_unit'] == 'HCGH EMERGENCY-ADULTS'] ## Check that the name is correct
    merged_df_ED.loc[:,'flag'] = merged_df_ED['flag'].apply(lambda x: x + 1000 if x < 0 else x) ## Want to see history

    # Metric 1: Total number of people in ED
    metric_1 = str(care_unit_df.loc[care_unit_df['care_unit'] == 'HCGH EMERGENCY-ADULTS']['enc_id'].nunique())

    ## Pass merged_df_ED to only search through criteria_events where patient was in ED.
    def search_history_flags(metric, flags, merged_df=merged_df):
      merged_df.loc[:,metric] = merged_df['flag'].apply(lambda x, flags=flags: True if x in flags else False)
      result = merged_df[['enc_id',metric]].groupby('enc_id').aggregate(np.sum)
      result = result.loc[result[metric] > 0].count()
      return result[metric]

    ## Get all patients that have a TREWS alert in their history
    alert_flags = [10,11]
    metric_2 = str(search_history_flags('TREWS_alert', alert_flags, merged_df_ED))

    ## Exit calc if there are no TREWS alerts. Need to manually examine data if there are no alerts.
    if metric_2 == '0':
      self.no_alerts = True
      return
    else:
      self.no_alerts = False

    ## Metrics 3,4,5,6 need code sepsis data
    metric_3 = None
    metric_4 = None
    metric_5 = None
    metric_6 = None

    ## Get all patients that have a manual override while in ED
    override_flags = range(50,67)
    metric_7 = str(search_history_flags('has_manual_override', override_flags, merged_df_ED))

    ## get trews_model_id
    model_id_query = "select value from trews_parameters where name='trews_jit_model_id';"
    model_id_df = pd.read_sql(sqlalchemy.text(model_id_query), self.connection, columns=['value'])
    model_id = model_id_df['value'].as_matrix().astype(int)[0]

    # read trews_jit_alerts
    query = """
                select enc_id, tsp, orgdf_details::json ->> 'alert' as jit_alert
                from trews_jit_score
                where model_id={0}
                and enc_id in ({1})
                and tsp >= '{2}'""".format(str(model_id), ', '.join([str(e) for e in valid_enc_ids]), str(start_tsp))
    trews_jit_df = pd.read_sql(sqlalchemy.text(query), self.connection, columns=['enc_id', 'tsp', 'jit_alert'])
    trews_jit_df['tsp'] = pd.to_datetime(trews_jit_df['tsp']).dt.tz_convert(timezone('utc'))
    trews_jit_df['jit_alert'] = trews_jit_df['jit_alert'].map({'True':1, 'False':0}).astype(float)

    # Only take scores that give alerts
    trews_jit_df = trews_jit_df.loc[trews_jit_df['jit_alert'] == 1]

    # Select manual overrides
    first_override_indices = merged_df_ED.loc[merged_df_ED['flag'].isin(override_flags)].groupby('enc_id', as_index=False)['update_date'].idxmin()
    first_override = merged_df_ED.loc[merged_df_ED.index.isin(first_override_indices)]

    if first_override.empty:
      metric_8 = str(0)
      metric_9 = str(None)
    else:
      ## Need right join in order to add first_override to every trews_jit_df entry to select for min tsp. Inner will delete.
      override_with_scores = pd.merge(first_override, trews_jit_df, how='right', on=['enc_id'])
      override_with_scores = override_with_scores.loc[override_with_scores['tsp'] >= override_with_scores['update_date']]
      earliest_jit_alert = override_with_scores.groupby('enc_id', as_index=False)['tsp'].idxmin()
      override_with_scores = override_with_scores.loc[override_with_scores.index.isin(earliest_jit_alert)]
      override_with_scores['delta'] = (override_with_scores['tsp'] - override_with_scores['update_date']) / pd.to_timedelta('1hr')

      metric_8 = str(override_with_scores['enc_id'].nunique())
      metric_9 = '{0:.3f}'.format(override_with_scores['delta'].median())

    first_alert_indices = merged_df_ED.loc[merged_df_ED['flag'].isin(alert_flags)].groupby('enc_id', as_index=False)['update_date'].idxmin()

    first_alerts = merged_df_ED.loc[merged_df_ED.index.isin(first_alert_indices)]
    first_alerts = first_alerts.rename(columns = {'update_date':'1st_alert_date'})

    ## Adds min and max date for arg order to first_alerts df.
    def search_cdm_t(order, first_alerts=first_alerts):
      # col names to appear in first_alerts.
      min_order = 'min_tsp_{0}'.format(order)
      max_order = 'max_tsp_{0}'.format(order)
      order_tsps = (cdmt_df.loc[(cdmt_df['fid'] == order) &
                                   (cdmt_df['enc_id'].isin(first_alerts['enc_id']))]
                       .groupby('enc_id', as_index=False)['tsp']
                       .aggregate({min_order:min, max_order:max}))
      first_alerts = pd.merge(first_alerts, order_tsps, on='enc_id', how='left')
      return first_alerts

    # Number of patients that have ordered Antibioitics after an alert
    first_alerts = search_cdm_t('cms_antibiotics_order', first_alerts)
    metric_10 = str(first_alerts.loc[(first_alerts['min_tsp_cms_antibiotics_order'] > first_alerts['1st_alert_date']), 'enc_id'].nunique())

    # Number of patients that have ordered blood culture after an alert
    first_alerts = search_cdm_t('blood_culture_order', first_alerts)
    metric_11 = str(first_alerts.loc[(first_alerts['min_tsp_blood_culture_order'] > first_alerts['1st_alert_date']), 'enc_id'].nunique())

    # Number of patients that receive lactate after an alert
    first_alerts = search_cdm_t('lactate_order', first_alerts)
    metric_12 = str(first_alerts.loc[(first_alerts['min_tsp_lactate_order'] > first_alerts['1st_alert_date']), 'enc_id'].nunique())

    # Number of patients that received a second lactate after an alert
    def search_second_lactate(merged_df_ED_row):
      if merged_df_ED_row['1st_lactate_date'] == False: # no lactate order return false
        return False
      else:
        ordered_dates = cdmt_df.loc[(cdmt_df['enc_id'] == merged_df_ED_row['enc_id']) & (cdmt_df['fid'] == 'lactate_order')]['tsp']
        ordered_dates = ordered_dates.sort_values()
        if ordered_dates.shape[0] < 2: # only 1 lactate order still return False
          return False
        else:
          return ordered_dates[1] ## Second lactate order

    ## Still need to fix metric_13 for repeat lactates
    #first_alerts['2nd_lactate_date'] = first_alerts.apply(search_second_lactate, axis=1)
    #metric_13 = str(first_alerts.shape[0] - first_alerts.loc[first_alerts['2nd_lactate_date'] == False].shape[0])
    metric_13 = metric_12 ## Placeholder for now

    ## Get all patients that have no infection recorded in their history
    metric_14 = str(search_history_flags('no_SOI_entered', [12,13], merged_df_ED))

    ## Get all patients that have been placed on sepsis pathway
    metric_15 = str(search_history_flags('on_sepsis_path', range(20,67), merged_df_ED))

    ## Get all patients that have no action taken despite TREWS alert.
    states_after_first_alert = merged_df_ED[['enc_id', 'update_date', 'flag']]
    states_after_first_alert = pd.merge(states_after_first_alert, first_alerts[['enc_id','1st_alert_date']], how='left')

    ## Could be that update date is the first alert date since cdm_t only tracks when something happens.
    states_after_first_alert = states_after_first_alert.loc[states_after_first_alert['update_date'] >= states_after_first_alert['1st_alert_date']]

    states_after_first_alert['action'] = states_after_first_alert['flag'] > 11
    states_after_first_alert['TREWS_active'] = states_after_first_alert['flag'] == 11
    states_after_first_alert['CMS_active'] = states_after_first_alert['flag'] == 10
    no_action = states_after_first_alert.groupby('enc_id', as_index=False)['action'].aggregate(np.sum)
    no_action = set(no_action.loc[no_action['action'] == 0]['enc_id'].unique())
    metric_16 = str(len(no_action))

    ## min, max, median time from alert to evaluation
    # Only considered eval-ed if SOI is_met is also true
    evals = merged_df.loc[(merged_df['name'] == 'suspicion_of_infection') & (merged_df['is_met'] == True)]

    first_eval_indices = evals.groupby('enc_id', as_index=False)['update_date'].idxmin()
    first_evals = evals.loc[evals.index.isin(first_eval_indices)][['enc_id','update_date']]
    first_evals.columns = ['enc_id', 'first_eval']

    ## If there are no evaluations, then report None for time from alert to eval.
    if first_evals.empty:
      metric_17_min = str(None)
      metric_17_max = str(None)
      metric_17_median = str(None)
    else:
      first_alerts = pd.merge(first_alerts, first_evals, how='left', on=['enc_id']) ## Not every alert had an eval
      first_alerts['delta'] = (first_alerts['first_eval'] - first_alerts['1st_alert_date']) / pd.to_timedelta('1hr')
      metric_17_min = '{0:.3f}'.format(first_alerts['delta'].min())
      metric_17_max = '{0:.3f}'.format(first_alerts['delta'].max())
      metric_17_median = '{0:.3f}'.format(first_alerts['delta'].median())

    metric_17 = '{0}'.format(', '.join([metric_17_min, metric_17_median, metric_17_max]))
    eval_patients = set(first_evals['enc_id'].unique())

    ## Remove patients that have been evaluated by a physician
    no_action = no_action.difference(eval_patients)
    no_action_patients = no_action

    ## Segment patients into 3 groups: still in ED, admitted to other care_unit, discharged
    first_admits = care_unit_df.groupby('enc_id', as_index=False)['enter_time'].idxmin()
    first_admits = care_unit_df.ix[first_admits]
    first_admits.rename(columns = {'care_unit':'1st_care_unit'}, inplace=True)

    last_admits = care_unit_df.groupby('enc_id', as_index=False)['enter_time'].idxmax()
    last_admits = care_unit_df.ix[last_admits]
    last_admits.rename(columns = {'care_unit':'last_care_unit'}, inplace=True)

    transferred_patients = pd.merge(first_admits[['enc_id', '1st_care_unit']], last_admits[['enc_id', 'last_care_unit']], how='inner')
    transferred_patients = set(transferred_patients.loc[(transferred_patients['1st_care_unit'] == 'HCGH EMERGENCY-ADULTS') & (transferred_patients['last_care_unit'] != 'HCGH EMERGENCY-ADULTS')]['enc_id'].unique())
    transferred_from_ED = no_action_patients.intersection(transferred_patients)
    no_action_patients = no_action_patients.difference(transferred_from_ED)

    discharged_patients = set(cdmt_df.loc[cdmt_df['fid'] == 'discharge']['enc_id'].unique())
    discharged_from_ED = no_action_patients.intersection(discharged_patients)
    metric_26_b = str(len(discharged_from_ED))
    no_action_patients = no_action_patients.difference(discharged_from_ED)
    currently_in_ED = no_action_patients

    #### Compute the metrics for each group ####
    ## Duration alert active while in ED
    min_max_alerts = (merged_df.loc[merged_df['flag'].isin(alert_flags)]
                      .groupby('enc_id', as_index=False)['update_date']
                      .agg({'first_alert':min, 'last_alert':max}))
    min_max_alerts_ED = (merged_df_ED.loc[merged_df_ED['flag'].isin(alert_flags)]
                      .groupby('enc_id', as_index=False)['update_date']
                      .agg({'first_alert':min, 'last_alert_ED':max}))

    all_non_alerts = merged_df[['enc_id', 'update_date', 'flag']] ## Line that needs to be altered for ED
    all_non_alerts = all_non_alerts.loc[all_non_alerts['flag'] < 10]
    first_non_alerts = pd.merge(all_non_alerts, min_max_alerts_ED, on='enc_id', how='left')
    first_non_alerts = first_non_alerts.loc[first_non_alerts['update_date'] > first_non_alerts['last_alert_ED']]
    first_non_alerts = first_non_alerts.groupby('enc_id', as_index=False).min()
    first_non_alerts.drop(['flag', 'first_alert', 'last_alert_ED'], axis=1, inplace=True)
    first_non_alerts.rename(columns={'update_date':'first_non_alert'}, inplace=True)

    ## Compute transfer time and discharge time from ED for each patient
    next_care_unit = care_unit_df.loc[care_unit_df['care_unit'] != 'HCGH EMERGENCY-ADULTS']
    # Keep care_unit for future metrics
    next_care_unit = next_care_unit.groupby('enc_id', as_index = False)[['enter_time', 'care_unit']].agg({'transfer_time': min})
    next_care_unit = next_care_unit['transfer_time'].loc[next_care_unit['enc_id'].isin(no_action)]
    next_care_unit.rename(columns={'enter_time':'transfer_time'},inplace=True)

    discharge_time = cdmt_df.loc[cdmt_df['fid'] == 'discharge']
    discharge_time = discharge_time.groupby('enc_id', as_index = False)['tsp'].agg({'discharge_time':min})
    discharge_time = discharge_time.loc[discharge_time['enc_id'].isin(no_action)]

    duration = pd.merge(min_max_alerts_ED, first_non_alerts, on='enc_id', how='left')
    duration = pd.merge(duration, next_care_unit, on='enc_id', how='left')
    try:
      duration = pd.merge(duration, discharge_time, on='enc_id', how='left')
    except IndexError:
      duration = duration

    ## Only keep patients that have no action taken
    duration = duration.loc[duration['enc_id'].isin(no_action)]
    def get_subgroup(row):
      if row['enc_id'] in discharged_from_ED:
        return 'discharged'
      elif row['enc_id'] in transferred_from_ED:
        return 'transferred'
      elif row['enc_id'] in currently_in_ED:
        return 'current'

    duration['group'] = duration.apply(get_subgroup, axis=1)

    ## Compute alert_end as either when state == 0 or upper bound of that category.
    """
    Helper function for apply over duration df.
    onlyED [boolean]: Change cutoff for patients that are
    """
    def get_alert_end(row):
      ## Change the upper bound for transferred patients based on ED restriction.
      upper_bound = end_tsp
      non_alert = row['first_non_alert']

      if row['group'] == 'transferred':
        if pd.isnull(non_alert):
          return upper_bound
        else:
          return min(non_alert, upper_bound)
      else: ## Select non_alert or upper bound for discharged or current patients
        if pd.isnull(non_alert):
          if row['group'] == 'discharged':
            return row['discharge_time']
          elif row['group'] == 'current':
            return end_tsp
        else:
          return non_alert

    def get_alert_end_ED(row):
      upper_bound = row['transfer_time']
      non_alert = row['first_non_alert_ED']

      if row['group'] == 'transferred':
        if pd.isnull(non_alert):
          return upper_bound
        else:
          return min(non_alert, upper_bound)
      else: ## Select non_alert or upper bound for discharged or current patients
        if pd.isnull(non_alert):
          if row['group'] == 'discharged':
            return row['discharge_time']
          elif row['group'] == 'current':
            return end_tsp
        else:
          return non_alert


    duration['alert_end'] = duration.apply(get_alert_end, axis=1)
    duration['alert_duration'] = (duration['alert_end'] - duration['last_alert_ED']) / pd.to_timedelta('1hour')

    ## Compute same metric but only on ED care units
    all_non_alerts_ED = merged_df_ED[['enc_id', 'update_date', 'flag']] ## Line that needs to be altered for ED
    all_non_alerts_ED = all_non_alerts_ED.loc[all_non_alerts_ED['flag'] < 10]
    first_non_alerts_ED = pd.merge(all_non_alerts_ED, min_max_alerts_ED, on='enc_id', how='left')
    first_non_alerts_ED = first_non_alerts_ED.loc[first_non_alerts_ED['update_date'] > first_non_alerts_ED['last_alert_ED']]
    first_non_alerts_ED = first_non_alerts_ED.groupby('enc_id', as_index=False).min()
    first_non_alerts_ED.drop(['flag', 'first_alert', 'last_alert_ED'], axis=1, inplace=True)
    first_non_alerts_ED.rename(columns={'update_date':'first_non_alert_ED'}, inplace=True)

    duration = pd.merge(duration, first_non_alerts_ED, on='enc_id', how='left')
    duration['alert_end_ED'] = duration.apply(get_alert_end_ED, axis=1)
    duration['alert_duration_ED'] = (duration['alert_end_ED'] - duration['last_alert_ED']) / pd.to_timedelta('1hour')

    ## Remove patients where they haven't had their alerts go unattended for at least 2 hours
    duration = duration.loc[duration['alert_duration_ED'] >= 2]
    no_action = set(duration['enc_id'].unique())
    no_action_patients = no_action
    metric_16 = str(len(no_action))
    transferred_from_ED = no_action_patients.intersection(transferred_from_ED)
    no_action_patients = no_action_patients.difference(transferred_from_ED)
    discharged_from_ED = no_action_patients.intersection(discharged_from_ED)
    no_action_patients = no_action_patients.difference(discharged_from_ED)
    still_in_ED = no_action_patients

    ## Alerts that were TREWS and alerts that were CMS
    ## Number of patients that have TREWS vs CMS alerts
    has_TREWS = (merged_df.loc[merged_df['flag'].isin([11])]
                 .groupby('enc_id', as_index=False)['update_date']
                 .agg({'has_TREWS_alert':min}))
    has_TREWS['has_TREWS_alert'] = 1
    has_CMS = (merged_df.loc[merged_df['flag'].isin([10])]
               .groupby('enc_id', as_index=False)['update_date']
               .agg({'has_CMS_alert':min}))
    has_CMS['has_CMS_alert'] = 1

    no_action_metrics = pd.merge(duration, has_TREWS, on='enc_id', how='left')
    no_action_metrics = pd.merge(no_action_metrics, has_CMS, on='enc_id', how='left')

    ## Alerts with at least 1 page visit
    query = """
                select enc_id, tsp, uid
                from user_interactions
                where action = 'page-get'
                and enc_id in ({0})""".format(', '.join([str(e) for e in valid_enc_ids]))
    page_gets = pd.read_sql(sqlalchemy.text(query), self.connection, columns=['enc_id', 'tsp', 'uid'])
    ## Remove all dev team member interactions
    dev_group = ['AZHAN2', 'KHENRY22', 'NRAWAT1', 'EHOOGES1']
    page_gets = page_gets.loc[~page_gets['uid'].isin(dev_group)]
    all_page_gets = page_gets
    alerted_page_gets = pd.merge(page_gets, no_action_metrics[['enc_id','first_alert']], on='enc_id', how='left')
    alerted_page_gets = alerted_page_gets.loc[alerted_page_gets['first_alert'] <= alerted_page_gets['tsp']]

    try:
      ## Build dictionary of page views by providers for each patient
      page_views = alerted_page_gets['uid'].groupby(alerted_page_gets['enc_id']).value_counts().to_frame('page_views')
      ## Add page views to the full metrics.
      no_action_metrics['page_views'] = no_action_metrics['enc_id'].apply(lambda x, page_views=page_views, alerted_page_gets=alerted_page_gets: page_views.ix[x].to_dict() if x in alerted_page_gets['enc_id'].unique() else False)
    except IndexError:
      ## No patient had a page view of alerted_page_gets is an empty dataframe.
      no_action_metrics['page_views'] = 0


    ## Numpy sum function returns 0 for nan. Since we want integer for counts, use helper func to turn nan to 0.
    def clean_sum(value):
      if pd.isnull(value):
        return 0
      else:
        return int(value)

    ## Check if enc_ids in no_action_metrics had an abx order or given lactate
    no_action_metrics = pd.merge(no_action_metrics, first_alerts[['enc_id', 'min_tsp_cms_antibiotics_order']], on='enc_id', how='left')
    no_action_metrics = pd.merge(no_action_metrics, first_alerts[['enc_id', 'min_tsp_lactate_order']], on='enc_id', how='left')
    no_action_metrics['has_abx_order'] = no_action_metrics['min_tsp_cms_antibiotics_order'].apply(lambda x: False if pd.isnull(x) else True)
    no_action_metrics['has_lactate_order'] = no_action_metrics['min_tsp_lactate_order'].apply(lambda x: False if pd.isnull(x) else True)
    no_action_metrics.drop('min_tsp_cms_antibiotics_order', inplace=True, axis=1)
    no_action_metrics.drop('min_tsp_lactate_order', inplace=True, axis=1)


    ## Leaving comments in for the quantile version. Not enough data to make meaningful quantiles atm.
    ## Subset no_action_metrics for each of the 3 groups
    discharged_metrics_df = no_action_metrics.loc[no_action_metrics['enc_id'].isin(discharged_from_ED)]
    discharged_metrics_results = [str(len(discharged_from_ED)),
                                  '{0:.3f}'.format(discharged_metrics_df['alert_duration'].median()),
                                  '{0:.3f}'.format(discharged_metrics_df['alert_duration_ED'].median()),
                                  #discharged_metrics_df['alert_duration'].quantile([0.25, 0.5, 0.75]),
                                  #discharged_metrics_df['alert_duration_ED'].quantile([0.25, 0.5, 0.75]),
                                  str(clean_sum(discharged_metrics_df['has_TREWS_alert'].sum())),
                                  str(clean_sum(discharged_metrics_df['has_CMS_alert'].sum())),
                                  str(clean_sum(discharged_metrics_df['has_abx_order'].sum())),
                                  str(clean_sum(discharged_metrics_df['has_lactate_order'].sum())),
                                  str(0)]

    transferred_metrics_df = no_action_metrics.loc[no_action_metrics['enc_id'].isin(transferred_from_ED)]
    transferred_metrics_results = [str(len(transferred_from_ED)),
                                   '{0:.3f}'.format(transferred_metrics_df['alert_duration'].median()),
                                   '{0:.3f}'.format(transferred_metrics_df['alert_duration_ED'].median()),
                                   #transferred_metrics_df['alert_duration'].quantile([0.25, 0.5, 0.75]),
                                   #transferred_metrics_df['alert_duration_ED'].quantile([0.25, 0.5, 0.75]),
                                   str(clean_sum(transferred_metrics_df['has_TREWS_alert'].sum())),
                                   str(clean_sum(transferred_metrics_df['has_CMS_alert'].sum())),
                                   str(clean_sum(transferred_metrics_df['has_abx_order'].sum())),
                                   str(clean_sum(transferred_metrics_df['has_lactate_order'].sum())),
                                   str(transferred_metrics_df.loc[transferred_metrics_df['care_unit'].str.contains('.*ICU')]['enc_id'].nunique())]

    currentPatient_metrics_df = no_action_metrics.loc[no_action_metrics['enc_id'].isin(currently_in_ED)]
    currentPatients_results = [str(len(currently_in_ED)),
                               '{0:.3f}'.format(currentPatient_metrics_df['alert_duration'].median()),
                               '{0:.3f}'.format(currentPatient_metrics_df['alert_duration_ED'].median()),
                               #currentPatient_metrics_df['alert_duration'].quantile([0.25, 0.5, 0.75]),
                               #currentPatient_metrics_df['alert_duration_ED'].quantile([0.25, 0.5, 0.75]),
                               str(clean_sum(currentPatient_metrics_df['has_TREWS_alert'].sum())),
                               str(clean_sum(currentPatient_metrics_df['has_CMS_alert'].sum())),
                               str(clean_sum(currentPatient_metrics_df['has_abx_order'].sum())),
                               str(clean_sum(currentPatient_metrics_df['has_lactate_order'].sum())),
                               str(0)]

    no_action_results = pd.DataFrame({'discharged_from_ED': discharged_metrics_results,
                                      'transferred_from_ED': transferred_metrics_results,
                                      'still_in_ED': currentPatients_results},
                                     index=['# of patients','Median alert duration (hrs)',
                                            'Median alert duration in ED (hrs)','# of TREWS alerts',
                                            '# of CMS alerts','# of patients with Abx order',
                                            '# of patients with lactate order','# of patients transferred to ICU'])
    no_action_results = no_action_results.transpose()
    self.no_action_results = no_action_results

    ## Build table to show the page_views
    all_page_views = discharged_metrics_df[['enc_id','page_views']]
    all_page_views['group'] = 'discharged'
    temp_transfer = transferred_metrics_df[['enc_id','page_views']]
    temp_transfer['group'] = 'transferred'
    all_page_views = all_page_views.append(temp_transfer, ignore_index=True)
    temp_current = currentPatient_metrics_df[['enc_id','page_views']]
    temp_current['group'] = 'in_ED'
    all_page_views = all_page_views.append(temp_current, ignore_index=True)
    all_page_views = all_page_views.loc[all_page_views['page_views'] != False]
    all_page_views['page_views'] = all_page_views['page_views'].apply(lambda x: x['page_views']) ## Remove page_views key from print

    self.all_page_views = all_page_views

    states_after_last_alert = merged_df_ED[['enc_id', 'update_date', 'flag']]
    states_after_last_alert = pd.merge(states_after_last_alert, first_alerts[['enc_id','1st_alert_date']], how='inner')

    ## Find all enc_ids that only have state 0s after their last alert (CMS and TREWS)
    last_alerts = merged_df_ED.loc[merged_df_ED['flag'].isin([10,11])]
    last_alerts = last_alerts.groupby('enc_id', as_index=False)['update_date'].agg({'last_alert': max})

    states_after_last_alert = pd.merge(states_after_last_alert, last_alerts[['enc_id', 'last_alert']], how='left')
    states_after_last_alert = states_after_last_alert.loc[states_after_last_alert['update_date'] > states_after_last_alert['last_alert']]
    states_after_last_alert['not_deactivated'] = states_after_last_alert['flag'] != 0
    dropouts = states_after_last_alert.groupby('enc_id', as_index=False)['not_deactivated'].agg(np.sum)
    dropouts = dropouts.loc[dropouts['not_deactivated'] == 0]
    metric_25 = str(dropouts['enc_id'].nunique())

    ## Get all the patients with ED care units
    ed = care_unit_df.loc[care_unit_df['care_unit'] == 'HCGH EMERGENCY-ADULTS']
    tmp_df = ed.groupby('enc_id', as_index=False).agg({'enter_time':min, 'leave_time':max})
    tmp_df['window_end'] = tmp_df['enter_time'] + pd.to_timedelta(3*60, unit='m') ## Used for SIRS criteria (metric 20).
    ed = tmp_df

    ''' Assign each entry of target_df a care unit based on tsp and enc_id. Remove all entries w/o a care_unit.

    Args:
        target_df: dataframe which gains care_unit_information. Must contain column 'tsp'
        care_unit_df: The desired subset of care_unit_df to use for assigning care_units.

    Returns:
        The same target_df but now with added columns of info from care_unit_df.
    '''
    def assign_care_units(target_df, care_unit_df):
      tmp_df = pd.merge(target_df, care_unit_df, how='left', on='enc_id')
      ind1 = tmp_df['tsp'] >= tmp_df['enter_time']
      ind2 = tmp_df['tsp'] < tmp_df['leave_time']
      ind3 = tmp_df['tsp'].isnull()
      tmp_df = tmp_df.loc[((ind1) & (ind2)) | ((ind1) & (ind3)), :]
      return tmp_df

    ## Get the patient treatment_team information from most recent cdmt_df entry.
    tt_df = cdmt_df.loc[cdmt_df['fid'] == 'treatment_team']
    ## Only take entries of cdmt_df while patients are in the ED.
    tt_df = assign_care_units(tt_df, ed)
    tt_indices = tt_df.groupby('enc_id', as_index=False)['tsp'].idxmax()
    tt_df = tt_df.loc[tt_indices]
    tt_df = tt_df.loc[tt_df['enc_id'].isin(no_action)]
    tt_df.drop(['window_end', 'min_enter_time'], axis=1, inplace=True)
    tt_df['value'] = tt_df['value'].apply(json.loads)
    """ Function to be `apply`-ed to each row of tt_df to extract the names of the attending physician for each patient.

    """
    def findProviders(row):
      json_list = row['value']
      providers = []
      for entry in json_list:
        if entry['role'] != 'Attending Provider':
          continue
        if (pd.to_datetime(entry['start']).tz_localize(timezone('utc')) >= row['enter_time']
            and pd.to_datetime(entry['start']).tz_localize(timezone('utc')) <= row['leave_time']):
          if entry['end'] == '' or pd.to_datetime(entry['end']).tz_localize(timezone('utc')) <= row['leave_time']:
            providers.append(entry['name'])
      return str(providers)

    ##tt_df['providers'] = tt_df.apply(findProviders, axis=1)


    all_Providers = {}
    for value in tt_df['value']:
      for entry in value:
        if entry['role'] == 'Attending Provider':
          if entry['name'] in all_Providers:
            all_Providers[entry['name']] += 1
          else:
            all_Providers[entry['name']] = 1


    self.all_Providers = all_Providers

    ## Get all patients that have a completed bundle
    completed_bundles = [21,23,26,28,31, 33, 35, 41, 43, 45, 51, 53, 61, 63, 65]
    metric_18 = str(search_history_flags('completed_bundle', completed_bundles))

    ## Get all patients that have an incomplete bundle
    expired_bundles = [22, 24, 27, 29, 32, 34, 36, 42, 44, 46, 52, 54, 62, 64, 66]
    metric_19 = str(search_history_flags('expired_bundle', expired_bundles))

    ## Number of people who meet SIRS criteria during first 3 hours of ED presentation.

    query = """
                select * from sep2_sirs
                where label_id in
                (select max(label_id) from sep2_sirs)
                and enc_id in ({0})""".format(', '.join([str(e) for e in valid_enc_ids]))

    sep2_sirs_df = pd.read_sql(sqlalchemy.text(query), self.connection)
    sep2_sirs_df['tsp'] = pd.to_datetime(sep2_sirs_df['tsp']).dt.tz_convert(timezone('utc'))

    ed_with_SIRS = assign_care_units(sep2_sirs_df, ed)
    # Cut out entries where tsp of SIRS measurement not within ED admit to end of 3 hr window
    ed_with_SIRS = ed_with_SIRS.loc[(ed_with_SIRS['tsp'] >= ed_with_SIRS['enter_time']) & (ed_with_SIRS['tsp'] < ed_with_SIRS['window_end'])]

    ## Evaluate how many times >= 2 SIRS criteria were met
    try:
      ed_with_SIRS['met_criteria'] = ed_with_SIRS.apply(lambda x: True if x['resp_rate_sirs'] + x['heart_rate_sirs'] + x['wbc_sirs'] + x['temperature_sirs'] >= 2 else False, axis=1)
      ed_met_SIRS = ed_with_SIRS[['enc_id','met_criteria']].groupby('enc_id', as_index=False).aggregate(np.sum)
      metric_20 = ed_met_SIRS.loc[ed_met_SIRS['met_criteria'] > 0]
      metric_20 = str(metric_20['enc_id'].nunique())
    except ValueError:
      metric_20 = str(0)

    ## Need to move to performance metrics in future.
    """
    four_day_abx = first_alerts.loc[first_alerts['enc_id'].isin(no_action)]
    four_day_abx['4_day_abx'] = four_day_abx['max_tsp_cms_antibiotics_order'] - four_day_abx['min_tsp_cms_antibiotics_order']
    four_day_abx['4_day_abx'] = four_day_abx['4_day_abx'] > pd.to_timedelta('4day')

    ## metric_21: Patients with no action but later turned out to be septic. Using 4-day abx as approx of confirm sepsis.
    ## Not sure if need to subset for patients where min_tsp_cms_abx_order also after alert like in metric 10
    #four_day_abx = four_day_abx.loc[four_day_abx['1st_alert_date'] < four_day_abx['min_tsp_cms_antibiotics_order']]
    #metric_21 = str(four_day_abx['4_day_abx'].sum())
    """

    trews_septic_shock_flags = [30,40]
    metric_22 = str(search_history_flags('trews_septic_shock', trews_septic_shock_flags))

    manual_septic_shock_flags = [60]
    metric_23 = str(search_history_flags('manual_septic_shock', manual_septic_shock_flags))

    # read lab evals from criteria events using json object key no_lab
    query = """
              select enc_id,
                     min(update_date::timestamptz) as update_date
              from criteria_events
              where name='trews_subalert'
              and value::json->>'no_lab'='false'
              and enc_id in ({0})
              group by enc_id;""".format(', '.join([str(e) for e in valid_enc_ids]))

    earliest_lab_evals = pd.read_sql(sqlalchemy.text(query), self.connection, columns=['enc_id', 'update_date'])
    earliest_lab_evals.rename(columns={'update_date':'first_lab_eval'}, inplace=True)
    earliest_lab_evals['first_lab_eval'] = pd.to_datetime(earliest_lab_evals['first_lab_eval']).dt.tz_convert(timezone('utc'))
    earliest_lab_eval_idx = earliest_lab_evals.groupby('enc_id', as_index=False)['first_lab_eval'].idxmin()
    earliest_lab_evals = earliest_lab_evals.ix[earliest_lab_eval_idx]

    first_alerts = pd.merge(first_alerts, earliest_lab_evals, how='left', on='enc_id')
    ## Number of patients that had their first TREWS alert before their first lab evaluation.
    metric_24 = str(first_alerts.loc[first_alerts['1st_alert_date'] < first_alerts['first_lab_eval']]['enc_id'].nunique())

    ## Compute metrics regarding discharged patients
    alerted_patients = set(first_alerts['enc_id'].unique())
    all_patients = set(care_unit_df.loc[care_unit_df['care_unit'] == 'HCGH EMERGENCY-ADULTS']['enc_id'].unique())
    transferred_out_ED = alerted_patients.intersection(transferred_patients)
    alerted_patients = alerted_patients.difference(transferred_out_ED)
    discharged_out_ED = alerted_patients.intersection(discharged_patients)
    ## Total num of patients discharged from ED
    metric_26_a = str(len(discharged_out_ED))

    ## Metric_26_c is finding all patients that were readmitted
    query = """
                select enc_id, pat_id
                from pat_enc
                where enc_id in ({0})""".format(', '.join([str(e) for e in valid_enc_ids]))
    pat_enc_df = pd.read_sql(sqlalchemy.text(query), self.connection)
    pat_enc_df = pat_enc_df.loc[pat_enc_df['enc_id'].isin(all_patients)]
    ## Get a set of all pat_ids that have more than one encounter/admission.
    readmits = pat_enc_df.groupby('pat_id', as_index=False).agg('count')
    readmits.rename(columns={'enc_id':'encounters'}, inplace=True)
    readmits = set(readmits.loc[readmits['encounters'] > 1]['pat_id'].unique())

    care_unit_df_30, cdmt_df_30 = self.get_care_unit_remove_homeMed(self.get_cdmt_df(valid_enc_ids), start_tsp - pd.to_timedelta('30day'))
    readmits_within_30d = pat_enc_df.loc[pat_enc_df['pat_id'].isin(readmits)]
    admit_times = (care_unit_df_30.loc[care_unit_df_30['care_unit'] == 'HCGH EMERGENCY-ADULTS']
                                  .loc[care_unit_df_30['enc_id'].isin(set(readmits_within_30d['enc_id'].unique()))]
                                  .groupby('enc_id', as_index=False)['enter_time'].agg('min'))

    try:
      readmits_within_30d = pd.merge(readmits_within_30d, admit_times, on='enc_id', how='left')

      ## Get the two most recent admit enc_ids and admit_times
      grouped = readmits_within_30d.groupby('pat_id').apply(lambda x: x.sort_values(by='enter_time', ascending=False).head(2))
      ## Find whether or not these readmitted patients had an alert while in ED.

      merged_df_30 = merge_with_care_unit(criteria_events_df, care_unit_df_30)
      merged_df_30_ED = merged_df_30.loc[merged_df_30['care_unit'] == 'HCGH EMERGENCY-ADULTS'] ## Check that the name is correct
      merged_df_30_ED.loc[:,'flag'] = merged_df_30_ED['flag'].apply(lambda x: x + 1000 if x < 0 else x) ## Want to see history
      readmitted_had_alert = merged_df_30_ED.loc[merged_df_30_ED['enc_id'].isin(set(grouped.enc_id.unique()))]
      search_history_flags('TREWS_alert', alert_flags, readmitted_had_alert)
      readmitted_had_alert = set(readmitted_had_alert.loc[readmitted_had_alert['TREWS_alert'] == 1]['enc_id'].unique())
      ## Find which of the remaining readmitted patients were discharged from the ED.
      ## Find first and last care units

      if len(readmitted_had_alert) == 0:
        metric_26_c = str(0)
      else:
        admits_30 = care_unit_df_30.loc[care_unit_df_30['enc_id'].isin(readmitted_had_alert)].groupby('enc_id', as_index=False)
        first_admits_30 = admits_30['enter_time'].idxmin()
        first_admits_30 = care_unit_df_30.ix[first_admits_30]
        first_admits_30.rename(columns = {'care_unit':'1st_care_unit'}, inplace=True)
        last_admits_30 = admits_30['enter_time'].idxmax()
        last_admits_30 = care_unit_df_30.ix[last_admits_30]
        last_admits_30.rename(columns = {'care_unit':'last_care_unit'}, inplace=True)
        ## Find and remove all transferred patients from potential list
        transferred_patients_30 = pd.merge(first_admits_30[['enc_id', '1st_care_unit']], last_admits_30[['enc_id', 'last_care_unit']], how='inner')
        transferred_patients_30 = set(transferred_patients_30.loc[(transferred_patients_30['1st_care_unit'] == 'HCGH EMERGENCY-ADULTS') & (transferred_patients_30['last_care_unit'] != 'HCGH EMERGENCY-ADULTS')]['enc_id'].unique())
        discharged_patients_30 = readmitted_had_alert.difference(transferred_patients_30)
        ## Remove all groups that do not have an enc_id in discharged_patients_30
        discharged_patients_30 = set(grouped.loc[grouped['enc_id'].isin(discharged_patients_30)]['pat_id'].unique())

        grouped = grouped.ix[discharged_patients_30]
        ## Remove all pat_ids that had their 2 most recent encounters more than 30 days ago.
        results = (grouped.groupby('pat_id')['enter_time'].max() - grouped.groupby('pat_id')['enter_time'].min()) / pd.to_timedelta('1day')
        results = results.loc[results <= 30]
        metric_26_c = str(results.index.nunique())

    except:
      metric_26_c = str(0)

    ## Missing metric_13: repeat lactate
    allMetrics = [metric_1, metric_2, metric_7, metric_8, metric_9, metric_10, metric_11, metric_12, metric_14, metric_15, metric_16, metric_25, metric_17, metric_18, metric_19, metric_20, metric_22, metric_23, metric_24, metric_26_a, metric_26_b, metric_26_c]
    desc1 = 'Total ED patients'
    desc2 = '# ED patients with TREWS alert'
    #desc3 = 'Number of people with code sepsis'
    #desc4 = 'Number of TREWS alerts that had code sepsis'
    #desc5 = 'Hours from code sepsis until TREWS'
    #desc6 = 'Number of code sepsis patients without a TREWS alert'
    desc7 = '# times manual override used'
    desc8 = '# manual overrides that later had an alert'
    desc9 = 'Median hours from manual override to alert'
    desc10 = '# alerts before antibiotics'
    desc11 = '# alerts before blood culture'
    desc12 = '# alerts before lactate'
    ## Still need to implement repeat lactate
    #desc13 = '# alerts before repeat lactate'
    desc14 = '# alerts that have no infection entered'
    desc15 = '# alerts that are put on sepsis pathway'
    desc16 = '# alerts that have no action taken'
    desc25 = '# alerts w/ no action, then deactivated'
    #desc16_a = '# alerts with no action for < 1hr'
    #desc16_b = '# alerts with no action for >= 1hr'
    #desc16_c = '# alerts with no action for >= 2hrs'
    desc17 = 'min, median, max hours from alert to evaluation'
    desc18 = '# alerts with complete bundle'
    desc19 = '# alerts with expired bundle'
    desc20 = '# ED patients with SIRS within first 3 hours'
    #desc21 = '# alerts with no action but had 4-day abx ordered'
    desc22 = '# alerts for septic shock'
    desc23 = '# manual overrides for septic shock'
    desc24 = '# alerts before first lab evaluations'
    desc26a = '# alerted patients discharged from the ED'
    desc26b = '# alerted patients w/ no action then discharged from ED'
    desc26c = '# re-admits w/ alert and was discharged from ED within last 30 days'
    ## Missing metric_13: repeat lactate
    allDesc = [desc1, desc2, desc7, desc8, desc9, desc10, desc11, desc12, desc14, desc15, desc16, desc25, desc17, desc18, desc19, desc20, desc22, desc23, desc24, desc26a, desc26b, desc26c]
    self.metrics_DF = pd.DataFrame({'Metrics': allDesc, 'Values': allMetrics})

  def to_html(self):
    pd.set_option('display.max_colwidth', 75)
    txt = '<h3>This section of the report metrics for patients who were in the ED between {s} and {e}</h3>'.format(s=self.report_start, e=self.report_end)
    ## Exit if there were no alerts given during the timeframe (metric_2 == 0).
    if self.no_alerts:
      txt += "No alerts were given for TREWS during this time period."
      return txt

    txt += self.metrics_DF.to_html()
    txt += '<h3>This table breaks down the patients that were alerted but had no action (metric 10)</h3>' + self.no_action_results.to_html()
    txt += '<h3>This table reports the page views for each patient in the three no action categories.</h3>' + self.all_page_views.to_html()

    txt += '<h3>The following is a list of the (name: # patients) of all {0} attending providers of the no action patients </h3>'.format(str(len(self.all_Providers)))
    txt += str(self.all_Providers)
    return txt

class alert_performance_metrics(metric):

  def __init__(self,connection, first_time_str, last_time_str):

    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'Alert Performance Stats'

    self.sepsis_performance_window = 24*7#long_window ## 24*7 hours window
    self.connection = connection
    # self.now = pd.to_datetime('now').tz_localize(timezone('utc'))
    try:
        self.now = pd.to_datetime(self.last_time_str).tz_localize(timezone('utc'), ambiguous='infer')
    except:
        self.now = pd.to_datetime(self.last_time_str).tz_localize(timezone('utc'))

    try:
        self.start_time = pd.to_datetime(self.first_time_str).tz_localize(timezone('utc'), ambiguous='infer')
    except:
        self.start_time = pd.to_datetime(self.first_time_str).tz_localize(timezone('utc'))

    self.window = (pd.to_datetime(last_time_str) - pd.to_datetime(first_time_str)) / pd.to_timedelta("1 hour")

  def get_enc_ids(self, discharge_time):
    query = """
        with bedded as (
        select distinct BP.enc_id
        from get_latest_enc_ids('HCGH') BP
        ),
        discharged as (
            select distinct enc_id from cdm_t
            where fid='discharge'
            and tsp > '{0}'
            and value::json ->> 'disposition' like '%HCGH%'
        ),
        candidate_encids as (
            select enc_id
            from discharged
            union
            select enc_id
            from bedded
        ),
        excluded_encids as (
            select distinct EXC.enc_id
            from ( select * from cdm_t where enc_id in (select distinct enc_id from candidate_encids) ) EXC
            inner join cdm_s on cdm_s.enc_id = EXC.enc_id and cdm_s.fid = 'age'
            inner join cdm_t on cdm_t.enc_id = EXC.enc_id and cdm_t.fid = 'care_unit'
            group by EXC.enc_id
            having count(*) filter (where cdm_s.value::numeric < 18) > 0
            or count(*) filter(where cdm_t.value in ('HCGH LABOR & DELIVERY', 'HCGH EMERGENCY-PEDS', 'HCGH 2C NICU', 'HCGH 1CX PEDIATRICS', 'HCGH 2N MCU')) > 0
            or count(*) filter(where cdm_t.value not like '%HCGH%') > 0
            union
            select distinct enc_id
            from cdm_s
            where fid = 'hospital'
            and enc_id in (select distinct enc_id from candidate_encids)
            and value <> 'HCGH'
        )
        select distinct enc_id
        from candidate_encids
        where enc_id not in
        (
            select enc_id from excluded_encids
        );
        """.format(str(discharge_time))

    encids_df = pd.read_sql(sqlalchemy.text(query), self.connection)
    return encids_df['enc_id'].as_matrix().astype(int)

  def build_care_unit_df(self, valid_enc_ids):
    query = """
            with first_inpatient_cdmt as (
                select enc_id, min(tsp::timestamptz) as tsp
                from cdm_t
                where enc_id in ({0})
                and not (fid ~ 'order' and value::text ~ 'Outpatient')
                group by enc_id
            ),
            care_unit_info as (
                select enc_id, tsp, fid, value
                from cdm_t
                where enc_id in ({0})
                and fid in ('care_unit', 'discharge')
            )
            select R.enc_id, R.tsp as enter_time,
                    (case when R.unit='Discharge' then R.tsp else coalesce(R.next_tsp, date_trunc('second', now())) end) as leave_time,
                    R.unit as care_unit
            from (
                select R.enc_id, R.tsp, R.unit,
                        lead(R.tsp, 1) over (PARTITION BY R.enc_id ORDER BY R.tsp,
                                            (case when R.unit = 'Arrival' then 0 when R.unit = 'Discharge' then 2 else 1 end)
                        ) as next_tsp,
                        lead(R.unit,1) OVER (PARTITION BY R.enc_id ORDER BY R.tsp,
                            (case when R.unit = 'Arrival' then 0 when R.unit = 'Discharge' then 2 else 1 end)
                        ) as next_unit,
                        first_value(R.unit) over (PARTITION by R.enc_id order by R.tsp,
                            (case when R.unit = 'Arrival' then 0 when R.unit = 'Discharge' then 2 else 1 end)
                        ) as first_unit
                from (
                    select R.enc_id, R.tsp, R.unit
                    from (
                        select enc_id, tsp, value as unit
                        from care_unit_info
                        where fid='care_unit'
                        union all
                        select enc_id, tsp, 'Discharge' as unit
                        from care_unit_info
                        where fid='discharge'
                        union all
                        select enc_id, tsp, 'Arrival' as unit
                        from first_inpatient_cdmt
                    ) R
                ) R
                order by R.enc_id, R.tsp,
                        (case when R.unit = 'Arrival' then 0 when R.unit = 'Discharge' then 2 else 1 end)
            ) R
            where R.unit <> 'Discharge'
            and R.tsp <> coalesce(R.next_tsp, date_trunc('second', now()))
            """.format(','.join([str(i) for i in valid_enc_ids]))
    care_unit_df = pd.read_sql(sqlalchemy.text(query), self.connection,
                          columns=['enc_id', 'enter_time', 'leave_time', 'care_unit'])
    care_unit_df['enter_time'] = pd.to_datetime(care_unit_df['enter_time']).dt.tz_convert(timezone('utc'))
    care_unit_df['leave_time'] = pd.to_datetime(care_unit_df['leave_time']).dt.tz_convert(timezone('utc'))
    care_unit_df['enc_id'] = care_unit_df['enc_id'].astype(int)
    care_unit_df.loc[care_unit_df['care_unit']=='Arrival', 'care_unit'] = 'HCGH EMERGENCY-ADULTS'

    return care_unit_df

  def get_data(self, start_time):

    ## get trews_model_id
    model_id_query = "select value from trews_parameters where name='trews_jit_model_id';"
    model_id_df = pd.read_sql(sqlalchemy.text(model_id_query), self.connection, columns=['value'])
    model_id = model_id_df['value'].as_matrix().astype(int)[0]

    ## get active enc_ids
    valid_enc_ids = self.get_enc_ids(start_time)

    ## build care_unit df
    care_unit_df = self.build_care_unit_df(valid_enc_ids)

    ## criteria_events table
    # have to apply time filter later because we want to know if they are already alerted
    query = """
            select distinct on (enc_id, event_id)
                    enc_id,
                    event_id,
                    update_date as tsp,
                   (case when flag::numeric>=0 then flag::numeric else flag::numeric+1000 end) as state
            from criteria_events
            where enc_id in ({0})
            order by enc_id, event_id;
          """.format(','.join([str(i) for i in valid_enc_ids]))
    criteria_events_df = pd.read_sql(sqlalchemy.text(query), self.connection,
                                     columns=['enc_id', 'event_id', 'tsp', 'state'])
    try:
      criteria_events_df['tsp'] = pd.to_datetime(criteria_events_df['tsp']).dt.tz_convert(timezone('utc'))
    except:
      criteria_events_df['tsp'] = pd.to_datetime(criteria_events_df['tsp']).dt.tz_localize(timezone('utc'))
    criteria_events_df['enc_id'] = criteria_events_df['enc_id'].astype(int)
    criteria_events_df.sort_values(by=['enc_id', 'tsp'])

    ## trews_jit_score table
    query = """
            select enc_id, (orgdf_details::json->>'pred_time')::timestamptz as pred_tsp,
                    creatinine_orgdf,
                    bilirubin_orgdf,
                    platelets_orgdf,
                    gcs_orgdf,
                    inr_orgdf,
                    hypotension_orgdf,
                    sbpm_hypotension,
                    map_hypotension,
                    delta_hypotension,
                    vasopressors_orgdf,
                    lactate_orgdf,
                    vent_orgdf,
                    (case when orgdf_details::json->>'alert'='True' then 1 else 0 end) as alert,
                    orgdf_details::json->>'model' as model
                from trews_jit_score
                where model_id={0}
                and enc_id in ({1})
                and tsp >= '{2}'
                order by enc_id, pred_tsp;
            """.format(str(model_id), ','.join([str(i) for i in valid_enc_ids]), str(start_time))
    trews_jit_df = pd.read_sql(sqlalchemy.text(query), self.connection,
                                     columns=['enc_id',
                                              'pred_',
                                              'creatinine_orgdf',
                                              'bilirubin_orgdf',
                                              'platelets_orgdf',
                                              'gcs_orgdf',
                                              'inr_orgdf',
                                              'hypotension_orgdf',
                                              'sbpm_hypotension',
                                              'map_hypotension',
                                              'delta_hypotension',
                                              'vasopressors_orgdf',
                                              'lactate_orgdf',
                                              'vent_orgdf',
                                              'alert',
                                              'model'])
    trews_jit_df.rename(columns={'pred_tsp':'tsp'}, inplace=True)
    try:
      trews_jit_df['tsp'] = pd.to_datetime(trews_jit_df['tsp']).dt.tz_convert(timezone('utc'))
    except:
      trews_jit_df['tsp'] = pd.to_datetime(trews_jit_df['tsp']).dt.tz_localize(timezone('utc'))
    trews_jit_df['enc_id'] = trews_jit_df['enc_id'].astype(int)
    trews_jit_df.sort_values(by=['enc_id', 'tsp'])

    ## sepsis labels
    query = """
            select enc_id, min(tsp) as event_tsp
            from cdm_labels
            where label_id = (select max(label_id) from label_version)
            and enc_id in ({0})
            and tsp >= '{1}'::timestamptz
            group by enc_id
            order by enc_id;
            """.format(','.join([str(i) for i in valid_enc_ids]), str(start_time))
    sepsis_labels_df = pd.read_sql(sqlalchemy.text(query), self.connection,
                                     columns=['enc_id', 'event_tsp'])
    sepsis_labels_df.rename(columns={'pred_tsp':'tsp'}, inplace=True)
    try:
      sepsis_labels_df['event_tsp'] = pd.to_datetime(sepsis_labels_df['event_tsp']).dt.tz_convert(timezone('utc'))
    except:
      sepsis_labels_df['event_tsp'] = pd.to_datetime(sepsis_labels_df['event_tsp']).dt.tz_localize(timezone('utc'))

    return valid_enc_ids, care_unit_df, criteria_events_df, trews_jit_df, sepsis_labels_df

  def calc(self):

      ## this window also covers the alert cnt window
      valid_enc_ids, care_unit_df, criteria_events_df, trews_jit_df, sepsis_labels_df = \
                  self.get_data(self.now - pd.to_timedelta(self.sepsis_performance_window, unit='h'))

      # # save results for now:
      # np.save("data/valid_enc_ids.npy", self.raw_data['valid_enc_ids'])
      # self.raw_data['care_unit_df'].to_csv('data/care_unit_df.csv', index=False)
      # self.raw_data['criteria_events_df'].to_csv('data/criteria_events_df.csv', index=False)
      # self.raw_data['trews_jit_df'].to_csv('data/trews_jit_df.csv', index=False)
      # self.raw_data['sepsis_labels_df'].to_csv('data/sepsis_labels_df.csv', index=False)

      criteria_events_df.sort_values(by=['enc_id', 'tsp'], inplace=True)

      alerted = criteria_events_df['state'] >= 11
      criteria_events_df.loc[alerted, 'alert'] = 1.0
      criteria_events_df.loc[~alerted, 'alert'] = 0.0

      manual_override = criteria_events_df['state'] >= 50
      criteria_events_df.loc[manual_override, 'manual_override'] = 1.0
      criteria_events_df.loc[~manual_override, 'manual_override'] = 0.0

      criteria_events_df.loc[0, 'manual_override'] = 1.0

      # if new alert is due to manual override, only count them if there is also an alert within 2 hours.
      alert_due_to_override = (criteria_events_df['alert']==1)&\
                              (criteria_events_df['manual_override']==1.0)
      has_override = criteria_events_df.loc[alert_due_to_override, ]
      if has_override.shape[0] > 0:
          criteria_events_df.loc[alert_due_to_override, 'alert'] = 0

      has_alert = trews_jit_df.loc[trews_jit_df['alert']==1,
                                   ['enc_id', 'tsp']].rename(columns={'tsp':'alert_tsp'})
      if has_override.shape[0] > 0 and has_alert.shape[0] > 0:
          tmp_df = pd.merge(has_override[['enc_id', 'tsp']],
                            has_alert[['enc_id', 'alert_tsp']], how='inner', on='enc_id')
          if tmp_df.shape[0] > 0:
              tmp_df = tmp_df.loc[tmp_df['alert_tsp'] <= tmp_df['tsp'] + pd.to_timedelta('1 hour')]
              if tmp_df.shape[0] > 0:
                  tmp_df['valid_manual_override'] = 0
                  idx = tmp_df.groupby('enc_id')['tsp'].idxmin()
                  tmp_df.loc[idx, 'valid_manual_override'] = 1

                  criteria_events_df = pd.merge(criteria_events_df,
                                                tmp_df.loc[idx,['enc_id', 'tsp', 'valid_manual_override']],
                                                how='left', on=['enc_id', 'tsp'])
                  criteria_events_df.loc[criteria_events_df['valid_manual_override']==1, 'alert'] = 1

      ## new_alert
      criteria_events_df['alert_change'] = criteria_events_df.groupby('enc_id')['alert'].diff()
      criteria_events_df['new_alert'] = 0
      # new_alert happens at the very first row for each enc_id if alerted or when there is a change from 0 to 1.
      criteria_events_df.loc[(criteria_events_df['alert_change']==1.0)|
                             ((criteria_events_df['alert_change'].isnull())&
                              (criteria_events_df['alert']==1.0)), 'new_alert'] = 1

      ## ************************ TPR/PPV stats

      # merge sepsis labels with care_unit
      sepsis_labels_df = pd.merge(sepsis_labels_df, care_unit_df,
                                 how='left', on='enc_id')
      sepsis_labels_df = sepsis_labels_df.loc[(sepsis_labels_df['event_tsp']>=sepsis_labels_df['enter_time'])&
                                              (sepsis_labels_df['event_tsp']<sepsis_labels_df['leave_time']),
                                             ['enc_id', 'care_unit', 'event_tsp']]

      alerted_df = criteria_events_df.loc[criteria_events_df['new_alert']==1, :]
      idx = alerted_df.groupby('enc_id')['new_alert'].idxmin()
      first_alert_df = alerted_df.loc[idx, ['enc_id', 'tsp']].rename(columns={'tsp':'alert_tsp'})
      first_alert_df = pd.merge(first_alert_df, care_unit_df, how='left', on='enc_id')
      first_alert_df = first_alert_df.loc[(first_alert_df['alert_tsp']>=first_alert_df['enter_time'])&
                                          (first_alert_df['alert_tsp']<first_alert_df['leave_time']),
                                         ['enc_id', 'alert_tsp', 'care_unit']].rename(columns={'care_unit':'alert_unit'})

      join_df = pd.merge(first_alert_df, sepsis_labels_df, how='outer', on='enc_id')

      has_alert = ~join_df['alert_tsp'].isnull()
      has_event = ~join_df['event_tsp'].isnull()
      alert_before_event = join_df['alert_tsp'] < join_df['event_tsp'] + pd.to_timedelta('3 hour')

      join_df.loc[has_alert & has_event & alert_before_event, 'TP'] = True
      join_df.loc[has_alert & has_event & (~alert_before_event), 'late_TP'] = True
      join_df.loc[has_alert & (~has_event), 'FP'] = True
      join_df.loc[(~has_alert) & has_event, 'FN'] = True

      join_df.loc[(join_df['TP']==True)&
                  (join_df['care_unit']==join_df['alert_unit']), 'detected_in_the_same_unit'] = True
      join_df.loc[(join_df['TP']==True)&
                  (join_df['care_unit']!=join_df['alert_unit']), 'detected_in_previous_units'] = True

      perf_cnt_df = join_df.loc[has_event, ].groupby('care_unit')['enc_id'].nunique()\
                    .reset_index(level=0).rename(columns={'enc_id':'# sepsis'})

      perf_cnt_df = pd.merge(perf_cnt_df,
                             join_df.loc[join_df['detected_in_the_same_unit']==True].\
                                      groupby('care_unit')['enc_id'].nunique().\
                                      reset_index(level=0).rename(columns={'enc_id':'# TPs (alerted in this unit)'}),
                            how='outer', on='care_unit')

      perf_cnt_df = pd.merge(perf_cnt_df,
                             join_df.loc[join_df['detected_in_previous_units']==True].\
                                      groupby('care_unit')['enc_id'].nunique().\
                                      reset_index(level=0).rename(columns={'enc_id':'# TPs (alerted in othre units)'}),
                            how='outer', on='care_unit')

      perf_cnt_df = pd.merge(perf_cnt_df,
                             join_df.loc[(join_df['FP']==True)|
                                         (join_df['late_TP']==True)].groupby('alert_unit')['enc_id'].nunique().\
                                      reset_index(level=0).rename(columns={'enc_id':'# FPs',
                                                                           'alert_unit':'care_unit'}),
                            how='outer', on='care_unit')

      for col in list(perf_cnt_df.columns[1:]):
          perf_cnt_df[col].fillna(0, inplace=True)
          perf_cnt_df[col] = perf_cnt_df[col].astype(int)

      self.perf_cnt_df = perf_cnt_df.sort_values(by='care_unit')

      perf_metrics = pd.DataFrame()
      perf_metrics.loc[0, '# sepsis'] = str(join_df.loc[has_event, 'enc_id'].nunique())
      perf_metrics.loc[0, '# alerts'] = str(join_df.loc[has_alert, 'enc_id'].nunique())
      perf_metrics.loc[0, 'TPR'] = '%.3f' %((join_df.loc[join_df['TP']==True, 'enc_id'].nunique()) / \
                                           float(join_df.loc[has_event, 'enc_id'].nunique()))
      perf_metrics.loc[0, 'PPV'] = '%.3f' %((join_df.loc[join_df['TP']==True, 'enc_id'].nunique()) / \
                                           float(join_df.loc[has_alert, 'enc_id'].nunique()))
      self.perf_metrics = perf_metrics

      detailed_perf_metrics = pd.DataFrame()
      detailed_perf_metrics.loc[0, '# Late TPs'] = '%d' %((join_df.loc[join_df['late_TP']==True, 'enc_id'].nunique()))
      detailed_perf_metrics.loc[0, 'missed cases'] = ', '.join([str(x) for x in
                                                      join_df.loc[(~join_df['event_tsp'].isnull())&
                                                                  (join_df['TP'].isnull())&
                                                                  (join_df['late_TP'].isnull()),
                                                                   'enc_id'].unique()])
      self.detailed_perf_metrics = detailed_perf_metrics

      ###*********************** Alert counts
      # get additonal tsps to see if they have active alerts in other units
      sub_care_unit = care_unit_df.loc[care_unit_df['enc_id'].isin(criteria_events_df['enc_id'])]
      enter_times = sub_care_unit[['enc_id', 'enter_time']].rename(columns={'enter_time':'tsp'})
      enter_times['tsp'] = enter_times['tsp'] + pd.to_timedelta('1 minute')
      leave_times = sub_care_unit[['enc_id', 'leave_time']].rename(columns={'leave_time':'tsp'})
      leave_times['tsp'] = leave_times['tsp'] - pd.to_timedelta('1 minute')

      df = pd.concat([enter_times, leave_times], axis=0, ignore_index=True)

      alert_df = pd.concat([criteria_events_df, df]).sort_values(by=['enc_id', 'tsp'])
      alert_df['alert'] = alert_df.groupby('enc_id')['alert'].ffill()
      alert_df.loc[alert_df['alert'].isnull(), 'alert'] = 0

      alert_df.loc[alert_df['new_alert'].isnull(), 'new_alert'] = 0

      # merge with care_unit
      alert_df_tsp_filter = (alert_df['tsp'] <= self.now) & (alert_df['tsp'] > self.start_time)
      care_unit_tsp_filter = ~((care_unit_df['enter_time'] > self.now) | \
                              (care_unit_df['leave_time'] < self.start_time))
      care_unit_df = care_unit_df.loc[care_unit_tsp_filter]
      alert_df = pd.merge(alert_df.loc[alert_df_tsp_filter],
                          care_unit_df,
                          how='left', on='enc_id')
      alert_df = alert_df.loc[(alert_df['tsp']>=alert_df['enter_time'])&
                              (alert_df['tsp']<alert_df['leave_time'])]

      cnt_df = care_unit_df.groupby('care_unit')['enc_id'].nunique().\
                  reset_index(level=0).rename(columns={'enc_id':'total # enc_ids'})

      # alerts present
      cnt_df = pd.merge(cnt_df,
                        alert_df.loc[alert_df['alert']==1].groupby('care_unit')['enc_id'].nunique().\
                                reset_index(level=0).rename(columns={'enc_id':'# alerts present'}),
                        how='left', on='care_unit')

      # alerts fired
      cnt_df = pd.merge(cnt_df,
                        alert_df.loc[alert_df['new_alert']==1].groupby('care_unit')['enc_id'].nunique().\
                                reset_index(level=0).rename(columns={'enc_id':'# alerts fired'}),
                        how='left', on='care_unit')


      for col in list(cnt_df.columns[1:]):
          cnt_df[col].fillna(0, inplace=True)
          cnt_df[col] = cnt_df[col].astype(int)

      self.cnt_df = cnt_df.sort_values(by='care_unit')

      self.total_cnts = pd.DataFrame()
      self.total_cnts.loc[0, 'total # enc_ids'] = '%d' %(care_unit_df['enc_id'].nunique())
      self.total_cnts.loc[0, '# enc_ids with alert present'] = '%d' %(alert_df.loc[alert_df['alert']==1, 'enc_id'].nunique())
      self.total_cnts.loc[0, '# enc_ids with alert fired'] = '%d' %(alert_df.loc[alert_df['new_alert']==1, 'enc_id'].nunique())

      ## prepare df to write into DB
      cnt_df['end_tsp'] = str(self.now)
      # create table if not exists
      query = """
              CREATE TABLE IF NOT EXISTS alert_stats (
                end_tsp           timestamptz,
                care_unit         text,
                total_enc_ids     int,
                alerts_present    int,
                alerts_fired      int
              );
              """
      self.connection.execute(sqlalchemy.text(query))
      cnt_df.rename(columns={'total # enc_ids':'total_enc_ids',
                             '# alerts present':'alerts_present',
                             '# alerts fired':'alerts_fired'}, inplace=True)
      cnt_df.to_sql('alert_stats', self.connection, index=False, if_exists='append')

      ## get history of alerts
      query = """
              select end_tsp, care_unit, total_enc_ids, alerts_present, alerts_fired
              from alert_stats
              where end_tsp >= '{0}'
              order by end_tsp desc, care_unit
              """.format(str(self.now - pd.to_timedelta(6*self.window, unit='h')))
      cnt_df_hist = pd.read_sql(sqlalchemy.text(query), self.connection,
                                columns=['end_tsp', 'care_unit', 'total_enc_ids', 'alerts_present', 'alerts_fired'])
      cnt_df_hist['end_tsp'] = pd.to_datetime(cnt_df_hist['end_tsp']).dt.tz_convert(timezone('utc'))

      cnt_df_hist['txt'] = cnt_df_hist.apply(lambda x: "%3d| %2d| %2d" %(x['total_enc_ids'], x['alerts_present'], x['alerts_fired']), axis=1)

      unq_units = cnt_df_hist['care_unit'].unique()
      for u0, unit in enumerate(unq_units):
          if u0 == 0:
              hist_df = cnt_df_hist.loc[cnt_df_hist['care_unit']==unit, ['end_tsp', 'txt']].rename(columns={'txt':unit})
          else:
              hist_df = pd.merge(hist_df,
                                 cnt_df_hist.loc[cnt_df_hist['care_unit']==unit, ['end_tsp', 'txt']].rename(columns={'txt':unit}),
                                 how='outer', on='end_tsp')
      for u0, unit in enumerate(unq_units):
          hist_df.loc[hist_df[unit].isnull(), unit] = "%3d| %2d| %2d" %(0,0,0)

      self.hist_df = hist_df.sort_values(by='end_tsp', ascending=False).reset_index(drop=True).loc[:12:2,:]
      self.hist_tsp_range = {'min':self.hist_df['end_tsp'].min(), 'max':self.hist_df['end_tsp'].max()}
      self.hist_df.rename(columns={'end_tsp':'End Time'}, inplace=True)

  def to_html(self):

      txt = "<h3>Total Number of Alerts</h3>" + self.total_cnts.to_html(index=False)
      txt += "<h3>Number of Alerts by Care Unit</h3>" + self.cnt_df.to_html(index=False)
      txt += "<h5># alerts fired = # enc_ids on whom the alert went from Off to On in this period."
      txt += "<br/># alerts On = # enc_ids whose alert was On at some time during this period but may have been fired before.</h5>"
      txt += "<h3>Performance Measures Over a 7-Day Period</h3>" + self.perf_metrics.to_html(index=False)
      txt += "<h3>Performance Measures Over a 7-Day Period by Care Unit</h3>" + self.perf_cnt_df.to_html(index=False)
      txt += "<h5>More Details:</h5>" + self.detailed_perf_metrics.to_html(index=False)
      txt += "<h3>History of alert statistics in each unit from %s to %s.</h3>" %(str(self.hist_tsp_range['min']), str(self.hist_tsp_range['max']))
      txt += "<h5># enc_ids| # alerts present| # alerts fired </h5> " +  self.hist_df.to_html(index=False)

      return txt


class suspicion_of_infection_modified(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'Suspicion Of Infection Entered'

  def calc(self):

    sus_mod_q = """
              with soi_table as (
                  select enc_id, tsp, event,
                        ((event::json->>'override_value')::json)->0->>'text' as txt,
                        event::json->>'uid' as jhed_id
                  from criteria_log
                  where event::json->>'name'='suspicion_of_infection'
                  and tsp::timestamptz between '{0}'::timestamptz and '{1}'::timestamptz
              ),
              treatment_team_join as (
                select S.enc_id, S.tsp as soi_tsp, S.jhed_id, S.txt,
                        T.team as team, T.tsp,
                        rank() over (partition by T.enc_id, S.tsp order by T.tsp desc) as row
                from soi_table S
                left join
                (
                    select enc_id, tsp, json_array_elements(value::json) as team
                    from cdm_t where fid='treatment_team'
                    and enc_id in (select distinct enc_id from soi_table)
                ) T
                on T.enc_id=S.enc_id
                and T.tsp::timestamptz <= S.tsp::timestamptz
              )
              select enc_id,
                      date_trunc('second', soi_tsp) as tsp, jhed_id,
                      team::json->>'name' as name,
                      team::json->>'role' as role_in_treatment_team,
                      txt as soi_text
              from treatment_team_join
              where row=1
              and
              (
                  (
                    team::json->>'id'= jhed_id
                    and char_length((team::json->>'end')::text)=0
                  )
                  OR
                  team is null
              )
              order by soi_tsp desc;""".format(self.first_time_str, self.last_time_str)

    # def remove_mess(str_in):
    #   if isinstance(str_in,str):
    #     return str_in[11:-3]
    #   else:
    #     return ''

    res_df = pd.read_sql(sqlalchemy.text(sus_mod_q), self.connection)
    # res_df['overide_value'] = res_df['overide_value'].apply(remove_mess)
    # res_df.drop(['pat_id'], 1, inplace=True)
    self.data = res_df

  def to_html(self):
    return self.data.to_html()


class user_engagement(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'User Engagement'

  def calc(self):
    '''
    1. enter infection
    2. enter no infection
    3. enter other infection
    4. enter fluids inappropriate
    5. enter abx inappropriate
    6. skip to sepsis bundle
    7. skip to shock bundle
    8. enter uncertain/keep monitoring
    '''
    user_engag_q = """
    with a1 as (
      select p.pat_id, l.tsp, l.event,
             l.event#>>'{{name}}' as name,
             (case when l.event#>>'{{event_type}}' = 'override'
                    and l.event#>>'{{name}}' = 'suspicion_of_infection'
                    and l.event#>>'{{override_value, 0, text}}' in
                            ('Unknown Source',
                             'Endocarditis',
                             'Meningitis',
                             'Bacteremia',
                             'Cellulitis',
                             'UTI',
                             'Pneumonia',
                             'Multiple Sources of Infection'
                             )
                then 'enter infection'
                when l.event#>>'{{event_type}}' = 'override'
                    and l.event#>>'{{name}}' = 'suspicion_of_infection'
                    and l.event#>>'{{override_value, 0, other}}' = 'true'
                then 'enter other infection'
                when l.event#>>'{{event_type}}' = 'override'
                    and l.event#>>'{{name}}' = 'suspicion_of_infection'
                    and l.event#>>'{{override_value, 0, text}}' = 'No Infection'
                then 'enter no infection'
                when l.event#>>'{{event_type}}' = 'override'
                    and l.event#>>'{{name}}' = 'crystalloid_fluid_order'
                    and l.event#>>'{{override_value, 0, text}}' ~ '^Clinically Inappropriate'
                    and l.event#>>'{{clear}}' = 'false'
                then 'enter fluids inappropriate'
                when l.event#>>'{{event_type}}' = 'override'
                    and l.event#>>'{{name}}' = 'antibiotics_order'
                    and l.event#>>'{{override_value, 0, text}}' ~ '^Clinically Inappropriate'
                    and l.event#>>'{{clear}}' = 'false'
                then 'enter abx inappropriate'
                when l.event#>>'{{event_type}}' = 'override'
                    and l.event#>>'{{name}}' = 'ui_severe_sepsis'
                    and l.event#>>'{{clear}}' = 'false'
                then 'skip to sepsis bundle'
                when l.event#>>'{{event_type}}' = 'override'
                    and l.event#>>'{{name}}' = 'ui_septic_shock'
                    and l.event#>>'{{clear}}' = 'false'
                then 'skip to shock bundle'
                when l.event#>>'{{event_type}}' = 'override'
                    and l.event#>>'{{name}}' = 'ui_deactivate'
                    and l.event#>>'{{clear}}' = 'false'
                then 'enter uncertain/keep monitoring'
                else l.event#>>'{{event_type}}' end) as type,
             l.event#>>'{{uid}}' as doc_id
      from criteria_log l
      inner join pat_enc p on l.enc_id = p.enc_id
    )
    select doc_id, type, count(pat_id) as num_pats
    from a1
    where doc_id is not null and
      tsp between \'{}\'::timestamptz and \'{}\'::timestamptz
    group by doc_id, type
    order by doc_id, type, num_pats
    """.format(self.first_time_str, self.last_time_str)

    res_df = pd.read_sql(sqlalchemy.text(user_engag_q), self.connection)
    self.data = res_df

  def to_html(self):
    return self.data.to_html()


class unique_usrs(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'Unique Users'

  def calc(self):
    # ===========================
    # Get timeseries metrics data from database
    # ===========================
    user_analytics_query = sqlalchemy.text(
      """select count(distinct uid), max(tsp) as time
         from user_interactions
         where tsp between \'{}\'::timestamptz and \'{}\'::timestamptz""".
          format(self.first_time_str, self.last_time_str))

    all_usr_dat = pd.read_sql(user_analytics_query, self.connection)
    self.data=all_usr_dat

  def to_cwm(self):
    data_dict = {
      'MetricName': 'num_unique_active_users',
      'Timestamp': self.data['time'].iloc[0],
      'Value': int(self.data['count'].iloc[0]),
      'Unit': 'Count',
    }
    return data_dict


class pats_seen_by_docs(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'Usage Statistics'

  def calc(self):
    num_pats_seen = """
                    with page_gets as (
                        select P.enc_id, R.action, R.tsp, R.uid, R.loc, U.role, U.name
                        from
                        (
                            select enc_id, csn, action, tsp, uid, loc
                            from user_interactions
                            where tsp::timestamptz between '{0}'::timestamptz and '{1}'::timestamptz
                            and action='page-get'
                        ) R
                        inner join
                        pat_enc P
                        on P.visit_id::text=R.csn::text
                        inner join
                        user_role U
                        on R.uid=U.id
                    ),
                    team_members_page_get as (

                      select U.enc_id as enc_id, U.uid, U.loc, U.tsp,
                              T.team,
                              U.name, U.role,
                              rank() over (partition by U.enc_id, U.tsp order by T.tsp desc) as row
                      from
                      page_gets U
                      left join
                      (
                          select enc_id, tsp, json_array_elements(value::json) as team
                          from cdm_t
                          where fid='treatment_team'
                          and enc_id in (select distinct enc_id from page_gets)
                      ) T
                      on U.enc_id=T.enc_id
                      and T.tsp::timestamptz <= U.tsp::timestamptz
                    ),
                    useful_table as (
                        select uid, name, tsp, role, loc,
                                enc_id,
                                team::json->>'role' as treatment_team_role
                        from team_members_page_get
                        where row=1
                        and
                        (
                          (team is null)
                          or
                          (
                            team::json->>'id'= uid
                            and char_length((team::json->>'end')::text)=0
                          )
                        )
                    )
                    select uid, max(name) as name,
                          max(loc) as hospital,
                          max(role) as role,
                          count (distinct enc_id) as num_pats_seen,
                          count(distinct enc_id) filter (where treatment_team_role is not null) num_pats_on_treatment_team,
                          count(distinct enc_id) filter (where treatment_team_role is null) num_pats_not_on_treatment_team
                    from useful_table
                    group by uid;""".format(self.first_time_str, self.last_time_str)

    def loc_to_english(str_in):
      loc_dict = {
        '1101': 'JHH',
        '1102': 'BMC',
        '1103': 'HCGH',
        '1104': 'Sibley',
        '1105': 'Suburban',
        '1107': 'KKI'
      }

      return loc_dict.get(str_in[0:4],'unknown(loc={})'.format(str_in))

    res_df = pd.read_sql(num_pats_seen, self.connection)
    res_df['hospital'] = res_df['hospital'].apply(loc_to_english)
    self.data = res_df

  def to_html(self):
    return self.data.to_html()


class get_sepsis_state_stats(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'Sepsis / Bundle Overview'

  def calc(self):
    state_group_list = [
      {'metric_name': 'pats_with_measurements', 'state_expr': 'between %s and %s' % (-50, 50), 'test_out': 10, 'var': 'total',
       'english': 'total (with measurements)'},
      {'metric_name': 'pats_with_severe_sepsis', 'state_expr': 'between %s and %s' % (20, 40), 'test_out': 10, 'var': 'sev',
       'english': 'had severe sepsis in this interval'},
      {'metric_name': 'pats_with_septic_shock', 'state_expr': 'between %s and %s' % (30, 40), 'test_out': 5, 'var': 'sho',
       'english': 'had septic shock in this interval'},
      {'metric_name': 'pats_with_sev_sep_no_sus', 'state_expr': 'in (%s)' % ', '.join([10, 12]), 'test_out': 5, 'var': 'sev_nosus',
       'english': 'had severe sepsis without sus in this interval'},
      {'metric_name': 'pats_with_sev_3hr_miss', 'state_expr': 'in (%s)' % ', '.join([32, 22]), 'test_out': 5, 'var': 'sev_3_m',
       'english': 'where the severe sepsis 3 hour bundle was missed'},
      {'metric_name': 'pats_with_sev_6hr_miss', 'state_expr': 'in (%s)' % ', '.join([34, 24]), 'test_out': 5, 'var': 'sev_6_m',
       'english': 'where the severe sepsis 6 hour bundle was missed'},
      {'metric_name': 'pats_with_sev_3hr_met', 'state_expr': 'in (%s)' % ', '.join([31, 21]), 'test_out': 5, 'var': 'sev_3_h',
       'english': 'where the severe sepsis 3 hour bundle was met'},
      {'metric_name': 'pats_with_sev_6hr_met', 'state_expr': 'in (%s)' % ', '.join([33, 23]), 'test_out': 5, 'var': 'sev_6_h',
       'english': 'where the severe sepsis 6 hour bundle was met'},
      {'metric_name': 'pats_with_sho_6hr_miss', 'state_expr': 'in (%s)' % ', '.join([36]), 'test_out': 5, 'var': 'sho_3_h',
       'english': 'where the septic shock 6 hour bundle was missed'},
      {'metric_name': 'pats_with_sho_6hr_met', 'state_expr': 'in (%s)' % ', '.join([35]), 'test_out': 5, 'var': 'sho_6_h',
       'english': 'where the septic shock 6 hour bundle was met'},
    ]

    source_tbl = """lambda_hist_pat_state_{now}""".format(now=datetime.utcnow().strftime("%Y%m%d%H%M%S"))

    get_hist_states = """
      create TEMPORARY table {tmp_tbl} as
      with all_pats_in_window as (
        select distinct enc_id
        from cdm_t
        where tsp between '{start}'::timestamptz and '{stop}'::timestamptz
      ),
      pat_state_updates as (
        select enc_id
          case when last(flag) >= 0 then last(flag) else last(flag) + 1000 END as last_pat_state,
          last(update_date) as tsp
        from criteria_events
        where flag != -1
        group by enc_id, event_id
        order by tsp
      )
      select p.pat_id, coalesce(psu.last_pat_state, 0) as pat_state
      from all_pats_in_window ap
      left join pat_state_updates psu on ap.enc_id = psu.enc_id
      left join pat_enc p on ap.enc_id = p.enc_Id
    """.format(tmp_tbl=source_tbl, start=self.first_time_str, stop=self.last_time_str)
    self.connection.execute(sqlalchemy.text(get_hist_states))

    state_group_out = []
    for state_group in state_group_list:
      query = sqlalchemy.text(
        """select count(distinct pat_id) as {name}
           from {table_name}
           where pat_state {state_expr};""".
          format(
          name=state_group['var'],
          state_expr=state_group['state_expr'],
          table_name=source_tbl,
        )
      )
      out_df = pd.read_sql(query, self.connection)
      state_group['results'] = out_df[state_group['var']].iloc[0]
      state_group_out.append(state_group)

    self.data=state_group_out

  def to_cwm(self):
    out = list()

    for state_group in self.data:
      out.append({
        'MetricName': state_group['metric_name'],
        'Timestamp': dt.utcnow(),
        'Value': int(state_group['results']),
        'Unit': 'Count',
      })
    return out

  def to_html(self):
    html = '<p>'
    for state_group in self.data:
      html += "{num} patients {english}<br>".format(num=state_group['results'], english=state_group['english'])
    html += '</p>'
    return html


class notification_stats(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'Notification Statistics'

  def calc(self):
    notification_q = """
    with flat_notifications as (
      select
        p.pat_id,
        to_timestamp(cast(n.message#>>'{{timestamp}}' as numeric)) as tsp,
        cast(n.message#>>'{{read}}' as boolean) as read,
        cast(n.message#>>'{{alert_code}}' as integer) alert_code
      from notifications n
      inner join pat_enc p on n.enc_id = p.enc_id
    ),
    num_notes_at_once as (
      select pat_id, tsp, count(distinct(alert_code)) as number_of_unread_notifications
      from
      flat_notifications
      where not read and tsp BETWEEN '{start}'::timestamptz and '{end}'::timestamptz
      group by pat_id, tsp
    ),
    max_notes_at_once as (
      select pat_id, max(number_of_unread_notifications) as max_unread_notifications
      from num_notes_at_once
      group by pat_id
    )
    select
      max_unread_notifications,
      count(distinct(pat_id)) as number_of_pats
    from max_notes_at_once
    group by max_unread_notifications
    order by max_unread_notifications;
    """.format(start=self.first_time_str, end=self.last_time_str)
    self.data = pd.read_sql(sqlalchemy.text(notification_q), self.connection)

  def to_html(self):
    return self.data.to_html()


class pats_with_threshold_crossings(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'TREWS JIT Threshold Crossings'

  def calc(self):
    # ===========================
    # Get timeseries metrics data from database
    # ===========================
    trews_thresh = sqlalchemy.text(
      """with trews_thresh as (
            select value from trews_parameters where name = 'trews_jit_threshold'
          ),
          trews_detections as (
            select enc_id, tsp, score, score > trews_thresh.value as crossing
            from trews_jit_score, trews_thresh
            where model_id in ( select value from trews_parameters where name = 'trews_jit_model_id' )
          ),
          pat_crossed_thresh as (
            select pe.pat_id, max(crossing::int) as crossed_threshold
            from trews_detections td
            inner join pat_enc pe on pe.enc_id = td.enc_id
            where tsp between \'{}\'::timestamptz and \'{}\'::timestamptz
            group by pat_id
          )
          select count(distinct pat_id) as num_pats
          from pat_crossed_thresh
          where crossed_threshold = 1""".
          format(self.first_time_str, self.last_time_str))

    trews_thresh_df = pd.read_sql(trews_thresh, self.connection)
    self.data=trews_thresh_df

  def to_html(self):
    html = "{num} patients had threshold crossings<br>".format(num=self.data['num_pats'].iloc[0])
    return html

  def to_cwm(self):
    data_dict = {
      'MetricName': 'num_pats_with_threshold_crossings',
      'Timestamp': dt.utcnow(),
      'Value': int(self.data['num_pats'].iloc[0]),
      'Unit': 'Count',
    }

    return data_dict


##################################
# TREWS and CMS Alert monitors
#

class alert_stats_totals(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'TREWS and CMS Alert Statistics for Recent Encounters'

  def calc(self):
    sql = ''''''

    res_df = pd.read_sql(sqlalchemy.text(sql), self.connection)
    self.data = res_df

  def to_html(self):
    return self.data.to_html()


class alert_stats_by_unit(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'TREWS and CMS Alert Statistics For Recent Encounters By Unit'

  def calc(self):
    sql = \
    '''
    select * from get_alert_stats_by_unit('%(start)s'::timestamptz, '%(end)s'::timestamptz)
    ''' % { 'start': self.first_time_str, 'end': self.last_time_str }

    res_df = pd.read_sql(sqlalchemy.text(sql), self.connection)
    self.data = res_df

  def to_html(self):
    return self.data.to_html()

  def to_cwm(self):
    res = []
    for i, row in self.data.iterrows():
      for label in ['trews_no_cms', 'cms_no_trews', 'trews_and_cms', 'any_trews', 'any_cms']:
        data_dict = {
          'MetricName': 'alert_count_' + label + '_' + row['care_unit'].replace(' ', '_'),
          'Timestamp': self.last_time_str,
          'Value': int(row[label]),
          'Unit': 'Count',
        }
        res.append(data_dict)
    return res

class alert_count_8hr(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'TREWS and CMS Alert Count over a 8hr Period'

  def calc(self):
    sql = ''''''
    res_df = pd.read_sql(sqlalchemy.text(sql), self.connection)
    self.data = res_df

  def to_html(self):
    return self.data.to_html()

  def to_cwm(self):
    res = []
    for i, row in self.data.iterrows():
      if row['name'].startswith('#'):
        label = 'trews' if 'TREWS' in row['name'] else 'cms'
        data_dict = {
          'MetricName': 'alert_count_' + label + '_8hr',
          'Timestamp': self.last_time_str,
          'Value': int(row['occurrences']),
          'Unit': 'Count',
        }
        res.append(data_dict)
    return res

class alert_evaluation_stats(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'TREWS and CMS Alert Evaluation over a 48hr Period'

  def calc(self):
    sql = ''''''
    res_df = pd.read_sql(sqlalchemy.text(sql), self.connection)
    self.data = res_df

  def to_html(self):
    return self.data.to_html()

class weekly_report(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'weekly-report'

  def calc(self):
    sql = \
    '''
    select * from get_weekly_individual_metrics();
    select * from update_individual_metrics_for_new_discharges();
    select * from indv_alert_metrics where sid = (select max(sid) from indv_alert_metrics);
    '''
    res_df = pd.read_sql(sqlalchemy.text(sql), self.connection)
    self.data = res_df

#---------------------------------
## Metric Factory
#---------------------------------

class metric_factory(object):
  def __init__(self,connection, first_time_str, last_time_str, metric_init_list):
    self.name = 'abstract metric'
    self.connection = connection
    self.first_time_str = first_time_str
    self.last_time_str = last_time_str
    self.metric_list = []

    for metric_init in metric_init_list:
      self.metric_list.append(metric_init(self.connection, self.first_time_str, self.last_time_str))

  def calc_all_metrics(self):
    for metric in self.metric_list:
      metric.calc()

  def build_report_body(self):
    html_body = ''
    for metric in self.metric_list:
      html_body += '<h1>{name}</h1><p>{out}</p>'.format(name=metric.name, out=metric.to_html())
    return html_body

  def get_cwm_output(self):
    cwm_list = []

    for metric in self.metric_list:
      out = metric.to_cwm()
      if isinstance(out,list):
        cwm_list += out
      elif isinstance(out,dict):
        cwm_list.append(out)
      else:
        raise(Warning("Unclear how to intepret output of {} to_cwm method".format(metric.name)))

    return cwm_list

  def get_output(self):
    output = []
    for metric in self.metric_list:
      out = (metric.name, metric.data)
      output.append(out)
    return output
