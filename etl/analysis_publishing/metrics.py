from datetime import datetime
import pandas as pd
import sqlalchemy
from datetime import datetime as dt
import numpy as np
from pytz import timezone
from collections import OrderedDict

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

class alert_performance_metrics(metric):

  def __init__(self,connection, first_time_str, last_time_str):

    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'alert_performance_stats'


    self.sepsis_performance_window = 24*7#long_window ## 24*7 hours window
    self.connection = connection
    # self.now = pd.to_datetime('now').tz_localize(timezone('utc'))
    self.now = pd.to_datetime(self.last_time_str).tz_localize(timezone('utc'))

    self.window = (pd.to_datetime(last_time_str) - pd.to_datetime(first_time_str)) / pd.to_timedelta("1 hour")

  def get_enc_ids(self, discharge_time):

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
                and value::json ->> 'disposition' like '%HCGH%'
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

  def get_care_unit(self, cdmt_df):

      care_unit_df = cdmt_df.loc[cdmt_df['fid']=='care_unit', ['enc_id', 'tsp', 'value']].copy()
      care_unit_df = care_unit_df.sort_values(by=['enc_id', 'tsp'])
      care_unit_df.rename(columns={'tsp':'enter_time', 'value':'care_unit'}, inplace=True)
      care_unit_df['leave_time'] = care_unit_df.groupby('enc_id')['enter_time'].shift(-1)

      # fill in the leave time on the last unit
      last_unit_tsp = care_unit_df.groupby('enc_id').agg({'enter_time':'max'})
      last_unit_tsp.reset_index(level=0, inplace=True)
      last_unit_tsp.rename(columns={'enter_time':'last_unit_tsp'}, inplace=True)
      discharge_tsp = cdmt_df.loc[cdmt_df['fid']=='discharge', ['enc_id', 'tsp']].copy()
      df = pd.merge(last_unit_tsp, discharge_tsp, on='enc_id', how='inner')

      # final step
      if df.shape[0] > 0:
          care_unit_df = pd.merge(care_unit_df, df, how='outer', on='enc_id')
          care_unit_df.loc[care_unit_df['last_unit_tsp']==care_unit_df['enter_time'], 'leave_time'] = \
                                          care_unit_df.loc[care_unit_df['last_unit_tsp']==care_unit_df['enter_time'], 'tsp']
          care_unit_df.drop(['tsp', 'last_unit_tsp'], axis=1, inplace=True)

      care_unit_df = care_unit_df.loc[care_unit_df['care_unit']!='Discharge']

      return care_unit_df

  def get_cdmt_df(self, valid_enc_ids):

        ### read cdm_t to get min/max_tsp and build care_unit_df
        query = """select enc_id, tsp, fid, value from cdm_t where enc_id in ({0})
                        order by enc_id, tsp""".format(', '.join([str(e) for e in valid_enc_ids]))
        cdmt_df = pd.read_sql(sqlalchemy.text(query), self.connection, columns=['enc_id', 'tsp', 'fid', 'value'])
        cdmt_df['tsp'] = pd.to_datetime(cdmt_df['tsp']).dt.tz_convert(timezone('utc'))

        return cdmt_df

  def calc(self):
    ## this is the time we started running trews -- don't go before this date.
    start_tsp = pd.to_datetime('2017-10-19 16:00:00+00:00').tz_localize(timezone('utc'))

    ## get trews_model_id
    model_id_query = "select value from trews_parameters where name='trews_jit_model_id';"
    model_id_df = pd.read_sql(sqlalchemy.text(model_id_query), self.connection, columns=['value'])
    model_id = model_id_df['value'].as_matrix().astype(int)[0]

    #### get_valid_enc_ids
    valid_enc_ids = self.get_enc_ids(start_tsp)

    # read trews_jit_alerts
    query = """
            select enc_id, tsp, orgdf_details::json ->> 'alert' as jit_alert
            from trews_jit_score
            where model_id={0}
            and enc_id in ({1})""".format(str(model_id), ', '.join([str(e) for e in valid_enc_ids]))
    trews_jit_df = pd.read_sql(sqlalchemy.text(query), self.connection, columns=['enc_id', 'tsp', 'jit_alert'])
    trews_jit_df['tsp'] = pd.to_datetime(trews_jit_df['tsp']).dt.tz_convert(timezone('utc'))
    trews_jit_df['jit_alert'] = trews_jit_df['jit_alert'].map({'True':1, 'False':0}).astype(float)

    # to build care_unit_df
    cdmt_df = self.get_cdmt_df(valid_enc_ids)

    ## compute care_unit_df
    care_unit_df = self.get_care_unit(cdmt_df)

    ## read cms, trews_sub_alerts from criteria_events
    query = """
            with sub_criteria_events as (
                select enc_id, event_id, flag, update_date,
                       min(measurement_time) filter (where name = 'trews_subalert' and is_met) as trews_subalert_onset,
                       count(*) filter (where name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and is_met) as sirs,
                       count(*) filter (where name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and is_met) as orgdf,
                       (array_agg(measurement_time order by measurement_time)  filter (where name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and is_met ) )[2]   as sirs_onset,
                       min(measurement_time) filter (where name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp','respiratory_failure','creatinine','bilirubin','platelet','inr','lactate') and is_met ) as organ_onset
                from criteria_events
                where enc_id in ({0})
                group by enc_id, event_id, flag, update_date
            )
            select enc_id, event_id, update_date as tsp, flag, min(trews_subalert_onset) as trews_subalert,
                min(greatest(sirs_onset, organ_onset)) filter (where sirs > 1 and orgdf > 0) as cms_onset
            from sub_criteria_events
            group by enc_id, event_id, flag, update_date
            order by enc_id, update_date, cms_onset""".format(', '.join([str(e) for e in valid_enc_ids]))
    union_df = pd.read_sql(sqlalchemy.text(query), self.connection,
                      columns=['enc_id', 'event_id', 'tsp', 'flag', 'trews_subalert', 'cms_onset'])
    union_df['tsp'] = pd.to_datetime(union_df['tsp']).dt.tz_convert(timezone('utc'))
    union_df['flag'] = union_df['flag'].astype(int)
    union_df['union_alert'] = 0
    union_df.loc[(~union_df['cms_onset'].isnull())|(~union_df['trews_subalert'].isnull())|(union_df['flag']>=10), 'union_alert'] = 1

    ## reading trews_subalerts again here because I want to get the deltas (note that no conditioning on is_met=true)
    query = """select enc_id, update_date as tsp, is_met as criteria_jit
                from criteria_events
                where name like 'trews_subalert'
                and enc_id in ({0});""".format(', '.join([str(e) for e in valid_enc_ids]))
    trews_subalert_df = pd.read_sql(sqlalchemy.text(query), self.connection, columns=['enc_id', 'tsp', 'criteria_jit'])
    trews_subalert_df['tsp'] = pd.to_datetime(trews_subalert_df['tsp']).dt.tz_convert(timezone('utc'))
    trews_subalert_df['criteria_jit'] = trews_subalert_df['criteria_jit'].astype(float)


    ####### merge all alert types and interpolate every 15 minutes
    alerts_df = cdmt_df[['enc_id', 'tsp']].copy()
    alerts_df = pd.merge(alerts_df, trews_jit_df, how='outer', on=['enc_id', 'tsp'])
    alerts_df = pd.merge(alerts_df, trews_subalert_df, how='outer', on=['enc_id', 'tsp'])
    alerts_df = pd.merge(alerts_df, union_df[['enc_id', 'tsp', 'union_alert']], how='outer', on=['enc_id', 'tsp'])
    alerts_df.drop_duplicates(subset=['enc_id', 'tsp'], inplace=True)
    alerts_df = alerts_df.sort_values(by=['enc_id', 'tsp'])

    ## fill in the first point for each enc_id to 0 if null
    first_tsp_index = alerts_df.groupby('enc_id', as_index=False)['tsp'].idxmin()
    columns = ['jit_alert', 'criteria_jit', 'union_alert']
    alerts_df.loc[first_tsp_index, columns] = alerts_df.loc[first_tsp_index, columns].fillna(0)
    alerts_df[columns] = alerts_df[columns].ffill()
    alerts_df.reset_index(drop=True, inplace=True)

    def interpolate_score_df(main_df, stepsize='5min'):

        dict_of_dfs = {k: v for k, v in main_df.groupby('enc_id')}
        enc_ids = dict_of_dfs.keys()
        grid_df1 = {}
        for e0, enc_id in enumerate(enc_ids):
            tmp_df = dict_of_dfs[enc_id].loc[dict_of_dfs[enc_id]['tsp']>=start_tsp,:].copy()
            if tmp_df.shape[0] == 0:
                continue
            df = tmp_df.set_index('tsp').resample(stepsize, closed='right').ffill()

            df['jit_delta'] = df['jit_alert'].diff()
            df['num_jit_delta'] = np.sum((~df['jit_delta'].isnull())*(df['jit_delta']!=0))
            df['crit_jit_delta'] = df['criteria_jit'].diff()
            df['num_crit_jit_delta'] = np.sum((~df['crit_jit_delta'].isnull())*(df['crit_jit_delta']!=0))
            df['union_alert_delta'] = df['union_alert'].diff()
            df['num_union_alert_delta'] = np.sum((~df['union_alert_delta'].isnull())*(df['union_alert_delta']!=0))

            df['jit_sim'] = np.sum(df['jit_alert']*df['criteria_jit'])/(np.sum(df['jit_alert']) + 1e-10)

            #### filtering everything by start_tsp

            grid_df1.update({enc_id:df.reset_index(level=0)})

        return pd.concat([grid_df1[x] for x in grid_df1.keys()])

    alerts_df = interpolate_score_df(alerts_df)

    def merge_with_care_unit(main_df, care_unit_df=care_unit_df):

        tmp_df = pd.merge(main_df, care_unit_df, how='left', on='enc_id')
        ind1 = tmp_df['tsp']>tmp_df['enter_time']
        ind2 = tmp_df['tsp']<tmp_df['leave_time']
        ind3 = tmp_df['leave_time'].isnull()
        tmp_df = tmp_df.loc[((ind1)&(ind2))|((ind1)&(ind3)), :]

        return tmp_df

    def get_alert_counts(main_df, alert_type, window=6):
        init_time = (self.now - pd.to_timedelta(window, unit='h'))#.tz_localize(timezone('utc'))

        tmp_df = merge_with_care_unit(main_df.loc[(main_df['tsp']>=init_time)&(main_df[alert_type]==1)])
        num_unq_enc_ids = len(tmp_df['enc_id'].unique())

        df = tmp_df.groupby('care_unit')['enc_id'].nunique()
        return num_unq_enc_ids, df

    ############### Alert counts

    total_cnts_dict = OrderedDict()

    ## 1) total number of patients by care_unit in the window
    init_time = (self.now - pd.to_timedelta(self.window, unit='h'))#.tz_localize(timezone('utc'))
    ind1 = (care_unit_df['leave_time']<init_time)&(~care_unit_df['leave_time'].isnull())
    df = care_unit_df.loc[(~ind1), :]
    aggregate_df = df.groupby('care_unit')['enc_id'].nunique().reset_index(level=0)
    aggregate_df.rename(columns={'enc_id':'total # of enc_ids'}, inplace=True)

    ## 2) total number of patients with alerts on
    total_cnts_dict['# encids with alert ON'], cnts_by_unit_df = get_alert_counts(alerts_df, 'union_alert', window=self.window)
    aggregate_df = pd.merge(aggregate_df, cnts_by_unit_df.reset_index(level=0), how='left', on='care_unit')
    aggregate_df['enc_id'] = aggregate_df['enc_id'].fillna(0).astype(int)
    aggregate_df.rename(columns={'enc_id':'# encids with alert ON'}, inplace=True)

    ## 3) total number of patients with trews alerts on
    total_cnts_dict['# encids with TREWS alert ON'], cnts_by_unit_df = get_alert_counts(alerts_df, 'jit_alert', window=self.window)
    aggregate_df = pd.merge(aggregate_df, cnts_by_unit_df.reset_index(level=0), how='left', on='care_unit')
    aggregate_df['enc_id'] = aggregate_df['enc_id'].fillna(0).astype(int)
    aggregate_df.rename(columns={'enc_id':'# encids with TREWS alert ON'}, inplace=True)


    enc_ids_with_trews_alert = alerts_df.loc[alerts_df['jit_alert']==1, 'enc_id'].unique()
    enc_ids_with_any_alert = alerts_df.loc[alerts_df['union_alert']==1, 'enc_id'].unique()
    enc_ids_with_only_cms = np.setdiff1d(enc_ids_with_any_alert, enc_ids_with_trews_alert)

    ## 4) total number of patients with alerts on  only due to CMS
    total_cnts_dict['# encids with only CMS alert ON'], cnts_by_unit_df = \
                        get_alert_counts(alerts_df.loc[alerts_df['enc_id'].isin(enc_ids_with_only_cms)],
                                                                'union_alert', window=self.window)
    aggregate_df = pd.merge(aggregate_df, cnts_by_unit_df.reset_index(level=0), how='left', on='care_unit')
    aggregate_df['enc_id'] = aggregate_df['enc_id'].fillna(0).astype(int)
    aggregate_df.rename(columns={'enc_id':'# encids with only CMS alert ON'}, inplace=True)

    ## 4) total number of patients with alert trigger
    total_cnts_dict['# alerts fired'], cnts_by_unit_df = get_alert_counts(alerts_df,
                'union_alert_delta', window=self.window)
    aggregate_df = pd.merge(aggregate_df, cnts_by_unit_df.reset_index(level=0), how='left', on='care_unit')
    aggregate_df['enc_id'] = aggregate_df['enc_id'].fillna(0).astype(int)
    aggregate_df.rename(columns={'enc_id':'# alerts fired'}, inplace=True)


    ##### output
    self.cnts_by_unit = aggregate_df
    self.total_counts = pd.DataFrame.from_dict(total_cnts_dict, orient='index')
    self.total_counts.rename(columns={0:'count'}, inplace=True)

    ################ Performance (TPR, PPV) based on cases with sepsis starting at start_tsp
    performance_df = aggregate_df['care_unit'].copy()

    ## read sepsis_labels
    query = """
        select distinct enc_id, tsp as sepsis_onset
        from cdm_labels
        where label_id =  (select max(label_id) from label_version)
        and enc_id in ({0})""".format(', '.join([str(e) for e in valid_enc_ids]))
    sepsis_df = pd.read_sql(sqlalchemy.text(query), self.connection, columns=['enc_id', 'tsepsis_onset'])
    sepsis_df['sepsis_onset'] = pd.to_datetime(sepsis_df['sepsis_onset']).dt.tz_convert(timezone('utc'))
    sepsis_df = sepsis_df.loc[sepsis_df['sepsis_onset']>=start_tsp, :]

    ## get first sepsis onset
    sepsis_df = sepsis_df[['enc_id', 'sepsis_onset']].copy()
    sepsis_df = sepsis_df.sort_values(by=['enc_id', 'sepsis_onset'])
    idx = sepsis_df.groupby('enc_id')['sepsis_onset'].idxmin()
    first_sepsis = sepsis_df.loc[idx, ['enc_id', 'sepsis_onset']]

    ## cut alerts made before and up to one hour after the first sepsis onset
    myalert_df = merge_with_care_unit(alerts_df)
    myalert_df = pd.merge(myalert_df, first_sepsis, how='left', on='enc_id')
    myalert_df['sep'] = False
    myalert_df.loc[~myalert_df['sepsis_onset'].isnull(), 'sep'] = True
    myalert_df.loc[myalert_df['sepsis_onset'].isnull(), 'sepsis_onset'] = myalert_df.loc[myalert_df['sepsis_onset'].isnull(),
                                                                        'tsp']
    myalert_df = myalert_df.loc[myalert_df['tsp']<=myalert_df['sepsis_onset']+pd.to_timedelta('1 hour')]

    def get_alert_performance(main_df, sep_df=first_sepsis):

        TP_df = main_df.loc[(main_df['union_alert']>0)&(main_df['sep']), :]
        FP_df = main_df.loc[(main_df['union_alert']>0)&(~main_df['sep']), :]

        TPs, FPs = TP_df['enc_id'].unique(), FP_df['enc_id'].unique()
        nTPs, nFPs = len(TPs), len(FPs)
        not_alerted = np.setdiff1d(main_df['enc_id'].unique(), np.union1d(TPs, FPs))
        nTNs = len(np.setdiff1d(not_alerted, sep_df['enc_id'].unique()))
        nFNs = len(np.intersect1d(not_alerted, sep_df['enc_id'].unique()))
        num_alerts = nTPs + nFPs

        cnts = OrderedDict([('# sepsis', '%d' %(len(sep_df['enc_id'].unique()))),
                            ('# alerted','%d' %int(num_alerts)),
                            ('TPR', '%.3f' %(float(nTPs)/(nTPs+nFNs))),
                            ('FPR', '%.3f' %(float(nFPs)/(nFPs+nTNs))),
                            ('PPV', '%0.3f' %(float(nTPs)/(nTPs+nFPs)))])

        cnt_df = TP_df.groupby('care_unit')['enc_id'].nunique().reset_index(level=0).copy()
        cnt_df['enc_id'] = cnt_df['enc_id'].astype(int)
        cnt_df.rename(columns={'enc_id':'# TPs'}, inplace=True)
        cnt_df = pd.merge(cnt_df, FP_df.groupby('care_unit')['enc_id'].nunique().reset_index(level=0),
                         how='outer', on='care_unit')
        cnt_df['enc_id'] = cnt_df['enc_id'].fillna(0).astype(int)
        cnt_df.rename(columns={'enc_id':'# FPs'}, inplace=True)

        tmp_df = sep_df.copy()
        tmp_df.rename(columns={'sepsis_onset':'tsp'}, inplace=True)
        tmp_df = merge_with_care_unit(tmp_df)
        cnt_df = pd.merge(cnt_df, tmp_df.groupby('care_unit')['enc_id'].nunique().reset_index(level=0),
                                     how='outer', on='care_unit')
        cnt_df['enc_id'] = cnt_df['enc_id'].fillna(0).astype(int)
        cnt_df.rename(columns={'enc_id':'# sepsis cases'}, inplace=True)

        ## FPs only due to CMS
        onlyCMS_FP_df = main_df.loc[(main_df['enc_id'].isin(enc_ids_with_only_cms))&
                                    (main_df['union_alert']>0)&
                                    (~main_df['sep']), :]
        cnt_df = pd.merge(cnt_df, onlyCMS_FP_df.groupby('care_unit')['enc_id'].nunique().reset_index(level=0).copy(),
                        how='outer', on='care_unit')
        cnt_df['enc_id'] = cnt_df['enc_id'].fillna(0).astype(int)
        cnt_df.rename(columns={'enc_id':'# CMS only FPs'}, inplace=True)

        ## TPs only due to CMS
        onlyCMS_TP_df = main_df.loc[(main_df['enc_id'].isin(enc_ids_with_only_cms))&
                                    (main_df['union_alert']>0)&
                                    (main_df['sep']), :]
        cnt_df = pd.merge(cnt_df, onlyCMS_TP_df.groupby('care_unit')['enc_id'].nunique().reset_index(level=0).copy(),
                        how='outer', on='care_unit')
        cnt_df['enc_id'] = cnt_df['enc_id'].fillna(0).astype(int)
        cnt_df.rename(columns={'enc_id':'# CMS only TPs'}, inplace=True)

        return cnts, cnt_df.fillna(0)

    # people with first sepsis onset within past (window) hours
    init_time = (self.now - pd.to_timedelta(self.sepsis_performance_window, unit='h'))#.tz_localize(timezone('utc'))
    cnts, performance_cnt_df = get_alert_performance(myalert_df.loc[myalert_df['tsp']>init_time],
                                                  sep_df=first_sepsis.loc[first_sepsis['sepsis_onset']>init_time])

    self.performance_metrics = pd.DataFrame.from_dict(cnts, orient='index').rename(columns={0:''}) # output
    self.performance_cnt_df = performance_cnt_df

  def to_html(self):

      txt = "<h3>Total Number of Alerts</h3>" + self.total_counts.to_html()
      txt += "<h5># alerts fired = # enc_ids on whom the alert went from Off to On in this period."
      txt += "<br/># alerts On = # enc_ids whose alert was On at some time during this period but may have been fired before.</h5>"
      txt += "<h3>Number of Alerts by Care Unit</h3>" + self.cnts_by_unit.to_html()
      txt += "<h3>Performance Measures Over a 7-Day Period" + self.performance_metrics.to_html()
      txt += "<h3>Performance Measures Over a 7-Day Period by Care Unit" + self.performance_cnt_df.to_html()

      return txt



class suspicion_of_infection_modified(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'Suspicion Of Infection Entered'

  def calc(self):
    sus_mod_q = """
    with a1 as (
      select p.pat_id, l.tsp, l.event,
             l.event#>>'{{name}}' as name,
             l.event#>>'{{event_type}}' as type,
             l.event#>>'{{override_value}}' as overide_value
      from criteria_log l
      inner join pat_enc p on l.enc_id = p.enc_id
    )
    select pat_id, tsp, event#>>'{{uid}}' as doc_id, overide_value
    from a1
    where name = 'suspicion_of_infection' and
          type = 'override' AND
          tsp between \'{}\'::timestamptz and \'{}\'::timestamptz
    """.format(self.first_time_str, self.last_time_str)

    def remove_mess(str_in):
      if isinstance(str_in,str):
        return str_in[11:-3]
      else:
        return ''

    res_df = pd.read_sql(sqlalchemy.text(sus_mod_q), self.connection)
    res_df['overide_value'] = res_df['overide_value'].apply(remove_mess)
    res_df.drop(['pat_id'], 1, inplace=True)
    self.data = res_df

  def to_html(self):
    return self.data.to_html()


class user_engagement(metric):
  def __init__(self,connection, first_time_str, last_time_str):
    super().__init__(connection, first_time_str, last_time_str)
    self.name = 'User Engagement'

  def calc(self):
    user_engag_q = """
    with a1 as (
      select p.pat_id, l.tsp, l.event,
             l.event#>>'{{name}}' as name,
             l.event#>>'{{event_type}}' as type,
             l.event#>>'{{override_value}}' as overide_value,
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
        select u.uid as doc_id,
               first(loc) as hospital,
               count(distinct p.pat_id) as num_pats_seen,
               min(u.tsp) as first_access,
               max(u.tsp) as last_access
        from user_interactions u
        inner join pat_enc p on u.enc_id = p.enc_id
        where u.action = 'page-get'
        and tsp between '{}'::timestamptz and '{}'::timestamptz
        group by u.uid;""".format(self.first_time_str, self.last_time_str)

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
    sql = \
    '''
    with
    bp_included as (
      select distinct BP.enc_id
      from get_latest_enc_ids('HCGH') BP
      inner join cdm_s on cdm_s.enc_id = BP.enc_id and cdm_s.fid = 'age'
      inner join cdm_t on cdm_t.enc_id = BP.enc_id and cdm_t.fid = 'care_unit'
      group by BP.enc_id
      having count(*) filter (where cdm_s.value::numeric <= 18) = 0
      and count(*) filter(where cdm_t.value in ('HCGH LABOR & DELIVERY', 'HCGH EMERGENCY-PEDS', 'HCGH 2C NICU', 'HCGH 1CX PEDIATRICS', 'HCGH 2N MCU')) = 0
    ),
    hcgh_discharged as (
      select distinct cdm_t.enc_id
      from cdm_t
      where cdm_t.fid =  'care_unit' and cdm_t.value like '%%HCGH%%'
      and cdm_t.enc_id not in (
        select distinct enc_id
        from (
          select enc_id from bp_included
          union all
          select cdm_t.enc_id
          from cdm_t
          where cdm_t.fid = 'care_unit'
          and cdm_t.value in ('HCGH LABOR & DELIVERY', 'HCGH EMERGENCY-PEDS', 'HCGH 2C NICU', 'HCGH 1CX PEDIATRICS', 'HCGH 2N MCU')
          group by cdm_t.enc_id
          union all
          select cdm_s.enc_id
          from cdm_s
          where cdm_s.fid = 'age' and cdm_s.value::numeric <= 18
        ) R
      )
    ),
    enc_included as (
      select distinct enc_id from hcgh_discharged
      union
      select distinct enc_id from bp_included
    )
    select 'Total Encounters for Bedded Patients in Time Range' as name, count(distinct E.enc_id) as num_encounters
    from bp_included E
    union all
    select 'Total Encounters With At Least One Measurement in Time Range' as name, count(distinct E.enc_id) as num_encounters
    from enc_included E
    inner join cdm_t on E.enc_id = cdm_t.enc_id
    and cdm_t.tsp between '%(start)s'::timestamptz and '%(end)s'::timestamptz
    union all
    select 'Total Encounters With State Changes In Time Range' as name, count(distinct enc_id) as num_encounters
    from (
        select p.pat_id, C.enc_id
        from criteria_events C
        inner join enc_included R on C.enc_id = R.enc_id
        inner join pat_enc p on c.enc_id = p.enc_id
        group by p.pat_id, C.enc_id
        having max(C.update_date) between '%(start)s'::timestamptz and '%(end)s'::timestamptz
    ) R
    union all
    select 'Total Encounters With Alerts' as name, count(distinct enc_id) as num_encounters
    from (
      select pat_id, enc_id,
             count(*) filter (where trews_subalert > 0) as any_trews,
             count(*) filter (where sirs > 1 and orgdf > 0) as any_cms
      from (
        select p.pat_id, C.enc_id, C.event_id, C.flag,
               count(*) filter (where name like 'trews_subalert' and is_met) as trews_subalert,
               count(*) filter (where name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and is_met) as sirs,
               count(*) filter (where name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and is_met) as orgdf
        from criteria_events C
        inner join enc_included R on C.enc_id = R.enc_id
        inner join pat_enc p on c.enc_id = p.enc_id
        group by p.pat_id, C.enc_id, C.event_id, C.flag
        having max(C.update_date) between '%(start)s'::timestamptz and '%(end)s'::timestamptz
      ) R
      group by pat_id, enc_id
    ) R
    where any_trews > 0 or any_cms > 0
    union all
    select 'TREWS, but no CMS' as name, count(distinct enc_id) as num_encounters
    from (
      select pat_id, enc_id,
             count(*) filter (where trews_subalert > 0) as any_trews,
             count(*) filter (where sirs > 1 and orgdf > 0) as any_cms
      from (
        select p.pat_id, C.enc_id, C.event_id, C.flag,
               count(*) filter (where name like 'trews_subalert' and is_met) as trews_subalert,
               count(*) filter (where name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and is_met) as sirs,
               count(*) filter (where name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and is_met) as orgdf
        from criteria_events C
        inner join enc_included R on C.enc_id = R.enc_id
        inner join pat_enc p on c.enc_id = p.enc_id
        group by p.pat_id, C.enc_id, C.event_id, C.flag
        having max(C.update_date) between '%(start)s'::timestamptz and '%(end)s'::timestamptz
      ) R
      group by pat_id, enc_id
    ) R
    where any_trews > 0 and any_cms = 0
    union all
    select 'CMS, but no TREWS' as name, count(distinct enc_id) as num_encounters
    from (
      select pat_id, enc_id,
             count(*) filter (where trews_subalert > 0) as any_trews,
             count(*) filter (where sirs > 1 and orgdf > 0) as any_cms
      from (
        select p.pat_id, C.enc_id, C.event_id, C.flag,
               count(*) filter (where name like 'trews_subalert' and is_met) as trews_subalert,
               count(*) filter (where name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and is_met) as sirs,
               count(*) filter (where name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and is_met) as orgdf
        from criteria_events C
        inner join enc_included R on C.enc_id = R.enc_id
        inner join pat_enc p on c.enc_id = p.enc_id
        group by p.pat_id, C.enc_id, C.event_id, C.flag
        having max(C.update_date) between '%(start)s'::timestamptz and '%(end)s'::timestamptz
      ) R
      group by pat_id, enc_id
    ) R
    where any_cms > 0 and any_trews = 0
    union all
    select 'TREWS and CMS Co-occurring' as name, count(distinct enc_id) as num_encounters
    from (
      select pat_id, enc_id,
             count(*) filter (where trews_subalert > 0 and sirs > 1 and orgdf > 0) as trews_and_cms
      from (
        select p.pat_id, C.enc_id, C.event_id, C.flag,
               count(*) filter (where name like 'trews_subalert' and is_met) as trews_subalert,
               count(*) filter (where name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and is_met) as sirs,
               count(*) filter (where name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and is_met) as orgdf
        from criteria_events C
        inner join enc_included R on C.enc_id = R.enc_id
        inner join pat_enc p on c.enc_id = p.enc_id
        group by p.pat_id, C.enc_id, C.event_id, C.flag
        having max(C.update_date) between '%(start)s'::timestamptz and '%(end)s'::timestamptz
      ) R
      group by pat_id, enc_id
    ) R
    where trews_and_cms > 0
    ''' % { 'start': self.first_time_str, 'end': self.last_time_str }

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
    sql = \
    '''
    with
    bp_included as (
      select distinct BP.enc_id
      from get_latest_enc_ids('HCGH') BP
      inner join cdm_s on cdm_s.enc_id = BP.enc_id and cdm_s.fid = 'age'
      inner join cdm_t on cdm_t.enc_id = BP.enc_id and cdm_t.fid = 'care_unit'
      group by BP.enc_id
      having count(*) filter (where cdm_s.value::numeric <= 18) = 0
      and count(*) filter(where cdm_t.value in ('HCGH LABOR & DELIVERY', 'HCGH EMERGENCY-PEDS', 'HCGH 2C NICU', 'HCGH 1CX PEDIATRICS', 'HCGH 2N MCU')) = 0
    ),
    hcgh_discharged as (
      select distinct cdm_t.enc_id
      from cdm_t
      where cdm_t.fid =  'care_unit' and cdm_t.value like '%%HCGH%%'
      and cdm_t.enc_id not in (
        select distinct enc_id
        from (
          select enc_id from bp_included
          union all
          select cdm_t.enc_id
          from cdm_t
          where cdm_t.fid = 'care_unit'
          and cdm_t.value in ('HCGH LABOR & DELIVERY', 'HCGH EMERGENCY-PEDS', 'HCGH 2C NICU', 'HCGH 1CX PEDIATRICS', 'HCGH 2N MCU')
          group by cdm_t.enc_id
          union all
          select cdm_s.enc_id
          from cdm_s
          where cdm_s.fid = 'age' and cdm_s.value::numeric <= 18
        ) R
      )
    ),
    confusion_table as (
      select c1.enc_id     as t_enc_id,
             c1.tsp        as t_time,
             c1.cnt        as n_t,
             c2.enc_id     as sev_sep,
             c2.tsp        as sep_time,
             c4.enc_id     as crit_enc_id,
             c4.t_no_c,
             c4.c_no_t,
             c4.t_and_c,
             c4.t          as criteria_trews_alerts,
             c4.c          as cms_alerts,
             c4.crit_t_time,
             c4.cms_time   as cms_alert_time
      from
      (
          (
              select distinct enc_id,
                     max(tsp) as tsp,
                     count(tsp) as cnt
              from trews_jit_score
              where model_id = (select max(value) from trews_parameters where name = 'trews_jit_model_id')
              and (orgdf_details::json ->> 'alert') = 'True'
              and enc_id in (select enc_id from get_latest_enc_ids('HCGH'))
              and tsp between '%(end)s'::timestamptz - interval '%(interval)s' and '%(end)s'::timestamptz
              group by enc_id
          ) c1
          full outer join
          (
              select distinct enc_id, min(tsp) as tsp
              from cdm_labels
              where label_id = (select max(label_id) from label_version)
              and tsp between '%(end)s'::timestamptz - interval '%(interval)s' and '%(end)s'::timestamptz
              group by enc_id
          ) c2
          on c1.enc_id=c2.enc_id
          full outer join
          (
              select  enc_id,
                      sum(trews_no_cms) as t_no_c,
                      sum(cms_no_trews) as c_no_t,
                      sum(trews_and_cms) as t_and_c,
                      sum(any_trews) as t,
                      sum(any_cms) as c,
                      max(trews_subalert_onset) as crit_t_time,
                      max(cms_onset) as cms_time
                  from
                  (
                      select R.pat_id, R.enc_id as enc_id, R.event_id,
                               max(R.update_date) as update_date,
                               count(*) filter (where R.trews_subalert > 0 and ( R.sirs < 2 or R.orgdf < 1 )) as trews_no_cms,
                               count(*) filter (where R.sirs > 1 and R.orgdf > 0 and R.trews_subalert = 0) as cms_no_trews,
                               count(*) filter (where R.trews_subalert > 0 and R.sirs > 1 and R.orgdf > 0) as trews_and_cms,
                               count(*) filter (where R.trews_subalert > 0) as any_trews,
                               count(*) filter (where R.sirs > 1 and R.orgdf > 0) as any_cms,
                               min(R.trews_subalert_onset) filter (where R.trews_subalert > 0) as trews_subalert_onset,
                               min(greatest(R.sirs_onset, R.organ_onset)) filter (where R.sirs > 1 and R.orgdf > 0) as cms_onset
                        from (
                          select p.pat_id, C.enc_id, C.event_id, C.flag,
                                 max(C.update_date) as update_date,
                                 count(*) filter (where C.name like 'trews_subalert' and C.is_met) as trews_subalert,
                                 count(*) filter (where C.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and C.is_met) as sirs,
                                 count(*) filter (where C.name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and C.is_met) as orgdf,
                                 (array_agg(C.measurement_time order by C.measurement_time)  filter (where C.name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and C.is_met ) )[2]   as sirs_onset,
                                 min(C.measurement_time) filter (where C.name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp','respiratory_failure','creatinine','bilirubin','platelet','inr','lactate') and C.is_met ) as organ_onset,
                                 min(C.measurement_time) filter (where C.name = 'trews_subalert' and C.is_met) as trews_subalert_onset
                          from criteria_events C
                          inner join (
                            select distinct cdm_t.enc_id
                            from cdm_t where cdm_t.fid =  'care_unit' and cdm_t.value like '%%HCGH%%'
                            and cdm_t.enc_id not in ( select distinct R.enc_id from get_latest_enc_ids('HCGH') R  )
                            union
                            select distinct BP.enc_id from get_latest_enc_ids('HCGH') BP
                          ) R on C.enc_id = R.enc_id
                          inner join pat_enc p on c.enc_id = p.enc_id
                          group by p.pat_id, C.enc_id, C.event_id, C.flag
                          having max(C.update_date) between '%(end)s'::timestamptz - interval '%(interval)s' and '%(end)s'::timestamptz
                        ) R
                        group by R.pat_id, R.enc_id, R.event_id
                  ) RR group by enc_id
          ) c4
          on coalesce(c1.enc_id, c2.enc_id)=c4.enc_id
      )
    )
    select S.*
    from (
      select ARRAY[
              'Total Sepsis Cases',
              '# of TREWS Alerts',
              'TREWS True Positive',
              '# of CMS Alerts',
              'CMS True Positive',
              'TREWS union CMS alerts',
              'Union True Positive'
             ]::text[] as names,
             ARRAY[
               count(sev_sep),
               count(t_enc_id),
               count(*) filter (where t_enc_id is not null and sev_sep is not null),
               count(cms_alerts) filter (where cms_alerts>0),
               count(*) filter (where cms_alert_time is not null and sev_sep is not null),
               count(coalesce(cms_alert_time, t_time)),
               count(*) filter (where coalesce(cms_alert_time, t_time) is not null and sev_sep is not null)
             ]::bigint[] as values
      from confusion_table C
      where coalesce(C.t_enc_id, C.sev_sep, C.crit_enc_id) in (
        select distinct enc_id from hcgh_discharged
        union
        select distinct enc_id from bp_included
      )
    ) R, unnest(R.names, R.values) S(name, occurrences)
    ''' % { 'start': self.first_time_str, 'end': self.last_time_str, 'interval': '8 hours' }

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
    sql = \
    '''
    with
    bp_included as (
      select distinct BP.enc_id
      from get_latest_enc_ids('HCGH') BP
      inner join cdm_s on cdm_s.enc_id = BP.enc_id and cdm_s.fid = 'age'
      inner join cdm_t on cdm_t.enc_id = BP.enc_id and cdm_t.fid = 'care_unit'
      group by BP.enc_id
      having count(*) filter (where cdm_s.value::numeric <= 18) = 0
      and count(*) filter(where cdm_t.value in ('HCGH LABOR & DELIVERY', 'HCGH EMERGENCY-PEDS', 'HCGH 2C NICU', 'HCGH 1CX PEDIATRICS', 'HCGH 2N MCU')) = 0
    ),
    hcgh_discharged as (
      select distinct cdm_t.enc_id
      from cdm_t
      where cdm_t.fid =  'care_unit' and cdm_t.value like '%%HCGH%%'
      and cdm_t.enc_id not in (
        select distinct enc_id
        from (
          select enc_id from bp_included
          union all
          select cdm_t.enc_id
          from cdm_t
          where cdm_t.fid = 'care_unit'
          and cdm_t.value in ('HCGH LABOR & DELIVERY', 'HCGH EMERGENCY-PEDS', 'HCGH 2C NICU', 'HCGH 1CX PEDIATRICS', 'HCGH 2N MCU')
          group by cdm_t.enc_id
          union all
          select cdm_s.enc_id
          from cdm_s
          where cdm_s.fid = 'age' and cdm_s.value::numeric <= 18
        ) R
      )
    ),
    confusion_table as (
      select c1.enc_id     as t_enc_id,
             c1.tsp        as t_time,
             c1.cnt        as n_t,
             c2.enc_id     as sev_sep,
             c2.tsp        as sep_time,
             c4.enc_id     as crit_enc_id,
             c4.t_no_c,
             c4.c_no_t,
             c4.t_and_c,
             c4.t          as criteria_trews_alerts,
             c4.c          as cms_alerts,
             c4.crit_t_time,
             c4.cms_time   as cms_alert_time
      from
      (
          (
              select distinct enc_id,
                     max(tsp) as tsp,
                     count(tsp) as cnt
              from trews_jit_score
              where model_id = (select max(value) from trews_parameters where name = 'trews_jit_model_id')
              and (orgdf_details::json ->> 'alert') = 'True'
              and enc_id in (select enc_id from get_latest_enc_ids('HCGH'))
              and tsp between '%(end)s'::timestamptz - interval '%(interval)s' and '%(end)s'::timestamptz
              group by enc_id
          ) c1
          full outer join
          (
              select distinct enc_id, min(tsp) as tsp
              from cdm_labels
              where label_id = (select max(label_id) from label_version)
              and tsp between '%(end)s'::timestamptz - interval '%(interval)s' and '%(end)s'::timestamptz
              group by enc_id
          ) c2
          on c1.enc_id=c2.enc_id
          full outer join
          (
              select  enc_id,
                      sum(trews_no_cms) as t_no_c,
                      sum(cms_no_trews) as c_no_t,
                      sum(trews_and_cms) as t_and_c,
                      sum(any_trews) as t,
                      sum(any_cms) as c,
                      max(trews_subalert_onset) as crit_t_time,
                      max(cms_onset) as cms_time
                  from
                  (
                      select R.pat_id, R.enc_id as enc_id, R.event_id,
                               max(R.update_date) as update_date,
                               count(*) filter (where R.trews_subalert > 0 and ( R.sirs < 2 or R.orgdf < 1 )) as trews_no_cms,
                               count(*) filter (where R.sirs > 1 and R.orgdf > 0 and R.trews_subalert = 0) as cms_no_trews,
                               count(*) filter (where R.trews_subalert > 0 and R.sirs > 1 and R.orgdf > 0) as trews_and_cms,
                               count(*) filter (where R.trews_subalert > 0) as any_trews,
                               count(*) filter (where R.sirs > 1 and R.orgdf > 0) as any_cms,
                               min(R.trews_subalert_onset) filter (where R.trews_subalert > 0) as trews_subalert_onset,
                               min(greatest(R.sirs_onset, R.organ_onset)) filter (where R.sirs > 1 and R.orgdf > 0) as cms_onset
                        from (
                          select p.pat_id, C.enc_id, C.event_id, C.flag,
                                 max(C.update_date) as update_date,
                                 count(*) filter (where C.name like 'trews_subalert' and C.is_met) as trews_subalert,
                                 count(*) filter (where C.name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and C.is_met) as sirs,
                                 count(*) filter (where C.name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and C.is_met) as orgdf,
                                 (array_agg(C.measurement_time order by C.measurement_time)  filter (where C.name in ('sirs_temp','heart_rate','respiratory_rate','wbc') and C.is_met ) )[2]   as sirs_onset,
                                 min(C.measurement_time) filter (where C.name in ('blood_pressure','mean_arterial_pressure','decrease_in_sbp','respiratory_failure','creatinine','bilirubin','platelet','inr','lactate') and C.is_met ) as organ_onset,
                                 min(C.measurement_time) filter (where C.name = 'trews_subalert' and C.is_met) as trews_subalert_onset
                          from criteria_events C
                          inner join (
                            select distinct cdm_t.enc_id
                            from cdm_t where cdm_t.fid =  'care_unit' and cdm_t.value like '%%HCGH%%'
                            and cdm_t.enc_id not in ( select distinct R.enc_id from get_latest_enc_ids('HCGH') R  )
                            union
                            select distinct BP.enc_id from get_latest_enc_ids('HCGH') BP
                          ) R on C.enc_id = R.enc_id
                          inner join pat_enc p on c.enc_id = p.enc_id
                          group by p.pat_id, C.enc_id, C.event_id, C.flag
                          having max(C.update_date) between '%(end)s'::timestamptz - interval '%(interval)s' and '%(end)s'::timestamptz
                        ) R
                        group by R.pat_id, R.enc_id, R.event_id
                  ) RR group by enc_id
          ) c4
          on coalesce(c1.enc_id, c2.enc_id)=c4.enc_id
      )
    )
    select S.*
    from (
      select ARRAY[
              'Total Sepsis Cases',
              '# of TREWS Alerts',
              'TREWS True Positive',
              '# of CMS Alerts',
              'CMS True Positive',
              'TREWS union CMS alerts',
              'Union True Positive'
             ]::text[] as names,
             ARRAY[
               count(sev_sep),
               count(t_enc_id),
               count(*) filter (where t_enc_id is not null and sev_sep is not null),
               count(cms_alerts) filter (where cms_alerts>0),
               count(*) filter (where cms_alert_time is not null and sev_sep is not null),
               count(coalesce(cms_alert_time, t_time)),
               count(*) filter (where coalesce(cms_alert_time, t_time) is not null and sev_sep is not null)
             ]::bigint[] as values
      from confusion_table C
      where coalesce(C.t_enc_id, C.sev_sep, C.crit_enc_id) in (
        select distinct enc_id from hcgh_discharged
        union
        select distinct enc_id from bp_included
      )
    ) R, unnest(R.names, R.values) S(name, occurrences)
    ''' % { 'start': self.first_time_str, 'end': self.last_time_str, 'interval': '48 hours' }

    res_df = pd.read_sql(sqlalchemy.text(sql), self.connection)
    self.data = res_df

  def to_html(self):
    return self.data.to_html()


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
