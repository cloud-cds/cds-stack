from datetime import datetime
import pandas as pd
import sqlalchemy
from datetime import datetime as dt


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
    self.name = 'Total Encounters, and Encounters with TREWS and CMS alerts'

  def calc(self):
    sql = \
    '''
    select 'Total Encounters In Time Range' as name, count(distinct enc_id) as num_encounters
    from (
        select p.pat_id, C.enc_id
        from criteria_events C
        inner join (
          select distinct enc_id
          from cdm_t where fid =  'care_unit' and value like '%%HCGH%%'
          and enc_id not in ( select distinct enc_id from get_latest_enc_ids('HCGH') )
          union
          select distinct enc_id from get_latest_enc_ids('HCGH')
        ) R on C.enc_id = R.enc_id
        inner join pat_enc p on c.enc_id = p.enc_id
        group by p.pat_id, C.enc_id
        having max(C.update_date) between '%(start)s'::timestamptz and '%(end)s'::timestamptz
    ) R
    union all
    select 'Total Encounters With Alerts' as name, count(distinct enc_id) as num_encounters
    from (
      select pat_id, enc_id,
             count(*) filter (where trews_subalert > 0 and ( sirs < 2 or orgdf < 1 )) as trews_no_cms,
             count(*) filter (where sirs > 1 and orgdf > 0 and trews_subalert = 0) as cms_no_trews,
             count(*) filter (where trews_subalert > 0 and sirs > 1 and orgdf > 0) as trews_and_cms,
             count(*) filter (where trews_subalert > 0) as any_trews,
             count(*) filter (where sirs > 1 and orgdf > 0) as any_cms
      from (
        select p.pat_id, C.enc_id, C.event_id, C.flag,
               count(*) filter (where name like 'trews_subalert' and is_met) as trews_subalert,
               count(*) filter (where name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and is_met) as sirs,
               count(*) filter (where name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and is_met) as orgdf
        from criteria_events C
        inner join (
          select distinct enc_id
          from cdm_t where fid =  'care_unit' and value like '%%HCGH%%'
          and enc_id not in ( select distinct enc_id from get_latest_enc_ids('HCGH') )
          union
          select distinct enc_id from get_latest_enc_ids('HCGH')
        ) R on C.enc_id = R.enc_id
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
             count(*) filter (where trews_subalert > 0 and ( sirs < 2 or orgdf < 1 )) as trews_no_cms,
             count(*) filter (where sirs > 1 and orgdf > 0 and trews_subalert = 0) as cms_no_trews,
             count(*) filter (where trews_subalert > 0 and sirs > 1 and orgdf > 0) as trews_and_cms,
             count(*) filter (where trews_subalert > 0) as any_trews,
             count(*) filter (where sirs > 1 and orgdf > 0) as any_cms
      from (
        select p.pat_id, C.enc_id, C.event_id, C.flag,
               count(*) filter (where name like 'trews_subalert' and is_met) as trews_subalert,
               count(*) filter (where name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and is_met) as sirs,
               count(*) filter (where name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and is_met) as orgdf
        from criteria_events C
        inner join (
          select distinct enc_id
          from cdm_t where fid =  'care_unit' and value like '%%HCGH%%'
          and enc_id not in ( select distinct enc_id from get_latest_enc_ids('HCGH') )
          union
          select distinct enc_id from get_latest_enc_ids('HCGH')
        ) R on C.enc_id = R.enc_id
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
             count(*) filter (where trews_subalert > 0 and ( sirs < 2 or orgdf < 1 )) as trews_no_cms,
             count(*) filter (where sirs > 1 and orgdf > 0 and trews_subalert = 0) as cms_no_trews,
             count(*) filter (where trews_subalert > 0 and sirs > 1 and orgdf > 0) as trews_and_cms,
             count(*) filter (where trews_subalert > 0) as any_trews,
             count(*) filter (where sirs > 1 and orgdf > 0) as any_cms
      from (
        select p.pat_id, C.enc_id, C.event_id, C.flag,
               count(*) filter (where name like 'trews_subalert' and is_met) as trews_subalert,
               count(*) filter (where name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and is_met) as sirs,
               count(*) filter (where name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and is_met) as orgdf
        from criteria_events C
        inner join (
          select distinct enc_id
          from cdm_t where fid =  'care_unit' and value like '%%HCGH%%'
          and enc_id not in ( select distinct enc_id from get_latest_enc_ids('HCGH') )
          union
          select distinct enc_id from get_latest_enc_ids('HCGH')
        ) R on C.enc_id = R.enc_id
        inner join pat_enc p on c.enc_id = p.enc_id
        group by p.pat_id, C.enc_id, C.event_id, C.flag
        having max(C.update_date) between '%(start)s'::timestamptz and '%(end)s'::timestamptz
      ) R
      group by pat_id, enc_id
    ) R
    where any_cms > 0 and any_trews = 0
    union all
    select 'TREWS and CMS Co-ocurring' as name, count(distinct enc_id) as num_encounters
    from (
      select pat_id, enc_id,
             count(*) filter (where trews_subalert > 0 and ( sirs < 2 or orgdf < 1 )) as trews_no_cms,
             count(*) filter (where sirs > 1 and orgdf > 0 and trews_subalert = 0) as cms_no_trews,
             count(*) filter (where trews_subalert > 0 and sirs > 1 and orgdf > 0) as trews_and_cms,
             count(*) filter (where trews_subalert > 0) as any_trews,
             count(*) filter (where sirs > 1 and orgdf > 0) as any_cms
      from (
        select p.pat_id, C.enc_id, C.event_id, C.flag,
               count(*) filter (where name like 'trews_subalert' and is_met) as trews_subalert,
               count(*) filter (where name in ('sirs_temp', 'heart_rate', 'respiratory_rate', 'wbc') and is_met) as sirs,
               count(*) filter (where name in ('blood_pressure', 'mean_arterial_pressure', 'decrease_in_sbp', 'respiratory_failure', 'creatinine', 'bilirubin', 'platelet', 'inr', 'lactate') and is_met) as orgdf
        from criteria_events C
        inner join (
          select distinct enc_id
          from cdm_t where fid =  'care_unit' and value like '%%HCGH%%'
          and enc_id not in ( select distinct enc_id from get_latest_enc_ids('HCGH') )
          union
          select distinct enc_id from get_latest_enc_ids('HCGH')
        ) R on C.enc_id = R.enc_id
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
    self.name = '# of Encounters with TREWS and CMS Alerts By Unit'

  def calc(self):
    sql = \
    '''
    select * from get_alert_stats_by_unit('%(start)s'::timestamptz, '%(end)s'::timestamptz)
    ''' % { 'start': self.first_time_str, 'end': self.last_time_str }

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












