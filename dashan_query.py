"""
dashan_query.py
"""
import os, sys, traceback
import json
import datetime
import logging
import pytz
from inpatient_updater import load
from sqlalchemy import create_engine
from sqlalchemy import text

logging.basicConfig(format='%(levelname)s|%(message)s', level=logging.INFO)

DB_CONN_STR = 'postgresql://{}:{}@{}:{}/{}'
user = os.environ['db_user']
host = os.environ['db_host']
db = os.environ['db_name']
port = os.environ['db_port']
password = os.environ['db_password']
epic_notifications = os.environ['epic_notifications']
DB_CONN_STR = DB_CONN_STR.format(user, password, host, port, db)
db_engine = create_engine(DB_CONN_STR)


##########################################
# Compact query implementations.
# These pull out multiple component TREWS data and metadata
# components in fewer queries, reducing the # DB roundtrips.
#

# For a patient, returns time series of:
# - trews scores
# - top 3 features that contributed to the each time point
def get_trews_contributors(pat_id):
    rank_limit = 3
    get_contributors_sql = \
    '''
    select tsp, trewscore, fid, cdm_value, rnk
    from calculate_trews_contributors('%(pid)s', %(rank_limit)s)
            as R(enc_id, tsp, trewscore, fid, trews_value, cdm_value, rnk)
    order by tsp
    ''' % {'pid': pat_id, 'rank_limit': rank_limit}

    conn = db_engine.connect()
    result = conn.execute(get_contributors_sql).fetchall()
    conn.close()

    timestamps = []
    trewscores = []
    tf_names   = [[] for i in range(rank_limit)]
    tf_values  = [[] for i in range(rank_limit)]

    epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)

    for row in result:
        rnk = int(row['rnk'])
        if rnk <= rank_limit:
            try:
                v = float(row['cdm_value'])
            except ValueError:
                v = str(row['cdm_value'])

            timestamps.append((row['tsp'] - epoch).total_seconds())
            trewscores.append(float(row['trewscore']))
            tf_names[rnk-1].append(str(row['fid']))
            tf_values[rnk-1].append(v)
        else:
            logging.warning("Invalid trews contributor rank: {}" % rnk)

    return {
        'timestamp'  : timestamps,
        'trewscore'  : trewscores,
        'tf_1_name'  : tf_names[0],
        'tf_1_value' : tf_values[0],
        'tf_2_name'  : tf_names[1],
        'tf_2_value' : tf_values[1],
        'tf_3_name'  : tf_names[2],
        'tf_3_value' : tf_values[2]
    }


# Single roundtrip retrieval of both notifications and history events.
def get_patient_events(pat_id):
    get_events_sql = \
    '''
    select 0 as event_type,
           notification_id as evt_id,
           null as tsp,
           message as payload
    from notifications where pat_id = '%(pat_id)s'
    union all
    select 1 as event_type,
           log_id as evt_id,
           date_part('epoch', tsp) as tsp,
           event as payload
    from criteria_log where pat_id = '%(pat_id)s'
    ''' % { 'pat_id': pat_id }

    conn = db_engine.connect()
    result = conn.execute(get_events_sql).fetchall()
    conn.close()

    notifications = []
    history = []

    for row in result:
        if row['event_type'] == 0:
            notification = row['payload']
            notification['timestamp'] = int(notification['timestamp'])
            notification['id'] = row['evt_id']
            notifications.append(notification)
        else:
            audit = row['payload']
            audit['log_id'] = row['evt_id']
            audit['pat_id'] = pat_id
            audit['timestamp'] = row['tsp']
            history.append(audit)

    return (notifications, history)


# For a patient, returns the:
# - trews threshold
# - admit time
# - activated/deactivated status
# - deterioration feedback timestamp, statuses and uid
#
def get_patient_profile(pat_id):
    get_patient_profile_sql = \
    '''
    select * from
    (
        select value as trews_threshold
        from trews_parameters where name = 'trews_threshold' limit 1
    ) TT
    full outer join
    (
        select value::timestamptz as admit_time
        from cdm_s inner join pat_enc on pat_enc.enc_id = cdm_s.enc_id
        where pat_id = '%(pid)s' and fid = 'admittime'
        order by value::timestamptz desc limit 1
    ) ADT on true
    full outer join
    (
        select deactivated from pat_status where pat_id = '%(pid)s' limit 1
    ) DEACT on true
    full outer join
    (
        select date_part('epoch', tsp) detf_tsp, deterioration, uid as detf_uid
        from deterioration_feedback where pat_id = '%(pid)s' limit 1
    ) DETF on true
    ''' % { 'pid': pat_id }
    conn = db_engine.connect()
    result = conn.execute(get_patient_profile_sql).fetchall()
    conn.close()

    profile = {
        'trews_threshold' : None,
        'admit_time'      : None,
        'deactivated'     : None,
        'detf_tsp'        : None,
        'deterioration'   : None,
        'detf_uid'        : None
    }

    if len(result) == 1:
        profile['trews_threshold'] = float("{:.2f}".format(float(result[0][0])))
        profile['admit_time']      = (result[0][1] - datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.UTC)).total_seconds()
        profile['deactivated']     = result[0][2]
        profile['detf_tsp']        = result[0][3]
        profile['deterioration']   = result[0][4]
        profile['detf_uid']        = result[0][5]

    return profile


def get_criteria(eid):
    get_criteria_sql = \
    '''
    select * from get_criteria('%s')
    ''' % eid
    conn = db_engine.connect()
    result = conn.execute(get_criteria_sql).fetchall()
    conn.close()
    return result


def get_criteria_log(eid):
    get_criteria_log_sql = \
    '''
    select log_id, pat_id, date_part('epoch', tsp) epoch, event from criteria_log
    where pat_id = '%s' order by tsp desc limit 25;
    ''' % eid
    auditlist = []
    conn = db_engine.connect()
    result = conn.execute(get_criteria_log_sql)
    conn.close()
    for row in result:
        audit = row['event']
        audit['log_id'] = row['log_id']
        audit['pat_id'] = row['pat_id']
        audit['timestamp'] = row['epoch']
        auditlist.append(audit)
    return auditlist


def get_notifications(eid):
    get_notifications_sql = \
    '''
    select * from notifications
    where pat_id = '%s'
    ''' % eid
    notifications = []
    conn = db_engine.connect()
    result = conn.execute(get_notifications_sql)
    conn.close()
    for row in result:
        notification = row['message']
        notification['timestamp'] = int(notification['timestamp'])
        notification['id'] = row['notification_id']
        notifications.append(notification)
    return notifications


def toggle_notification_read(eid, notification_id, as_read):
    toggle_notifications_sql = \
    '''
    with update_notifications as
    (   update notifications
        set message = jsonb_set(message::jsonb, '{read}'::text[], '%(val)s'::jsonb, false)
        where pat_id = '%(pid)s' and notification_id = %(nid)s
        RETURNING *
    )
    insert into criteria_log (pat_id, tsp, event, update_date)
    select
            '%(pid)s',
            now(),
            json_build_object('event_type', 'toggle_notifications', 'message', n.message),
            now()
    from update_notifications n;
    ''' % {'pid': eid, 'nid': notification_id, 'val': str(as_read).lower()}
    logging.info("toggle_notifications_read:" + toggle_notifications_sql)
    conn = db_engine.connect()
    conn.execute(toggle_notifications_sql)
    conn.close()
    push_notifications_to_epic(eid)


def temp_c_to_f(c):
    return c * 1.8 + 32

def override_criteria(eid, name, value='[{}]', user='user', clear=False):
    if name == 'sirs_temp' and not clear:
        value[0]['lower'] = temp_c_to_f(float(value[0]['lower']))
        value[0]['upper'] = temp_c_to_f(float(value[0]['upper']))
    params = {
        'user': ("'" + user + "'") if not clear else 'null',
        'val': ("'" + (json.dumps(value) if isinstance(value, list) else value) + "'") if not clear else 'null',
        'name': name if name != 'sus-edit' else 'suspicion_of_infection',
        'pid': eid,
        'user_log': user,
        'val_log': (json.dumps(value) if isinstance(value, list) else value) if not clear else 'null',
        'clear_log': 'true' if clear else 'false'
    }
    override_sql = \
    '''
    update criteria set
        override_time = now(),
        update_date = now(),
        override_value = %(val)s,
        override_user = %(user)s
    where pat_id = '%(pid)s' and name = '%(name)s';
    insert into criteria_log (pat_id, tsp, event, update_date)
    values (
            '%(pid)s',
            now(),
            '{"event_type": "override", "name":"%(name)s", "uid":"%(user_log)s", "override_value":%(val_log)s, "clear":%(clear_log)s}',
            now()
        );
    select override_criteria_snapshot('%(pid)s');
    ''' % params
    logging.info("override_criteria sql:" + override_sql)
    conn = db_engine.connect()
    conn.execute(override_sql)
    conn.close()
    push_notifications_to_epic(eid)


def reset_patient(eid, uid='user', event_id=None):
    event_where_clause = '' if event_id is None or event_id == 'None' else 'and event_id = %(evid)s' % {'evid' : event_id }
    reset_sql = """
    update criteria_events set flag = -1
    where pat_id = '%(pid)s' %(where_clause)s;
    insert into criteria_log (pat_id, tsp, event, update_date)
    values (
            '%(pid)s',
            now(),
            '{"event_type": "reset", "uid":"%(uid)s"}',
            now()
        );
    delete from notifications where pat_id = '%(pid)s';
    select advance_criteria_snapshot('%(pid)s');
    """ % {'pid': eid, 'where_clause': event_where_clause, 'uid': uid}
    logging.info("reset_patient:" + reset_sql)
    conn = db_engine.connect()
    conn.execute(reset_sql)
    conn.close()
    push_notifications_to_epic(eid)


def deactivate(eid, uid, deactivated):
    deactivate_sql = '''
    select * from deactivate('%(pid)s', %(deactivated)s);
    insert into criteria_log (pat_id, tsp, event, update_date)
    values (
            '%(pid)s',
            now(),
            '{"event_type": "deactivate", "uid":"%(uid)s", "deactivated": %(deactivated)s}',
            now()
        );
    ''' % {'pid': eid, "deactivated": 'true' if deactivated else "false", "uid":uid}
    logging.info("deactivate user:" + deactivate_sql)
    conn = db_engine.connect()
    conn.execute(text(deactivate_sql).execution_options(autocommit=True))
    conn.close()
    push_notifications_to_epic(eid)


def get_deactivated(eid):
    conn = db_engine.connect()
    deactivated = conn.execute("select deactivated from pat_status where pat_id = '%s'" % eid).fetchall()
    conn.close()
    if len(deactivated) == 1 and deactivated[0][0] is True:
        return True
    else:
        return False


def set_deterioration_feedback(eid, deterioration_feedback, uid):
    deterioration_sql = '''
    select * from set_deterioration_feedback('%(pid)s', now(), '%(deterioration)s', '%(uid)s');
    ''' % {'pid': eid, 'deterioration': json.dumps(deterioration_feedback), 'uid':uid}
    logging.info("set_deterioration_feedback user:" + deterioration_sql)
    conn = db_engine.connect()
    conn.execute(text(deterioration_sql).execution_options(autocommit=True))
    conn.close()


def get_deterioration_feedback(eid):
    conn = db_engine.connect()
    df = conn.execute("select pat_id, date_part('epoch', tsp) tsp, deterioration, uid from deterioration_feedback where pat_id = '%s' limit 1" % eid).fetchall()
    conn.close()
    if len(df) == 1:
        return {"tsp": df[0][1], "deterioration": df[0][2], "uid": df[0][3]}


def push_notifications_to_epic(eid):
    if epic_notifications is not None and int(epic_notifications):
        notifications_sql = \
        '''
        select * from get_notifications_for_epic('%s');
        ''' % eid
        conn = db_engine.connect()
        notifications = conn.execute(notifications_sql).fetchall()
        conn.close()

        if notifications:
            patients = [{'pat_id': n['pat_id'], 'visit_id': n['visit_id'], 'notifications': n['count'],
                            'current_time': datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                        } for n in notifications]

            logging.info("sending notifications to epic")
            client_id = os.environ['jhapi_client_id'],
            client_secret = os.environ['jhapi_client_secret']
            loader = load.Loader('prod', client_id, client_secret)
            loader.load_notifications(patients)
        else:
            logging.info("no notifications")


def eid_exist(eid):
    connection = db_engine.connect()
    result = connection.execute("select * from pat_enc where pat_id = '%s' limit 1" % eid)
    connection.close()
    for row in result:
        return True
    return False


def save_feedback(doc_id, pat_id, dep_id, feedback):
    conn = db_engine.connect()
    feedback_sql = '''
        INSERT INTO feedback_log (doc_id, tsp, pat_id, dep_id, feedback)
        VALUES ('%(doc)s', now(), '%(pat)s', '%(dep)s', '%(fb)s');
        ''' % {'doc': doc_id, 'pat': pat_id, 'dep': dep_id, 'fb': feedback}
    try:
        conn.execute(feedback_sql)
    except Exception as e:
        logging.warning(e.message)
        traceback.print_exc()

    conn.close()


if __name__ == '__main__':
    # eid = 'E1000109xx'
    # print eid_exist(eid)
    eid = 'E100194473'
    print(eid_exist(eid))
    df = get_trews(eid)
    print(df.head())
    df_trews = df.drop(['enc_id','trewscore','tsp'],1)

    cdm = get_cdm(eid)

    sorted_trews = [row.sort_values(ascending=False) for idx, row in df_trews.iterrows()]

    names =  [row.index[0] for row in sorted_trews]
    vals = []
    for i, row in enumerate(sorted_trews):
        fid = row.index[0]
        if fid in cdm.iloc[i]:
            vals.append(cdm.iloc[i][fid])
        else:
            vals.append(0)
    for i, n in enumerate(names):
        print(n, vals[i])
