"""
criteria.py
calculate online criteria
"""
import datetime
import ujson as json




def archive(dashan, job_id):
  archive_sql = """
  -- find patients who are not online for one week
  and archive their measurements and criteria
  insert into criteria_meas_archive
  (
    select c.pat_id from criteria c
        left join workspace.%(job)s_bedded_patients_transformed bp
          on bp.pat_id = c.pat_id
        left join criteria_meas meas
          on meas.pat_id = c.pat_id and now() - meas.tsp > interval '7 days'
        where bp.pat_id is null
    );
  insert into criteria_archive
  (
    select c.pat_id from criteria c
        left join workspace.%(job)s_bedded_patients_transformed bp
          on bp.pat_id = c.pat_id
        where bp.pat_id is null
    );
  """ % {'job': job_id}
  dashan.query_with_sql(archive_sql)

def rm_discharged_patients(dashan, results):
  # find which patients are out of current bedded patients
  current_patients = [pt_result[0]['pat_id'] for pt_result in results if len(pt_result) > 0]
  for pt in current_patients:
    print "current", pt
  select_pat_ids = "select distinct pat_id from criteria"
  pat_ids = dashan.select_with_sql(select_pat_ids)
  discharged_patients = []
  for pt in pat_ids:
    pt = pt[0]
    if pt in current_patients:
      pass
      # print "current:", pt
    else:
      discharged_patients.append(pt)
      print "discharged:", pt
  # move out-of-date patients to archive
  if len(discharged_patients) > 0:
    pat_ids = '(' + ','.join(["'%s'" % pt for pt in discharged_patients]) + ')'
    rm_sql = """
    delete from criteria_meas where pat_id in %(pat_id)s;
    delete from criteria where pat_id in %(pat_id)s;
    """ % {'pat_id': pat_ids}
    dashan.query_with_sql(rm_sql)

def upsert_meas(dashan, job_id):
  # insert all results to the measurement table
  upsert_meas_sql = \
  """INSERT INTO criteria_meas (pat_id, tsp, fid, value, update_date)
          select pat_id, tsp::timestamptz, fid, last(fs.value), last(NOW() )
          from workspace.%(job)s_flowsheets_transformed fs
          where tsp <> 'NaT' and tsp::timestamptz < now()
          group by pat_id, tsp, fid
    ON CONFLICT (pat_id, tsp, fid)
      DO UPDATE SET value = EXCLUDED.value, update_date = NOW();
    INSERT INTO criteria_meas (pat_id, tsp, fid, value, update_date)
          select pat_id, tsp::timestamptz, fid, last(lr.value), last(NOW() )
          from workspace.%(job)s_lab_results_transformed lr
          where tsp <> 'NaT' and tsp::timestamptz < now()
          group by pat_id, tsp, fid
    ON CONFLICT (pat_id, tsp, fid)
      DO UPDATE SET value = EXCLUDED.value, update_date = NOW();
    INSERT INTO criteria_meas (pat_id, tsp, fid, value, update_date)
          select pat_id, tsp::timestamptz, fid, last(lo.status), last(NOW() )
          from workspace.%(job)s_lab_orders_transformed lo
          where tsp <> 'NaT' and tsp::timestamptz < now()
          group by pat_id, tsp, fid
    ON CONFLICT (pat_id, tsp, fid)
      DO UPDATE SET value = EXCLUDED.value, update_date = NOW();
    INSERT INTO criteria_meas (pat_id, tsp, fid, value, update_date)
          select pat_id, tsp::timestamptz, fid, last(mar.dose_value), last(NOW() )
          from workspace.%(job)s_med_admin_transformed mar
          where tsp <> 'NaT' and tsp::timestamptz < now()
          group by pat_id, tsp, fid
    ON CONFLICT (pat_id, tsp, fid)
      DO UPDATE SET value = EXCLUDED.value, update_date = NOW();
    INSERT INTO criteria_meas (pat_id, tsp, fid, value, update_date)
          select pat_id, tsp::timestamptz, fid, last(mo.dose), last(NOW() )
          from workspace.%(job)s_med_orders_transformed mo
          where tsp <> 'NaT' and tsp::timestamptz < now()
          group by pat_id, tsp, fid
    ON CONFLICT (pat_id, tsp, fid)
      DO UPDATE SET value = EXCLUDED.value, update_date = NOW();
    delete from criteria_meas where value = '';
  """ % {'job': job_id}
  dashan.query_with_sql(upsert_meas_sql)

def advance_criteria_snapshot(dashan):
  dashan.query_with_sql("select advance_criteria_snapshot();")

def garbage_collection(dashan):
  dashan.query_with_sql("select garbage_collection();")

def calculate(dashan, job_id):
  dashan.log.info("start garbage_collection")
  garbage_collection(dashan)
  dashan.log.info("completed garbage_collection")
  dashan.log.info("upserting meas")
  upsert_meas(dashan, job_id)
  dashan.log.info("upserted meas")
  dashan.log.info("advancing criteria snapshot")
  advance_criteria_snapshot(dashan)
  dashan.log.info("advanced criteria snapshot")