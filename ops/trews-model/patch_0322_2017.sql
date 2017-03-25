UPDATE cdm_s
SET value = 0
WHERE fid = 'gender'
  AND value = 'Female';

UPDATE cdm_s
SET value = 1
WHERE fid = 'gender'
  AND value = 'Male';

ALTER TABLE trews
ADD bun                     double precision,
ADD cardio_sofa             double precision,
ADD creatinine              double precision,
ADD emphysema_hist          double precision,
ADD esrd_prob               double precision,
ADD gcs                     double precision,
ADD gender                  double precision,
ADD heart_arrhythmias_prob  double precision,
ADD heart_failure_hist      double precision,
ADD hematologic_sofa        double precision,
ADD lipase                  double precision,
ADD mapm                    double precision,
ADD pao2                   double precision,
ADD paco2                   double precision,
ADD resp_rate               double precision,
ADD resp_sofa               double precision,
ADD sirs_hr_oor             double precision,
ADD sirs_wbc_oor            double precision,
ADD temperature             double precision,
ADD wbc                     double precision,
ADD amylase                      double precision,
ADD nbp_dias                     double precision,
ADD renal_sofa                   double precision,
ADD urine_output_24hr            double precision,
ADD worst_sofa                   double precision;
