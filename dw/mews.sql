DROP TABLE IF EXISTS mews;
CREATE TABLE mews (
    dataset_id                             integer,
    enc_id                                 integer,
    tsp                                    timestamptz,
    resp_rate_score                        numeric,
    heart_rate_score                       numeric,
    nbp_sys_score                          numeric,
    gcs_score                              numeric,
    temp_score                             numeric,
    urine_score                            numeric,
    mews_score                             numeric
);

CREATE OR REPLACE FUNCTION calculate_mews(_dataset_id integer) RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
  insert into mews
  select R.*, coalesce(S.urine_score, 0) as urine_score,
         (resp_rate_score + heart_rate_score + nbp_sys_score + gcs_score + temp_score + coalesce(urine_score, 0)) as mews_score
  from (
    select
      dataset_id, enc_id, tsp,
      (case
        when resp_rate < 9 then 2
        when resp_rate >= 9 and resp_rate < 15 then 0
        when resp_rate >= 15 and resp_rate <= 20 then 1
        when resp_rate > 20 and resp_rate <= 30 then 2
        else 3 -- resp_rate > 30
        end
      ) as resp_rate_score,

      (case
        when heart_rate < 40 then 2
        when heart_rate between 40 and 50 then 1
        when heart_rate > 50 and heart_rate <= 100 then 0
        when heart_rate > 100 and heart_rate <= 110 then 1
        when heart_rate > 110 and heart_rate < 129 then 2
        else 3
        end
      ) as heart_rate_score,

      (case
        when nbp_sys < 70 then 3
        when nbp_sys > 70 and nbp_sys <= 80 then 2
        when nbp_sys > 80 and nbp_sys <= 100 then 1
        when nbp_sys > 100 and nbp_sys < 200 then 0
        when nbp_sys > 200 then 2
        end
      ) as nbp_sys_score,

      (case
          when gcs > 14 then 0
          when gcs >= 12 and gcs <= 14 then 1
          when gcs >= 8 and gcs < 12 then 2
          when gcs >= 3 and gcs < 8 then 3
        end
      ) as gcs_score,

      (case
          when temperature < 95 then 2
          when temperature >= 95 and temperature < 96.8 then 1
          when temperature >= 96.8 and temperature < 100.4 then 0
          when temperature >= 100.4 and temperature < 101.48 then 1
          else 2
        end
      ) as temp_score
    from cdm_twf
    where dataset_id = coalesce(_dataset_id, dataset_id)
  ) R
  left join (
    with UO as (
      select enc_id, tsp, value from cdm_t
      where dataset_id = coalesce(_dataset_id, dataset_id)
      and fid = 'urine_output'
    )
    select U.enc_id, U.tsp,
           (case
              when urine_output_1hr < 10 then 3
              when urine_output_1hr >= 10 and urine_output_1hr < 30 then 2
              when urine_output_1hr >= 30 and urine_output_1hr < 45 then 1
              else 0
            end) as urine_score
    from (
      select uo2.enc_id, uo2.tsp,
             (coalesce(sum(case when uo1.tsp >= uo2.tsp - interval '2 hours' and uo1.tsp < uo2.tsp - interval '1 hour' then uo1.value::numeric else null end), 0)
              + coalesce(sum(case when uo1.tsp between uo2.tsp - interval '1 hour' and uo2.tsp then uo1.value::numeric else null end), 0)
             ) / 2 as urine_output_1hr
      from UO uo1 inner join UO uo2
        on uo1.enc_id = uo2.enc_id and uo1.tsp between uo2.tsp - interval '2 hours' and uo2.tsp
      group by uo2.enc_id, uo2.tsp
    ) U
  ) S
  on R.enc_id = S.enc_id and R.tsp = S.tsp
  order by R.enc_id, R.tsp;

  return;
END; $function$;

