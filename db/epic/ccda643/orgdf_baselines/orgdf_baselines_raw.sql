drop table orgdf_baselines_raw;
create table orgdf_baselines_raw (
  csn_id text,
  pat_id text,
  base_name text,
  datetime  text,
  unit      text,
  value     text
);
\copy orgdf_baselines_raw from '/home/ubuntu/zad/mnt/orgdf_baselines/orgdf_baselines_raw.rpt' with csv delimiter as E'\t' NULL 'NULL';


with transformed as (
  select csn_id, pat_id, base_name, datetime,
    (case when datetime::timestamptz < '2017-11-05 02:00:00'::timestamptz then datetime::timestamptz + interval '4 hours' else datetime::timestamptz + interval '5 hours' end) tsp,
    unit,
    (case when value ~ '^(>|<)' then substring(value from '.$')
      when not isnumeric(value) then null
      else value end)::numeric as "value"
  from orgdf_baselines_raw where value is not null
),
filtered as (
  select * from transformed
  where (base_name = 'INR' and value between 0.01 and 12)
    or (base_name = 'CREATININE' and value between 0.1 and 40)
    or (base_name = 'PLT' and value between 0.1 and 1200)
    or (base_name = 'BILITOT' and value between 0 and 50)
),
encs as (
  select csn_id, pat_id,
  first(value order by value) filter (where base_name = 'CREATININE') creatinine,
  first(tsp order by value) filter (where base_name = 'CREATININE') creatinine_tsp,
  first(value order by value) filter (where base_name = 'INR') inr,
  first(tsp order by value) filter (where base_name = 'INR') inr_tsp,
  first(value order by value) filter (where base_name = 'BILITOT') bilirubin,
  first(tsp order by value) filter (where base_name = 'BILITOT') bilirubin_tsp,
  first(value order by value desc) filter (where base_name = 'PLT') platelets,
  first(tsp order by value desc) filter (where base_name = 'PLT') platelets_tsp
  from filtered
  group by csn_id, pat_id
),
baselines as (
  select pat_id, last(creatinine order by creatinine_tsp) as creatinine,
  last(creatinine_tsp order by creatinine_tsp) as creatinine_tsp,
  last(inr order by inr_tsp) as inr,
  last(inr_tsp order by inr_tsp) as inr_tsp,
  last(bilirubin order by bilirubin_tsp) as bilirubin,
  last(bilirubin_tsp order by bilirubin_tsp) as bilirubin_tsp,
  last(platelets order by platelets_tsp) as platelets,
  last(platelets_tsp order by platelets_tsp) as platelets_tsp
  from encs
  group by pat_id
)
insert into orgdf_baselines
  select pat_id, creatinine, inr, bilirubin, platelets, creatinine_tsp, inr_tsp, bilirubin_tsp, platelets_tsp from baselines
  on conflict (pat_id) do update set
  creatinine = (case when orgdf_baselines.creatinine is null or orgdf_baselines.creatinine_tsp < Excluded.creatinine_tsp then Excluded.creatinine else orgdf_baselines.creatinine end),
  inr = (case when orgdf_baselines.inr is null or orgdf_baselines.inr_tsp < Excluded.inr_tsp then Excluded.inr else orgdf_baselines.inr end),
  bilirubin = (case when orgdf_baselines.bilirubin is null or orgdf_baselines.bilirubin_tsp < Excluded.bilirubin_tsp then Excluded.bilirubin else orgdf_baselines.bilirubin end),
  platelets = (case when orgdf_baselines.platelets is null or orgdf_baselines.platelets_tsp < Excluded.platelets_tsp then Excluded.platelets else orgdf_baselines.platelets end),
  creatinine_tsp = (case when orgdf_baselines.creatinine_tsp is null or orgdf_baselines.creatinine_tsp < Excluded.creatinine_tsp then Excluded.creatinine_tsp else orgdf_baselines.creatinine_tsp end),
  inr_tsp = (case when orgdf_baselines.inr_tsp is null or orgdf_baselines.inr_tsp < Excluded.inr_tsp then Excluded.inr_tsp else orgdf_baselines.inr_tsp end),
  platelets_tsp = (case when orgdf_baselines.platelets_tsp is null or orgdf_baselines.platelets_tsp < Excluded.platelets_tsp then Excluded.platelets_tsp else orgdf_baselines.platelets_tsp end),
  bilirubin_tsp = (case when orgdf_baselines.bilirubin_tsp is null or orgdf_baselines.bilirubin_tsp < Excluded.bilirubin_tsp then Excluded.bilirubin_tsp else orgdf_baselines.bilirubin_tsp end);
