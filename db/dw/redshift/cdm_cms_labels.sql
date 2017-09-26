create or replace function isnumeric (aval varchar(max))
  returns bool
IMMUTABLE
as $$
    try:
       x = float(aval);
    except:
       return False;
    else:
       return True;
$$ language plpythonu;


create or replace function criteria_value_met(/*m_value*/ varchar, /*d_ovalue*/ varchar(max)) returns boolean immutable as $func$
  select (case
    when isnumeric($1)
    then
    (case
      when isnumeric(json_extract_path_text($2, 'lower'))
      and  isnumeric(json_extract_path_text($2, 'upper'))
      then coalesce( not (
            $1::double precision
                between json_extract_path_text($2, 'lower')::double precision
                and json_extract_path_text($2, 'upper')::double precision
          ), false)

      when isnumeric(json_extract_path_text($2, 'lower'))
      then
          coalesce( not (
            $1::double precision between json_extract_path_text($2, 'lower')::double precision and $1::double precision
          ), false)

      when isnumeric(json_extract_path_text($2, 'upper'))
      then
          coalesce( not (
            $1::double precision between $1::double precision and json_extract_path_text($2, 'upper')::double precision
          ), false)
      else false
      end
    )
    else false
    end
  );
$func$ language sql;


create or replace function decrease_in_sbp_met(/*pat_sbp*/ numeric, /*m_value*/ varchar, /*d_ovalue*/ varchar(max)) returns boolean immutable as $func$
  select (
    case
      when isnumeric($2) and isnumeric(json_extract_path_text($3, 'upper'))
      then coalesce( $1 - $2::double precision > json_extract_path_text($3, 'upper')::double precision, false)

      when isnumeric($2)
      then coalesce( $1 - $2::double precision > $2::double precision, false)

      else false
      end
  );
$func$ language sql;

create or replace function decrease_in_sbp_met(/*pat_sbp*/ double precision, /*m_value*/ varchar, /*d_ovalue*/ varchar(max)) returns boolean immutable as $func$
  select (
    case
      when isnumeric($2) and isnumeric(json_extract_path_text($3, 'upper'))
      then coalesce( $1 - $2::double precision > json_extract_path_text($3, 'upper')::double precision, false)

      when isnumeric($2)
      then coalesce( $1 - $2::double precision > $2::double precision, false)

      else false
      end
  );
$func$ language sql;


create or replace function urine_output_met(/*urine_output*/ numeric, /*weight*/ numeric, /*_dataset_id*/ integer) returns boolean immutable
as $func$
  select coalesce((
      $1 / coalesce( $2, ( select value::double precision from cdm_g where fid = 'weight_popmean' and dataset_id = $3 ) )
          < 0.5
  ), false);
$func$ language sql;


create or replace function urine_output_met(/*urine_output*/ double precision, /*weight*/ double precision, /*_dataset_id*/ integer) returns boolean immutable
as $func$
  select coalesce((
      $1 / coalesce( $2, ( select value::double precision from cdm_g where fid = 'weight_popmean' and dataset_id = $3 ) )
          < 0.5
  ), false);
$func$ language sql;


create or replace function dose_order_status(/*order_fid*/ text) returns text immutable
as $func$
  select case when $1 in ('cms_antibiotics_order', 'crystalloid_fluid_order', 'vasopressors_dose_order') then 'Ordered'
              when $1 in ('cms_antibiotics', 'crystalloid_fluid', 'vasopressors_dose') then 'Completed'
              else null
          end;
$func$ language sql;


create or replace function dose_order_met(/*order_fid*/ text, /*dose_value*/ numeric, /*dose_limit*/ numeric)
    returns boolean immutable
as $func$
    select case when dose_order_status($1) = 'Completed' and $3 = 0 then true
                when dose_order_status($1) = 'Completed' then $2 > $3
                else false
            end;
$func$ language sql;


create or replace function dose_order_met(/*order_fid*/ text, /*dose_value*/ double precision, /*dose_limit*/ double precision)
    returns boolean immutable
as $func$
    select case when dose_order_status($1) = 'Completed' and $3 = 0 then true
                when dose_order_status($1) = 'Completed' then $2 > $3
                else false
            end;
$func$ language sql;


create or replace function order_status(/*order_fid*/ text, /*value_text*/ text) returns text immutable
as $func$
  select case when $1 = 'lactate_order' and $2 in (
                      'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result',
                      'Final', 'Final result', 'Edited Result - FINAL',
                      'Completed', 'Corrected', 'Not Indicated'
                  ) then 'Completed'

              when $1 = 'lactate_order' and $2 in ('None', 'Signed') then 'Ordered'

              when $1 = 'blood_culture_order' and $2 in (
                      'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result',
                      'Final', 'Final result', 'Edited Result - FINAL',
                      'Completed', 'Corrected', 'Not Indicated'
                  ) then 'Completed'

              when $1 = 'blood_culture_order' and $2 in ('None', 'Signed') then 'Ordered'
              else null
          end;
$func$ language sql;


create or replace function order_met(/*order_name*/ text, /*order_value*/ text) returns boolean immutable
as $func$
    select case when $1 = 'blood_culture_order'
                    then $2 in (
                        'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result',
                        'Final', 'Final result', 'Edited Result - FINAL',
                        'Completed', 'Corrected', 'Not Indicated'
                    )

                when $1 = 'initial_lactate_order' or $1 = 'repeat_lactate_order'
                    then $2 in (
                        'In process', 'In  process', 'Sent', 'Preliminary', 'Preliminary result',
                        'Final', 'Final result', 'Edited Result - FINAL',
                        'Completed', 'Corrected', 'Not Indicated'
                    )
                else false
            end;
$func$ language sql;
