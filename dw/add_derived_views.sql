

CREATE OR REPLACE FUNCTION calculate_historical_criteria(this_pat_id text)
 RETURNS table(window_ts                        timestamptz,
               pat_id                           varchar(50),
               pat_state                        INTEGER
               )
 LANGUAGE plpgsql
AS $function$
DECLARE
    window_size interval := get_parameter('lookbackhours')::interval;
BEGIN
    create temporary table new_criteria_windows as
        select window_ends.tsp as ts, new_criteria.*
        from (  select distinct meas.pat_id, meas.tsp from criteria_meas meas
                where meas.pat_id = coalesce(this_pat_id, meas.pat_id)
--                 and meas.tsp between ts_start and ts_end
        ) window_ends
        inner join lateral calculate_criteria(
            coalesce(this_pat_id, window_ends.pat_id), window_ends.tsp - window_size, window_ends.tsp
        ) new_criteria
        on window_ends.pat_id = new_criteria.pat_id;

    return query
            select sw.*
            from get_window_states('new_criteria_windows', this_pat_id) sw;
    drop table new_criteria_windows;
    return;
END; $function$;