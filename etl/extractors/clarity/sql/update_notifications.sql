-- update notifications
CREATE OR REPLACE FUNCTION update_notifications() 
RETURNS void AS $$
BEGIN
-- severe sepsis notifications
-- for patient who met
-- suspicion of infection
-- sirs (at least two sirs criteria)
-- organ dysfunction (at lesat one)
delete from notifications;

with severe_sepsis as
(
    select c.pat_id, 
        json_build_object('alert_code', '200', 'read', false,'timestamp', 
               date_part('epoch', GREATEST(c.override_time, c2.onset_tsp, c3.onset_tsp ) ) * 1000) message
    from criteria c
        inner join (select pat_id, count(*), (array_agg(measurement_time order by measurement_time))[2]  onset_tsp from criteria
                where name ~ 'sirs_temp|heart_rate|respiratory_rate|wbc' 
                    and (is_met is true or override_user is not null)
                group by pat_id
            ) c2
            on c2.pat_id = c.pat_id and c2.count > 1
        inner join (select pat_id, count(*), min(measurement_time) onset_tsp from criteria
                where name ~ 'blood_pressure|mean_arterial_pressure|ecrease_in_sbp|espiratory_failure|reatinine|ilirubin|latelet|nr|actate' 
                    and (is_met is true or override_user is not null)
                group by pat_id
            ) c3
            on c3.pat_id = c.pat_id and c3.count > 0
        where c.name = 'suspicion_of_infection' and c.override_user is not null
) ,
severe_sepsis_notifications as 
(
    insert into notifications (pat_id, message)
    select pat_id, message
    from severe_sepsis
    returning pat_id, message->>'timestamp'
),
septic_shock_notifications as 
(
    insert into notifications (pat_id, message)
    select c1.pat_id,
        json_build_object('alert_code', '201', 'read', false,'timestamp', 
               date_part('epoch', GREATEST(c.timestamp, coalesce(c1.override_time, c1.measurement_time), c2.onset_tsp) ) * 1000)
    from severe_sepsis_notifications c
        inner join criteria c1 on c.pat_id = c1.pat_id
        inner join (
            select pat_id, count(*), (array_agg(measurement_time order by measurement_time))[2]  onset_tsp from criteria 
                where name ~ 'blood_pressure|mean_arterial_pressure|decrease_in_sbp|lactate' 
                    and (is_met is true or override_user is not null)
                group by pat_id
            ) c2
            on c2.pat_id = c1.pat_id
    where c1.name = 'crystalloid_fluid' and (c1.is_met is true or c1.override_user is not null)
        and c2.count > 0
    returning pat_id
)
-- trewscore notifications
insert into notifications_performance (pat_id, message)
    select pat_id, 
        json_build_object('alert_code', '100', 'read', false, 'timestamp', min(tsp))
    from trews inner join pat_enc on pat_enc.enc_id = trews.enc_id
    where trewscore > 0.6
    group by pat_id;

RETURN;


END;  

$$ LANGUAGE PLPGSQL;


-- update notifications
CREATE OR REPLACE FUNCTION update_pat_notifications(this_pat_id text) 
RETURNS void AS $$
BEGIN
-- severe sepsis notifications
-- for patient who met
-- suspicion of infection
-- sirs (at least two sirs criteria)
-- organ dysfunction (at lesat one)
delete from notifications where pat_id = this_pat_id;

with severe_sepsis as
(
    select c.pat_id, 
        json_build_object('alert_code', '200', 'read', false,'timestamp', 
               date_part('epoch', GREATEST(c.override_time, c2.onset_tsp, c3.onset_tsp ) ) * 1000) message
    from criteria c
        inner join (select pat_id, count(*), (array_agg(measurement_time order by measurement_time))[2]  onset_tsp from criteria
                where name ~ 'sirs_temp|heart_rate|respiratory_rate|wbc' 
                    and (is_met is true or override_user is not null) and pat_id = this_pat_id
                group by pat_id
            ) c2 
            on c2.pat_id = c.pat_id and c2.count > 1
        inner join (select pat_id, count(*), min(measurement_time) onset_tsp from criteria
                where name ~ 'blood_pressure|mean_arterial_pressure|ecrease_in_sbp|espiratory_failure|reatinine|ilirubin|latelet|nr|actate' 
                    and (is_met is true or override_user is not null) and pat_id = this_pat_id
                group by pat_id
            ) c3
            on c3.pat_id = c.pat_id and c3.count > 0
        where c.pat_id = this_pat_id and c.name = 'suspicion_of_infection' and c.override_user is not null
) ,
severe_sepsis_notifications as 
(
    insert into notifications (pat_id, message)
    select pat_id, message from severe_sepsis
    returning pat_id, message->>'timestamp'
)
insert into notifications (pat_id, message)
    select c1.pat_id,
        json_build_object('alert_code', '201', 'read', false,'timestamp', 
               date_part('epoch', GREATEST(c.timestamp, coalesce(c1.override_time, c1.measurement_time), c2.onset_tsp) ) * 1000)
    from severe_sepsis_notifications c
        inner join criteria c1 on c.pat_id = c1.pat_id
        inner join (
            select pat_id, count(*), (array_agg(measurement_time order by measurement_time))[2]  onset_tsp from criteria 
                where name ~ 'blood_pressure|mean_arterial_pressure|decrease_in_sbp|lactate' 
                    and (is_met is true or override_user is not null) and pat_id = this_pat_id
                group by pat_id
            ) c2
            on c2.pat_id = c1.pat_id
    where c1.name = 'crystalloid_fluid' and (c1.is_met is true or c1.override_user is not null)
        and c2.count > 0 and pat_id = this_pat_id;


RETURN;


END;  
$$ LANGUAGE PLPGSQL;




