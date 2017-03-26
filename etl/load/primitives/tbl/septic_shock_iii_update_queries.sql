update cdm_twf set septic_shock_iii=value from  (select c1.enc_id, c1.tsp, 1 as value from (select enc_id, tsp, qsofa, worst_sofa, lactate, fluid_resuscitation, vasopressor_resuscitation, hypotension_intp, lactate_c, hypotension_intp_c from cdm_twf ) c1
join (select * from cdm_t where fid='suspicion_of_infection') c2
on c1.enc_id=c2.enc_id and c2.tsp <=c1.tsp and c2.tsp >= c1.tsp - interval '6 hours') c3
where cdm_twf.enc_id=c3.enc_id and cdm_twf.tsp=c3.tsp;

update cdm_twf set septic_shock_iii=value  from (select c1.enc_id, c1.tsp, 2 as value , c2.lactate_c|c2.hypotension_intp_c confidence from (select enc_id, tsp, qsofa, worst_sofa, lactate, fluid_resuscitation, vasopressor_resuscitation, hypotension_intp, lactate_c, hypotension_intp_c from cdm_twf ) c1
join (select enc_id, tsp, qsofa, worst_sofa, lactate, fluid_resuscitation, vasopressor_resuscitation, hypotension_intp, lactate_c, hypotension_intp_c from cdm_twf ) c2
on c1.enc_id=c2.enc_id and c2.tsp <=c1.tsp and c2.tsp >= c1.tsp - interval '6 hours'
where c2.qsofa >= 2 and c2.worst_sofa >= 2 and c2.lactate >2 and c2.lactate_c < 24 
and ((c1.hypotension_intp::integer=1 and c1.hypotension_intp_c < 24 and c1.fluid_resuscitation::integer =1) or c1.vasopressor_resuscitation::integer =1)) c3
where cdm_twf.enc_id=c3.enc_id and cdm_twf.tsp=c3.tsp and cdm_twf.septic_shock_iii =1;