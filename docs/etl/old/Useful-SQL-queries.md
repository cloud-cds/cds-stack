#Who has septic shock onset?
   ```{sql}
   select distinct enc_id from cdm_twf where septic_shock = 1;
   ```
#How many septic shock onset cases were dead?
   ```{sql}
   select count(*) from (
       select distinct enc_id from cdm_twf where septic_shock = 1
       INTERSECT
       select distinct enc_id from cdm_t where fid = 'discharge' and value::json->>'disposition' ~* 'expired')               
       as tab
   ```