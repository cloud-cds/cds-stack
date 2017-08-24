# EWS SERVER V8.2
The update for this version is to 

1. include all necessary features for the sepsis application
2. populate HC_EPIC data with v8.2 configuration
3. do first round test on sepsis machine learning application

## TODO List

* Include urine output
* Include fluid input
  + from flowsheet: select rows where FLO_MEAS_NAME = 'INTRAVENOUS INTAKE' or 'R PIGGYBACK IV VOLUME'
  + from MAR: select medication where dose > 500ml and exclude dialysis:
    
    select distinct "display_name" from "MedicationAdministration" where lower("MedUnit") = 'ml' and  cast("Dose" as float) > 500 and lower("display_name") not like '%dialys%'  order by "display_name";

* Define two modes -- `no-add` and `add` -- for importing data samples with the same encounter id and timestamp.
Measurement features, e.g., heart rate, creatinine, etc, use `no-add` mode, which means if two samples, e.g., heart rate, observed at the same time for the same patient, we use the first sample and discard the second one; Dosage features and intake/output features use `add` mode, i.e., when multiple samples observed at the same time for the same patient, we add all the values together.
* Modify `dose` and `status` features: set all 'status' features to be deprecated. modify sql queries for all `dose` feature and only import them with status as "given","given during downtime","given by other", and "new bag".
* Fillin None values for feature: cardio_sofa, renal_sofa, hepatic_sofa, minutes_since_antibiotics, minutes_since_organ_fail, sirs_intp, urine_output_24hr, urine_output_6hr, sufficient_fluid_replacement
  + for all sofa, intp, output, and sufficient_fluid_replacement features, initialize value to be zero
  + for all minutes features, initialize value to be NAN
