Data Quality Report
===================

Non-present features
```sql
select * from cdm_feature_present(_dataset_id)
where count = 0 order by id;
```

--------------------

### HCGH-1m (D4 and D6) `Updated on 2017-06-20`
```
                id                 | cdm_table | count
-----------------------------------+-----------+-------
 acebutolol_dose                   |           |     0
 acute_kidney_failure              |           |     0
 admittime                         |           |     0
 aids_diag                         |           |     0
 aids_hist                         |           |     0
 bronchiectasis_hist               |           |     0
 chest_xray                        |           |     0
 dialysis_diag                     |           |     0
 dialysis_hist                     |           |     0
 emphysema_prob                    |           |     0
 hemorrhage                        |           |     0
 hepatic_failure                   |           |     0
 hepatic_failure_inhosp            |           |     0
 hospital                          |           |     0
 hypersensitivity_pneumonitis_diag |           |     0
 hypersensitivity_pneumonitis_hist |           |     0
 hypersensitivity_pneumonitis_prob |           |     0
 immunodeficiency_diag             |           |     0
 immunodeficiency_prob             |           |     0
 isincardiacicu                    |           |     0
 levophed_dose                     |           |     0
 line_sepsis                       |           |     0
 milrinone_dose                    |           |     0
 rapamycin_dose                    |           |     0
 tiotropium_dose                   |           |     0
 tobramycin_dose                   |           |     0
 transfuse_platelets               |           |     0
 transfuse_rbc                     |           |     0
(28 rows)

```

### HCGH-1y (D1) `Updated on 2017-06-20`
```
                id                 | cdm_table | count
-----------------------------------+-----------+-------
 admittime                         |           |     0
 aids_diag                         |           |     0
 aids_hist                         |           |     0
 chest_xray                        |           |     0
 dialysis_diag                     |           |     0
 dialysis_hist                     |           |     0
 hemorrhage                        |           |     0
 hospital                          |           |     0
 hypersensitivity_pneumonitis_diag |           |     0
 hypersensitivity_pneumonitis_prob |           |     0
 isincardiacicu                    |           |     0
 levophed_dose                     |           |     0
 rapamycin_dose                    |           |     0
(13 rows)

```

### HCGH-3y (D3)
```
2017-06-20
                id                 | cdm_table | count
-----------------------------------+-----------+-------
 admittime                         |           |     0
 aids_diag                         |           |     0
 aids_hist                         |           |     0
 chest_xray                        |           |     0
 dialysis_diag                     |           |     0
 dialysis_hist                     |           |     0
 hemorrhage                        |           |     0
 hospital                          |           |     0
 hypersensitivity_pneumonitis_prob |           |     0
 isincardiacicu                    |           |     0
 levophed_dose                     |           |     0
 rapamycin_dose                    |           |     0
(12 rows)

```

### JHH-10d (D8)
```
2017-06-20
          id          | cdm_table | count
----------------------+-----------+-------
 acebutolol_dose      |           |     0
 aclidinium_dose      |           |     0
 acute_kidney_failure |           |     0
 admittime            |           |     0
 aids_diag            |           |     0
 aids_hist            |           |     0
 bipap                |           |     0
 bronchiectasis_prob  |           |     0
 chest_xray           |           |     0
 cpap                 |           |     0
 dialysis_diag        |           |     0
 dialysis_hist        |           |     0
 emphysema_prob       |           |     0
 enalaprilat_dose     |           |     0
 hemorrhage           |           |     0
 hospital             |           |     0
 isincardiacicu       |           |     0
 levophed_dose        |           |     0
 prednisolone_dose    |           |     0
 ramipril_dose        |           |     0
 rapamycin_dose       |           |     0
 rifampin_dose        |           |     0
 theophylline_dose    |           |     0
 tobramycin_dose      |           |     0
(24 rows)
```

### JHH-1m (D10)
```
2017-06-18
        id         | cdm_table | count
-------------------+-----------+-------
 acebutolol_dose   |           |     0
 aclidinium_dose   |           |     0
 admittime         |           |     0
 aids_diag         |           |     0
 aids_hist         |           |     0
 any_beta_blocker  |           |     0
 bipap             |           |     0
 bisoprolol_dose   |           |     0
 chest_xray        |           |     0
 cpap              |           |     0
 dialysis_diag     |           |     0
 dialysis_hist     |           |     0
 hemorrhage        |           |     0
 hospital          |           |     0
 isincardiacicu    |           |     0
 levophed_dose     |           |     0
 rapamycin_dose    |           |     0
 theophylline_dose |           |     0
(18 rows)
```

### BMC-10d (D9)
```
2017-06-20
          id          | cdm_table | count
----------------------+-----------+-------
 acebutolol_dose      |           |     0
 aclidinium_dose      |           |     0
 acute_kidney_failure |           |     0
 admittime            |           |     0
 aids_diag            |           |     0
 aids_hist            |           |     0
 bipap                |           |     0
 bisoprolol_dose      |           |     0
 bronchiectasis_prob  |           |     0
 chest_xray           |           |     0
 cpap                 |           |     0
 dialysis_diag        |           |     0
 dialysis_hist        |           |     0
 enalaprilat_dose     |           |     0
 hemorrhage           |           |     0
 hospital             |           |     0
 isincardiacicu       |           |     0
 levophed_dose        |           |     0
 prednisolone_dose    |           |     0
 ramipril_dose        |           |     0
 rapamycin_dose       |           |     0
 rifampin_dose        |           |     0
 theophylline_dose    |           |     0
 tobramycin_dose      |           |     0
(24 rows)
```


### OPS-DEV-1m (D2 between 2017-05-01 and 2017-06-01)
```
                id                 | cdm_table | count
-----------------------------------+-----------+-------
 pneumonia_sepsis_approx           |           |     0
 aids_hist                         |           |     0
 amiodarone_dose                   |           |     0
 intubation                        |           |     0
 salmeterol_dose                   |           |     0
 bisoprolol_dose                   |           |     0
 prednisolone_dose                 |           |     0
 hydrocortisone_dose               |           |     0
 betamethasone_dose                |           |     0
 dabigatran_dose                   |           |     0
 rivaroxaban_dose                  |           |     0
 line_sepsis                       |           |     0
 fludrocortisone_dose              |           |     0
 doxazosin_dose                    |           |     0
 levophed_injection_dose           |           |     0
 vasopressin_dose                  |           |     0
 bronchitis_inhosp                 |           |     0
 intra_abdominal_sepsis            |           |     0
 prednisone_dose                   |           |     0
 tiotropium_dose                   |           |     0
 acute_kidney_failure_inhosp       |           |     0
 ramipril_dose                     |           |     0
 lisinopril_dose                   |           |     0
 hypersensitivity_pneumonitis_hist |           |     0
 chronic_kidney_hist               |           |     0
 rapamycin_dose                    |           |     0
 heart_attack                      |           |     0
 stroke                            |           |     0
 pulmonary_emboli_inhosp           |           |     0
 tobramycin_dose                   |           |     0
 hepatic_failure_inhosp            |           |     0
 chronic_airway_obstruction_diag   |           |     0
 infections_angus_diag             |           |     0
 asthma_diag                       |           |     0
 etomidate_dose                    |           |     0
 verapamil_dose                    |           |     0
 propofol_dose                     |           |     0
 propranolol_dose                  |           |     0
 line_sepsis_approx                |           |     0
 any_inotrope                      |           |     0
 transfuse_cryoprecipitate         |           |     0
 pulmonary_emboli                  |           |     0
 hepatic_failure                   |           |     0
 transfuse_platelets               |           |     0
 severe_pancreatitis_inhosp        |           |     0
 ekg_proc                          |           |     0
 severe_pancreatitis               |           |     0
 any_antibiotics_order             |           |     0
 dialysis_hist                     |           |     0
 asthma_hist                       |           |     0
 ards_inhosp                       |           |     0
 diabetes_hist                     |           |     0
 asthma_prob                       |           |     0
 culture_order                     |           |     0
 suspicion_of_infection            |           |     0
 apixaban_dose                     |           |     0
 rifampin_dose                     |           |     0
 dexamethasone_dose                |           |     0
 biliary_sepsis                    |           |     0
 mri_proc                          |           |     0
 isincardiacicu                    |           |     0
 peritonitis_approx                |           |     0
 stroke_inhosp                     |           |     0
 atenolol_dose                     |           |     0
 dialysis                          |           |     0
 bronchiectasis_diag               |           |     0
 ct_proc                           |           |     0
 pancreatitis_chronic_hist         |           |     0
 nadolol_dose                      |           |     0
 phenytoin_dose                    |           |     0
 dialysis_diag                     |           |     0
 gi_bleed_inhosp                   |           |     0
 ards                              |           |     0
 immunodeficiency_hist             |           |     0
 penicillin_g_dose                 |           |     0
 enalaprilat_dose                  |           |     0
 enalapril_dose                    |           |     0
 levalbuterol_dose                 |           |     0
 any_anticoagulant                 |           |     0
 acute_kidney_failure              |           |     0
 metoprolol_dose                   |           |     0
 levophed_dose                     |           |     0
 diltiazem_dose                    |           |     0
 ards_approx                       |           |     0
 immunodeficiency_prob             |           |     0
 bronchitis                        |           |     0
 transfuse_plasma                  |           |     0
 emphysema_prob                    |           |     0
 heart_attack_inhosp               |           |     0
 any_beta_blocker                  |           |     0
 epogen_dose                       |           |     0
 hypersensitivity_pneumonitis_prob |           |     0
 any_pressor                       |           |     0
 hemorrhage_inhosp                 |           |     0
 hydralazine_dose                  |           |     0
 hemorrhage                        |           |     0
 chronic_airway_obstruction_hist   |           |     0
 chest_xray                        |           |     0
 acebutolol_dose                   |           |     0
 bacterial_culture                 |           |     0
 lorazepam_dose                    |           |     0
 albuterol_dose                    |           |     0
 aids_diag                         |           |     0
 uro_sepsis                        |           |     0
 sepsis_note                       |           |     0
 gi_bleed                          |           |     0
 discharge                         |           |     0
 catheter                          |           |     0
 midazolam_dose                    |           |     0
 cellulitis_approx                 |           |     0
 immuno_comp_hist                  |           |     0
 renal_insufficiency_hist          |           |     0
 infections_angus_hist             |           |     0
 pneumonia_sepsis                  |           |     0
 warfarin_dose                     |           |     0
 urosepsis_approx                  |           |     0
 biliary_sepsis_approx             |           |     0
 carvedilol_dose                   |           |     0
 lidocaine_dose                    |           |     0
 sotalol_dose                      |           |     0
 transfuse_rbc                     |           |     0
 pancreatitis_chronic_prob         |           |     0
 clonidine_dose                    |           |     0
 insulin_dose                      |           |     0
 emphysema_diag                    |           |     0
 organ_insufficiency_diag          |           |     0
 immunodeficiency_diag             |           |     0
 heart_arrhythmias_hist            |           |     0
 heparin_dose                      |           |     0
 theophylline_dose                 |           |     0
 furosemide_dose                   |           |     0
 uti_approx                        |           |     0
 any_antibiotics                   |           |     0
 bronchiectasis_hist               |           |     0
 pneumonia_approx                  |           |     0
 aclidinium_dose                   |           |     0
 hypersensitivity_pneumonitis_diag |           |     0
 bronchiectasis_prob               |           |     0
 methylprednisolone_dose           |           |     0
 diazepam_dose                     |           |     0
 any_glucocorticoid                |           |     0
(141 rows)
```
### JHH-1m
```
                  id                  |   cdm_table   | count
--------------------------------------+---------------+--------
 any_beta_blocker                     |               |      0
 dialysis_hist                        |               |      0
 rapamycin_dose                       |               |      0
 hemorrhage                           |               |      0
 hospital                             |               |      0
 aclidinium_dose                      |               |      0
 aids_hist                            |               |      0
 dialysis_diag                        |               |      0
 admittime                            |               |      0
 theophylline_dose                    |               |      0
 cpap                                 |               |      0
 bisoprolol_dose                      |               |      0
 acebutolol_dose                      |               |      0
 bipap                                |               |      0
 aids_diag                            |               |      0
 isincardiacicu                       |               |      0
 chest_xray                           |               |      0
 levophed_dose                        |               |      0

```
### BMC-1m
```
                  id                  |   cdm_table   | count
--------------------------------------+---------------+--------
 acebutolol_dose                      |               |      0
 bisoprolol_dose                      |               |      0
 bipap                                |               |      0
 nadolol_dose                         |               |      0
 aids_diag                            |               |      0
 levophed_dose                        |               |      0
 levalbuterol_dose                    |               |      0
 aids_hist                            |               |      0
 isincardiacicu                       |               |      0
 ramipril_dose                        |               |      0
 rapamycin_dose                       |               |      0
 immunodeficiency_prob                |               |      0
 hypersensitivity_pneumonitis_prob    |               |      0
 hospital                             |               |      0
 chest_xray                           |               |      0
 hepatic_failure_inhosp               |               |      0
 hepatic_failure                      |               |      0
 hemorrhage                           |               |      0
 admittime                            |               |      0
 ards_inhosp                          |               |      0
 ards                                 |               |      0
 acute_kidney_failure                 |               |      0
 any_beta_blocker                     |               |      0
 enalaprilat_dose                     |               |      0
 vent                                 |               |      0
 dialysis_hist                        |               |      0
 dialysis_diag                        |               |      0
 cpap                                 |               |      0
 aclidinium_dose                      |               |      0

```
### Daily (D7 between 2017-06-14 and 2017-06-21)
```
-----------------------------------+-----------+-------
 acebutolol_dose                   |           |     0
 acute_kidney_failure              |           |     0
 admittime                         |           |     0
 bipap                             |           |     0
 blood_culture_order               |           |     0
 chest_xray                        |           |     0
 cpap                              |           |     0
 hemorrhage                        |           |     0
 hepatic_failure                   |           |     0
 hepatic_failure_inhosp            |           |     0
 hospital                          |           |     0
 hypersensitivity_pneumonitis_diag |           |     0
 isincardiacicu                    |           |     0
 lactate_order                     |           |     0
 levophed_dose                     |           |     0
 rapamycin_dose                    |           |     0
(16 rows)
```

### BMC-1y (D13)
```
        id         | cdm_table | count
-------------------+-----------+-------
 acebutolol_dose   |           |     0
 admittime         |           |     0
 aids_diag         |           |     0
 aids_hist         |           |     0
 chest_xray        |           |     0
 dialysis_diag     |           |     0
 dialysis_hist     |           |     0
 hemorrhage        |           |     0
 hospital          |           |     0
 isincardiacicu    |           |     0
 levalbuterol_dose |           |     0
 levophed_dose     |           |     0
 rapamycin_dose    |           |     0
 vent              |           |     0
(14 rows)

```

Dataset Difference
------------------
`Updated on 2017-06-08`
### Top 10 `mean_diff_ratio`
Use this query to see details:
```sql
-- diff only
select id, cdm_table,
             jsonb_pretty(diff) diff
      from cdm_feature_diff(left_dataset, right_dataset)
      where (diff->>'mean_diff_ratio')::numeric > 0.01 order by (diff->>'mean_diff_ratio')::numeric desc limit 10;
-- detailed
select id, cdm_table,
             jsonb_pretty(diff) diff,
             jsonb_pretty(left_stats) left_stats,
             jsonb_pretty(right_stats) right_stats
      from cdm_feature_diff(left_dataset, right_dataset)
      where (diff->>'mean_diff_ratio')::numeric > 0.01 order by (diff->>'mean_diff_ratio')::numeric desc limit 10;
```
#### HCGH-1y (D1) vs. HCGH-1m (D4)
```
2017-06-20
          id           |   cdm_table   |               diff
-----------------------+---------------+----------------------------------
 vasopressors_dose     | criteria_meas | {                               +
                       |               |     "5%_diff_ratio": 0.046,     +
                       |               |     "25%_diff_ratio": 0.100,    +
                       |               |     "50%_diff_ratio": 0.100,    +
                       |               |     "75%_diff_ratio": 0.600,    +
                       |               |     "95%_diff_ratio": 1.900,    +
                       |               |     "max_diff_ratio": 39908.894,+
                       |               |     "min_diff_ratio": 0.000,    +
                       |               |     "mean_diff_ratio": 7.038    +
                       |               | }
 vasopressors_dose     | cdm_t         | {                               +
                       |               |     "5%_diff_ratio": 0.046,     +
                       |               |     "25%_diff_ratio": 0.100,    +
                       |               |     "50%_diff_ratio": 0.100,    +
                       |               |     "75%_diff_ratio": 0.600,    +
                       |               |     "95%_diff_ratio": 1.900,    +
                       |               |     "max_diff_ratio": 39908.894,+
                       |               |     "min_diff_ratio": 0.000,    +
                       |               |     "mean_diff_ratio": 7.038    +
                       |               | }
 acute_pancreatitis    | cdm_twf       | {                               +
                       |               |     "5%_diff_ratio": 0.000,     +
                       |               |     "25%_diff_ratio": 0.000,    +
                       |               |     "50%_diff_ratio": 0.000,    +
                       |               |     "75%_diff_ratio": 0.000,    +
                       |               |     "95%_diff_ratio": 0.000,    +
                       |               |     "max_diff_ratio": 0.000,    +
                       |               |     "min_diff_ratio": 0.000,    +
                       |               |     "mean_diff_ratio": 3.231    +
                       |               | }
 acute_liver_failure   | cdm_twf       | {                               +
                       |               |     "5%_diff_ratio": 0.000,     +
                       |               |     "25%_diff_ratio": 0.000,    +
                       |               |     "50%_diff_ratio": 0.000,    +
                       |               |     "75%_diff_ratio": 0.000,    +
                       |               |     "95%_diff_ratio": 0.000,    +
                       |               |     "max_diff_ratio": 0.000,    +
                       |               |     "min_diff_ratio": 0.000,    +
                       |               |     "mean_diff_ratio": 0.890    +
                       |               | }
 alt_liver_enzymes     | cdm_t         | {                               +
                       |               |     "5%_diff_ratio": 0.008,     +
                       |               |     "25%_diff_ratio": 0.016,    +
                       |               |     "50%_diff_ratio": 0.080,    +
                       |               |     "75%_diff_ratio": 1.051,    +
                       |               |     "95%_diff_ratio": 1.910,    +
                       |               |     "max_diff_ratio": 33.428,   +
                       |               |     "min_diff_ratio": 0.000,    +
                       |               |     "mean_diff_ratio": 0.732    +
                       |               | }
 amylase               | cdm_t         | {                               +
                       |               |     "5%_diff_ratio": 0.060,     +
                       |               |     "25%_diff_ratio": 0.077,    +
                       |               |     "50%_diff_ratio": 0.052,    +
                       |               |     "75%_diff_ratio": 0.094,    +
                       |               |     "95%_diff_ratio": 2.757,    +
                       |               |     "max_diff_ratio": 48.177,   +
                       |               |     "min_diff_ratio": 0.043,    +
                       |               |     "mean_diff_ratio": 0.439    +
                       |               | }
 cms_antibiotics_order | cdm_t         | {                               +
                       |               |     "5%_diff_ratio": 0.000,     +
                       |               |     "25%_diff_ratio": 0.001,    +
                       |               |     "50%_diff_ratio": 1.831,    +
                       |               |     "75%_diff_ratio": 0.476,    +
                       |               |     "95%_diff_ratio": 0.000,    +
                       |               |     "max_diff_ratio": 4.755,    +
                       |               |     "min_diff_ratio": 0.000,    +
                       |               |     "mean_diff_ratio": 0.367    +
                       |               | }
 cms_antibiotics_order | criteria_meas | {                               +
                       |               |     "5%_diff_ratio": 0.000,     +
                       |               |     "25%_diff_ratio": 0.001,    +
                       |               |     "50%_diff_ratio": 1.831,    +
                       |               |     "75%_diff_ratio": 0.476,    +
                       |               |     "95%_diff_ratio": 0.000,    +
                       |               |     "max_diff_ratio": 4.755,    +
                       |               |     "min_diff_ratio": 0.000,    +
                       |               |     "mean_diff_ratio": 0.367    +
                       |               | }
 hemoglobin_change     | cdm_twf       | {                               +
                       |               |     "5%_diff_ratio": 1.261,     +
                       |               |     "25%_diff_ratio": 0.000,    +
                       |               |     "50%_diff_ratio": 0.000,    +
                       |               |     "75%_diff_ratio": 0.000,    +
                       |               |     "95%_diff_ratio": 2.460,    +
                       |               |     "max_diff_ratio": 38.355,   +
                       |               |     "min_diff_ratio": 19.178,   +
                       |               |     "mean_diff_ratio": 0.319    +
                       |               | }
 ddimer                | cdm_t         | {                               +
                       |               |     "5%_diff_ratio": 0.000,     +
                       |               |     "25%_diff_ratio": 0.029,    +
                       |               |     "50%_diff_ratio": 0.058,    +
                       |               |     "75%_diff_ratio": 0.165,    +
                       |               |     "95%_diff_ratio": 0.311,    +
                       |               |     "max_diff_ratio": 20.019,   +
                       |               |     "min_diff_ratio": 0.000,    +
                       |               |     "mean_diff_ratio": 0.295    +
                       |               | }
(10 rows)
```
#### HCGH-3y (D3) vs. HCGH-1y (D1)
```
2017-06-20
          id          |   cdm_table   |              diff
----------------------+---------------+--------------------------------
 rass                 | cdm_t         | {                             +
                      |               |     "5%_diff_ratio": 4.251,   +
                      |               |     "25%_diff_ratio": 4.251,  +
                      |               |     "50%_diff_ratio": 0.000,  +
                      |               |     "75%_diff_ratio": 0.000,  +
                      |               |     "95%_diff_ratio": 0.000,  +
                      |               |     "max_diff_ratio": 0.000,  +
                      |               |     "min_diff_ratio": 0.000,  +
                      |               |     "mean_diff_ratio": 0.726  +
                      |               | }
 alt_liver_enzymes    | cdm_t         | {                             +
                      |               |     "5%_diff_ratio": 0.009,   +
                      |               |     "25%_diff_ratio": 0.363,  +
                      |               |     "50%_diff_ratio": 0.629,  +
                      |               |     "75%_diff_ratio": 0.368,  +
                      |               |     "95%_diff_ratio": 1.378,  +
                      |               |     "max_diff_ratio": 23.201, +
                      |               |     "min_diff_ratio": 0.000,  +
                      |               |     "mean_diff_ratio": 0.508  +
                      |               | }
 acute_pancreatitis   | cdm_twf       | {                             +
                      |               |     "5%_diff_ratio": 0.000,   +
                      |               |     "25%_diff_ratio": 0.000,  +
                      |               |     "50%_diff_ratio": 0.000,  +
                      |               |     "75%_diff_ratio": 0.000,  +
                      |               |     "95%_diff_ratio": 0.000,  +
                      |               |     "max_diff_ratio": 0.000,  +
                      |               |     "min_diff_ratio": 0.000,  +
                      |               |     "mean_diff_ratio": 0.435  +
                      |               | }
 urine_output_6hr     | cdm_twf       | {                             +
                      |               |     "5%_diff_ratio": 0.000,   +
                      |               |     "25%_diff_ratio": 0.000,  +
                      |               |     "50%_diff_ratio": 0.000,  +
                      |               |     "75%_diff_ratio": 1.109,  +
                      |               |     "95%_diff_ratio": 1.109,  +
                      |               |     "max_diff_ratio": 240.235,+
                      |               |     "min_diff_ratio": 0.000,  +
                      |               |     "mean_diff_ratio": 0.282  +
                      |               | }
 urine_output_24hr    | cdm_twf       | {                             +
                      |               |     "5%_diff_ratio": 0.000,   +
                      |               |     "25%_diff_ratio": 0.000,  +
                      |               |     "50%_diff_ratio": 0.000,  +
                      |               |     "75%_diff_ratio": 0.692,  +
                      |               |     "95%_diff_ratio": 0.923,  +
                      |               |     "max_diff_ratio": 68.695, +
                      |               |     "min_diff_ratio": 0.000,  +
                      |               |     "mean_diff_ratio": 0.271  +
                      |               | }
 obstructive_pe_shock | cdm_twf       | {                             +
                      |               |     "5%_diff_ratio": 0.000,   +
                      |               |     "25%_diff_ratio": 0.000,  +
                      |               |     "50%_diff_ratio": 0.000,  +
                      |               |     "75%_diff_ratio": 0.000,  +
                      |               |     "95%_diff_ratio": 0.000,  +
                      |               |     "max_diff_ratio": 0.000,  +
                      |               |     "min_diff_ratio": 0.000,  +
                      |               |     "mean_diff_ratio": 0.206  +
                      |               | }
 renal_sofa           | cdm_twf       | {                             +
                      |               |     "5%_diff_ratio": 0.000,   +
                      |               |     "25%_diff_ratio": 0.000,  +
                      |               |     "50%_diff_ratio": 0.000,  +
                      |               |     "75%_diff_ratio": 0.649,  +
                      |               |     "95%_diff_ratio": 0.000,  +
                      |               |     "max_diff_ratio": 0.000,  +
                      |               |     "min_diff_ratio": 0.000,  +
                      |               |     "mean_diff_ratio": 0.162  +
                      |               | }
 crystalloid_fluid    | criteria_meas | {                             +
                      |               |     "5%_diff_ratio": 0.000,   +
                      |               |     "25%_diff_ratio": 0.000,  +
                      |               |     "50%_diff_ratio": 0.000,  +
                      |               |     "75%_diff_ratio": 0.002,  +
                      |               |     "95%_diff_ratio": 0.000,  +
                      |               |     "max_diff_ratio": 35.230, +
                      |               |     "min_diff_ratio": 0.000,  +
                      |               |     "mean_diff_ratio": 0.151  +
                      |               | }
 crystalloid_fluid    | cdm_t         | {                             +
                      |               |     "5%_diff_ratio": 0.000,   +
                      |               |     "25%_diff_ratio": 0.000,  +
                      |               |     "50%_diff_ratio": 0.000,  +
                      |               |     "75%_diff_ratio": 0.002,  +
                      |               |     "95%_diff_ratio": 0.000,  +
                      |               |     "max_diff_ratio": 35.230, +
                      |               |     "min_diff_ratio": 0.000,  +
                      |               |     "mean_diff_ratio": 0.151  +
                      |               | }
 neurologic_sofa      | cdm_twf       | {                             +
                      |               |     "5%_diff_ratio": 0.000,   +
                      |               |     "25%_diff_ratio": 0.993,  +
                      |               |     "50%_diff_ratio": 0.000,  +
                      |               |     "75%_diff_ratio": 0.000,  +
                      |               |     "95%_diff_ratio": 0.000,  +
                      |               |     "max_diff_ratio": 0.000,  +
                      |               |     "min_diff_ratio": 0.000,  +
                      |               |     "mean_diff_ratio": 0.151  +
                      |               | }
(10 rows)
```

#### DEV (D2 between 2017-05-01 and 2017-06-01) vs. HCGH-1m (D4)
```
            id            |   cdm_table   |              diff
--------------------------+---------------+--------------------------------
 penicillin_dose          | cdm_t         | {                             +
                          |               |     "25%_diff_ratio": 0.000,  +
                          |               |     "50%_diff_ratio": 0.000,  +
                          |               |     "75%_diff_ratio": 66.846, +
                          |               |     "max_diff_ratio": 133.692,+
                          |               |     "min_diff_ratio": 0.218,  +
                          |               |     "mean_diff_ratio": 33.822 +
                          |               | }
 dobutamine_dose          | cdm_t         | {                             +
                          |               |     "25%_diff_ratio": 0.000,  +
                          |               |     "50%_diff_ratio": 1.842,  +
                          |               |     "75%_diff_ratio": 1.974,  +
                          |               |     "max_diff_ratio": 2.632,  +
                          |               |     "min_diff_ratio": 0.132,  +
                          |               |     "mean_diff_ratio": 1.156  +
                          |               | }
 bilirubin                | criteria_meas | {                             +
                          |               |     "25%_diff_ratio": 0.000,  +
                          |               |     "50%_diff_ratio": 0.155,  +
                          |               |     "75%_diff_ratio": 0.155,  +
                          |               |     "max_diff_ratio": 12.112, +
                          |               |     "min_diff_ratio": 0.000,  +
                          |               |     "mean_diff_ratio": 1.019  +
                          |               | }
 oxacillin_dose           | cdm_t         | {                             +
                          |               |     "25%_diff_ratio": 0.715,  +
                          |               |     "50%_diff_ratio": 0.715,  +
                          |               |     "75%_diff_ratio": 1.432,  +
                          |               |     "max_diff_ratio": 1.432,  +
                          |               |     "min_diff_ratio": 0.715,  +
                          |               |     "mean_diff_ratio": 0.999  +
                          |               | }
 ceftazidime_dose         | cdm_t         | {                             +
                          |               |     "25%_diff_ratio": 0.705,  +
                          |               |     "50%_diff_ratio": 0.704,  +
                          |               |     "75%_diff_ratio": 1.410,  +
                          |               |     "max_diff_ratio": 1.410,  +
                          |               |     "min_diff_ratio": 0.705,  +
                          |               |     "mean_diff_ratio": 0.999  +
                          |               | }
 epinephrine_dose         | cdm_t         | {                             +
                          |               |     "25%_diff_ratio": 0.323,  +
                          |               |     "50%_diff_ratio": 1.076,  +
                          |               |     "75%_diff_ratio": 1.073,  +
                          |               |     "max_diff_ratio": 16.133, +
                          |               |     "min_diff_ratio": 0.001,  +
                          |               |     "mean_diff_ratio": 0.997  +
                          |               | }
 cefazolin_dose           | cdm_t         | {                             +
                          |               |     "25%_diff_ratio": 1.081,  +
                          |               |     "50%_diff_ratio": 1.081,  +
                          |               |     "75%_diff_ratio": 1.081,  +
                          |               |     "max_diff_ratio": 2.161,  +
                          |               |     "min_diff_ratio": 0.000,  +
                          |               |     "mean_diff_ratio": 0.996  +
                          |               | }
 piperacillin_tazbac_dose | cdm_t         | {                             +
                          |               |     "25%_diff_ratio": 1.022,  +
                          |               |     "50%_diff_ratio": 1.022,  +
                          |               |     "75%_diff_ratio": 1.022,  +
                          |               |     "max_diff_ratio": 0.454,  +
                          |               |     "min_diff_ratio": 0.273,  +
                          |               |     "mean_diff_ratio": 0.991  +
                          |               | }
 ampicillin_dose          | cdm_t         | {                             +
                          |               |     "25%_diff_ratio": 0.680,  +
                          |               |     "50%_diff_ratio": 1.360,  +
                          |               |     "75%_diff_ratio": 1.360,  +
                          |               |     "max_diff_ratio": 0.000,  +
                          |               |     "min_diff_ratio": 0.000,  +
                          |               |     "mean_diff_ratio": 0.962  +
                          |               | }
 ceftriaxone_dose         | cdm_t         | {                             +
                          |               |     "25%_diff_ratio": 1.022,  +
                          |               |     "50%_diff_ratio": 1.022,  +
                          |               |     "75%_diff_ratio": 1.022,  +
                          |               |     "max_diff_ratio": 0.000,  +
                          |               |     "min_diff_ratio": 0.000,  +
                          |               |     "mean_diff_ratio": 0.942  +
                          |               | }
(10 rows)

```

#### JHH-10d (D8) vs. HCGH-1m (D4)
```
2017-06-16
         id          |   cdm_table   |              diff
---------------------+---------------+--------------------------------
 acute_pancreatitis  | cdm_twf       | {                             +
                     |               |     "5%_diff_ratio": 0.000,   +
                     |               |     "25%_diff_ratio": 0.000,  +
                     |               |     "50%_diff_ratio": 0.000,  +
                     |               |     "75%_diff_ratio": 0.000,  +
                     |               |     "95%_diff_ratio": 0.000,  +
                     |               |     "max_diff_ratio": 0.000,  +
                     |               |     "min_diff_ratio": 0.000,  +
                     |               |     "mean_diff_ratio": 36.162 +
                     |               | }
 alt_liver_enzymes   | cdm_t         | {                             +
                     |               |     "5%_diff_ratio": 0.048,   +
                     |               |     "25%_diff_ratio": 0.645,  +
                     |               |     "50%_diff_ratio": 1.250,  +
                     |               |     "75%_diff_ratio": 2.324,  +
                     |               |     "95%_diff_ratio": 6.447,  +
                     |               |     "max_diff_ratio": 168.411,+
                     |               |     "min_diff_ratio": 0.016,  +
                     |               |     "mean_diff_ratio": 2.859  +
                     |               | }
 acute_liver_failure | cdm_twf       | {                             +
                     |               |     "5%_diff_ratio": 0.000,   +
                     |               |     "25%_diff_ratio": 0.000,  +
                     |               |     "50%_diff_ratio": 0.000,  +
                     |               |     "75%_diff_ratio": 0.000,  +
                     |               |     "95%_diff_ratio": 0.000,  +
                     |               |     "max_diff_ratio": 0.000,  +
                     |               |     "min_diff_ratio": 0.000,  +
                     |               |     "mean_diff_ratio": 2.783  +
                     |               | }
 ddimer              | cdm_t         | {                             +
                     |               |     "5%_diff_ratio": 0.039,   +
                     |               |     "25%_diff_ratio": 0.248,  +
                     |               |     "50%_diff_ratio": 1.118,  +
                     |               |     "75%_diff_ratio": 2.143,  +
                     |               |     "95%_diff_ratio": 2.969,  +
                     |               |     "max_diff_ratio": 1.701,  +
                     |               |     "min_diff_ratio": 0.073,  +
                     |               |     "mean_diff_ratio": 1.140  +
                     |               | }
 hepatic_sofa        | cdm_twf       | {                             +
                     |               |     "5%_diff_ratio": 0.000,   +
                     |               |     "25%_diff_ratio": 0.000,  +
                     |               |     "50%_diff_ratio": 0.000,  +
                     |               |     "75%_diff_ratio": 0.000,  +
                     |               |     "95%_diff_ratio": 11.319, +
                     |               |     "max_diff_ratio": 0.000,  +
                     |               |     "min_diff_ratio": 0.000,  +
                     |               |     "mean_diff_ratio": 1.125  +
                     |               | }
 hematologic_sofa    | cdm_twf       | {                             +
                     |               |     "5%_diff_ratio": 0.000,   +
                     |               |     "25%_diff_ratio": 0.000,  +
                     |               |     "50%_diff_ratio": 0.000,  +
                     |               |     "75%_diff_ratio": 0.000,  +
                     |               |     "95%_diff_ratio": 5.562,  +
                     |               |     "max_diff_ratio": 0.000,  +
                     |               |     "min_diff_ratio": 0.000,  +
                     |               |     "mean_diff_ratio": 0.984  +
                     |               | }
 septic_shock        | cdm_twf       | {                             +
                     |               |     "5%_diff_ratio": 0.000,   +
                     |               |     "25%_diff_ratio": 0.000,  +
                     |               |     "50%_diff_ratio": 0.000,  +
                     |               |     "75%_diff_ratio": 0.000,  +
                     |               |     "95%_diff_ratio": 0.000,  +
                     |               |     "max_diff_ratio": 0.000,  +
                     |               |     "min_diff_ratio": 0.000,  +
                     |               |     "mean_diff_ratio": 0.885  +
                     |               | }
 vasopressors_dose   | cdm_t         | {                             +
                     |               |     "5%_diff_ratio": 0.003,   +
                     |               |     "25%_diff_ratio": 0.096,  +
                     |               |     "50%_diff_ratio": 0.488,  +
                     |               |     "75%_diff_ratio": 0.947,  +
                     |               |     "95%_diff_ratio": 2.100,  +
                     |               |     "max_diff_ratio": 7.202,  +
                     |               |     "min_diff_ratio": 0.000,  +
                     |               |     "mean_diff_ratio": 0.853  +
                     |               | }
 vasopressors_dose   | criteria_meas | {                             +
                     |               |     "5%_diff_ratio": 0.003,   +
                     |               |     "25%_diff_ratio": 0.096,  +
                     |               |     "50%_diff_ratio": 0.488,  +
                     |               |     "75%_diff_ratio": 0.947,  +
                     |               |     "95%_diff_ratio": 2.100,  +
                     |               |     "max_diff_ratio": 7.202,  +
                     |               |     "min_diff_ratio": 0.000,  +
                     |               |     "mean_diff_ratio": 0.853  +
                     |               | }
 urine_output_24hr   | cdm_twf       | {                             +
                     |               |     "5%_diff_ratio": 0.000,   +
                     |               |     "25%_diff_ratio": 0.000,  +
                     |               |     "50%_diff_ratio": 0.617,  +
                     |               |     "75%_diff_ratio": 1.634,  +
                     |               |     "95%_diff_ratio": 2.090,  +
                     |               |     "max_diff_ratio": 6.269,  +
                     |               |     "min_diff_ratio": 7.362,  +
                     |               |     "mean_diff_ratio": 0.822  +
                     |               | }
(10 rows)

```

#### BMC-10d (D9) vs. HCGH-1m (D4)
```
2017-06-20
          id           |   cdm_table   |             diff
-----------------------+---------------+-------------------------------
 lipase                | cdm_t         | {                            +
                       |               |     "5%_diff_ratio": 0.398,  +
                       |               |     "25%_diff_ratio": 0.746, +
                       |               |     "50%_diff_ratio": 1.035, +
                       |               |     "75%_diff_ratio": 1.661, +
                       |               |     "95%_diff_ratio": 2.089, +
                       |               |     "max_diff_ratio": 42.524,+
                       |               |     "min_diff_ratio": 0.269, +
                       |               |     "mean_diff_ratio": 1.045 +
                       |               | }
 septic_shock          | cdm_twf       | {                            +
                       |               |     "5%_diff_ratio": 0.000,  +
                       |               |     "25%_diff_ratio": 0.000, +
                       |               |     "50%_diff_ratio": 0.000, +
                       |               |     "75%_diff_ratio": 0.000, +
                       |               |     "95%_diff_ratio": 0.000, +
                       |               |     "max_diff_ratio": 0.000, +
                       |               |     "min_diff_ratio": 0.000, +
                       |               |     "mean_diff_ratio": 0.933 +
                       |               | }
 vasopressors_dose     | criteria_meas | {                            +
                       |               |     "5%_diff_ratio": 0.001,  +
                       |               |     "25%_diff_ratio": 0.095, +
                       |               |     "50%_diff_ratio": 0.490, +
                       |               |     "75%_diff_ratio": 0.975, +
                       |               |     "95%_diff_ratio": 2.501, +
                       |               |     "max_diff_ratio": 95.021,+
                       |               |     "min_diff_ratio": 0.000, +
                       |               |     "mean_diff_ratio": 0.921 +
                       |               | }
 vasopressors_dose     | cdm_t         | {                            +
                       |               |     "5%_diff_ratio": 0.001,  +
                       |               |     "25%_diff_ratio": 0.095, +
                       |               |     "50%_diff_ratio": 0.490, +
                       |               |     "75%_diff_ratio": 0.975, +
                       |               |     "95%_diff_ratio": 2.501, +
                       |               |     "max_diff_ratio": 95.021,+
                       |               |     "min_diff_ratio": 0.000, +
                       |               |     "mean_diff_ratio": 0.921 +
                       |               | }
 cms_antibiotics_order | criteria_meas | {                            +
                       |               |     "5%_diff_ratio": 0.000,  +
                       |               |     "25%_diff_ratio": 0.593, +
                       |               |     "50%_diff_ratio": 2.306, +
                       |               |     "75%_diff_ratio": 0.476, +
                       |               |     "95%_diff_ratio": 2.378, +
                       |               |     "max_diff_ratio": 4.755, +
                       |               |     "min_diff_ratio": 0.000, +
                       |               |     "mean_diff_ratio": 0.797 +
                       |               | }
 cms_antibiotics_order | cdm_t         | {                            +
                       |               |     "5%_diff_ratio": 0.000,  +
                       |               |     "25%_diff_ratio": 0.593, +
                       |               |     "50%_diff_ratio": 2.306, +
                       |               |     "75%_diff_ratio": 0.476, +
                       |               |     "95%_diff_ratio": 2.378, +
                       |               |     "max_diff_ratio": 4.755, +
                       |               |     "min_diff_ratio": 0.000, +
                       |               |     "mean_diff_ratio": 0.797 +
                       |               | }
 urine_output_24hr     | cdm_twf       | {                            +
                       |               |     "5%_diff_ratio": 0.000,  +
                       |               |     "25%_diff_ratio": 0.000, +
                       |               |     "50%_diff_ratio": 0.380, +
                       |               |     "75%_diff_ratio": 1.330, +
                       |               |     "95%_diff_ratio": 0.817, +
                       |               |     "max_diff_ratio": 14.711,+
                       |               |     "min_diff_ratio": 10.067,+
                       |               |     "mean_diff_ratio": 0.541 +
                       |               | }
 cms_antibiotics       | criteria_meas | {                            +
                       |               |     "5%_diff_ratio": 0.000,  +
                       |               |     "25%_diff_ratio": 0.000, +
                       |               |     "50%_diff_ratio": 0.000, +
                       |               |     "75%_diff_ratio": 0.000, +
                       |               |     "95%_diff_ratio": 2.124, +
                       |               |     "max_diff_ratio": 4.248, +
                       |               |     "min_diff_ratio": 0.000, +
                       |               |     "mean_diff_ratio": 0.503 +
                       |               | }
 cms_antibiotics       | cdm_t         | {                            +
                       |               |     "5%_diff_ratio": 0.000,  +
                       |               |     "25%_diff_ratio": 0.000, +
                       |               |     "50%_diff_ratio": 0.000, +
                       |               |     "75%_diff_ratio": 0.000, +
                       |               |     "95%_diff_ratio": 2.124, +
                       |               |     "max_diff_ratio": 4.248, +
                       |               |     "min_diff_ratio": 0.000, +
                       |               |     "mean_diff_ratio": 0.503 +
                       |               | }
 metabolic_acidosis    | cdm_twf       | {                            +
                       |               |     "5%_diff_ratio": 0.000,  +
                       |               |     "25%_diff_ratio": 0.000, +
                       |               |     "50%_diff_ratio": 0.000, +
                       |               |     "75%_diff_ratio": 0.000, +
                       |               |     "95%_diff_ratio": 0.000, +
                       |               |     "max_diff_ratio": 0.000, +
                       |               |     "min_diff_ratio": 0.000, +
                       |               |     "mean_diff_ratio": 0.500 +
                       |               | }
(10 rows)
```
#### JHH-1m (D10) vs. HCGH-1m (D4)
```
2017-06-18
         id          |   cdm_table   |             diff
---------------------+---------------+-------------------------------
 acute_pancreatitis  | cdm_twf       | {                            +
                     |               |     "5%_diff_ratio": 0.000,  +
                     |               |     "25%_diff_ratio": 0.000, +
                     |               |     "50%_diff_ratio": 0.000, +
                     |               |     "75%_diff_ratio": 0.000, +
                     |               |     "95%_diff_ratio": 0.000, +
                     |               |     "max_diff_ratio": 0.000, +
                     |               |     "min_diff_ratio": 0.000, +
                     |               |     "mean_diff_ratio": 33.648+
                     |               | }
 alt_liver_enzymes   | cdm_t         | {                            +
                     |               |     "5%_diff_ratio": 0.048,  +
                     |               |     "25%_diff_ratio": 0.724, +
                     |               |     "50%_diff_ratio": 1.329, +
                     |               |     "75%_diff_ratio": 2.244, +
                     |               |     "95%_diff_ratio": 6.765, +
                     |               |     "max_diff_ratio": 97.497,+
                     |               |     "min_diff_ratio": 0.008, +
                     |               |     "mean_diff_ratio": 2.622 +
                     |               | }
 acute_liver_failure | cdm_twf       | {                            +
                     |               |     "5%_diff_ratio": 0.000,  +
                     |               |     "25%_diff_ratio": 0.000, +
                     |               |     "50%_diff_ratio": 0.000, +
                     |               |     "75%_diff_ratio": 0.000, +
                     |               |     "95%_diff_ratio": 0.000, +
                     |               |     "max_diff_ratio": 0.000, +
                     |               |     "min_diff_ratio": 0.000, +
                     |               |     "mean_diff_ratio": 1.087 +
                     |               | }
 vasopressors_dose   | criteria_meas | {                            +
                     |               |     "5%_diff_ratio": 0.003,  +
                     |               |     "25%_diff_ratio": 0.096, +
                     |               |     "50%_diff_ratio": 0.487, +
                     |               |     "75%_diff_ratio": 0.920, +
                     |               |     "95%_diff_ratio": 2.100, +
                     |               |     "max_diff_ratio": 7.202, +
                     |               |     "min_diff_ratio": 0.000, +
                     |               |     "mean_diff_ratio": 0.862 +
                     |               | }
 vasopressors_dose   | cdm_t         | {                            +
                     |               |     "5%_diff_ratio": 0.003,  +
                     |               |     "25%_diff_ratio": 0.096, +
                     |               |     "50%_diff_ratio": 0.487, +
                     |               |     "75%_diff_ratio": 0.920, +
                     |               |     "95%_diff_ratio": 2.100, +
                     |               |     "max_diff_ratio": 7.202, +
                     |               |     "min_diff_ratio": 0.000, +
                     |               |     "mean_diff_ratio": 0.862 +
                     |               | }
 septic_shock        | cdm_twf       | {                            +
                     |               |     "5%_diff_ratio": 0.000,  +
                     |               |     "25%_diff_ratio": 0.000, +
                     |               |     "50%_diff_ratio": 0.000, +
                     |               |     "75%_diff_ratio": 0.000, +
                     |               |     "95%_diff_ratio": 0.000, +
                     |               |     "max_diff_ratio": 0.000, +
                     |               |     "min_diff_ratio": 0.000, +
                     |               |     "mean_diff_ratio": 0.830 +
                     |               | }
 ptt                 | criteria_meas | {                            +
                     |               |     "5%_diff_ratio": 0.531,  +
                     |               |     "25%_diff_ratio": 0.606, +
                     |               |     "50%_diff_ratio": 0.716, +
                     |               |     "75%_diff_ratio": 1.158, +
                     |               |     "95%_diff_ratio": 1.055, +
                     |               |     "max_diff_ratio": 0.000, +
                     |               |     "min_diff_ratio": 0.161, +
                     |               |     "mean_diff_ratio": 0.813 +
                     |               | }
 ptt                 | cdm_t         | {                            +
                     |               |     "5%_diff_ratio": 0.531,  +
                     |               |     "25%_diff_ratio": 0.606, +
                     |               |     "50%_diff_ratio": 0.716, +
                     |               |     "75%_diff_ratio": 1.158, +
                     |               |     "95%_diff_ratio": 1.055, +
                     |               |     "max_diff_ratio": 0.000, +
                     |               |     "min_diff_ratio": 0.161, +
                     |               |     "mean_diff_ratio": 0.813 +
                     |               | }
 ddimer              | cdm_t         | {                            +
                     |               |     "5%_diff_ratio": 0.058,  +
                     |               |     "25%_diff_ratio": 0.102, +
                     |               |     "50%_diff_ratio": 0.418, +
                     |               |     "75%_diff_ratio": 1.778, +
                     |               |     "95%_diff_ratio": 3.154, +
                     |               |     "max_diff_ratio": 0.039, +
                     |               |     "min_diff_ratio": 0.102, +
                     |               |     "mean_diff_ratio": 0.785 +
                     |               | }
 hematologic_sofa    | cdm_twf       | {                            +
                     |               |     "5%_diff_ratio": 0.000,  +
                     |               |     "25%_diff_ratio": 0.000, +
                     |               |     "50%_diff_ratio": 0.000, +
                     |               |     "75%_diff_ratio": 0.000, +
                     |               |     "95%_diff_ratio": 5.562, +
                     |               |     "max_diff_ratio": 0.000, +
                     |               |     "min_diff_ratio": 0.000, +
                     |               |     "mean_diff_ratio": 0.763 +
                     |               | }
(10 rows)
```

#### BMC-1m (D11) vs. HCGH-1m (D4)

```
           id            |   cdm_table   |              diff
-------------------------+---------------+--------------------------------
 vasopressors_dose_order | cdm_t         | {                             +
                         |               |     "5%_diff_ratio": 0.535,   +
                         |               |     "25%_diff_ratio": 0.385,  +
                         |               |     "50%_diff_ratio": 0.961,  +
                         |               |     "75%_diff_ratio": 11.216, +
                         |               |     "95%_diff_ratio": 11.216, +
                         |               |     "max_diff_ratio": 22.432, +
                         |               |     "min_diff_ratio": 0.000,  +
                         |               |     "mean_diff_ratio": 4.085  +
                         |               | }
 vasopressors_dose_order | criteria_meas | {                             +
                         |               |     "5%_diff_ratio": 0.535,   +
                         |               |     "25%_diff_ratio": 0.385,  +
                         |               |     "50%_diff_ratio": 0.961,  +
                         |               |     "75%_diff_ratio": 11.216, +
                         |               |     "95%_diff_ratio": 11.216, +
                         |               |     "max_diff_ratio": 22.432, +
                         |               |     "min_diff_ratio": 0.000,  +
                         |               |     "mean_diff_ratio": 4.085  +
                         |               | }
 lipase                  | cdm_t         | {                             +
                         |               |     "5%_diff_ratio": 0.356,   +
                         |               |     "25%_diff_ratio": 0.732,  +
                         |               |     "50%_diff_ratio": 1.049,  +
                         |               |     "75%_diff_ratio": 1.583,  +
                         |               |     "95%_diff_ratio": 3.948,  +
                         |               |     "max_diff_ratio": 183.879,+
                         |               |     "min_diff_ratio": 0.198,  +
                         |               |     "mean_diff_ratio": 1.605  +
                         |               | }
 vasopressors_dose       | cdm_t         | {                             +
                         |               |     "5%_diff_ratio": 0.015,   +
                         |               |     "25%_diff_ratio": 0.051,  +
                         |               |     "50%_diff_ratio": 0.265,  +
                         |               |     "75%_diff_ratio": 0.683,  +
                         |               |     "95%_diff_ratio": 4.237,  +
                         |               |     "max_diff_ratio": 21.952, +
                         |               |     "min_diff_ratio": 0.000,  +
                         |               |     "mean_diff_ratio": 0.938  +
                         |               | }
 vasopressors_dose       | criteria_meas | {                             +
                         |               |     "5%_diff_ratio": 0.015,   +
                         |               |     "25%_diff_ratio": 0.051,  +
                         |               |     "50%_diff_ratio": 0.265,  +
                         |               |     "75%_diff_ratio": 0.683,  +
                         |               |     "95%_diff_ratio": 4.237,  +
                         |               |     "max_diff_ratio": 21.952, +
                         |               |     "min_diff_ratio": 0.000,  +
                         |               |     "mean_diff_ratio": 0.938  +
                         |               | }
 crystalloid_fluid_order | cdm_t         | {                             +
                         |               |     "5%_diff_ratio": 0.028,   +
                         |               |     "25%_diff_ratio": 0.000,  +
                         |               |     "50%_diff_ratio": 1.415,  +
                         |               |     "75%_diff_ratio": 3.007,  +
                         |               |     "95%_diff_ratio": 0.000,  +
                         |               |     "max_diff_ratio": 5.306,  +
                         |               |     "min_diff_ratio": 0.032,  +
                         |               |     "mean_diff_ratio": 0.859  +
                         |               | }
 crystalloid_fluid_order | criteria_meas | {                             +
                         |               |     "5%_diff_ratio": 0.028,   +
                         |               |     "25%_diff_ratio": 0.000,  +
                         |               |     "50%_diff_ratio": 1.415,  +
                         |               |     "75%_diff_ratio": 3.007,  +
                         |               |     "95%_diff_ratio": 0.000,  +
                         |               |     "max_diff_ratio": 5.306,  +
                         |               |     "min_diff_ratio": 0.032,  +
                         |               |     "mean_diff_ratio": 0.859  +
                         |               | }
 septic_shock            | cdm_twf       | {                             +
                         |               |     "5%_diff_ratio": 0.000,   +
                         |               |     "25%_diff_ratio": 0.000,  +
                         |               |     "50%_diff_ratio": 0.000,  +
                         |               |     "75%_diff_ratio": 0.000,  +
                         |               |     "95%_diff_ratio": 0.000,  +
                         |               |     "max_diff_ratio": 0.000,  +
                         |               |     "min_diff_ratio": 0.000,  +
                         |               |     "mean_diff_ratio": 0.804  +
                         |               | }
 ddimer                  | cdm_t         | {                             +
                         |               |     "5%_diff_ratio": 0.000,   +
                         |               |     "25%_diff_ratio": 0.077,  +
                         |               |     "50%_diff_ratio": 0.299,  +
                         |               |     "75%_diff_ratio": 1.198,  +
                         |               |     "95%_diff_ratio": 1.839,  +
                         |               |     "max_diff_ratio": 0.000,  +
                         |               |     "min_diff_ratio": 0.000,  +
                         |               |     "mean_diff_ratio": 0.757  +
                         |               | }
 urine_output_24hr       | cdm_twf       | {                             +
                         |               |     "5%_diff_ratio": 0.000,   +
                         |               |     "25%_diff_ratio": 0.000,  +
                         |               |     "50%_diff_ratio": 0.207,  +
                         |               |     "75%_diff_ratio": 1.632,  +
                         |               |     "95%_diff_ratio": 1.346,  +
                         |               |     "max_diff_ratio": 8.593,  +
                         |               |     "min_diff_ratio": 4.555,  +
                         |               |     "mean_diff_ratio": 0.659  +
                         |               | }
(10 rows)

```

#### BMC-1y (D13) vs. HCGH-1y (D1)
```
         id         |   cdm_table   |              diff
--------------------+---------------+--------------------------------
 lipase             | cdm_t         | {                             +
                    |               |     "5%_diff_ratio": 0.423,   +
                    |               |     "25%_diff_ratio": 0.739,  +
                    |               |     "50%_diff_ratio": 1.054,  +
                    |               |     "75%_diff_ratio": 1.635,  +
                    |               |     "95%_diff_ratio": 5.800,  +
                    |               |     "max_diff_ratio": 262.130,+
                    |               |     "min_diff_ratio": 0.069,  +
                    |               |     "mean_diff_ratio": 2.217  +
                    |               | }
 alt_liver_enzymes  | cdm_t         | {                             +
                    |               |     "5%_diff_ratio": 0.083,   +
                    |               |     "25%_diff_ratio": 0.730,  +
                    |               |     "50%_diff_ratio": 1.043,  +
                    |               |     "75%_diff_ratio": 1.103,  +
                    |               |     "95%_diff_ratio": 3.951,  +
                    |               |     "max_diff_ratio": 514.556,+
                    |               |     "min_diff_ratio": 0.000,  +
                    |               |     "mean_diff_ratio": 1.747  +
                    |               | }
 acute_pancreatitis | cdm_twf       | {                             +
                    |               |     "5%_diff_ratio": 0.000,   +
                    |               |     "25%_diff_ratio": 0.000,  +
                    |               |     "50%_diff_ratio": 0.000,  +
                    |               |     "75%_diff_ratio": 0.000,  +
                    |               |     "95%_diff_ratio": 0.000,  +
                    |               |     "max_diff_ratio": 0.000,  +
                    |               |     "min_diff_ratio": 0.000,  +
                    |               |     "mean_diff_ratio": 1.551  +
                    |               | }
 septic_shock       | cdm_twf       | {                             +
                    |               |     "5%_diff_ratio": 0.000,   +
                    |               |     "25%_diff_ratio": 0.000,  +
                    |               |     "50%_diff_ratio": 0.000,  +
                    |               |     "75%_diff_ratio": 0.000,  +
                    |               |     "95%_diff_ratio": 0.000,  +
                    |               |     "max_diff_ratio": 0.000,  +
                    |               |     "min_diff_ratio": 0.000,  +
                    |               |     "mean_diff_ratio": 0.842  +
                    |               | }
 bands              | cdm_t         | {                             +
                    |               |     "5%_diff_ratio": 0.000,   +
                    |               |     "25%_diff_ratio": 0.267,  +
                    |               |     "50%_diff_ratio": 0.533,  +
                    |               |     "75%_diff_ratio": 1.200,  +
                    |               |     "95%_diff_ratio": 2.133,  +
                    |               |     "max_diff_ratio": 1.333,  +
                    |               |     "min_diff_ratio": 0.000,  +
                    |               |     "mean_diff_ratio": 0.797  +
                    |               | }
 bands              | criteria_meas | {                             +
                    |               |     "5%_diff_ratio": 0.000,   +
                    |               |     "25%_diff_ratio": 0.267,  +
                    |               |     "50%_diff_ratio": 0.533,  +
                    |               |     "75%_diff_ratio": 1.200,  +
                    |               |     "95%_diff_ratio": 2.133,  +
                    |               |     "max_diff_ratio": 1.333,  +
                    |               |     "min_diff_ratio": 0.000,  +
                    |               |     "mean_diff_ratio": 0.797  +
                    |               | }
 urine_output_24hr  | cdm_twf       | {                             +
                    |               |     "5%_diff_ratio": 0.000,   +
                    |               |     "25%_diff_ratio": 0.000,  +
                    |               |     "50%_diff_ratio": 0.397,  +
                    |               |     "75%_diff_ratio": 1.357,  +
                    |               |     "95%_diff_ratio": 1.302,  +
                    |               |     "max_diff_ratio": 9.603,  +
                    |               |     "min_diff_ratio": 26.684, +
                    |               |     "mean_diff_ratio": 0.630  +
                    |               | }
 qsofa              | cdm_twf       | {                             +
                    |               |     "5%_diff_ratio": 0.000,   +
                    |               |     "25%_diff_ratio": 0.000,  +
                    |               |     "50%_diff_ratio": 0.000,  +
                    |               |     "75%_diff_ratio": 4.421,  +
                    |               |     "95%_diff_ratio": 4.421,  +
                    |               |     "max_diff_ratio": 0.000,  +
                    |               |     "min_diff_ratio": 0.000,  +
                    |               |     "mean_diff_ratio": 0.559  +
                    |               | }
 amylase            | cdm_t         | {                             +
                    |               |     "5%_diff_ratio": 0.036,   +
                    |               |     "25%_diff_ratio": 0.060,  +
                    |               |     "50%_diff_ratio": 0.101,  +
                    |               |     "75%_diff_ratio": 0.191,  +
                    |               |     "95%_diff_ratio": 3.199,  +
                    |               |     "max_diff_ratio": 3.927,  +
                    |               |     "min_diff_ratio": 0.054,  +
                    |               |     "mean_diff_ratio": 0.520  +
                    |               | }
 urine_output_6hr   | cdm_twf       | {                             +
                    |               |     "5%_diff_ratio": 0.000,   +
                    |               |     "25%_diff_ratio": 0.000,  +
                    |               |     "50%_diff_ratio": 0.000,  +
                    |               |     "75%_diff_ratio": 1.109,  +
                    |               |     "95%_diff_ratio": 0.832,  +
                    |               |     "max_diff_ratio": 1.553,  +
                    |               |     "min_diff_ratio": 38.265, +
                    |               |     "mean_diff_ratio": 0.489  +
                    |               | }
(10 rows)

```