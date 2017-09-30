TREWS Criteria v2 2017-09-29
============================


|Category |Name        | Definition    | Comment |
|---------|------------|:--------------|:--------|
| **ORDER** |`antibiotics_order` | TBD | v1   |
| **ORDER** |`blood_culture_order` | TBD | v1 |
| **ORDER** |`crystalloid_fluid_order` | TBD | v1 |
| **ORDER** |`initial_lactate_order` | TBD | v1   |
| **ORDER** |`repeat_lactate_order` | TBD | v1    |
| **ORDER** |`vasopressors_order` | TBD | v1  |
| **ORG_DYS** |`bilirubin` | 1 if bilirubin > 2 else 0 | v1 |
| **ORG_DYS** |`blood_pressure` | TBD | v1    |
| **ORG_DYS** |`creatinine` | TBD | v1    |
| **ORG_DYS** |`decrease_in_sbp` | TBD | v1   |
| **ORG_DYS** |`inr` | TBD | v1   |
| **ORG_DYS** |`lactate` | TBD | v1   |
| **ORG_DYS** |`mean_arterial_pressure` | TBD | v1    |
| **ORG_DYS** |`platelet` | TBD | v1  |
| **ORG_DYS** |`respiratory_failure` | TBD | v1   |
| **ORG_DYS** |`trews_bilirubin` | 1 if bilirubin > greatest(2, baseline_platelet) else 0 | NEW   |
| **ORG_DYS** |`trews_creatinine` | 1 if creatinine >= 1.5 and creatinine >= 0.5 + baseline_creatinine and not esrd | NEW  |
| **ORG_DYS** |`trews_gcs` | 1 if gcs < 13 and not stroke and not propofol else 0 |NEW |
| **ORG_DYS** |`trews_inr` | 1 if inr > 1.5 and inr > 0.5 + baseline_inr and warfarin else 0 |NEW |
| **ORG_DYS** |`trews_lactate` | TBD |NEW |
| **ORG_DYS** |`trews_platelet` | 1 if platelet < least(100, 0.5 * baseline_platelet) | NEW   |
| **ORG_DYS** |`trews_vent` | TBD |NEW |
| **SEP_SHO** |`crystalloid_fluid` | TBD | v1 |
| **SEP_SHO** |`hypotension_dsbp` | TBD | v1  |
| **SEP_SHO** |`hypotension_map` | TBD | v1   |
| **SEP_SHO** |`initial_lactate` | TBD | v1   |
| **SEP_SHO** |`systolic_bp` | TBD | v1   |
| **SIRS** |`heart_rate` | TBD | v1   |
| **SIRS** |`respiratory_rate` | TBD | v1 |
| **SIRS** |`sirs_temp` | TBD | v1    |
| **SIRS** |`wbc` | TBD | v1  |
| **SUS** |`suspicion_of_infection` | TBD | v1    |
| **TREWS**   |`trews` | TBD | NEW |