# V8 TODO List
1. Populate new2 excel file
  - Make pre-review on new2.excel @Andong (done; will go back after we import the new config files)
    + fill_in_args -> window_size_in_hours
    + all fillin function names -> last_value_in_window
  - Generate cdm_feature.csv, cdm_function.csv, and feature_mapping.csv @Jose
  - Populate measured features and run fillin function @Andong
  - Generate derive features @Andong

2. System update
  - Design and implement confidence category for all features (including a confidence value dictionary and rules) @Jose and @Andong
    + add confidence value for all categories (add confidence column in cdm_g, cdm_s, cdm_m, cdm_t, change all columns `fid_C` in cdm_twf to `fid_c`)
    + create [confidence dictionary](https://github.com/junr03/emr_system/wiki/Confidence-Tag-Dictionary)
      
  - Complete all derive functions @Jose and @Andong
  - Need a function called `getDependentFeatures(inputFeatureList)` which return a list of features dependent on the input feature list @Jose
  - Divide function.py into a folder func/ with files: derive.py, transform.py, fillin.py  
  - New fillin function name: last_value_in_window
  

***

# V7 TODO List
1.  DB Link Extraction `LOW PRIORITY`
  -  Define a config file format
  -  Write config file for MIMIC and EPIC
  -  Code to replicate external source in server
2.  Mapping from source replica to CDM
  -  Mapping config file `1 week` `Narges`   Version 1 - Complete by Sept 25th (this does not have David's approval).
  -  Write Mapping functions `1 day` `Jose` By Sunday, Sept 27
  -  Create Mapping pipeline: extract from source, transform to CDM format `2 days` `Jose`  By Sunday, Sept 27
3.  DB Setup
  -  Finalize Schema design `1 day` `@Andong` and `Jose` **DONE**
  -  Database management functions: create database, and tables `1 day` `Andong`   **DONE**
          Bottlenecked on postgres update. Complete before Mon, Sept 28
4.  `Depends on 3` DB access API: `Insert`, `Update`, `Query` records `2 days` `Andong`  **DONE**
5.  `Depends on 2, 4` Populate CDM Pipeline `3 days` `Andong` and `Jose` By end of day Tuesday, code is done and unit tests complete. Then populate CDM (deadline for having a fully populated CDM may be delayed by a day or two.)
6.  `Depends on 4, 7` ML app pipeline   SPEC to be determined for 6 / 7 on Sept 30th (Wednesday)
7.  Implement App
  -  Interpolate Function
  -  Feature transformation
  -  Model Training
  -  Prediction
8. Scheduler
9. Rest API

## Milestones
1.  Populate CDM: `2`, `3`, `4`, `5`
2.  Hookup ML Job to CDM: `6`
3.  Fully functioning App: `7`