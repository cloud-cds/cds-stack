1. Create a new branch via GitHub: `git checkout -b zad-add-new-features`

2. Add new feature into configuration files:
    1. Add the feature definition row into `dashan_core/conf/CDM_Feature.csv`
    2. Add the feature mapping row into `dashan_core/conf/dblink/hcgh_1608/feature_mapping.csv`
    3. (Optional) Add new functions into `dashan_core/conf/CDM_Function.csv`
    
3. Add new feature into database configuration tables:
    1. Insert new function into cdm_function table
    2. Insert new feature definition into cdm_feature table
    3. Insert new feature mapping row(s) into dblink_feature_mapping table

4. Populate values from HC_EPIC to database table (under scripts/share)
    1. `python populate_features_sequencially.py <database_name> <feature name> <debug_or_not>`
    2. (ONLY for TWF feature) `python interpolate.py <database_name> <feature name>`
    3. (TBD: update all derived features depending on this feature)

5. Debug     
    1. Check whether the values existed in HC_EPIC have been populated into CDM