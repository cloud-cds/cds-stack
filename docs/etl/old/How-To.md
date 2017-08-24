# How to deploy dashan_core to rambo?
1\. ssh to rambo and create a folder for your dashan code base

```
mkdir /udata/my_dashan
```

2\. upload the dashan_core code base from your computer to the folder at rambo

```
scp -r dashan_core rambo.isi.jhu.edu:/udata/my_dashan
```

3\. install python environment at rambo  

```
cd /udata/my_dashan
virtualenv dashan_venv
source dashan_venv/bin/activate
pip install -r dashan_core/requirements.txt
```

# How to populate measured features?
 **NOTE: If you modify the specification in CDM_Feature.csv or CDM_Function.csv for an existing feature/function, the corresponding tables in CDM does not automatically update the feature/function specification. You will need to manually update the cdm_feature/cdm_function table in CDM.** 

1. If the feature is not defined in `/conf/cdm_feature.csv` or `/conf/dblink/<dbname>/<dbname>_feature.csv` (only used for this particular dblink), define it first;
1. If the transform function is not added in `conf/cdm_function.csv` or `/conf/dblink/<dbname>/<dbname>_function.csv`, add it;
1. Add or update the feature mapping configuration for this feature in `/conf/dblink/<dbname>/feature_mapping.csv`
1. Upload your `dashan_core` code to your server, e.g, rambo.
1. populate the feature into the database.
   
   ```
    cd scripts/share
    python add_new_feature.py <dbname> <fid>
   ```

1. Debug the values in the database. If you need to modify the function code, modify it, and re-run the previous step.

# How to derive features? 
1. If the feature is not defined in CDM Dictionary (on Dropbox), define it first;
1. If the feature is not added in `conf/CDM_Feature.csv`, add it; 
1. If the derive function is not defined, write the derive function under dashan_core/src/ews_server/derive_functions, and register your function in dashan_core/src/ews_server/derive_functions/__init__.py, i.e., add an line like `from your_function import *`
1. If the function is not added in `conf/CDM_Function,csv`, add it;
1. Upload your `dashan_core` code to your server, e.g, rambo.
1. If the feature does not exist in the database table, e.g, `cdm_twf`, add it.

   ```
    cd scripts/share
    python add_new_feature.py <dbname> <fid>
   ```
1. Debug the values in the database. If you need to modify the function code, modify it, and re-run the previous step.
1. If this measured feature has been successfully updated, then we need to regenerate the features depending on this feature too

   ```
   python derive.py <dbname> <fid> dependent
   ```

# How to draw feature dependency graph?
1. requirements [TODO]
1. You need to download dashan_core 
1. run script 
   
   ```
   cd scripts/
   # print full dependency graph
   python draw_dependency.py <table_file_name>
   # print dependency graph for feature <feature>
   python draw_dependency.py <table_file_name> <feature>
   ```

# How to fillin all TWF features?
   
   ```
   cd dashan_core/scripts
   python dashan_datalink_main.py hcgh_1608 hcgh_1608 etl f
   ```

# How to derive all derivative features?

   ```{Python}
   cd dashan_core/script
   python dashan_datalink_main.py hcgh_1608 hcgh_1608 etl d
   # the first hcgh_1608 is the dashan instance ID; the second one is the datalink ID.
   ```

If you run into errors, after you fix the error, you can continue your derive process from the error:

```
cd dashan_core/scripts/share
python derive.py hcgh_1608 [fid_fixed_error] append
```

# How to run the pipeline to generate all features in CDM?

   ```
   cd dashan_core/script
   python dashan_datalink_main.py hcgh_1608 hcgh_1608 etl tfd
   ```
