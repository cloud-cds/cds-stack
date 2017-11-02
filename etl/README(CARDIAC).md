# ETL on Cardiogenic Database (cardiac_db_small)

The python script *planner.py* dominates the whole ETL process, which looks like the structure below,

```
etl/  
├── clarity2dw/  
│   ├── **planner.py**  
│   ├── **extractor.py**  
│   └── conf/  
│       ├── **feature_mapping.csv**  
│       └── **feature_mapping_cardiac.csv**  
│  
├── load/  
│   └── primitives/  
│       └── tbl/  
│            └── **cardiogenic_shock_feats.py**  
│  
├── core/  
│   ├── config.py  
│   ├── **engine.py**  
│   ├── exceptions.py  
│   └── task.py  
│  
└── transforms/  
    ├── primitives/  
    │   ├── df/  
    │   ├── row/  
    │   │    └── **transforms.py**  
    │   ├── native/  
    │   └── tbl/  
    └── pipelines/
        └── **clarity_2_cdm_pd.py**
```

        
### Steps of running ETL with Amazon Web Services    

0. Make sure every feature listed in **feature_mapping.csv** and **feature_mapping_cardiac.csv** is loaded onto table *cdm_feature* with corresponding dataset_id (e.g., 1000). If not, use the following command to add.

      insert into cdm_feature(...) values(...)

1. Make sure your AWS configuration is correct.
        For now, ETL uses the cluster **cluster-dev-ml.jh**. Use the following kubectl command to comfirm.  
        `kubectl config get-contexts`  

   If not setting correctly, you can use `kubectl config use-context cluster-dev-ml.jh.opsdx.io` to change. Also, remember to specify your AWS CONFIG first. Look for CONFIG file on /home/ubuntu/yanif/kubeconfig on dev controller.
       
2. Turn on Auto Scaling Groups on AWS.  
  Go to Auto Scaling Groups pages on AWS. For now, ETL uses **c2dw-etl.cluster-dev-ml.jh.opsdx.io** to perform ETL process.
        Edit it and change the minimum number to 1.  

3. Edit .yaml file for setting environment variables.   
       For reference, go to `dashan-universe/cloud/aws/stage3/dev-ml-services/c2dw-etl/` and look for **cardiac_1001.yaml**. Such as *dataset_id*, *database*, *min_tsp* and so on.

4. c2dw-etl-secrets (optional).  
       Sometimes, the secrets file is missing. Use kubectl command kubectl get secret to check if **c2dw-etl-secrets** is present. If not, look for it in `/c2dw-etl/` and add it.  
 
5. Run ETL.  
        Use the following kubectl command to control ETL process.    

        Create an ETL job   
        kubectl create -f <file.yaml>            

        Check ETL process and task name    
        kubectl get po   

        Check log of ETL 
        kubectl logs <running-job-name-got-from-previous-command>  

        Terminate an ETL job  
        kubectl delete jobs/cardiac-etl-job   
        

6. Turn off Auto Scaling Groups after ETL is finished.  
    

### Steps of running ETL with Dev comtroller (less efficient)

1. Copy dahsan-universe/etl and dahsan-universe/db to dev controller by scp.

2. Make a virtual environment of python 3 with virtualenverapper (or equivalent).

3. Install requirement (dashan-emr/dashan-universe/requirement.txt).

4. Install etl using `pip install etl`.

5. Create a screen for setting environment variable and virtual environment.
    
        Create a new screen  
        screen -S <name>  

        Reattach to a detached screen  
        screen -r <name>  

        Kill a screen  
        screen -X -S <name> quit      

        List all screens   
        screen -list  
    
6. Make sure the environment variables are set correctly **in a screen**.

        export min_tsp="2016-04-27"  
        export dataset_id="1"  
        export clarity_workspace="clarity_1y"  
        export nprocs="8"  
        export num_derive_groups="8"  
        export vacuum_temp_table="True"  
        export db_host=$dw_host  
        export db_port="5432"  
        export db_name=$dw_name  
        export db_password=$dw_password  
        export feature_mapping="feature_mapping.csv"  

        export extract_init=False  
        export populate_patients=False  
        export fillin=False  
        export derive=False  
        export offline_criteria_processing=False  

        export transform_fids="amlodipine_valsartan_dose"   

7. Run ETL. 

        With pipeline to a log file.  
        python etl/clarity2dw/planner.py 2>&1 | tee logfile.log    

        Without pipeline to a log file.  
        python etl/clarity2dw/planner.py 2>&1  
        
    


 
