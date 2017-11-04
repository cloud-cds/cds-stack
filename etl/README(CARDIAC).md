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

## Steps of running ETL with Amazon Web Services    

0. Make sure every feature listed in **feature_mapping.csv** and **feature_mapping_cardiac.csv** is loaded onto table *cdm_feature* with corresponding dataset_id (e.g., 1000). If not, use the following command to add.

      `insert into cdm_feature(...) values(...)`

1. Make sure your AWS configuration is correct.  
 For now, ETL uses the cluster **cluster-dev-ml.jh**. Use  
  `kubectl config get-contexts`  
    
   to confirm. If not setting correctly, use  
  `kubectl config use-context cluster-dev-ml.jh.opsdx.io`  
    
  to change. Also, remember to specify the kubectl config file first. Look for the kubectl config file on `/home/ubuntu/yanif/kubeconfig` on dev controller.
       
2. Turn on Auto Scaling Groups on AWS.  
  Go to Auto Scaling Groups pages on AWS. For now, ETL uses **c2dw-etl.cluster-dev-ml.jh.opsdx.io** to do the ETL process. Edit this node group and change the minimum number to 1.  

3. Edit .yaml file for setting environment variables.   
       For reference, go to `dashan-universe/cloud/aws/stage3/dev-ml-services/c2dw-etl/` and look for **cardiac_1001.yaml**. Such as *dataset_id*, *database*, *min_tsp* and so on.

4. c2dw-etl-secrets (optional).  
       Sometimes, the secrets file is missing. Use kubectl command kubectl get secret to check if **c2dw-etl-secrets** is present. If not, look for it in `dashan-universe/cloud/aws/stage3/dev-ml-services/c2dw-etl/` and add it.  
 
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
    

## Steps of running ETL with Dev comtroller (less efficient)

1. Copy dahsan-universe/etl and dahsan-universe/db to dev controller by scp.

2. Make a virtual environment of Python 3 with virtualenverapper (or equivalent).

3. Install requirement (`dashan-emr/dashan-universe/requirement.txt`).

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
    
6. Make sure the environment variables are set correctly ***in a screen***. Other specifiable parameters can be found in .ymal file in `dashan-universe/cloud/aws/stage3/dev-ml-services/c2dw-etl/`. 

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

        export offline_criteria_processing=False  
        export extract_init=True  
        export populate_patients=True  
        export fillin=True  
        export derive=True  

7. Run ETL. 

        With pipeline to a log file.  
        python etl/clarity2dw/planner.py 2>&1 | tee logfile.log    

        Without pipeline to a log file.  
        python etl/clarity2dw/planner.py 
        
 
## Note

ETL consists of many subtasks, including *extract_init*, *populate_patients* and other general extraction/transform. Setting *extract_init* as True will erase every record in cdm tables with corresponding dataset_id. *populate_patients* encodes enc_id by pat_id<sup>1</sup> for each dataset_id.  

Running a complete ETL will delete every record in cdm tables with corresponding dataset_id, and then do feature extraction according to *feature_mapping.csv* and *feature_mapping_cardiac.csv* (and other mapping files). After feature extraction/tranfomation, the loaded records will be filled into corresponding cdm tables based on *cdm_feature.csv*. To avoid incorrect filling in, constraints are specified in tabl *cdm_feature* in PostgreSQL.

For more details about ETL, please look it up in **planner.py**.

<sup>1</sup>pat_id: pat_id uniquely identifies patients among all dataset_id while enc_id is used repetitively for each dataset_id. Cross reference between pat_id and (dataset_id, enc_id) with pat_enc in PostgreSQL.  
<sup>2</sup>Staging tabless: The tables that start with a capital letter when listing all tables by `\d` in PostgreSQL.