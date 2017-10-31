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

## Check list before running ETL

- Make sure every feature listed in **feature_mapping.csv** and **feature_mapping_cardiac.csv** is loaded onto table *cdm_feature* with corresponding dataset_id (e.g., 1000). If not, use the following command to add.

      insert into cdm_feature(...) values(...)
        
### Steps of running ETL with Amazon Web Services    

1. Make sure your AWS configuration is correct  
        For now, ETL uses the cluster **cluster-dev-ml.jh**. Use the following kubectl command to comfirm.  
        `kubectl config get-contexts`  
    
2. Turn on Auto Scaling Groups on AWS  
        Go to Auto Scaling Groups pages on AWS. For now, ETL uses **c2dw-etl.cluster-dev-ml.jh.opsdx.io** to perform ETL process.  
        Edit it and change the minimum number to 1.  

3. Edit .yaml file for setting environment variables   
        For reference, go to `dashan-universe/cloud/aws/stage3/dev-ml-services/c2dw-etl/ and look for **cardiac_1001.yaml**.  

4. c2dw-etl-secrets (optional)  
       Sometimes, the secrets file is missing. Use kubectl command kubectl get secret to check if *c2dw-etl-secrets* is present. If not, look for it in `/c2dw-etl/` and add it.  
 
5. Run ETL  
        Use the following kubectl command to control ETL process.    
 
        
        kubectl create -f <file.yaml>            
        kubectl get po   
        kubectl logs <running-job-name-got-from-previous-command>  
        kubectl delete jobs/cardiac-etl-job   
        

6. Turn off Auto Scaling Groups after ETL is finished  
    


 
