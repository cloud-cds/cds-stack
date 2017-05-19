# Up Front:
 - CDM, the Common Data Model, is a database schema designed by Andong to hold healthcare data, for ease of use in ML models.

 - There are many tables inside CDM, the most import ones are cdm_twf, cdm_t, cdm_s, and cdm_feature, more about this tables can be found at: https://github.com/dashan-emr/dashan_core/wiki/database_schema and https://github.com/dashan-emr/dashan_core/wiki/Confidence-Flag-Dictionary

 - Your task will be to add features to CDM sufficient to build this cardiac deterioration model. 
 	The repos needed to add features to CDM are:
		https://github.com/dashan-emr/dashan-etl
		https://github.com/dashan-emr/dashan-db
	The repo needed to actually build the model is:
		https://github.com/dashan-emr/dashan_app_sepsis

 - Writing to the database using our toolset risks data loss, so while you're developing your algorithum you should use your own DB, which you can safely write to.

 - Code can be developed and tested locally (i.e. without ssh-ing to RAMBO or the dev or prod machines) using a VPN. Yanif is working to set you all up with VPN access. 

 - Currently, it is only possible to train and grade models with features in cdm_twf. Adhiraj will eventually work on training on features in cdm_t. This means that for now, all features which you indend to be used in the model directly should be put in cdm_twf, while other features which may be used for things like determining the population, can be put in other tables

 - In general, writing to CDM_TWF is harder and risks data loss.

# Steps to add measured features to CDM:
## Step 0: Learn
Understand, at a high level, the code in
 - dashan-etl/etl/clarity2dw/planner.py

This is the main function of the code which moves data from EPICs clarity database to our database, and calculates and derived features.

Understand, at a high level, database schema as defined by the bash script
 -- dashan_db/dw/create_db.sh (and the SQL scripts it calls)



## Step 1: Find The Data in Clarity
Using the dictionary tables below:
 - lab_dict
 - lab_proc_dict
 - flowsheet_dict

Understand where the data is in clarity. Look at the data in the clarity database to get some intution about what the data looks like.

There are a few versions of a couple of tables (e.g. Labs, Labs_643). In those cases both should be checked.
Dict to table Mapping
"lab_dict" to "Labs" and "Labs_643";
"lab_proc_dict" to "OrderProcs" and "OrderProcs_643"
"flowsheet_dict  to "FlowsheetValue","FlowsheetValue-LDA","FlowsheetValue_643"

In some cases the dicts may missing or misleading entries, it's always good to check against the tables to see themselves to see if an extry exsits;
TODO: Write Global Search function which looks for a string in all the dicts, and in the right entries in all of the tables, and outputs a count.

Andong has access to all of the data in clarity, but it is possible that not all of it has been extracted to the clarity tables in our database. If you're unable to find the data where you believe it should be from the dictionaries in the clarity tables in your database, document what you can't find, and the query which you used to look. We'll batch up as list of such things, and hand them to Andong or collaborators at EPIC.


## Step 2: Extract the Data
Write sql/python function to extract measured feature(s) from clarity

- see the queries in dashan-etl/etl/clarity2dw/conf/feature_mapping.csv for simple extractions which follow common patterns.
- see  the functions in dashan-etl/etl/transforms/pipelines/clarity_2_cdm_pd.py for an example of custom extraction function, useful for more complicated extractions, or extractions which make use of python libraries.

These functions can often be easily developed and intially tested offline without running the entire ETL.

## Step 3: Document the Extraction
Select a dataset you'd like to do the extractio on.

Update CDM_feature.csv file in
dashan-db/dw/clarity2dw/datasetX/CDM_Feature.csv
(this adds metadata for human and programatic use)

Run the create_c2dw.sql file located in that same directory to push your updates to the database the cdm_feature.csv to upload the file to database

Update feature_mapping.csv file for the etl located at dashan-etl/etl/clarity2dw/conf you indend to use to tell the ETL how to extract your feature(s).

## Step 4: Update CDM_TWF Schema (If it's a TWF feature)
If the feature you'd like to add is a CDM_TWF feature, update the CDM_TWF schema located in

dashan-db/dw/clarity2dw/create_dbschema.sql

note, running that file will delete all the data in CDM. The ETL will have to be-run to move the data from clarity back to CDM. I'd epect you'll have to do this a number of times in your development.
  - One of you could write

 All updates to the schema are risky. If you update the schema in any way you'll have to understand the implication across all etls, an easy ETL to forget is the op2dw


## Step 6:
setup, run, and debug the ET

# Steps to add derived features to CDM (Template Method) :

TODO

# Steps to add derived features to CDM (Completely Custom Method) :
## Step 1: Write and document Pseudocode
Write and document pseudo code for the derived feature you would like to add, and add that pseudocode to the dropbox CDM_Definition folder


## Step 2: Implement Derive Function
You're derive function must support both having and not having a dataset_id to 
support both the datawherehouse and operational schemas. 

Temporary versions of cdm_twf table are created for each etl run. You're custom function is responsible for 
joining the appropriate cdm_twf tables together to get the information required. There are some functions written in derive_helper.py to accomplish this

The Template Method makes these steps easier.

## Step 3: Update CDM_Feature  / CDM_Function
Select a dataset you'd like to do the extractio on.

Update CDM_feature.csv file in dashan-db/dw/clarity2dw/datasetX/CDM_Feature.csv to reflect your new feature this adds metadata for human and programatic use). Make sure to include

Update CDM_function in the same directory to document the function that you're using to derive the feature.

Run the create_c2dw.sql file located in that same directory to push your updates to the database the cdm_feature.csv to upload the file to database

Import your extraction function into dashan-etl/etl/load/tbl/derive.py

## Step 4: Update CDM_Feature



