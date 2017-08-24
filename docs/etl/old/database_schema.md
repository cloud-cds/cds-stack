# Database Schema

## Quick Links
- [DBLINK](#dblink-table)
- [PAT_ENC](#pat_enc-table)
- [DBLINK_FEATURE_MAPPING](#dblink_feature_mapping-table)
- [CDM_FEATURE](#cdm_feature-table)
- [CDM_FUNCTION](#cdm_function-table)
- [CDM_S](#cdm_s-table)
- [CDM_M](#cdm_m-table)
- [CDM_T](#cdm_t-table)
- [CDM_TWF](#cdm_twf-table)

As mentioned in the architecture overview, CDM is a common data model that will be the starting point for constructing a variety of machine learning applications. Therefore, its schema has been design with the philosophy that CDM will be *~98% static*. That is, CDM will experience changes as the system matures, but we expect a low-frequent and small number of changes to occur. Therefore, CDM will benefit from being physically stored as a series of tables in a relational database; PostrgreSQL for this particular system. The following is a diagram of what will constitute what we have called **CDM**.

![image](images/database_schema_0918.png)

In addtion, we use JSON format to support flexible user-specific features in CDM table. The values of user defined features will be saved as a list of JSON objects, and each JSON object contains the user-specific features for each application. An example is as follow:
```json
{
   "user1": {"f1":3, "f1_C":1},
   "user2": {"f4":2.3, "f4_C":0}
}
```


## `DBLINK` Table

This table contains information about all the source systems connected to the DataSource.

| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `DBLINK_ID` | varchar(20) | no | the dblink\_id, primary key |
| `DBLINK_TYPE` | varchar(20) | no | "schedule" or "streaming" |
| `SCHEDULE` | text | yes | the schedule in text, e.g., "weekly" |
| `DATA_LOAD_TYPE` | varchar(20) | no | "incremental" or "full" |
| `CONNECTION_TYPE` | varchar(20) | no | use the connection type to call different connector, e.g, "postgresql" or "web" |
| `CONNECTION_SETTING_JSON` | text | no | connection setting in json format (including host, port, user, password, etc.). The setting will be used to establish  connection(s) |

## `PAT_ENC` Table

This table is a mapping of all the patient encounters contained in the DataSource.

| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `ENC_ID` | serial | no | the internal patient id, primary key |
| `DBLINK_ID` | varchar(20) | no | the dblink where this patient came from, foreign key to the `DBLINK` table |
| `VISIT_ID` | varchar(50) | no | the encounter ID in the source database |
| `PAT_ID` | varchar(50) | no | the patient ID in the source database |
| `DEPT_ID`| varchar(50)| yes| the department ID in the source database |

## `DBLINK_FEATURE_MAPPING` Table

This table serves as an index to find a CDM feature in all the different source systems. That is, this table provides knowledge for where all the measured features to be imported to CDM live in their source systems.

| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `FID` | varchar(20) | no | the cdm_feature name, foreign key in the `CDM_FEATURE` table |
| `DBLINK_ID` | varchar(20) | no | the dblink where this particular feature maps to, foreign key in the `DBLINK` table |
| `DBTABLE` | varchar(50) | yes | the table where this feature locates (if this dblink is connected to a database) |
| `SELECT_COLS` | text | yes | the columns needed to be selected in the SQL statement (if this dblink is connected to a database) |
| `WHERE_CONDITIONS` | text | yes | the conditions after `WHERE` in the SQL statement (if this dblink is connected to a database) |
| `TRANSFORM_FUNC_ID` | varchar(20) | yes | function used to normalize this feature to CDM, foreign key in the `CDM_FUNCTION` table |
| `URL` | text | yes | the url to request the feature (if this dblink is connected to a web server) |

## `CDM_FEATURE` Table

This table acts as a dictionary of features present in `RECORD` table.

| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `FID` | varchar(50) | no | the feature id, primary key |
| `CATEGORY` | varchar(50) | no | the feature category, i.e., "SINGLE", "MULTIPLE", "TIMESTAMPED", "TIMESTAMPED_WITH_FILLIN" |
| `DATA_TYPE` | varchar(20) | no | the data type of the feature |
| `IS_MEASURED` | boolean | no | is this featured measured or not (derived) |
| `IS_DEPRECATED` | boolean | no | is this feature currently in use |
| `FILLIN_FUNC_ID` | varchar(50) | yes | function used to fill in missing values, null if no missing values were calculated. Foreign key in the `CDM_FUNCTION` table. |
| `DERIVE_FUNC_ID` | varchar(50) | yes | function used to derive this feature, null if the feature is a measured feature. Foreign key in the `CDM_FUNCTION` table. |
| `DERIVE_FUNC_INPUT` | text | yes | feature list delimited by comma as the input of the derive function. |
| `DESCRIPTION` | text | yes | description of the meaning of this feature. In the case that it is a derived feature listing the dependencies is good practice |
| `VERSION` | varchar(50) | no | version of the feature |

Note that four categories of features are supported:

| Feature Category (abbr.)   | Description  | Example |
| :------------ |:---------------|:------------|
| single response (`SINGLE`)   | Each patient's encounter only contains one feature value  | e.g., age, gender, etc. |
| multiple response (`MULTIPLE`) | Each patient's encounter contains multiple feature values        |   e.g., diagnoses |
| timestamped response (`TIMESTAMPED`) | Each patient's encounter contains multiple timestamped feature values        |  e.g., medication events |
| timestamped response with fillin (`TIMESTAMPED_WITH_FILLIN`) | Each patient's encounter contains full (after fillin) timestamped feature values         |   e.g., vital signs, labs, etc. |

([more detail](https://github.com/junr03/emr_system/issues/15))

## `CDM_FUNCTION` Table

This table will serve as a method for versioning the functions used to populate CDM.

| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `FUNC_ID` | varchar(50) | no | unique name of the function, primary key |
| `FUNC_TYPE` | varchar(20) | no | "transform", "fillin", or "derive" |
| `DESCRIPTION` | text | no | detailed description of the function |


## `CDM_TWF` Table

This table holds all the CDM and user-specific TWF (Timestamped With Fillin) Features for each patient at each time stamp recorded.

| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `ENC_ID` | integer | no | patient encounter id, foreign key in the `PAT_ENC` table |
| `TSP` | timestamp | no | timestamp of when the measured features were obtained |
| `<FID>` | `CMD_FEATURE.DATA_TYPE` | yes | value of the feature, the data type is defined at `CMD_FEATURE.DATA_TYPE` |
| `<FID>_C` | real | yes | confidence of the feature. 1 if measured and not field in, <= 1 otherwise (depending on how confident the function the was used to generate this feature is in the value generated). |
| `USER_SPECIFIC` | JSON | yes | use JSON format to make app specific features flexible to adjust by users |

## `CDM_S` Table
| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `ENC_ID` | integer | no | patient encounter id, foreign key in the `PAT_ENC` table |
| `FID` | varchar(50) | no | the feature ID, foreign key in the `CDM_FEATURE` table |
| `VALUE` | text | yes | value of the feature, the data type is defined at `CMD_FEATURE.DATA_TYPE` |

## `CDM_M` Table
| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `ENC_ID` | integer | no | patient encounter id, foreign key in the `PAT_ENC` table, primary key |
| `FID` | varchar(50) | no | the feature ID, foreign key in the `CDM_FEATURE` table, primary key |
| `LINE` | smallint | no | the line of this value, primary key |
| `VALUE` | text | yes | value of the feature, the data type is defined at `CMD_FEATURE.DATA_TYPE` |

## `CDM_T` Table
| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `ENC_ID` | integer | no | patient encounter id, foreign key in the `PAT_ENC` table, primary key |
| `TSP` | timestamp | no | the timestamp of this value, primary key |
| `FID` | varchar(50) | no | the feature ID, foreign key in the `CDM_FEATURE` table, primary key |
| `VALUE` | text | yes | value of the feature, the data type is defined at `CMD_FEATURE.DATA_TYPE` |

## `USER` Table

This table holds all the users currently registered in the system. `admin` is the default administrator user.

| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `UID` | varchar(20) | no | user id, primary key, e.g., 'admin' |
| `PASSWORD` | chkpass | no | the password to log into the system |

## `USER_FEATURE` Table

This table acts as a dictionary of user-specific features.

| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `FID` | varchar(20) | no | the feature id, primary key |
| `UID` | varchar(20) | no | the user id, primary key, foreign key of 'USER' table |
| `DATA_TYPE` | varchar(20) | no | the data type of the feature |
| `IS_DEPRECATED` | boolean | no | is this feature currently in use |
| `CDM_FEATURE_INPUT` | json | no | the CDM feature(s) used as input to generate this user-specific feature  |
| `FILLIN_FUNC_ID` | varchar(20) | yes | function used to fill in missing values, null if using the CDM fillin function. Foreign key in the `USER_FUNCTION` table. |
| `DERIVE_FUNC_ID` | varchar(20) | yes | function used to derive this feature, null if no derive function used. Foreign key in the `USER_FUNCTION` table. |
| `DESCRIPTION` | text | yes | description of the meaning of this feature. |
| `VERSION` | Integer | no | version of the feature |

## `<UID>_S` Table
| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `ENC_ID` | integer | no | patient encounter id, foreign key in the `PAT_ENC` table |
| `FID` | varchar(20) | no | the feature ID, foreign key in the `USER_FEATURE` table |
| `VALUE` | text | yes | value of the feature, the data type is defined at `USER_FEATURE.DATA_TYPE` |

## `<UID>_M` Table
| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `ENC_ID` | integer | no | patient encounter id, foreign key in the `PAT_ENC` table, primary key |
| `FID` | varchar(20) | no | the feature ID, foreign key in the `USER_FEATURE` table, primary key |
| `LINE` | integer | no | the line of this value, primary key |
| `VALUE` | text | yes | value of the feature, the data type is defined at `USER_FEATURE.DATA_TYPE` |

## `<UID>_T` Table
| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `ENC_ID` | integer | no | patient encounter id, foreign key in the `PAT_ENC` table, primary key |
| `TSP` | timestamp | no | the timestamp of this value, primary key |
| `FID` | varchar(20) | no | the feature ID, foreign key in the `USER_FEATURE` table, primary key |
| `VALUE` | text | yes | value of the feature, the data type is defined at `USER_FEATURE.DATA_TYPE` |


## `USER_FUNCTION` Table

This table will serve as a method for versioning the functions used to populate user-specific features.

| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `FUNC_ID` | varchar(20) | no | unique name of the function, primary key |
| `UID` | varchar(20) | no | user id, primary key, foreign key of 'USER' table |
| `FUNC_TYPE` | varchar(20) | no | "fillin", or "derive" |
| `DESCRIPTION` | text | no | detailed description of the function |

## `ML_JOB` Table

This table holds all the user defined ML jobs currently registered in the system.

| Field Name | Data Type | Allow Nulls | Field Description |
|:----------:|:---------:|:-----------:|:-----------------:|
| `JID` | varchar(20) | no | job id, primary key |
| `UID` | varchar(20) | no | user id, primary key, foreign key of 'USER' table |
| `INPUT` | json | no | define the data sources used as input |
| `OUTPUT` | json | yes | define the output DBLink(s) to report results |
| `ML_MODULE` | json | no | define the ML module used in training or prediction, the module code can be located in the ML module library |