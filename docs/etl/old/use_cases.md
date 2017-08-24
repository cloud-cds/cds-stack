# Use Cases

## Install the system (admin)
1. Prerequisites
  1. Install PostgreSQL database.
  2. Create a PostgreSQL user for this system

2. Create database schema
  1. Create database schema in PostgreSQL and create tables (load datasource.ini)

## Start/Stop the server
1. Start/Stop the web server
2. Start/Stop the scheduler for both DBLink data loading tasks submitted by admin and offline training tasks submitted by app users
3. Start/Stop the streaming service, which includes streaming DBLink submitted by admin and online prediction tasks submitted by app users

## Submit a DBLink job (admin)
1. Edit DBLink configuration files
  1. Specify DBLink id
  2. Specify DBLink type: scheduled | streaming
  3. If it is a scheduled DBLink, edit a schedule for this DBLink
  4. Specify data load type: incremental | full
  5. Specify connection type: postgresql | web
  6. Specify connection settings in JSON format
  7. Specify how to extract feature in feature_list.csv
2. Submit DBLink to the server
  1. The server load the DBLink config file
  2. Create the DBLink item in `DBLINK` table
  3. Load features in feature_list.csv one by one; make sure each feature exists in `CDM_FEATURE` table; then save the feature into `DBLINK_FEATURE`

## Add/Delete a user (admin)
1. Create user with id and password
2. Delete user
  1. Delete the uesr item in `USER` table
  2. Delete apps belonged to this user
  3. When deleting app, remove all app-specific features related to this app

## Add/Delete an ML job (user)
1. Edit ML job config file
  1. Define job id
  2. Define job schedule
  3. Define input data range
  4. Define output DBLink(s)
  5. Define ML module
2. Submit ML job
  1. Submit ML config file
  2. Submit ML module files
(Delete user-specific feature can be done by calling client API with the job id as input; or set the job to deprecated in case it may be used in the future)

## Add/Delete user-specific feature (user)
1. Edit user feature config file
  1. Define feature id
  2. Define how to calculate this feature, including CDM feature input, fillin function, derive function
2. Submit user feature config file through client API
(Delete user-specific feature can be done by calling client API with the feature id as input)

## Add/Delete user-specific function (user)
1. Edit user feature config file
  1. Define function id and type ("fillin" or "derive")
  2. Write the function code
2. Submit the config file and code
