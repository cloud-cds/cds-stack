# Software Design Specification [Real-time Sepsis Early Warning System (project name TBD) release 1]

## Revision History

| Version |     Name    | Reason for Changes |    Date    |
|:-------:|:-----------:|:------------------:|:----------:|
| 0.1     | Andong Zhan | Initial Revision   | 08/12/2015 |
| 0.2     | Andong Zhan | Change DB schema based on the discussion with Narges and Katie   | 08/12/2015 |
| 0.3     | Andong Zhan | Finalize DB schema based on the test on MIMIC2   | 08/12/2015 |
| 0.4.0     | Jose Nino | Change design doc to Markdown file in order to track revisions via version control. Add design specifications for the **DataSource architecture** based on discussion with Andong on 09/04/2015 | 09/05/2015 |

## Approved By

*Approvals should be obtained from project manager, and all developers working on the project.*

| Version |     Name    | Reason for Changes |    Date    |
|:-------:|:-----------:|:------------------:|:----------:|

##  Introduction

### Purpose

This design will detail the implementation of the requirements as defined in the Software Requirements Specification – [project name TBD].

### System Overview

This project develops a real-time Sepsis early warning system to predict risk of adverse events using data that are routinely collected in the EHR. It implements a unified data source to extract Sepsis related features real-timely from EHR and a machine-learning framework to generate a Targeted Real-Time Early Warning score (TREWScore) by using the data form the data source.

### Design Map

*TBD - Summarize the information contained within this document or the family of design artifacts. Define all major design artifacts and/or major sections of this document and if appropriate, provide a brief summary of each.  Discuss any significant relationships between design artifacts and other project artifacts.*

### Definitions and Acronyms

- **Source System**: the systems that provide the RAW data for our system. That is the EMR systems that provide us with data, eg. the Epic Database at Howard County.
-  **Measured Featured**: features that come from the source system. This includes features which might have values that are filled in in the RAW to CDM pipeline.
-  **Derived Feature**: features that are not present in the source systems.
-  **CDM**: the Common Data Model. The agreed upon, source agnostic, data model that is shared by all applications.
-  **CDM Feature**: a feature present in CDM.
-  **DBLink**: module used to connect to and extract data from a source system.
-  **DataSource**: the component of the system that provides a unified logical and physical platform for the system's data.
-  **ML Core**: the component of the system that deals with the registered applications that make use of the data stored in the DataSource.

##  Design Considerations

### Assumptions

TBD

### Constraints

TBD

### System Environment

The system is cross platform and can reside in the PCMS or Linux system. The database used to store the data will be PostgreSQL.  

### Design Methodology
*TBD (Optional) - Summarize the approach that will be used to create and evolve the designs for this system. Cover any processes, conventions, policies, techniques or other issues which will guide design work.*

### Functionality

#### Features

1.  Offline training
  -  Caching online prediction
  -  Fill in missing data
  -  Learn model parameters
  -  Create new derived features
  -  Statistical reports
  -  Schedule
    *  Periodically, e.g., weekly
    *  Manually

2.  Online training
  -  Generate predication online
  -  Pull new data, store or cache result, return result to 3rd party DB.

#### High-Level Dataflow

#####  Initialization:
`[datasource]`


##### Offline Training:
`[datasource]`

- Common: the following data flow is triggered manually or by scheduler (e.g., weekly)
  1.  Copy 3rd party EMR data into RAW table; essentially a replica of the source data (or as close as a replica as permitted by the source system).
  2.  Transform measured features in RAW table to source agnostic measured features in CDM table.
  3.  Fill-in missing values for transformed measured features.
  4.  Populate calculated features in CDM.
  5.  Generate statistical reports (*optional*)
- App-Specific: the following data flow is triggered manually or by an app-specific scheduler.
  1.  Grab features from CDM (optional)
  2.  Fill-in missing values for CDM features (optional)
  3.  Calculate app-specific features (optional)
  4.  Build app-specific data source.

`[ml core]`

1.  Obtain data from the app-specific data source.
2.  Train the model.
3.  Return learned parameters and predictions.

`[datasource]`

1.  Save learned parameters and predictions to app-specific datasource.

##### Online Training:

`[datasource]`

- Common:
  1.  Polling or listening on new raw feature values and cache them
  2.  Transform the new raw feature values into common format
  3.  Fill-in missing values, calculate derived features,  and cache common feature values
  4.  Send new common feature values to registered app
- App-specific:
  1.  Fill-in missing values (optional)
  2.  Calculate app-specific features (optional)

`[ml core]`

3.  Predict based on the model
4.  Return prediction and app-specific feature values back to DataSource

`[datasource]`

1.  Send prediction back to the DBLink or web service.
2.  Save cached raw features, common features, and app-specific features to respective data source.
3. Save learned parameters and predictions to the app-specific data source.

### Risks and Volatile Areas
*(TBD) None have been identified.*

## Architecture Overview

### Overview

The architecture of this system is depicted by Figure 1. The system can be divided into two components: the DataSource and the ML Core. The goal of this design is two-fold: 1) separates ML-based analysis from the data integration logic; 2) provides unified data access APIs to overcome the heterogeneity among different EMR databases.

### DataSource

The **DataSource** component of the system provides a unified logical and physical platform for the system's data. The DataSource platform deals with EMR data in three logical divisions: RAW, CDM, and App-Specific; which are further described below. The DataSource component provides a ready-to-use API for machine-learning tasks. It implements a web service to provide REST (http-based) APIs for data access.

#### RAW

The DataSource uses DBLink modules to extract data from health care providers to create a replica (or as close to a replica as allowed by the source system) of the source data internally. The objective of this data replication is to have readily available data sources in the event of change in the downstream processing of the data. It is important to point out that the data in the RAW logical division is source specific and is often very heterogenous between different source systems.

#### Common Data Model (CDM)

The CDM is the strength of the DataSource component. It is created by a set of **minimal transformations** over the RAW data to create a singular common data model. This data model will by the starting point that users can use to create their App-specific data sources. System administrators will define minimal transformations over the RAW data sources in order to map the disparate RAW data to common terminology in the CDM. There are three types of minimal transformations, which happen sequentially one after the other:

1.  **Transform RAW features into CDM.** That is adjust data types, feature name, units of measurement in order to have a common language for measurements from source systems. For instance, System *A* might record Sodium levels on a column called `NA_levels` in `milligrams` as a `String`, while System *B* might do it in a column called `sodium` in `grams` as a `Double`. The purpose of this first pass of transformations is to convert all of these data sources into the same data types, with the same units of measurement, under the same feature name. All of these features can be thought of as **Measured Features**, because they are values that were recorded from a patient.
2.  **Fill-in values for the transformed Measured Features.** If the System Administrators have established that all users of CDM might benefit from a particular inference scheme that will fill-in missing values for a particular measured feature, the administrators will define these method as a function to be executed in the transformation of data from RAW to CDM.
3.  **Compute Common Calculated Features.** **Calculated Features** are features that have been derived from one or more measured features. Moreover, the calculated features that reside in CDM are features that the system administrators have deemed valuable to multiple Apps, thus they pertain in CDM.

The CDM will then expose a dictionary of features that users can use in their specific applications. Therefore, CDM is the starting point for any researcher using DataSource.

#### App-Specific

Researchers can define 3 types of data based on the features exposed by CDM via a configuration file:

1.  **Direct Copy from CDM**: is a feature that is going to be directly used from its CDM definition.
2.  **Manipulation of CDM features**: is a CDM feature that is going to be manipulated in the App-specific data source. For instance, thresholding a feature, or defining a function to fill-in missing values in the CDM feature.
3.  **New features**: features that are not defined in CDM.

The researcher will define their features, and functions used to generate them (in the case of `2` and `3`) and the DataSource platform will take care of generating the app-specific data source, which will then expose an API to access the features from the ML Core.

### ML Core

The ML Core component is where the machine-learning applications live. Each application has two modes: training mode and prediction mode. In training mode, the ML Core queries the entire data records for a specific patient from the Data Source in a batch way, and then train the application's model. In prediction mode, the ML Core queries data records within the latest window for a particular patient and formulates a prediction.

## CDM Physical Description

As mentioned in the architecture overview, CDM is a common data model that will be the starting point for constructing a variety of machine learning applications. Therefore, its schema has been design with the philosophy that CDM will be *~98% static*. That is, CDM will experience changes as the system matures, but we expect a low-frequent and small number of changes to occur. Therefore, CDM will benefit from being physically stored as a series of tables in a relational database; PostrgreSQL for this particular system. The following is a diagram of what will constitute what we have called **CDM**.

![image](images/database_schema_0909.png)

[Read more about database table definitions](database_schema.md)
