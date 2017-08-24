# Note of HC_EPIC at rambo

## Tables

```
HC_EPIC=# \d
                 List of relations
 Schema |           Name           | Type  | Owner  
--------+--------------------------+-------+--------
 public | ADT_Feed                 | table | matt
 public | Demographics             | table | matt
 public | Diagnoses                | table | matt
 public | FlowsheetValue           | table | narges
 public | LDAs                     | table | matt
 public | Labs                     | table | narges
 public | MedicalHistory           | table | matt
 public | MedicationAdministration | table | narges
 public | OrderMed                 | table | zad
 public | ProblemList              | table | matt
(10 rows)
```

### ADT_Feed (Admission Discharge Transfer Feed)
```
HC_EPIC=# \d+ "ADT_Feed" 
                               Table "public.ADT_Feed"
       Column        |           Type           | Modifiers | Storage  | Description 
---------------------+--------------------------+-----------+----------+-------------
 CSN_ID              | uuid                     |           | plain    | 
 EventType           | character varying        |           | extended | 
 effective_time      | timestamp with time zone |           | plain    | 
 PatientClassAtEvent | character varying        |           | extended | 
 DEPARTMENT_NAME     | character varying        |           | extended | 
 ROOM_NAME           | character varying        |           | extended | 

HC_EPIC=# select * from "ADT_Feed" order by "CSN_ID" limit 10;
                CSN_ID                |  EventType   |     effective_time     | PatientClassAtEvent |    DEPARTMENT_NAME    | ROOM_NAME 
--------------------------------------+--------------+------------------------+---------------------+-----------------------+-----------
 000181ec-3127-4ada-b44d-e768dbd2e728 | Admission    | 2015-06-08 16:45:00-04 | Emergency           | HCGH EMERGENCY-ADULTS | ED24
 000181ec-3127-4ada-b44d-e768dbd2e728 | Discharge    | 2015-06-16 16:14:00-04 | Inpatient           | HCGH 2S JAS           | 2214
 000181ec-3127-4ada-b44d-e768dbd2e728 | Transfer In  | 2015-06-09 00:04:00-04 | Inpatient           | HCGH 2S JAS           | 2214
 000181ec-3127-4ada-b44d-e768dbd2e728 | Transfer Out | 2015-06-09 00:04:00-04 | Inpatient           | HCGH EMERGENCY-ADULTS | ED24
 0004589b-8c95-4d93-ba44-7679ea6851ec | Transfer Out | 2014-09-17 23:50:00-04 | Inpatient           | HCGH EMERGENCY-ADULTS | ED01
 0004589b-8c95-4d93-ba44-7679ea6851ec | Admission    | 2014-09-17 19:20:00-04 | Emergency           | HCGH EMERGENCY-ADULTS | ED01
 0004589b-8c95-4d93-ba44-7679ea6851ec | Discharge    | 2014-09-27 14:20:00-04 | Inpatient           | HCGH 2P SURGERY       | 2115
 0004589b-8c95-4d93-ba44-7679ea6851ec | Transfer In  | 2014-09-17 23:50:00-04 | Inpatient           | HCGH 2P SURGERY       | 2115
 0005e686-e721-4572-bf19-4319459902a0 | Transfer Out | 2013-06-20 01:57:00-04 | Inpatient           | HCGH MAIN OR PERIOP   | HCGH MAIN
 0005e686-e721-4572-bf19-4319459902a0 | Admission    | 2013-06-17 07:26:00-04 | Emergency           | HCGH EMERGENCY-ADULTS | ED14
(10 rows) 
```

### Demographics
```
HC_EPIC=# \d+ "Demographics" 
                             Table "public.Demographics"
        Column        |           Type           | Modifiers | Storage  | Description 
----------------------+--------------------------+-----------+----------+-------------
 CSN_ID               | uuid                     |           | plain    | 
 pat_id               | uuid                     |           | plain    | 
 ADT_ARRIVAL_TIME     | timestamp with time zone |           | plain    | 
 ED_DEPARTURE_TIME    | timestamp with time zone |           | plain    | 
 HOSP_ADMSN_TIME      | timestamp with time zone |           | plain    | 
 HOSP_DISCH_TIME      | timestamp with time zone |           | plain    | 
 AgeDuringVisit       | character varying        |           | extended | 
 Gender               | character varying        | not null  | extended | 
 IsEDPatient          | integer                  | not null  | plain    | 
 AdmittingDepartment  | character varying        |           | extended | 
 DischargeDepartment  | character varying        |           | extended | 
 DischargeDisposition | character varying        |           | extended | 

 HC_EPIC=# select * from "Demographics" order by "CSN_ID" limit 10;
                CSN_ID                |                pat_id                |    ADT_ARRIVAL_TIME    |   ED_DEPARTURE_TIME    |    HOSP_ADMSN_TIME    
 |    HOSP_DISCH_TIME     | AgeDuringVisit | Gender | IsEDPatient | AdmittingDepartment | DischargeDepartment |    DischargeDisposition     
--------------------------------------+--------------------------------------+------------------------+------------------------+-----------------------
-+------------------------+----------------+--------+-------------+---------------------+---------------------+-----------------------------
 000181ec-3127-4ada-b44d-e768dbd2e728 | 58058f88-33a3-49bb-a889-7d1c6a6cabf6 | 2015-06-08 16:27:00-04 | 2015-06-09 00:04:00-04 | 2015-06-08 16:45:00-04
 | 2015-06-16 16:14:00-04 | 83             | Male   |           1 | HCGH 2S JAS         | HCGH 2S JAS         | Home or Self Care
 0004589b-8c95-4d93-ba44-7679ea6851ec | ea3a868d-3559-41ab-a8b0-50a8da7fd10f | 2014-09-17 19:19:00-04 | 2014-09-17 23:50:00-04 | 2014-09-17 19:20:00-04
 | 2014-09-27 14:20:00-04 | 69             | Female |           1 | HCGH 2P SURGERY     | HCGH 2P SURGERY     | Home or Self Care
 0005e686-e721-4572-bf19-4319459902a0 | 7246e395-b7f7-42fb-9e1f-78be19df63c9 | 2013-06-17 07:17:00-04 | 2013-06-17 17:42:00-04 | 2013-06-17 07:26:00-04
 | 2013-06-22 16:45:00-04 | 86             | Female |           1 | HCGH 2S JAS         | HCGH 2S JAS         | Home-Health Care Svc
 000773ec-d6b2-4d7e-8e40-35b3f185897a | 9d763467-47fd-44fd-8072-b7b11c72e3b9 | 2015-05-16 01:36:00-04 | 2015-05-16 08:18:00-04 | 2015-05-16 01:42:00-04
 | 2015-05-18 14:35:00-04 | 73             | Female |           1 | HCGH 2S JAS         | HCGH 2S JAS         | Home or Self Care
 000a6e27-b0d0-406e-9987-6cdfa3f51b39 | 27121901-c6fc-4607-a0c9-0079a2eb1e14 | 2014-09-18 16:14:00-04 | 2014-09-18 23:02:00-04 | 2014-09-18 17:42:00-04
 | 2014-09-19 11:08:00-04 | 35             | Female |           1 | HCGH 1N OBSERVATION | HCGH 1N OBSERVATION | Left Against Medical Advice
 000a9a18-c619-4996-a5af-b3f3945f1084 | 39fe2c5f-a4dd-4b02-aefc-b734a8ade801 | 2013-08-14 13:29:00-04 | 2013-08-15 04:12:00-04 | 2013-08-14 17:14:00-04
 | 2013-08-25 21:34:00-04 | 75             | Male   |           1 | HCGH 2P SURGERY     | HCGH 2P SURGERY     | Left Against Medical Advice
 000e0936-2b3c-4b1d-b558-aa9b5544cd82 | 0f7b33ae-5871-464c-a9ac-9e818b42b8c8 | 2015-03-07 10:30:00-05 | 2015-03-07 20:50:00-05 | 2015-03-07 11:34:00-05
 | 2015-03-16 16:28:00-04 | 65             | Male   |           1 | HCGH 3P TELEMETRY   | HCGH 4 SOUTH/ONC    | Home or Self Care
 0010fa66-56d3-4393-b154-0974d0d66b48 | 73db2d94-5c85-44dc-9eae-bd152d534f96 | 2014-11-06 20:53:00-05 | 2014-11-06 23:49:00-05 | 2014-11-06 21:03:00-05
 | 2014-11-15 11:30:00-05 | 46             | Female |           1 | HCGH 4 SOUTH/ONC    | HCGH 4 SOUTH/ONC    | Home or Self Care
 0011d638-c5d5-4d2b-976c-08035678741d | 069ca9d7-8606-4e15-8767-dca638c41b5b | 2015-04-01 10:45:00-04 | 2015-04-01 19:50:00-04 | 2015-04-01 15:12:00-04
 | 2015-04-02 19:41:00-04 | 61             | Female |           1 | HCGH 1N OBSERVATION | HCGH 1N OBSERVATION | Home or Self Care
 0014be53-9aea-47b7-8f0e-e4c28886e9eb | a9585fbf-2a75-4854-a1aa-6c3beae0b638 | 2014-01-03 23:11:00-05 | 2014-01-04 07:38:00-05 | 2014-01-04 00:41:00-05
 | 2014-01-06 14:12:00-05 | 41             | Female |           1 | HCGH 1N OBSERVATION | HCGH 2S JAS         | Home or Self Care


```

### Diagnosis
```
HC_EPIC=# \d+ "Diagnoses" 
                           Table "public.Diagnoses"
       Column        |       Type        | Modifiers | Storage  | Description 
---------------------+-------------------+-----------+----------+-------------
 CSN_ID              | uuid              |           | plain    | 
 DX_ID               | numeric           | not null  | main     | Diagnosis ID
 DX_ED_YN            | character varying |           | extended | Definitively identifies an encounter diagnosis (18400) as being an ED clinical impression.
 PRIMARY_DX_YN       | character varying |           | extended | 
 line                | integer           | not null  | plain    | 
 diagName            | character varying |           | extended | 
 Code                | character varying |           | extended | 
 Annotation          | character varying |           | extended | 
 COMMENTS            | character varying |           | extended | 
 DX_CHRONIC_YN       | character varying |           | extended | 
 ICD-9 Code category | character varying |           | extended | 

 HC_EPIC=# select * from "Diagnoses" order by "CSN_ID" limit 10;
                CSN_ID                |  DX_ID  | DX_ED_YN | PRIMARY_DX_YN | line |                diagName                 |  Code  | Annotation | COM
MENTS | DX_CHRONIC_YN |                     ICD-9 Code category                     
--------------------------------------+---------+----------+---------------+------+-----------------------------------------+--------+------------+----
------+---------------+-------------------------------------------------------------
 0004589b-8c95-4d93-ba44-7679ea6851ec |  548349 | Y        | Y             |    1 | COPD with acute exacerbation            | 491.21 |            |    
      | N             | Diseases of the respiratory system
 0005e686-e721-4572-bf19-4319459902a0 |   65452 | Y        | Y             |    1 | Low back pain                           | 724.2  |            |    
      | N             | Diseases of the musculoskeltal system and connective tissue
 0005e686-e721-4572-bf19-4319459902a0 |  189918 | Y        | N             |    3 | Constipation                            | 564.00 |            |    
      | N             | Diseases of the digestive system
 0005e686-e721-4572-bf19-4319459902a0 |  258598 | Y        | N             |    4 | Inability to walk                       | 719.7  |            |    
      | N             | Diseases of the musculoskeltal system and connective tissue
 0005e686-e721-4572-bf19-4319459902a0 |  215058 | Y        | N             |    2 | Muscle spasm                            | 728.85 |            |    
      | N             | Diseases of the musculoskeltal system and connective tissue
 000773ec-d6b2-4d7e-8e40-35b3f185897a |  118920 | Y        | N             |    1 | Altered mental status                   | 780.97 |            |    
      | N             | Symptoms, signs, and ill-defined conditions
 000a6e27-b0d0-406e-9987-6cdfa3f51b39 |  125822 | Y        | Y             |    1 | Chest pain                              | 786.50 |            |    
      | N             | Symptoms, signs, and ill-defined conditions
 000a9a18-c619-4996-a5af-b3f3945f1084 | 1031683 | Y        | N             |    2 | Internal jugular vein thrombosis, right | 453.86 |            |    
      | N             | Diseases of the circulatory system
 000a9a18-c619-4996-a5af-b3f3945f1084 |  118671 | Y        | Y             |    1 | Chronic pain syndrome                   | 338.4  |            |    
      | N             | Disease of the nervous system
 000e0936-2b3c-4b1d-b558-aa9b5544cd82 |  218337 | Y        | Y             |    1 | New onset atrial fibrillation           | 427.31 |            |    
      | N             | Diseases of the circulatory system
(10 rows)

```

### FlowsheetValue (Multiple measurements per day)

```
HC_EPIC=# \d+ "FlowsheetValue" 
                              Table "public.FlowsheetValue"
        Column        |            Type             | Modifiers | Storage  | Description 
----------------------+-----------------------------+-----------+----------+-------------
 CSN_ID               | uuid                        |           | plain    | 
 FLO_MEAS_NAME        | character varying(100)      |           | extended | 
 DISP_NAME            | character varying(100)      |           | extended | 
 FLO_MEAS_ID          | character varying(15)       |           | extended | 
 TimeTaken            | timestamp without time zone |           | plain    | 
 FlowsheetVAlueType   | character varying(100)      |           | extended | 
 ConvertedWeightValue | real                        |           | plain    | 
 Value                | character varying(150)      |           | extended | 

 HC_EPIC=# select * from "FlowsheetValue" order by "CSN_ID" limit 10;
                CSN_ID                |         FLO_MEAS_NAME          |        DISP_NAME         | FLO_MEAS_ID |      TimeTaken      | FlowsheetVAlueT
ype | ConvertedWeightValue | Value  
--------------------------------------+--------------------------------+--------------------------+-------------+---------------------+----------------
----+----------------------+--------
 000181ec-3127-4ada-b44d-e768dbd2e728 | RESPIRATIONS                   | Resp                     | 9           | 2015-06-15 15:00:00 | Respiratory Rat
e   |                      | 18
 000181ec-3127-4ada-b44d-e768dbd2e728 | PULSE                          | Pulse                    | 8           | 2015-06-15 03:26:00 | Pulse          
    |                      | 56
 000181ec-3127-4ada-b44d-e768dbd2e728 | PULSE                          | Pulse                    | 8           | 2015-06-12 19:32:00 | Pulse          
    |                      | 58
 000181ec-3127-4ada-b44d-e768dbd2e728 | R CPN GLASGOW COMA SCALE SCORE | Glasgow Coma Scale Score | 401001      | 2015-06-14 20:49:00 | Glasgow Coma Sc
ore |                      | 14
 000181ec-3127-4ada-b44d-e768dbd2e728 | BLOOD PRESSURE                 | BP                       | 5           | 2015-06-09 15:23:00 | Blood Pressure 
    |                      | 108/75
 000181ec-3127-4ada-b44d-e768dbd2e728 | BLOOD PRESSURE                 | BP                       | 5           | 2015-06-15 23:32:00 | Blood Pressure 
    |                      | 121/62
 000181ec-3127-4ada-b44d-e768dbd2e728 | RESPIRATIONS                   | Resp                     | 9           | 2015-06-10 15:26:00 | Respiratory Rat
e   |                      | 20
 000181ec-3127-4ada-b44d-e768dbd2e728 | RESPIRATIONS                   | Resp                     | 9           | 2015-06-11 17:04:00 | Respiratory Rat
e   |                      | 18
 000181ec-3127-4ada-b44d-e768dbd2e728 | PULSE OXIMETRY                 | SpO2                     | 10          | 2015-06-09 03:12:00 | SPO2           
    |                      | 98
 000181ec-3127-4ada-b44d-e768dbd2e728 | PULSE                          | Pulse                    | 8           | 2015-06-16 03:32:00 | Pulse          
    |                      | 57

```

### LDAs (Line, Drain, Airways)
```
HC_EPIC=# \d+ "LDAs" 
                                Table "public.LDAs"
       Column       |           Type           | Modifiers | Storage  | Description 
--------------------+--------------------------+-----------+----------+-------------
 PAT_ID             | uuid                     |           | plain    | 
 PLACEMENT_INSTANT  | timestamp with time zone |           | plain    | 
 FLO_MEAS_NAME      | character varying        |           | extended | 
 DISP_NAME          | character varying        |           | extended | 
 PROPERTIES_DISPLAY | character varying        |           | extended | 
 SITE               | character varying        |           | extended | 
 REMOVAL_DTTM       | timestamp with time zone |           | plain    | 

HC_EPIC=# select * from "LDAs" order by "PAT_ID" limit 10;
                PAT_ID                |   PLACEMENT_INSTANT    |         FLO_MEAS_NAME         |     DISP_NAME      |                                  
                                                                                     PROPERTIES_DISPLAY                                                
                                                                       |      SITE       |      REMOVAL_DTTM      
--------------------------------------+------------------------+-------------------------------+--------------------+----------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------+-----------------+------------------------
 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 2014-05-22 00:00:00-04 | LDA JHM IP WOUND              | Wound              | Resolved date/Resolved time: 07/1
6/14 1006  Date First Assessed: 05/22/14   Wound Type: Chronic Ulcer  Etiology: Venous Ulcer  Location: Leg  Location Orientation: Left;Medial         
                                                                       |                 | 2014-07-16 10:06:00-04
 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 2014-06-20 13:00:00-04 | LDA JHM IP PICC SINGLE LUMEN  | PICC Single Lumen  | Placement Date/Time: 06/20/14 130
0   Hand Hygiene: Yes  Proper Skin Antisepsis: Chlorhexidine  Sterile Full Body Drape: Yes  Maximal Sterile Barriers: Yes  Procedure Type: Elective  Mo
re than 3 puncture attempts: No  Inserted by (name of staff): dr morto | Other (Comment) | 
 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 2014-06-16 14:03:00-04 | LDA JHM IP PERIPHERAL IV      | Peripheral IV      | Removal Date/Time: 06/20/14 1829 
 Placement Date/Time: 06/16/14 1403   Orientation: Right  Location: Forearm  Size (Gauge): 18 G  Inserted by (name of staff): amanda  Insertion attempt
s: 1                                                                   | Forearm         | 2014-06-20 18:29:00-04
 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 2014-06-19 10:59:00-04 | LDA JHM IP PERIPHERAL IV      | Peripheral IV      | Removal Date/Time: 06/20/14 1829 
 Placement Date/Time: 06/19/14 1059   Orientation: Left  Location: Forearm  Size (Gauge): 22 G  Insertion attempts: 1                                  
                                                                       | Forearm         | 2014-06-20 18:29:00-04
 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 2014-06-16 09:50:00-04 | LDA JHM IP PERIPHERAL IV      | Peripheral IV      | Removal Date/Time: 06/20/14 1829 
 Placement Date/Time: 06/16/14 0950   Orientation: Left  Location: Antecubital  Size (Gauge): 20 G  Insertion attempts: 1                              
                                                                       | Antecubital     | 2014-06-20 18:29:00-04
 001d8bac-1aa6-4b25-8ec3-2de4a13ef114 | 2015-05-16 14:04:00-04 | LDA JHM IP SURGICAL INCISION  | Surgical Incision  | Date First Assessed/Time First As
sessed: 05/16/15 1404   Location: Abdomen                                                                                                              
                                                                       |                 | 
 001d8bac-1aa6-4b25-8ec3-2de4a13ef114 | 2015-05-16 00:09:00-04 | LDA JHM IP PERIPHERAL IV      | Peripheral IV      | Removal Date/Time: 05/18/15 1824 
 Placement Date/Time: 05/16/15 0009   Orientation: Right  Location: Antecubital  Size (Gauge): 20 G  Inserted by (name of staff): justin harab  Inserti
on attempts: 1  Removal Reason: Drainage                               | Antecubital     | 2015-05-18 18:24:00-04
 001d8bac-1aa6-4b25-8ec3-2de4a13ef114 | 2014-03-11 09:59:00-04 | LDA JHM IP PERIPHERAL IV      | Peripheral IV      | Removal Date/Time: 07/12/14 1645 
 Placement Date/Time: 03/11/14 0959   Description (Comment): prep with chlorhexidine  Orientation: Right  Location: Antecubital  Size (Gauge): 20 G  In
serted by (name of staff): PCT Daniel Overmier  Insertion attempts: 1  | Antecubital     | 2014-07-12 16:45:00-04
 001d8bac-1aa6-4b25-8ec3-2de4a13ef114 | 2015-05-16 14:05:00-04 | LDA JHM IP OPEN/CLOSED DRAINS | Open/Closed Drains | Placement Date/Time: 05/16/15 140
5   Inserted by (name of staff): Javeed Khan, MD  Tube Number: 1  Orientation: Anterior  Location: Abdomen  Drain Tube Type: Bulb                      
                                                                       |                 | 
 001d8bac-1aa6-4b25-8ec3-2de4a13ef114 | 2015-05-16 13:07:00-04 | LDA JHM IP ETT                | ETT                | Removal Date/Time: 05/16/15 1421 
 Placement Date/Time: 05/16/15 1307   Mask Ventilation: Not attempted  Technique: Rapid sequence  Laryngoscope: MacIntosh blade  Blade Size: 4  View:: 
Grade 1, vocal cord seen  Insertion attempts: 1  Placement Verifica... |                 | 2015-05-16 14:21:00-04
(10 rows)

```

### Labs (multiple measurements per day)
```
HC_EPIC=# \d+ "Labs" 
                                 Table "public.Labs"
      Column       |            Type             | Modifiers | Storage  | Description 
-------------------+-----------------------------+-----------+----------+-------------
 CSN_ID            | uuid                        |           | plain    | 
 BASE_NAME         | character varying(50)       |           | extended | 
 NAME              | character varying(50)       |           | extended | 
 EXTERNAL_NAME     | character varying(50)       |           | extended | 
 RESULT_TIME       | timestamp without time zone |           | plain    | 
 REFERENCE_UNIT    | character varying(30)       |           | extended | 
 ResultValue       | character varying(200)      |           | extended | 
 COMPONENT_COMMENT | character varying(200)      |           | extended | 
 ORDER_PROC_ID     | character varying(15)       |           | extended | 
Has OIDs: no

HC_EPIC=# select * from "Labs" order by "CSN_ID","RESULT_TIME" limit 10;
                CSN_ID                | BASE_NAME |            NAME             |      EXTERNAL_NAME      |     RESULT_TIME     | REFERENCE_UNIT | Resu
ltValue | COMPONENT_COMMENT | ORDER_PROC_ID 
--------------------------------------+-----------+-----------------------------+-------------------------+---------------------+----------------+-----
--------+-------------------+---------------
 000181ec-3127-4ada-b44d-e768dbd2e728 | WBC       | WHITE BLOOD CELL COUNT      | White Blood Cell Count  | 2015-06-08 19:50:00 | K/mm3          | 11.8
6       |                   | 211976950
 000181ec-3127-4ada-b44d-e768dbd2e728 | RDW       | RED CELL DISTRIBUTION WIDTH | RBC Distribution Width  | 2015-06-08 19:50:00 | %              | 14.5
        |                   | 211976950
 000181ec-3127-4ada-b44d-e768dbd2e728 | MCH       | MEAN CORPUSCULAR HGB        | Mean Corpus HgB         | 2015-06-08 19:50:00 | pg             | 29.5
        |                   | 211976950
 000181ec-3127-4ada-b44d-e768dbd2e728 | PLT       | PLATELET COUNT              | Platelet Count          | 2015-06-08 19:50:00 | K/mm3          | 133 
        |                   | 211976950
 000181ec-3127-4ada-b44d-e768dbd2e728 | MPV       | MEAN PLATELET VOLUME        | Mean Platelet Volume    | 2015-06-08 19:50:00 | fL             | 10.1
        |                   | 211976950
 000181ec-3127-4ada-b44d-e768dbd2e728 | NRBC      | NRBC NUMBER                 | Nucleated RBC Number    | 2015-06-08 19:50:00 | K/mm3          | 0   
        |                   | 211976950
 000181ec-3127-4ada-b44d-e768dbd2e728 | HGB       | HEMOGLOBIN                  | Hemoglobin              | 2015-06-08 19:50:00 | gm/dL          | 11.3
        |                   | 211976950
 000181ec-3127-4ada-b44d-e768dbd2e728 | HCT       | HEMATOCRIT                  | Hematocrit              | 2015-06-08 19:50:00 | %              | 35.2
        |                   | 211976950
 000181ec-3127-4ada-b44d-e768dbd2e728 | MCV       | MEAN CORPUSCULAR VOLUME     | Mean Corpuscular Volume | 2015-06-08 19:50:00 | fL             | 92  
        |                   | 211976950
 000181ec-3127-4ada-b44d-e768dbd2e728 | RBC       | RED BLOOD CELL COUNT        | Red Blood Cell Count    | 2015-06-08 19:50:00 | M/mm3          | 3.83
        |                   | 211976950
(10 rows)


```

### MedicalHistory (Diagnosis History)
```
HC_EPIC=# \d+ "MedicalHistory" 
                            Table "public.MedicalHistory"
       Column        |           Type           | Modifiers | Storage  | Description 
---------------------+--------------------------+-----------+----------+-------------
 CSN_ID              | uuid                     |           | plain    | 
 PATIENTID           | uuid                     |           | plain    | 
 DEPARTMENTID        | character varying        |           | extended | 
 diagName            | character varying        |           | extended | 
 Code                | character varying        |           | extended | 
 ICD-9 Code category | character varying        |           | extended | 
 COMMENTS            | character varying        |           | extended | 
 Annotation          | character varying        |           | extended | 
 Medical_Hx_Date     | character varying        |           | extended | 
 ENC_Date            | timestamp with time zone |           | plain    | 

 HC_EPIC=# select * from "MedicalHistory" order by "PATIENTID", "CSN_ID" limit 10;
                CSN_ID                |              PATIENTID               | DEPARTMENTID |                         diagName                         
 |  Code  |             ICD-9 Code category             | COMMENTS | Annotation | Medical_Hx_Date |        ENC_Date        
--------------------------------------+--------------------------------------+--------------+----------------------------------------------------------
-+--------+---------------------------------------------+----------+------------+-----------------+------------------------
 06c78237-cb6c-4c9d-99a8-2570517eb823 | 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 110300130    | Balance problem                                          
 | 781.99 | Symptoms, signs, and ill-defined conditions |          |            |                 | 2014-06-16 10:52:00-04
 06c78237-cb6c-4c9d-99a8-2570517eb823 | 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 110300130    | HTN (hypertension)                                       
 | 401.9  | Diseases of the circulatory system          |          |            |                 | 2014-06-16 10:52:00-04
 06c78237-cb6c-4c9d-99a8-2570517eb823 | 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 110300130    | Dizziness                                                
 | 780.4  | Symptoms, signs, and ill-defined conditions |          |            |                 | 2014-06-16 10:52:00-04
 06c78237-cb6c-4c9d-99a8-2570517eb823 | 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 110300130    | DVT (deep venous thrombosis)                             
 | 453.40 | Diseases of the circulatory system          |          |            | 1/2008          | 2014-06-16 10:52:00-04
 06c78237-cb6c-4c9d-99a8-2570517eb823 | 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 110300130    | Diverticulosis                                           
 | 562.10 | Diseases of the digestive system            |          |            | 3/11/2010       | 2014-06-16 10:52:00-04
 06c78237-cb6c-4c9d-99a8-2570517eb823 | 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 110300130    | Venous hypertension, chronic, with ulcer and inflammation
 | 459.33 | Diseases of the circulatory system          |          |            |                 | 2014-06-16 10:52:00-04
 06c78237-cb6c-4c9d-99a8-2570517eb823 | 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 110300130    | Ringing in ears                                          
 | 388.30 | Diseases of the sense organs                |          |            |                 | 2014-06-16 10:52:00-04
 06c78237-cb6c-4c9d-99a8-2570517eb823 | 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 110300130    | PE (pulmonary embolism)                                  
 | 415.19 | Diseases of the circulatory system          |          |            | 1/2008          | 2014-06-16 10:52:00-04
 06c78237-cb6c-4c9d-99a8-2570517eb823 | 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 110300130    | Venous stasis of lower extremity                         
 | 459.81 | Diseases of the circulatory system          |          |            |                 | 2014-06-16 10:52:00-04
 06c78237-cb6c-4c9d-99a8-2570517eb823 | 000c0ef4-b9fb-48d1-9447-5cfcf195fe98 | 110300130    | Edema, peripheral                                        
 | 782.3  | Symptoms, signs, and ill-defined conditions |          |            |                 | 2014-06-16 10:52:00-04
(10 rows)

```

### MedicationAdministration (Medication Events based on Medication Order)
```
HC_EPIC=# \d+ "MedicationAdministration" 
                       Table "public.MedicationAdministration"
      Column       |            Type             | Modifiers | Storage  | Description 
-------------------+-----------------------------+-----------+----------+-------------
 CSN_ID            | uuid                        |           | plain    | 
 display_name      | character varying(200)      |           | extended | 
 ORDER_INST        | timestamp without time zone |           | plain    | 
 TimeActionTaken   | timestamp without time zone |           | plain    | 
 ActionTaken       | character varying(50)       |           | extended | 
 MAR_ORIG_DUE_TM   | timestamp without time zone |           | plain    | 
 SCHEDULED_TIME    | timestamp without time zone |           | plain    | 
 MedRoute          | character varying(50)       |           | extended | 
 Dose              | real                        |           | plain    | 
 MedUnit           | character varying(50)       |           | extended | 
 MIN_DISCRETE_DOSE | real                        |           | plain    | 
 MAX_DISCRETE_DOSE | real                        |           | plain    | 

HC_EPIC=# select * from "MedicationAdministration" order by "CSN_ID" limit 10;
                CSN_ID                |                   display_name                   |     ORDER_INST      |   TimeActionTaken   | ActionTaken |   
MAR_ORIG_DUE_TM   |   SCHEDULED_TIME    |  MedRoute   | Dose | MedUnit | MIN_DISCRETE_DOSE | MAX_DISCRETE_DOSE 
--------------------------------------+--------------------------------------------------+---------------------+---------------------+-------------+---
------------------+---------------------+-------------+------+---------+-------------------+-------------------
 000181ec-3127-4ada-b44d-e768dbd2e728 | donepezil (ARICEPT) tablet 10 mg                 | 2015-06-09 06:50:00 | 2015-06-11 19:49:00 | Given       | 20
15-06-11 21:00:00 | 2015-06-11 21:00:00 | Oral        |   10 | mg      |                10 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | donepezil (ARICEPT) tablet 10 mg                 | 2015-06-09 06:50:00 | 2015-06-12 19:54:00 | Given       | 20
15-06-12 21:00:00 | 2015-06-12 21:00:00 | Oral        |   10 | mg      |                10 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | donepezil (ARICEPT) tablet 10 mg                 | 2015-06-09 06:50:00 | 2015-06-09 22:16:00 | Given       | 20
15-06-09 21:00:00 | 2015-06-09 21:00:00 | Oral        |   10 | mg      |                10 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | donepezil (ARICEPT) tablet 10 mg                 | 2015-06-09 06:50:00 | 2015-06-10 20:01:00 | Given       | 20
15-06-10 21:00:00 | 2015-06-10 21:00:00 | Oral        |   10 | mg      |                10 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | magnesium sulfate IVPB 1 g/100 mL D5W Premix     | 2015-06-16 12:06:00 | 2015-06-16 13:14:00 | Given       | 20
15-06-16 13:00:00 | 2015-06-16 13:00:00 | Intravenous |    1 | g       |                 1 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | vancomycin (VANCOCIN) IVPB 1 g/200 mL D5W Premix | 2015-06-08 23:08:00 | 2015-06-09 00:07:00 | New Bag     | 20
15-06-08 23:10:00 | 2015-06-08 23:10:00 | Intravenous |    1 | g       |                 1 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | acetaminophen (TYLENOL) tablet 650 mg            | 2015-06-08 20:33:00 | 2015-06-08 20:51:00 | Given       | 20
15-06-08 20:34:00 | 2015-06-08 20:34:00 | Oral        |  650 | mg      |               650 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | zolpidem (AMBIEN) tablet 5 mg                    | 2015-06-11 20:52:00 | 2015-06-11 21:27:00 | Given       | 20
15-06-11 21:27:00 | 2015-06-11 21:27:00 | Oral        |    5 | mg      |                 5 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | donepezil (ARICEPT) tablet 10 mg                 | 2015-06-09 06:50:00 | 2015-06-14 20:34:00 | Given       | 20
15-06-14 21:00:00 | 2015-06-14 21:00:00 | Oral        |   10 | mg      |                10 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | donepezil (ARICEPT) tablet 10 mg                 | 2015-06-09 06:50:00 | 2015-06-13 21:58:00 | Given       | 20
15-06-13 21:00:00 | 2015-06-13 21:00:00 | Oral        |   10 | mg      |                10 |                  
(10 rows)

```

### OrderMed (Medication Orders)

```
HC_EPIC-# \d+ "OrderMed" 
                               Table "public.OrderMed"
      Column       |            Type             | Modifiers | Storage  | Description 
-------------------+-----------------------------+-----------+----------+-------------
 CSN_ID            | uuid                        |           | plain    | 
 display_name      | character varying(200)      |           | extended | 
 ORDER_INST        | timestamp without time zone |           | plain    | 
 MedRoute          | character varying(50)       |           | extended | 
 Dose              | character varying(20)       |           | extended | 
 MedUnit           | character varying(50)       |           | extended | 
 MIN_DISCRETE_DOSE | real                        |           | plain    | 
 MAX_DISCRETE_DOSE | real                        |           | plain    | 

 HC_EPIC=# select * from "OrderMed" order by "CSN_ID" limit 10;
                CSN_ID                |                               display_name                               |     ORDER_INST      |   MedRoute   |
 Dose | MedUnit | MIN_DISCRETE_DOSE | MAX_DISCRETE_DOSE 
--------------------------------------+--------------------------------------------------------------------------+---------------------+--------------+
------+---------+-------------------+-------------------
 000181ec-3127-4ada-b44d-e768dbd2e728 | pravastatin (PRAVACHOL) tablet 40 mg                                     | 2015-06-09 06:50:00 | Oral         |
 40   | mg      |                40 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | pantoprazole (PROTONIX) EC tablet 40 mg                                  | 2015-06-09 06:50:00 | Oral         |
 40   | mg      |                40 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | fidaxomicin (DIFICID) tablet 200 mg                                      | 2015-06-10 12:33:00 | Oral         |
 200  | mg      |               200 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | metroNIDAZOLE (FLAGYL) tablet 500 mg                                     | 2015-06-09 00:12:00 | Oral         |
 500  | mg      |               500 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | magnesium oxide (MAG-OX) tablet 400 mg                                   | 2015-06-16 12:06:00 | Oral         |
 400  | mg      |               400 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | ondansetron (PF) (zoFRAN) 4 mg/2 mL injection syringe 4 mg               | 2015-06-09 01:33:00 | Intravenous  |
 4    | mg      |                 4 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | potassium chloride (K-LOR) packet 40 mEq                                 | 2015-06-10 09:13:00 | Oral         |
 40   | mEq     |                40 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | zolpidem (AMBIEN) tablet 5 mg                                            | 2015-06-11 20:52:00 | Oral         |
 5    | mg      |                 5 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | heparin (porcine) 5,000 unit/mL injection 5,000 Units                    | 2015-06-09 01:33:00 | Subcutaneous |
 5000 | Units   |              5000 |                  
 000181ec-3127-4ada-b44d-e768dbd2e728 | lactobacillus acidophilus (LACTINEX) per chewable tablet: Dose: 4 tablet | 2015-06-09 06:50:00 | Oral         |
 4    | tablet  |                 4 |                  
(10 rows)
```


### ProblemList
```
 HC_EPIC=# \d+ "ProblemList" 
                              Table "public.ProblemList"
       Column       |            Type             | Modifiers | Storage  | Description 
--------------------+-----------------------------+-----------+----------+-------------
 pat_id             | uuid                        |           | plain    | 
 csn_id             | uuid                        |           | plain    | 
 departmentid       | character varying(50)       |           | extended | 
 firstdocumented    | timestamp without time zone |           | plain    | 
 resolveddate       | timestamp without time zone |           | plain    | 
 problemstatus      | character varying(300)      |           | extended | 
 hospitaldiagnosis  | integer                     |           | plain    | 
 presentonadmission | character varying(100)      |           | extended | 
 chronic            | integer                     |           | plain    | 
 diagname           | character varying(200)      |           | extended | 
 code               | character varying(20)       |           | extended | 
 codecategory       | character varying(255)      |           | extended |

 HC_EPIC=# select * from "ProblemList" order by "csn_id" limit 10;
                pat_id                |                csn_id                | departmentid |   firstdocumented   |    resolveddate     | problemstatus
 | hospitaldiagnosis | presentonadmission | chronic |              diagname               |  code  |                        codecategory               
          
--------------------------------------+--------------------------------------+--------------+---------------------+---------------------+--------------
-+-------------------+--------------------+---------+-------------------------------------+--------+---------------------------------------------------
----------
 58058f88-33a3-49bb-a889-7d1c6a6cabf6 | 000181ec-3127-4ada-b44d-e768dbd2e728 | 110300180    | 2015-06-08 00:00:00 | 2500-01-01 00:00:00 | Active       
 |                 1 | Unknown            |       0 | Change in mental status             | 780.97 | Symptoms, signs, and ill-defined conditions
 58058f88-33a3-49bb-a889-7d1c6a6cabf6 | 000181ec-3127-4ada-b44d-e768dbd2e728 | 110300180    | 2015-06-08 00:00:00 | 2500-01-01 00:00:00 | Active       
 |                 1 | Unknown            |       0 | Fever and chills                    | 780.60 | Symptoms, signs, and ill-defined conditions
 7246e395-b7f7-42fb-9e1f-78be19df63c9 | 0005e686-e721-4572-bf19-4319459902a0 | 110300180    | 2013-06-17 00:00:00 | 2014-01-23 00:00:00 | Resolved     
 |                 0 | *Unspecified       |       1 | Lower back pain                     | 724.2  | Diseases of the musculoskeltal system and connecti
ve tissue
 9d763467-47fd-44fd-8072-b7b11c72e3b9 | 000773ec-d6b2-4d7e-8e40-35b3f185897a | 110300180    | 2015-05-16 00:00:00 | 2500-01-01 00:00:00 | Active       
 |                 1 | Unknown            |       0 | Encephalopathy                      | 348.30 | Disease of the nervous system
 9d763467-47fd-44fd-8072-b7b11c72e3b9 | 000773ec-d6b2-4d7e-8e40-35b3f185897a | 110300180    | 2015-05-16 00:00:00 | 2500-01-01 00:00:00 | Active       
 |                 1 | Unknown            |       0 | UTI (lower urinary tract infection) | 599.0  | Disease of the genitourinary system
 27121901-c6fc-4607-a0c9-0079a2eb1e14 | 000a6e27-b0d0-406e-9987-6cdfa3f51b39 | 110300280    | 2014-09-18 00:00:00 | 2500-01-01 00:00:00 | Active       
 |                 1 | Unknown            |       0 | Chest pain                          | 786.50 | Symptoms, signs, and ill-defined conditions
 39fe2c5f-a4dd-4b02-aefc-b734a8ade801 | 000a9a18-c619-4996-a5af-b3f3945f1084 | 110300110    | 2013-08-25 00:00:00 | 2500-01-01 00:00:00 | Active       
 |                 0 | Unknown            |       0 | Leukocytosis                        | 288.60 | Diseases of the blood and blood-forming organs
 39fe2c5f-a4dd-4b02-aefc-b734a8ade801 | 000a9a18-c619-4996-a5af-b3f3945f1084 | 110300110    | 2013-08-15 00:00:00 | 2500-01-01 00:00:00 | Active       
 |                 0 | Yes                |       0 | Thrombosis                          | 453.9  | Diseases of the circulatory system
 0f7b33ae-5871-464c-a9ac-9e818b42b8c8 | 000e0936-2b3c-4b1d-b558-aa9b5544cd82 | 110300140    | 2015-03-07 00:00:00 | 2500-01-01 00:00:00 | Active       
 |                 1 | Unknown            |       0 | A-fib                               | 427.31 | Diseases of the circulatory system
 0f7b33ae-5871-464c-a9ac-9e818b42b8c8 | 000e0936-2b3c-4b1d-b558-aa9b5544cd82 | 110300140    | 2015-03-07 00:00:00 | 2500-01-01 00:00:00 | Active       
 |                 1 | Unknown            |       0 | Nausea & vomiting                   | 787.01 | Symptoms, signs, and ill-defined conditions
(10 rows)

 ```