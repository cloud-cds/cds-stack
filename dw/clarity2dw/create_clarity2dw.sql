-- create clarity2dw dataset

INSERT INTO dw_version (dataset_id, created, description)
VALUES (1,
        now(),
        'clarity2dw');

INSERT INTO model_version (model_id, created, description)
VALUES (1,
        now(),
        'clarity2dw default model');

DELETE
FROM cdm_feature
WHERE dataset_id = 1;
DELETE
FROM cdm_function
WHERE dataset_id = 1;

 \COPY cdm_function FROM '/home/ubuntu/dashan-db/dw/clarity2dw/CDM_Function.csv' WITH csv header DELIMITER AS ',';
 \COPY cdm_feature FROM '/home/ubuntu/dashan-db/dw/clarity2dw/CDM_Feature.csv' WITH csv header DELIMITER AS ',';


DELETE
FROM cdm_g
WHERE dataset_id = 1 and model_id = 1;

 \COPY cdm_g FROM '/home/ubuntu/dashan-db/dw/clarity2dw/CDM_G.csv' WITH csv header DELIMITER AS ',';