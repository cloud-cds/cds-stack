DELETE
FROM parameters
WHERE dataset_id = 1;

\COPY parameters FROM 'parameters.csv' WITH csv header DELIMITER AS ',';