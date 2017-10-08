create table cdm_twf_test AS
SELECT id,
random() col1,
random() col2,
random() col3,
random() col4,
random() col5,
random() col6,
random() col7,
random() col8,
random() col9,
random() col10
FROM generate_Series(1,1000000) id;