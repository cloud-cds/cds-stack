copy dw_version
from 's3://opsdx-clarity-etl-stage/dw-s3-export/public.dw_version.csv'
iam_role 'arn:aws:iam::359300513585:role/redshift_role'
region 'us-east-1'
format as csv QUOTE '\b' DELIMITER '\t' NULL 'NULL';

copy pat_enc
from 's3://opsdx-clarity-etl-stage/dw-s3-export/public.pat_enc.csv'
iam_role 'arn:aws:iam::359300513585:role/redshift_role'
region 'us-east-1'
format as csv QUOTE '\b' DELIMITER '\t' NULL 'NULL';

copy cdm_t
from 's3://opsdx-clarity-etl-stage/dw-s3-export/public.cdm_t.csv'
iam_role 'arn:aws:iam::359300513585:role/redshift_role'
region 'us-east-1'
format as csv QUOTE '\b' DELIMITER '\t' NULL 'NULL';

copy cdm_g
from 's3://opsdx-clarity-etl-stage/dw-s3-export/public.cdm_g.csv'
iam_role 'arn:aws:iam::359300513585:role/redshift_role'
region 'us-east-1'
format as csv QUOTE '\b' DELIMITER '\t' NULL 'NULL';


copy cdm_twf
from 's3://opsdx-clarity-etl-stage/dw-s3-export/public.cdm_twf.csv'
iam_role 'arn:aws:iam::359300513585:role/redshift_role'
region 'us-east-1'
format as csv QUOTE '\b' DELIMITER '\t' NULL 'NULL';

copy criteria_default
from 's3://opsdx-clarity-etl-stage/dw-s3-export/public.criteria_default.csv'
iam_role 'arn:aws:iam::359300513585:role/redshift_role'
region 'us-east-1'
format as csv QUOTE '\b' DELIMITER '\t' NULL 'NULL';


copy cdm_notes
from 's3://opsdx-clarity-etl-stage/dw-s3-export/cdm_notes_d1.csv'
iam_role 'arn:aws:iam::359300513585:role/redshift_role'
region 'us-east-1'
format as csv QUOTE '\b' DELIMITER '\t' NULL 'NULL';

copy cdm_notes
from 's3://opsdx-clarity-etl-stage/dw-s3-export/cdm_notes_d3.csv'
iam_role 'arn:aws:iam::359300513585:role/redshift_role'
region 'us-east-1'
format as csv QUOTE '\b' DELIMITER '\t' NULL 'NULL';

copy cdm_notes
from 's3://opsdx-clarity-etl-stage/dw-s3-export/cdm_notes_d12.csv'
iam_role 'arn:aws:iam::359300513585:role/redshift_role'
region 'us-east-1'
format as csv QUOTE '\b' DELIMITER '\t' NULL 'NULL';

copy cdm_notes
from 's3://opsdx-clarity-etl-stage/dw-s3-export/cdm_notes_d13.csv'
iam_role 'arn:aws:iam::359300513585:role/redshift_role'
region 'us-east-1'
format as csv QUOTE '\b' DELIMITER '\t' NULL 'NULL';


insert into cdm_window_offsets_15mins values ( -360 );
insert into cdm_window_offsets_15mins values ( -345 );
insert into cdm_window_offsets_15mins values ( -330 );
insert into cdm_window_offsets_15mins values ( -315 );
insert into cdm_window_offsets_15mins values ( -300 );
insert into cdm_window_offsets_15mins values ( -285 );
insert into cdm_window_offsets_15mins values ( -270 );
insert into cdm_window_offsets_15mins values ( -255 );
insert into cdm_window_offsets_15mins values ( -240 );
insert into cdm_window_offsets_15mins values ( -225 );
insert into cdm_window_offsets_15mins values ( -210 );
insert into cdm_window_offsets_15mins values ( -195 );
insert into cdm_window_offsets_15mins values ( -180 );
insert into cdm_window_offsets_15mins values ( -165 );
insert into cdm_window_offsets_15mins values ( -150 );
insert into cdm_window_offsets_15mins values ( -135 );
insert into cdm_window_offsets_15mins values ( -120 );
insert into cdm_window_offsets_15mins values ( -105 );
insert into cdm_window_offsets_15mins values (  -90 );
insert into cdm_window_offsets_15mins values (  -75 );
insert into cdm_window_offsets_15mins values (  -60 );
insert into cdm_window_offsets_15mins values (  -45 );
insert into cdm_window_offsets_15mins values (  -30 );
insert into cdm_window_offsets_15mins values (  -15 );
insert into cdm_window_offsets_15mins values (    0 );
insert into cdm_window_offsets_15mins values (   15 );
insert into cdm_window_offsets_15mins values (   30 );
insert into cdm_window_offsets_15mins values (   45 );
insert into cdm_window_offsets_15mins values (   60 );
insert into cdm_window_offsets_15mins values (   75 );
insert into cdm_window_offsets_15mins values (   90 );
insert into cdm_window_offsets_15mins values (  105 );
insert into cdm_window_offsets_15mins values (  120 );
insert into cdm_window_offsets_15mins values (  135 );
insert into cdm_window_offsets_15mins values (  150 );
insert into cdm_window_offsets_15mins values (  165 );
insert into cdm_window_offsets_15mins values (  180 );
insert into cdm_window_offsets_15mins values (  195 );
insert into cdm_window_offsets_15mins values (  210 );
insert into cdm_window_offsets_15mins values (  225 );
insert into cdm_window_offsets_15mins values (  240 );
insert into cdm_window_offsets_15mins values (  255 );
insert into cdm_window_offsets_15mins values (  270 );
insert into cdm_window_offsets_15mins values (  285 );
insert into cdm_window_offsets_15mins values (  300 );
insert into cdm_window_offsets_15mins values (  315 );
insert into cdm_window_offsets_15mins values (  330 );
insert into cdm_window_offsets_15mins values (  345 );
insert into cdm_window_offsets_15mins values (  360 );
insert into cdm_window_offsets_15mins values (  375 );
insert into cdm_window_offsets_15mins values (  390 );
insert into cdm_window_offsets_15mins values (  405 );
insert into cdm_window_offsets_15mins values (  420 );


insert into cdm_window_offsets_3hr values ( -360 );
insert into cdm_window_offsets_3hr values ( -180 );
insert into cdm_window_offsets_3hr values (    0 );
insert into cdm_window_offsets_3hr values (  180 );
insert into cdm_window_offsets_3hr values (  360 );
