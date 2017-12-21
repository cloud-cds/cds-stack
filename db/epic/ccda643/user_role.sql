select emp.USER_ID,emp.name,ser.PROV_TYPE
from CLARITY_EMP emp with (nolock)
inner join CLARITY_SER ser with (nolock) on EMP.PROV_ID = SER.PROV_ID
where emp.user_id in ('')


create table user_role
(
  id text,
  name text,
  role text
);

\copy user_role from '/home/ubuntu/zad/mnt/user_role.rpt' with csv delimiter as E'\t' NULL 'NULL';

update user_role set id = 'AAGUIL12' where name = 'AGUILAR, ANTONIO'


