show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'copy from datachecks start time: ' || systimestamp from dual;

prompt nawqa_sites
begin
  execute immediate 'drop table nawqa_sites cascade constraints purge';
exception
  when others then
    if sqlcode != -00942 then
      raise;
    end if;
end;
/

create table nawqa_sites as
select * from nawqa_sites@nwq_data_checks.er.usgs.gov;
grant select on nawqa_sites to wqp_core;

select 'copy from datachecks end time: ' || systimestamp from dual;
