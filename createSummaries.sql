show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'build summary tables start time: ' || systimestamp from dual;

exec etl_helper.create_summaries('nwis');

select 'build summary tables end time: ' || systimestamp from dual;
