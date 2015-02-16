show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'build indexes start time: ' || systimestamp from dual;

begin
   etl_helper.create_station_indexes('nwis');
   etl_helper.create_result_indexes('nwis');
   etl_helper.create_station_sum_indexes('nwis');
   etl_helper.create_result_sum_indexes('nwis');
   etl_helper.create_result_ct_sum_indexes('nwis');
   etl_helper.create_result_nr_sum_indexes('nwis');
end;
/

select 'build indexes end time: ' || systimestamp from dual;
