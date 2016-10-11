show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'copy from nemi start time: ' || systimestamp from dual;

prompt wqp_nemi_nwis_crosswalk
truncate table wqp_nemi_nwis_crosswalk;
insert /*+ append parallel(4) */ into wqp_nemi_nwis_crosswalk
select analytical_procedure_source,
       analytical_procedure_id,
       citation_method_num,
       method_id,
       wqp_source,
       method_type,
       case
         when method_id is not null
           then
             case method_type
               when 'analytical'
                 then 'https://www.nemi.gov/methods/method_summary/' || method_id || '/'
               when 'statistical'
                 then 'https://www.nemi.gov/methods/sams_method_summary/' || method_id || '/'
               end
         else 
           null 
       end
  from (select wqp_nemi_nwis_crosswalk.*,
               count(*) over (partition by analytical_procedure_source, analytical_procedure_id) cnt
          from wqp_nemi_nwis_crosswalk@nemi.er.usgs.gov)
 where cnt = 1;
commit;

select 'copy from nemi end time: ' || systimestamp from dual;
