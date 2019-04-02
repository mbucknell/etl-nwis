show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform res_detect_qnt_limit start time: ' || systimestamp from dual;

prompt dropping nwis r_detect_qnt_lmt indexes
exec etl_helper_r_detect_qnt_lmt.drop_indexes('nwis');

prompt populating nwis r_detect_qnt_lmt
truncate table r_detect_qnt_lmt_swap_nwis;

insert /*+ append parallel(4) */
  into r_detect_qnt_lmt_swap_nwis(data_source_id, data_source, station_id, site_id, event_date, activity, analytical_method,
                                  characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code,
                                  organization_name, project_id, assemblage_sampled_name, sample_tissue_taxonomic_name, activity_id,
                                  result_id, detection_limit_id, detection_limit, detection_limit_unit, detection_limit_desc)
select /*+ parallel(4) */
       data_source_id,
       data_source,
       station_id,
       site_id,
       event_date,
       activity,
       analytical_method,
       characteristic_name,
       characteristic_type,
       sample_media,
       organization,
       site_type,
       huc,
       governmental_unit_code,
       organization_name,
       project_id,
       assemblage_sampled_name,
       sample_tissue_taxonomic_name,
       activity_id,
       result_id,
       result_id detection_limit_id,
       detection_limit,
       detection_limit_unit,
       detection_limit_desc
  from result_swap_nwis
 where detection_limit is not null or
       detection_limit_unit is not null or
       detection_limit_desc is not null;
commit;
select 'Building r_detect_qnt_lmt_swap_nwis complete: ' || systimestamp from dual;

prompt building nwis r_detect_qnt_lmt indexes
exec etl_helper_r_detect_qnt_lmt.create_indexes('nwis');

select 'transform r_detect_qnt_lmt end time: ' || systimestamp from dual;
