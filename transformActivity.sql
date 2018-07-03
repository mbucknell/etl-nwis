show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform activity start time: ' || systimestamp from dual;

prompt building activity_swap_nwis 

prompt dropping nwis activity indexes
exec etl_helper_activity.drop_indexes('nwis');

set define off;
truncate table activity_swap_nwis;
insert /*+ append parallel(4) */
  into activity_swap_nwis (data_source_id, data_source, station_id, site_id, event_date, activity, sample_media,
                           organization, site_type, huc, governmental_unit_code, organization_name, activity_id,
                           activity_type_code, activity_media_subdiv_name, activity_start_time, act_start_time_zone,
                           activity_stop_date, activity_stop_time, act_stop_time_zone, activity_relative_depth_name,
                           activity_depth, activity_depth_unit, activity_depth_ref_point, activity_upper_depth,
                           activity_upper_depth_unit, activity_lower_depth, activity_lower_depth_unit, project_id,
                           activity_conducting_org, activity_comment, sample_aqfr_name, hydrologic_condition_name,
                           hydrologic_event_name, activity_latitude, activity_longitude, activity_source_map_scale,
                           act_horizontal_accuracy, act_horizontal_accuracy_unit, act_horizontal_collect_method,
                           act_horizontal_datum_name, assemblage_sampled_name, act_collection_duration,
                           act_collection_duration_unit, act_sam_compnt_name, act_sam_compnt_place_in_series,
                           act_reach_length, act_reach_length_unit, act_reach_width, act_reach_width_unit, act_pass_count,
                           net_type_name, act_net_surface_area, act_net_surface_area_unit, act_net_mesh_size,
                           act_net_mesh_size_unit, act_boat_speed, act_boat_speed_unit, act_current_speed,
                           act_current_speed_unit, toxicity_test_type_name, sample_collect_method_id,
                           sample_collect_method_ctx, sample_collect_method_name, act_sam_collect_meth_qual_type,
                           act_sam_collect_meth_desc, sample_collect_equip_name, act_sam_collect_equip_comments,
                           act_sam_prep_meth_id, act_sam_prep_meth_context, act_sam_prep_meth_name,
                           act_sam_prep_meth_qual_type, act_sam_prep_meth_desc, sample_container_type,
                           sample_container_color, act_sam_chemical_preservative, thermal_preservative_name,
                           act_sam_transport_storage_desc, result_count)
select 2 data_source_id,
       'NWIS' data_source,
       s.station_id,
       s.site_id,
       trunc(samp.sample_start_dt) event_date,
       samp.nwis_host || '.' || samp.qw_db_no || '.' || samp.record_no activity,
       nwis_wqx_medium_cd.wqx_act_med_nm sample_media,
       s.organization,
       s.site_type,
       s.huc,
       s.governmental_unit_code,
       s.organization_name,
       samp.sample_id activity_id,
       case 
         when samp.samp_type_cd = 'A' then 'Not determined'
         when samp.samp_type_cd = 'B' then 'Quality Control Sample-Other'
         when samp.samp_type_cd = 'H' then 'Sample-Composite Without Parents'
         when samp.samp_type_cd = '1' then 'Quality Control Sample-Field Spike'
         when samp.samp_type_cd = '2' then 'Quality Control Sample-Field Blank'
         when samp.samp_type_cd = '3' then 'Quality Control Sample-Reference Sample'
         when samp.samp_type_cd = '4' then 'Quality Control Sample-Blind'
         when samp.samp_type_cd = '5' then 'Quality Control Sample-Field Replicate'
         when samp.samp_type_cd = '6' then 'Quality Control Sample-Reference Material'
         when samp.samp_type_cd = '7' then 'Quality Control Sample-Field Replicate'
         when samp.samp_type_cd = '8' then 'Quality Control Sample-Spike Solution'
         when samp.samp_type_cd = '9' then 'Sample-Routine'
         else 'Unknown'
       end activity_type_code,
       nwis_wqx_medium_cd.wqx_act_med_sub activity_media_subdiv_name,
       case 
         when samp.sample_start_sg in ('m', 'h') then to_char(samp.sample_start_dt, 'hh24:mi:ss')
         else null
       end activity_start_time,
       case
         when samp.sample_start_dt is not null and samp.sample_start_sg in ('h','m') then samp.sample_start_time_datum_cd
         else null
       end act_start_time_zone,
       case
         when samp.sample_end_sg in ('m', 'h', 'D') then substr(samp.sample_end_dt, 1, 10)
         when samp.sample_end_sg = 'M' then substr(samp.sample_end_dt, 1, 7)
         when samp.sample_end_sg = 'Y' then substr(samp.sample_end_dt, 1, 4)
         else null
       end activity_stop_date,
       case when samp.sample_end_sg in ('m', 'h') then substr(samp.sample_end_dt,12) else null end activity_stop_time,
       case 
         when samp.sample_end_dt is not null and samp.sample_end_sg in ('h', 'm') then samp.sample_start_time_datum_cd
         else null
       end act_stop_time_zone,
       null activity_relative_depth_name,
       coalesce(parameter.V00003, parameter.V00098, parameter.V78890, parameter.V78891) activity_depth,
       case
         when parameter.V00003 is not null then 'feet'
         when parameter.V00098 is not null then 'meters'
         when parameter.V78890 is not null then 'feet'
         when parameter.V78891 is not null then 'meters'
         else null
       end activity_depth_unit,
       case
         when parameter.V00003 is not null or
              parameter.V00098 is not null
           then null
         when parameter.V78890 is not null or 
              parameter.V78891 is not null
           then 'Below mean sea level'
         when parameter.V72015 is not null
           then 'Below land-surface datum'
         when parameter.V82047 is not null
           then null
         when parameter.V72016 is not null
           then 'Below land-surface datum'
         when parameter.V82048 is not null
           then null
         else null
       end activity_depth_ref_point,
       coalesce(parameter.V72015, parameter.V82047) activity_upper_depth,
       nvl2(coalesce(parameter.V72015, parameter.V82047),
            case
              when parameter.V72015 is not null then 'feet'
              when parameter.V82047 is not null then 'meters'
              when parameter.V72016 is not null then 'feet'
              when parameter.V82048 is not null then 'meters'
              else null
            end,
            null) activity_upper_depth_unit,
       case
         when parameter.V72015 is not null then parameter.V72016
         when parameter.V82047 is not null then parameter.V82048
         when parameter.V72016 is not null then parameter.V72016
         when parameter.V82048 is not null then parameter.V82048
         else null
       end activity_lower_depth,
       nvl2(case
              when parameter.V72015 is not null then parameter.V72016
              when parameter.V82047 is not null then parameter.V82048
              when parameter.V72016 is not null then parameter.V72016
              when parameter.V82048 is not null then parameter.V82048
              else null
            end,
            case
              when parameter.V72015 is not null then 'feet'
              when parameter.V82047 is not null then 'meters'
              when parameter.V72016 is not null then 'feet'
              when parameter.V82048 is not null then 'meters'
              else null
            end,
            null) activity_lower_depth_unit,
       nwis_ws_star.etl_helper.determine_project_id(nawqa_sites.site_no,
                                                    parameter.v50280,
                                                    parameter.v71999,
                                                    samp.sample_start_dt,
                                                    samp.project_cd) project_id,
       coalesce(proto_org2.proto_org_nm, samp.coll_ent_cd) activity_conducting_org,
       trim(samp.sample_lab_cm_tx) activity_comment,
       aqfr.aqfr_nm sample_aqfr_name,
       hyd_cond_cd.hyd_cond_nm hydrologic_condition_name,
       hyd_event_cd.hyd_event_nm hydrologic_event_name,
       null activity_latitude,
       null activity_longitude,
       null activity_source_map_scale,
       null act_horizontal_accuracy,
       null act_horizontal_accuracy_unit,
       null act_horizontal_collect_method,
       null act_horizontal_datum_name,
       null assemblage_sampled_name,
       null act_collection_duration,
       null act_collection_duration_unit,
       null act_sam_compnt_name,
       null act_sam_compnt_place_in_series,
       null act_reach_length,
       null act_reach_length_unit,
       null act_reach_width,
       null act_reach_width_unit,
       null act_pass_count,
       null net_type_name,
       null act_net_surface_area,
       null act_net_surface_area_unit,
       null act_net_mesh_size,
       null act_net_mesh_size_unit,
       null act_boat_speed,
       null act_boat_speed_unit,
       null act_current_speed,
       null act_current_speed_unit,
       null toxicity_test_type_name,
       case
         when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
           then parameter.V82398
         else 'USGS'
       end sample_collect_method_id,
       case
         when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
           then cast('USGS parameter code 82398' as varchar2(25))
         else 'USGS'
       end sample_collect_method_ctx,
       case
         when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
           then parameter.v82398_fxd_tx
         else 'USGS'
       end sample_collect_method_name,
       null act_sam_collect_meth_qual_type,
       null act_sam_collect_meth_desc,
       case
         when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
           then parameter.v84164_fxd_tx
         else 'Unknown'
       end sample_collect_equip_name,
       null act_sam_collect_equip_comments,
       null act_sam_prep_meth_id,
       null act_sam_prep_meth_context,
       null act_sam_prep_meth_name,
       null act_sam_prep_meth_qual_type,
       null act_sam_prep_meth_desc,
       null sample_container_type,
       null sample_container_color,
       null act_sam_chemical_preservative,
       null thermal_preservative_name,
       null act_sam_transport_storage_desc
       (select count(*) from nwis_ws_star.qw_result where qw_result.sample_id = samp.sample_id) result_count
  from nwis_ws_star.qw_sample samp
       join nwis_ws_star.sitefile site
         on samp.site_id = site.site_id
       join station_swap_nwis s
         on site.site_id = s.station_id
       left join nwis_ws_star.tu
         on to_number(samp.tu_id) = tu.tu_id
       left join nwis_ws_star.nwis_wqx_medium_cd
         on samp.medium_cd = nwis_wqx_medium_cd.nwis_medium_cd
       left join nwis_ws_star.body_part
         on samp.body_part_id = body_part.body_part_id
       left join nwis_ws_star.proto_org proto_org2
         on samp.coll_ent_cd = proto_org2.proto_org_cd
       left join nwis_ws_star.hyd_event_cd
         on samp.hyd_event_cd = hyd_event_cd.hyd_event_cd
       left join nwis_ws_star.hyd_cond_cd
         on samp.hyd_cond_cd = hyd_cond_cd.hyd_cond_cd
       join nwis_ws_star.nwis_district_cds_by_host dist
         on site.district_cd = dist.district_cd and
            site.nwis_host = dist.host_name
       left join nwis_ws_star.aqfr
         on samp.aqfr_cd = aqfr.aqfr_cd and
            site.state_cd = aqfr.state_cd
       left join nwis_ws_star.sample_parameter parameter
         on samp.sample_id = parameter.sample_id
       left join nwis_ws_star.nawqa_sites
         on site.site_no = nawqa_sites.site_no and
            site.agency_cd = nawqa_sites.agency_cd
 where samp.sample_web_cd = 'Y' and
       samp.qw_db_no = '01' and
       site.dec_lat_va <> 0 and
       site.dec_long_va <> 0 and
       site.db_no = '01' and
       site.site_web_cd = 'Y' and
       site.site_tp_cd not in ('FA-WTP', 'FA-WWTP', 'FA-TEP', 'FA-HP');

prompt building nwis activity indexes
exec etl_helper_activity.create_indexes('nwis');

select 'transform activity end time: ' || systimestamp from dual;