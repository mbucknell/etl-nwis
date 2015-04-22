show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform result start time: ' || systimestamp from dual;

prompt dropping nwis pc_result indexes
exec etl_helper.drop_indexes('pc_result_swap_nwis');

prompt populating nwis pc_result
truncate table pc_result_swap_nwis;

insert /*+ append parallel(4) */
  into pc_result_swap_nwis (data_source_id, data_source, station_id, site_id, event_date, analytical_method, p_code, activity,
                            characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code,
                            organization_name, activity_type_code, activity_media_subdiv_name, activity_start_time,
                            act_start_time_zone, activity_stop_date, activity_stop_time, act_stop_time_zone, activity_depth,
                            activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,
                            activity_lower_depth, activity_lower_depth_unit, project_id,
                            activity_conducting_org, activity_comment, sample_aqfr_name, hydrologic_condition_name, hydrologic_event_name,
                            sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name, sample_collect_equip_name,
                            result_id, result_detection_condition_tx, sample_fraction_type, result_measure_value, result_unit,
                            result_meas_qual_code, result_value_status, statistic_type, result_value_type, weight_basis_type, duration_basis,
                            temperature_basis_level, particle_size, precision, result_comment, result_depth_meas_value,
                            result_depth_meas_unit_code, result_depth_alt_ref_pt_txt, sample_tissue_taxonomic_name,
                            sample_tissue_anatomy_name, analytical_procedure_id, analytical_procedure_source, analytical_method_name,
                            analytical_method_citation, lab_name, analysis_date_time, lab_remark, detection_limit, detection_limit_unit,
                            detection_limit_desc, analysis_prep_date_tx)
select 2 data_source_id,
       cast('NWIS' as varchar2(4000 char)) data_source,
       s.station_id,
       s.site_id,
       trunc(samp.sample_start_dt) event_date,
       nemi.nemi_url analytical_method,
       r.parameter_cd p_code,
       samp.nwis_host || '.' || samp.qw_db_no || '.' || samp.record_no activity,
       parm.srsname characteristic_name,
       parm.parm_seq_grp_nm characteristic_type,
       nwis_wqx_medium_cd.wqx_act_med_nm sample_media,
       s.organization,
       s.site_type,
       s.huc,
       s.governmental_unit_code,
       s.organization_name,
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
       coalesce(parameter.V00003, parameter.V00098, parameter.V78890, parameter.V78891) activity_depth,
       case
         when parameter.V00003 is not null then 'feet'
         when parameter.V00098 is not null then 'meters'
         when parameter.V78890 is not null then 'feet'
         when parameter.V78891 is not null then 'meters'
         else null
       end activity_depth_unit,
       case
         when parameter.V00003 is not null or parameter.V00098 is not null then null
         when parameter.V78890 is not null or parameter.V78891 is not null then 'Below mean sea level'
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
       nwis_ws_star.determine_project_id(nawqa_sites.site_no,
                            parameter.v50280,
                            parameter.v71999,
                            parameter.v71999_fxd_nm,
                            samp.sample_start_dt,
                            samp.project_cd) project_id,
       coalesce(proto_org2.proto_org_nm, samp.coll_ent_cd) activity_conducting_org,
       trim(samp.sample_lab_cm_tx) activity_comment,
       aqfr.aqfr_nm sample_aqfr_name,
       hyd_cond_cd.hyd_cond_nm hydrologic_condition_name,
       hyd_event_cd.hyd_event_nm hydrologic_event_name,
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
       case
         when parameter.v84164_fxd_tx is not null and parameter.v82398_fxd_tx is not null
           then parameter.v84164_fxd_tx
         else 'Unknown'
       end sample_collect_equip_name,
       rownum result_id,
       case
         when r.remark_cd = 'U' then 'Not Detected'
         when r.remark_cd = 'V' then 'Systematic Contamination'
         when r.remark_cd = 'S' then null
         when r.remark_cd = 'M' then 'Detected Not Quantified'
         when r.remark_cd = 'N' then 'Detected Not Quantified'
         when r.remark_cd = 'A' then null
         when r.remark_cd = '<' then 'Not Detected'
         when r.remark_cd = '>' then 'Present Above Quantification Limit'
         else null
       end result_detection_condition_tx,
       parm.parm_frac_tx sample_fraction_type,
       nvl(fxd.fxd_tx,
           case
             when r.remark_cd in ('U', 'M', 'N', '<', '>') then null
             when r.remark_cd in ('R', 'V', 'S', 'E', 'A') or r.remark_cd is null then r.result_va
             else r.result_va
           end) result_measure_value,
       nvl2(nvl(fxd.fxd_tx,
                case
                  when r.remark_cd in ('U', 'M', 'N', '<', '>') then null
                  when r.remark_cd in ('R', 'V', 'S', 'E', 'A') or r.remark_cd is null then r.result_va
                  else r.result_va
                end),
            parm.parm_unt_tx, null) result_unit,
       null result_meas_qual_code,
       case
         when r.dqi_cd = 'S' then 'Preliminary'
         when r.dqi_cd = 'A' then 'Historical'
         when r.dqi_cd = 'R' then 'Accepted'
         else null
       end result_value_status,
       nvl(parm.parm_stat_tx,
            case
              when r.remark_cd = 'S' then 'MPN'
              when r.remark_cd = 'A' then 'mean'
              else null
            end) statistic_type,
       case
         when r.result_md is null then 'Calculated'
         when r.remark_cd = 'E' then 'Estimated'
         else 'Actual'
       end result_value_type,
       parm.parm_wt_tx weight_basis_type,
       parm.parm_tm_tx duration_basis,
       parm.parm_temp_tx temperature_basis_level,
       parm.parm_size_tx particle_size,
       r.lab_std_va precision,
       trim(r.result_lab_cm_tx) result_comment,
       null result_depth_meas_value,
       null result_depth_meas_unit_code,
       null result_depth_alt_ref_pt_txt,
       case
         when parm.PARM_MEDIUM_TX = 'Biological Tissue'
           then tu.tu_1_nm ||
                  case when tu.tu_2_cd is not null then ' ' || tu.tu_2_cd end ||
                  case when tu.tu_2_nm is not null then ' ' || tu.tu_2_nm end ||
                  case when tu.tu_3_cd is not null then ' ' || tu.tu_3_cd end ||
                  case when tu.tu_3_nm is not null then ' ' || tu.tu_3_nm end ||
                  case when tu.tu_4_cd is not null then ' ' || tu.tu_4_cd end ||
                  case when tu.tu_4_nm is not null then ' ' || tu.tu_4_nm end
       end sample_tissue_taxonomic_name,
       case
         when parm.parm_medium_tx = 'Biological Tissue' then body_part.body_part_nm
         else null
       end sample_tissue_anatomy_name,
       r.meth_cd analytical_procedure_id,
       case
         when r.meth_cd is not null then 'USGS'
         else null
       end analytical_procedure_source,
       meth.meth_nm analytical_method_name,
       meth.cit_nm analytical_method_citation,
       proto_org.proto_org_nm lab_name,
       case 
         when r.anl_dt is not null
           then substr(r.anl_dt, 1, 4) || '-' || substr(r.anl_dt, 5, 2) || '-' || substr(r.anl_dt, 7, 2)
         else null
       end analysis_date_time,
       trim(val_qual_cd1.val_qual_nm ||
            val_qual_cd2.val_qual_nm ||
            val_qual_cd3.val_qual_nm ||
            val_qual_cd4.val_qual_nm ||
            val_qual_cd5.val_qual_nm ||
            case
              when r.remark_cd = 'R' then 'Result below sample specific critical level.'
              else null
            end) lab_remark,
       case
         when r.remark_cd = '<' and r.rpt_lev_va is null then r.result_va
         when r.remark_cd = '<' and to_number(r.result_unrnd_va) > to_number(r.rpt_lev_va) then r.result_va
         when r.remark_cd = '>' then r.result_va
         when r.remark_cd in ('N', 'U') and r.rpt_lev_va is not null then r.rpt_lev_va
         when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
         when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then nvl(z_parm_meth.multiplier, parm.multiplier)
         when nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then r.rpt_lev_va
         else null
       end detection_limit,
       nvl2(case
              when r.remark_cd = '<' and r.rpt_lev_va is null then r.result_va
              when r.remark_cd = '<' and to_number(r.result_unrnd_va) > to_number(r.rpt_lev_va) then r.result_va
              when r.remark_cd = '>' then r.result_va
              when r.remark_cd in ('N', 'U') and r.rpt_lev_va is not null then r.rpt_lev_va
              when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
              when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then nvl(z_parm_meth.multiplier, parm.multiplier)
              when nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then r.rpt_lev_va
              else null
            end,
            parm.parm_unt_tx,
            null) detection_limit_unit,
       nvl2(case
              when r.remark_cd = '<' and r.rpt_lev_va is null then r.result_va
              when r.remark_cd = '<' and to_number(r.result_unrnd_va) > to_number(r.rpt_lev_va) then r.result_va
              when r.remark_cd = '>' then r.result_va
              when r.remark_cd in ('N', 'U') and r.rpt_lev_va is not null then r.rpt_lev_va
              when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
              when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then nvl(z_parm_meth.multiplier, parm.multiplier)
              when nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then r.rpt_lev_va
              else null
            end,
            case
              when r.remark_cd = '<' and r.rpt_lev_va is null then 'Historical Lower Reporting Limit'
              when r.remark_cd = '<' and to_number(r.result_unrnd_va) > to_number(r.rpt_lev_va) then 'Elevated Detection Limit'
              when r.remark_cd = '<' and nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then nwis_wqx_rpt_lev_cd.wqx_rpt_lev_nm
              when r.remark_cd = '>' then 'Upper Reporting Limit'
              when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
              when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then 'Lower Quantitation Limit'
              when r.remark_cd = 'M' and r.rpt_lev_va is not null then nwis_wqx_rpt_lev_cd.wqx_rpt_lev_nm
              when r.rpt_lev_va is not null then nwis_wqx_rpt_lev_cd.wqx_rpt_lev_nm
              else null
            end,
            null) detection_limit_desc,
       case
         when r.prep_dt is not null then substr(r.prep_dt, 1, 4) || '-' || substr(r.prep_dt, 5, 2) || '-' || substr(r.prep_dt, 7, 2)
         else null
       end analysis_prep_date_tx 
  from nwis_ws_star.qw_result r
       join nwis_ws_star.qw_sample samp
         on r.sample_id = samp.sample_id
       join nwis_ws_star.sitefile site
         on samp.site_id = site.site_id
       join station_swap_nwis s
         on site.site_id = s.station_id
       left join nwis_ws_star.lu_tz
         on samp.sample_start_time_datum_cd = lu_tz.tz_cd
       left join nwis_ws_star.tu
         on to_number(samp.tu_id) = tu.tu_id
       left join nwis_ws_star.nwis_wqx_medium_cd
         on samp.medium_cd = nwis_wqx_medium_cd.nwis_medium_cd
       left join nwis_ws_star.body_part
         on samp.body_part_id = body_part.body_part_id
       join nwis_ws_star.parm
         on r.parameter_cd = parm.parm_cd
       left join nwis_ws_star.fxd
         on r.parameter_cd = fxd.parm_cd and
            case when r.result_va = '0.0' then '0' else r.result_va end = fxd.fxd_va
       left join nwis_ws_star.proto_org
         on r.anl_ent_cd = proto_org.proto_org_cd
       left join nwis_ws_star.proto_org proto_org2
         on samp.coll_ent_cd = proto_org2.proto_org_cd
       left join nwis_ws_star.meth_with_cit meth
         on r.meth_cd = meth.meth_cd
       left join nwis_ws_star.z_parm_meth
         on r.parameter_cd = z_parm_meth.parm_cd and
            r.meth_cd = z_parm_meth.meth_cd
       left join nwis_ws_star.nwis_wqx_rpt_lev_cd
         on r.rpt_lev_cd = nwis_wqx_rpt_lev_cd.rpt_lev_cd
       left join nwis_ws_star.val_qual_cd val_qual_cd1
         on substr(r.val_qual_tx, 1, 1) = val_qual_cd1.val_qual_cd
       left join nwis_ws_star.val_qual_cd val_qual_cd2
         on substr(r.val_qual_tx, 2, 1) = val_qual_cd2.val_qual_cd
       left join nwis_ws_star.val_qual_cd val_qual_cd3
         on substr(r.val_qual_tx, 3, 1) = val_qual_cd3.val_qual_cd
       left join nwis_ws_star.val_qual_cd val_qual_cd4
         on substr(r.val_qual_tx, 4, 1) = val_qual_cd4.val_qual_cd
       left join nwis_ws_star.val_qual_cd val_qual_cd5
         on substr(r.val_qual_tx, 5, 1) = val_qual_cd5.val_qual_cd
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
       left join nwis_ws_star.wqp_nemi_nwis_crosswalk nemi
         on nvl2(r.meth_cd, 'USGS', null) = nemi.analytical_procedure_source and
            trim(r.meth_cd) = nemi.analytical_procedure_id
       left join nwis_ws_star.sample_parameter parameter
         on samp.sample_id = parameter.sample_id
       left join nwis_ws_star.nawqa_sites
         on site.site_no = nawqa_sites.site_no and
            upper(site.nwis_host) = upper(nawqa_sites.nwis_host_nm) and
            site.db_no = nawqa_sites.db_no
 where r.result_web_cd = 'Y' and
       (r.result_va is not null or
        r.rpt_lev_va is not null or
        r.remark_cd is not null) and
       samp.sample_web_cd = 'Y' and
       samp.qw_db_no = '01' and
       site.dec_lat_va <> 0 and
       site.dec_long_va <> 0 and
       site.db_no = '01' and
       site.site_web_cd = 'Y' and
       site.site_tp_cd not in ('FA-WTP', 'FA-WWTP', 'FA-TEP', 'FA-HP') and
       site.nwis_host not in ('fltlhsr001', 'fltpasr001', 'flalssr003')
     order by s.station_id;

commit;

select 'transform result end time: ' || systimestamp from dual;
