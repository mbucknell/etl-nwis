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

insert /*+ append nologging parallel */
  into pc_result_swap_nwis (wqp_id, data_source_id, data_source, station_id, site_id, event_date, analytical_method, p_code, activity,
                            characteristic_name, characteristic_type, sample_media, organization, site_type, huc_12, governmental_unit_code,
                            geom, organization_name, activity_type_code, activity_media_subdiv_name, activity_start_time,
                            act_start_time_zone, activity_stop_date, activity_stop_time, act_stop_time_zone, activity_depth,
                            activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,
                            activity_lower_depth, activity_lower_depth_unit, activity_uprlwr_depth_ref_pt, project_id,
                            activity_conducting_org, activity_comment, sample_aqfr_name, hydrologic_condition_name, hydrologic_event_name,
                            sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name, sample_collect_equip_name,
                            result_id, result_detection_condition_tx, sample_fraction_type, result_measure_value, result_unit,
                            result_meas_qual_code, result_value_status, statistic_type, result_value_type, weight_basis_type, duration_basis,
                            temperature_basis_level, particle_size, precision, result_comment, result_depth_meas_value,
                            result_depth_meas_unit_code, result_depth_alt_ref_pt_txt, sample_tissue_taxonomic_name,
                            sample_tissue_anatomy_name, analytical_procedure_id, analytical_procedure_source, analytical_method_name,
                            analytical_method_citation, lab_name, analysis_date_time, lab_remark, myql, myqlunits, myqldesc,
                            analysis_prep_date_tx)
select rownum wqp_id,
       2 data_source_id,
       cast('NWIS' as varchar2(4000 char)) data_source,
       s.station_id,
       s.site_id,
       trunc(samp.sample_start_dt) event_date,
       nemi.nemi_url analytical_method,
       r.parameter_cd p_code,
       samp.nwis_host || '.' || samp.qw_db_no || '.' || samp.record_no activity,
       parm.srsname characteristic_name,
       parm.parm_seq_grp_nm characteristic_type,
       wqx_medium_cd.wqx_act_med_nm sample_media,
       s.organization,
       s.site_type,
       s.huc_12,
       s.governmental_unit_code,
       s.geom,
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
       wqx_medium_cd.wqx_act_med_sub activity_media_subdiv_name,
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
       case when parameter.V72015 is not null then 'Below land-surface datum'
         when parameter.V82047 is not null then ''
         when parameter.V72016 is not null then 'Below land-surface datum'
         when parameter.V82048 is not null then ''
         else null
       end activity_uprlwr_depth_ref_pt,
       coalesce(parameter.v71999_fxd_nm, samp.project_cd, 'USGS') project_id,
       coalesce(proto_org2.proto_org_nm, samp.coll_ent_cd) activity_conducting_org,
       trim(samp.sample_lab_cm_tx) activity_comment,
       aqfr.sample_aqfr_name,
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
       trim(parm.parm_size_tx) particle_size,
       r.lab_std_va precision,
       trim(r.result_lab_cm_tx) result_comment,
       null result_depth_meas_value,
       null result_depth_meas_unit_code,
       null result_depth_alt_ref_pt_txt,
       case
         when parm.PARM_MEDIUM_TX = 'Biological Tissue' then tu.composite_tu_name
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
       end myql,
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
            null) myqlunits,
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
            null) myqldesc,
       case
         when r.prep_dt is not null then substr(r.prep_dt, 1, 4) || '-' || substr(r.prep_dt, 5, 2) || '-' || substr(r.prep_dt, 7, 2)
         else null
       end analysis_prep_date_tx 
  from nwis_qw_result r,
       nwis_qw_sample samp,
       (select tz_cd, tz_utc_offset_tm
          from nwq_stg_lu_tz
         where tz_cd is not null
        union 
        select tz_dst_cd, tz_dst_utc_offset_tm tz_utc_offset_tm
          from nwq_stg_lu_tz
        where tz_dst_cd is not null) lu_tz,
        nwis_sitefile site,
        station_swap_nwis s,
        (select /*+ full(p2) parallel(p2, 4) full(fxd_71999) parallel(fxd_71999, 4) full(fxd_82398) parallel(fxd_82398, 4)
                    full(fxd_84164) parallel(fxd_84164, 4)
                    use_hash(p2) use_hash(fxd_71999) use_hash(82398) use_hash(84164) */
                sample_id,
                v71999,
                v50280,
                v72015,
                v82047,
                v72016,
                v82048,
                v00003,
                v00098,
                v78890,
                v78891,
                v82398,
                v84164,
                v71999_fxd_nm,
                v82398_fxd_tx,
                v84164_fxd_tx
           from (select /*+ full(p1) parallel(p1, 4) */
                        sample_id,
                        max(case when parameter_cd = '71999' then result_unrnd_va else null end) AS V71999,
                        max(case when parameter_cd = '50280' then result_unrnd_va else null end) AS V50280,
                        max(case when parameter_cd = '72015' then result_unrnd_va else null end) AS V72015,
                        max(case when parameter_cd = '82047' then result_unrnd_va else null end) AS V82047,
                        max(case when parameter_cd = '72016' then result_unrnd_va else null end) AS V72016,
                        max(case when parameter_cd = '82048' then result_unrnd_va else null end) AS V82048,
                        max(case when parameter_cd = '00003' then result_unrnd_va else null end) AS V00003,
                        max(case when parameter_cd = '00098' then result_unrnd_va else null end) AS V00098,
                        max(case when parameter_cd = '78890' then result_unrnd_va else null end) AS V78890,
                        max(case when parameter_cd = '78891' then result_unrnd_va else null end) AS V78891,
                        max(case when parameter_cd = '82398' then result_unrnd_va else null end) AS V82398,
                        max(case when parameter_cd = '84164' then result_unrnd_va else null end) AS V84164
                   from nwis_qw_result p1
                  where result_web_cd = 'Y' and
                        parameter_cd in ('71999', '50280', '72015', '82047', '72016', '82048', '00003', '00098', '78890', '78891', '82398', '84164')
                     group by sample_id) p2,
                 (select fxd_nm v71999_fxd_nm, fxd_va from nwis_ws_stg_fxd where parm_cd = '71999') fxd_71999,
                 (select fxd_tx v82398_fxd_tx, fxd_va from nwis_ws_stg_fxd where parm_cd = '82398') fxd_82398,
                 (select fxd_tx v84164_fxd_tx, fxd_va from nwis_ws_stg_fxd where parm_cd = '84164') fxd_84164
          where p2.v71999 = fxd_71999.fxd_va(+) and
                p2.v82398 = fxd_82398.fxd_va(+) and
                p2.v84164 = fxd_84164.fxd_va(+)) parameter,
        (select tu_id,
                trim(tu_1_nm) ||
                  case when trim(tu_2_cd) is not null then ' ' || trim(tu_2_cd) end ||
                  case when trim(tu_2_nm) is not null then ' ' || trim(tu_2_nm) end ||
                  case when trim(tu_3_cd) is not null then ' ' || trim(tu_3_cd) end ||
                  case when trim(tu_3_nm) is not null then ' ' || trim(tu_3_nm) end ||
                  case when trim(tu_4_cd) is not null then ' ' || trim(tu_4_cd) end ||
                  case when trim(tu_4_nm) is not null then ' ' || trim(tu_4_nm) end AS composite_tu_name
           from nwis_ws_stg_tu) tu,
        (select trim(wqx_act_med_nm) wqx_act_med_nm,
                trim(wqx_act_med_sub) wqx_act_med_sub,
                trim(nwis_medium_cd) medium_cd
           from nwis_wqx_medium_cd) wqx_medium_cd,
        (select trim(body_part_nm) body_part_nm,
                trim(body_part_id) body_part_id
           from nwis_ws_stg_body_part) body_part,
        (select /*+ full(a) full(b) full(z_parm_meth2) use_hash(a) use_hash(b) use_hash(z_parm_meth2) */
                a.parm_unt_tx,
                a.parm_frac_tx,
                a.parm_medium_tx,
                a.parm_stat_tx,
                a.parm_wt_tx,
                a.parm_temp_tx,
                a.parm_tm_tx,
                a.parm_cd,
                a.parm_size_tx,
                b.parm_seq_grp_nm,
                z_parm_alias.srsname,
                z_parm_alias.srsid,
                z_parm_alias.casrn,
                z_parm_meth2.multiplier
           from nwq_stg_lu_parm a,
                nwq_stg_lu_parm_seq_grp_cd b,
                (select parm_cd,
                        max(case when parm_alias_cd = 'SRSNAME' then parm_alias_nm else null end) AS srsname,
                        max(case when parm_alias_cd = 'SRSID'   then parm_alias_nm else null end) AS srsid  ,
                        max(case when parm_alias_cd = 'CASRN'   then parm_alias_nm else null end) AS casrn
                   from nwq_stg_lu_parm_alias
                      group by parm_cd
                      having max(case when parm_alias_cd = 'SRSNAME' then parm_alias_nm else null end) is not null) z_parm_alias,
                (select decode(REGEXP_INSTR(PARM_METH_RND_TX, '[1-9]', 1, 1),
                               1, '0.001',
                               2, '0.01',
                               3, '0.1',
                               4, '1.',
                               5, '10',
                               6, '100',
                               7, '1000',
                               8, '10000',
                               9, '100000') multiplier,
                        parm_cd
                   from nwq_stg_lu_parm_meth
                  where meth_cd is null) z_parm_meth2
          where a.parm_public_fg = 'Y' and
                a.parm_seq_grp_cd = b.parm_seq_grp_cd(+) and
                a.parm_cd = z_parm_alias.parm_cd and
                a.parm_cd = z_parm_meth2.parm_cd(+)) parm,
        (select fxd_tx,
                parm_cd,
                fxd_va
           from nwis_ws_stg_fxd) fxd,
        (select proto_org_nm,
                proto_org_cd
           from nwis_ws_stg_proto_org) proto_org,
        (select proto_org_nm,
                proto_org_cd
           from nwis_ws_stg_proto_org) proto_org2,
        (select /*+ full(meth1) full(z_cit_meth) use_hash(meth1) use_hash(z_cit_meth) */
                meth1.meth_cd,
                meth1.meth_nm,
                z_cit_meth.cit_nm
           from nwis_ws_stg_meth meth1,
                (select meth_cd, min(cit_nm) cit_nm from nwis_ws_stg_z_cit_meth group by meth_cd) z_cit_meth
          where meth1.meth_cd = z_cit_meth.meth_cd(+)) meth,
        (select decode(REGEXP_INSTR(PARM_METH_RND_TX, '[1-9]', 1, 1),
                       1, '0.001',
                       2, '0.01',
                       3, '0.1',
                       4, '1.',
                       5, '10',
                       6, '100',
                       7, '1000',
                       8, '10000',
                       9, '100000') multiplier,
                parm_cd,
                meth_cd
           from nwq_stg_lu_parm_meth) z_parm_meth,
        (select rpt_lev_cd, wqx_rpt_lev_nm from nwis_wqx_rpt_lev_cd) nwis_wqx_rpt_lev_cd,
        (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from nwis_ws_stg_val_qual_cd) val_qual_cd1,
        (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from nwis_ws_stg_val_qual_cd) val_qual_cd2,
        (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from nwis_ws_stg_val_qual_cd) val_qual_cd3,
        (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from nwis_ws_stg_val_qual_cd) val_qual_cd4,
        (select val_qual_nm || '. ' val_qual_nm, val_qual_cd from nwis_ws_stg_val_qual_cd) val_qual_cd5,
        (select trim(hyd_event_cd) hyd_event_cd, trim(hyd_event_nm) hyd_event_nm from nwis_ws_stg_hyd_event_cd) hyd_event_cd,
        (select trim(hyd_cond_cd) hyd_cond_cd, trim(hyd_cond_nm) hyd_cond_nm from nwis_ws_stg_hyd_cond_cd) hyd_cond_cd,
        (select district_cd, host_name from nwis_district_cds_by_host) dist,
        (select aqfr_cd, state_cd, trim(aqfr_nm) as SAMPLE_AQFR_NAME from nwis_ws_stg_aqfr) aqfr,
        (select analytical_procedure_source,
                analytical_procedure_id,
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
                end nemi_url
           from wqp_nemi_nwis_crosswalk) nemi
 where
    r.result_web_cd    = 'Y'                         and
   (r.RESULT_VA        is not null  OR
    r.RPT_LEV_VA       is not null  OR
    r.REMARK_CD        is not null)                  and
    r.sample_id        = samp.sample_id              and
    r.parameter_cd     = parm.parm_cd              and  /* not outer join on z_parm or z_parm_alias */
    r.parameter_cd     = fxd.parm_cd(+)              and
    case when r.result_va = '0.0' then '0' else r.result_va end = fxd.fxd_va(+) and
    r.anl_ent_cd       = proto_org.proto_org_cd(+)   and
    r.meth_cd          = meth.meth_cd(+)             and
    r.parameter_cd     = z_parm_meth.parm_cd(+)      and
    r.meth_cd          = z_parm_meth.meth_cd(+)      and
    r.rpt_lev_cd       = nwis_wqx_rpt_lev_cd.rpt_lev_cd(+) and
    substr(r.val_qual_tx, 1, 1) = val_qual_cd1.val_qual_cd(+) and
    substr(r.val_qual_tx, 2, 1) = val_qual_cd2.val_qual_cd(+) and
    substr(r.val_qual_tx, 3, 1) = val_qual_cd3.val_qual_cd(+) and
    substr(r.val_qual_tx, 4, 1) = val_qual_cd4.val_qual_cd(+) and
    substr(r.val_qual_tx, 5, 1) = val_qual_cd5.val_qual_cd(+) and
    samp.sample_web_cd = 'Y'                         and
    samp.qw_db_no      = '01'                        and
    samp.site_id       = site.site_id                  and
    samp.sample_id     = parameter.sample_id(+)        and
    to_number(samp.tu_id) = tu.tu_id(+)                and
    samp.medium_cd     = wqx_medium_cd.medium_cd(+)    and
    samp.body_part_id  = body_part.body_part_id(+)     and
    samp.coll_ent_cd   = proto_org2.proto_org_cd(+)    and
    samp.hyd_event_cd  = hyd_event_cd.hyd_event_cd(+)  and
    samp.hyd_cond_cd   = hyd_cond_cd.hyd_cond_cd(+)    and
    site.dec_lat_va    <> 0                            and
    site.dec_long_va   <> 0                            and
    site.db_no         = '01'                        and
    site.site_web_cd   = 'Y'                         and
    site.site_tp_cd not in ('FA-WTP', 'FA-WWTP', 'FA-TEP', 'FA-HP')  and
    site.nwis_host  not in ('fltlhsr001', 'fltpasr001', 'flalssr003') and
    site.district_cd   = dist.district_cd              and
    site.nwis_host     = dist.host_name and
    samp.SAMPLE_START_TIME_DATUM_CD = lu_tz.tz_cd(+) and
    samp.aqfr_cd  = aqfr.aqfr_cd (+) and
    site.state_cd = aqfr.state_cd(+) and
    nvl2(r.meth_cd, 'USGS', null) = nemi.analytical_procedure_source(+) and
    trim(r.meth_cd) = nemi.analytical_procedure_id(+) and
    site.site_id = s.station_id;


commit;

select 'transform result end time: ' || systimestamp from dual;
