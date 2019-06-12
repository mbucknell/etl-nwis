insert into result_swap_nwis (data_source_id, data_source, station_id, site_id, event_date, analytical_method, p_code, activity,
                       characteristic_name, characteristic_type, sample_media, organization, site_type, huc, governmental_unit_code, geom,
                       organization_name, activity_id, activity_type_code, activity_media_subdiv_name, activity_start_time,
                       act_start_time_zone, activity_stop_date, activity_stop_time, act_stop_time_zone, activity_depth,
                       activity_depth_unit, activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit,
                       activity_lower_depth, activity_lower_depth_unit, project_id,
                       activity_conducting_org, activity_comment, sample_aqfr_name, hydrologic_condition_name, hydrologic_event_name,
                       sample_collect_method_id, sample_collect_method_ctx, sample_collect_method_name, sample_collect_equip_name,
                       result_id, result_detection_condition_tx, sample_fraction_type, result_measure_value, result_unit,
                       result_value_status, statistic_type, result_value_type, weight_basis_type, duration_basis,
                       temperature_basis_level, particle_size, precision, result_comment, sample_tissue_taxonomic_name,
                       sample_tissue_anatomy_name, analytical_procedure_id, analytical_procedure_source, analytical_method_name,
                       analytical_method_citation, lab_name, analysis_start_date, lab_remark, detection_limit, detection_limit_unit,
                       detection_limit_desc, analysis_prep_date_tx)
select
    activity_swap_nwis.data_source_id,
    activity_swap_nwis.data_source,
    activity_swap_nwis.station_id,
    activity_swap_nwis.site_id,
    activity_swap_nwis.event_date,
    nemi.nemi_url analytical_method,
    r.parameter_cd p_code,
    activity_swap_nwis.activity,
    coalesce(parm.wqpcrosswalk, parm.srsname) characterstic_name,
    parm.parm_seq_grp_nm characteristic_type,
    activity_swap_nwis.sample_media,
    activity_swap_nwis.organization,
    activity_swap_nwis.site_type,
    activity_swap_nwis.huc,
    activity_swap_nwis.governmental_unit_code,
    activity_swap_nwis.geom,
    activity_swap_nwis.organization_name,
    activity_swap_nwis.activity_id,
    activity_swap_nwis.activity_type_code,
    activity_swap_nwis.activity_media_subdiv_name,
    activity_swap_nwis.activity_start_time,
    activity_swap_nwis.act_start_time_zone,
    activity_swap_nwis.activity_stop_date,
    activity_swap_nwis.activity_stop_time,
    activity_swap_nwis.act_stop_time_zone,
    activity_swap_nwis.activity_depth,
    activity_swap_nwis.activity_depth_unit,
    activity_swap_nwis.activity_depth_ref_point,
    activity_swap_nwis.activity_upper_depth,
    activity_swap_nwis.activity_upper_depth_unit,
    activity_swap_nwis.activity_lower_depth,
    activity_swap_nwis.activity_lower_depth_unit,
    activity_swap_nwis.project_id,
    activity_swap_nwis.activity_conducting_org,
    activity_swap_nwis.activity_comment,
    activity_swap_nwis.sample_aqfr_name,
    activity_swap_nwis.hydrologic_condition_name,
    activity_swap_nwis.hydrologic_event_name,
    activity_swap_nwis.sample_collect_method_id,
    activity_swap_nwis.sample_collect_method_ctx,
    activity_swap_nwis.sample_collect_method_name,
    activity_swap_nwis.sample_collect_equip_name,
    r.result_id,
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
    coalesce(fxd.fxd_tx,
        case
            when r.remark_cd in ('U', 'M', 'N', '<', '>') then null
            when r.remark_cd in ('R', 'V', 'S', 'E', 'A') or r.remark_cd is null then r.result_va
            else r.result_va
        end) result_measure_value,
    case when coalesce(
                fxd.fxd_tx,
                case
                   when r.remark_cd in ('U', 'M', 'N', '<', '>') then null
                   when r.remark_cd in ('R', 'V', 'S', 'E', 'A') or r.remark_cd is null then r.result_va
                   else r.result_va
                end) is not null then parm.parm_unt_tx
        else null
    end result_unit,
    case
        when r.dqi_cd = 'S' then 'Preliminary'
        when r.dqi_cd = 'A' then 'Historical'
        when r.dqi_cd = 'R' then 'Accepted'
        else null
    end result_value_status,
    coalesce(
        parm.parm_stat_tx,
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
    r.lab_std_va r_precision,
    trim(r.result_lab_cm_tx) result_comment,
    case
        when parm.parm_medium_tx = 'Biological Tissue'
            then (select tu.tu_1_nm ||
                         case when tu.tu_2_cd is not null then ' ' || tu.tu_2_cd end ||
                         case when tu.tu_2_nm is not null then ' ' || tu.tu_2_nm end ||
                         case when tu.tu_3_cd is not null then ' ' || tu.tu_3_cd end ||
                         case when tu.tu_3_nm is not null then ' ' || tu.tu_3_nm end ||
                         case when tu.tu_4_cd is not null then ' ' || tu.tu_4_cd end ||
                         case when tu.tu_4_nm is not null then ' ' || tu.tu_4_nm end
                  from nwis.tu
                  where qw_sample.tu_id = cast(tu.tu_id as varchar))
        else null
    end sample_tissue_taxonomic_name,
    case
        when parm.parm_medium_tx = 'Biological Tissue'
            then (select body_part_nm
                  from nwis.body_part
                  where qw_sample.body_part_id = cast(body_part.body_part_id as varchar))
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
            then substring(r.anl_dt from 1 for 4) || '-' || substring(r.anl_dt from 5 for 2) || '-' || substring(r.anl_dt from 7 for 2)
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
         end
    ) lab_remark,
    case
        when r.remark_cd = '<' and r.rpt_lev_va is null then r.result_va
        when r.remark_cd = '<' and cast(r.result_unrnd_va as numeric) > cast(r.rpt_lev_va as numeric) then r.result_va
        when r.remark_cd = '>' then r.result_va
        when r.remark_cd in ('N', 'U') and r.rpt_lev_va is not null then r.rpt_lev_va
        when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
        when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then coalesce(parm_meth.multiplier, parm.multiplier)
        when nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then r.rpt_lev_va
        else null
    end detection_limit,
    case
        when (case
            when r.remark_cd = '<' and r.rpt_lev_va is null then r.result_va
            when r.remark_cd = '<' and cast(r.result_unrnd_va as numeric) > cast(r.rpt_lev_va as numeric) then r.result_va
            when r.remark_cd = '>' then r.result_va
            when r.remark_cd in ('N', 'U') and r.rpt_lev_va is not null then r.rpt_lev_va
            when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
            when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then coalesce(parm_meth.multiplier, parm.multiplier)
            when nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then r.rpt_lev_va
            else null
        end) is not null then parm.parm_unt_tx
        else null
    end detection_limit_unit,
    case when (
        case
            when r.remark_cd = '<' and r.rpt_lev_va is null then r.result_va
            when r.remark_cd = '<' and cast(r.result_unrnd_va as numeric) > cast(r.rpt_lev_va as numeric) then r.result_va
            when r.remark_cd = '>' then r.result_va
            when r.remark_cd in ('N', 'U') and r.rpt_lev_va is not null then r.rpt_lev_va
            when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
            when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then coalesce(parm_meth.multiplier, parm.multiplier)
            when nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then r.rpt_lev_va
            else null
        end) is not null then
            (case
                when r.remark_cd = '<' and r.rpt_lev_va is null then 'Historical Lower Reporting Limit'
                when r.remark_cd = '<' and cast(r.result_unrnd_va as numeric) > cast(r.rpt_lev_va as numeric) then 'Elevated Detection Limit'
                when r.remark_cd = '<' and nwis_wqx_rpt_lev_cd.rpt_lev_cd is not null then nwis_wqx_rpt_lev_cd.wqx_rpt_lev_nm
                when r.remark_cd = '>' then 'Upper Reporting Limit'
                when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is null then null
                when r.remark_cd = 'M' and r.rpt_lev_va is null and r.result_unrnd_va is not null then 'Lower Quantitation Limit'
                when r.remark_cd = 'M' and r.rpt_lev_va is not null then nwis_wqx_rpt_lev_cd.wqx_rpt_lev_nm
                when r.rpt_lev_va is not null then nwis_wqx_rpt_lev_cd.wqx_rpt_lev_nm
                else null
            end)
        else null
    end detection_limit_desc,
    case
        when r.prep_dt is not null then substring(r.prep_dt from 1 for 4) || '-' || substring(r.prep_dt from 5 for 2) || '-' || substring(r.prep_dt from 7 for 2)
        else null
    end analysis_prep_date_tx
from nwis.qw_result r
         join activity_swap_nwis
              on r.sample_id = activity_swap_nwis.activity_id
         join nwis.parm
              on r.parameter_cd = parm.parm_cd
         left join nwis.fxd
                   on r.parameter_cd = fxd.parm_cd and
                      case when r.result_va = '0.0' then '0' else r.result_va end = cast(fxd.fxd_va as varchar)
         left join nwis.proto_org
                   on r.anl_ent_cd = proto_org.proto_org_cd
         left join nwis.meth_with_cit meth
                   on r.meth_cd = meth.meth_cd
         left join nwis.parm_meth
                   on r.parameter_cd = parm_meth.parm_cd and
                      r.meth_cd = parm_meth.meth_cd
         left join nwis.nwis_wqx_rpt_lev_cd
                   on r.rpt_lev_cd = nwis_wqx_rpt_lev_cd.rpt_lev_cd
         left join nwis.val_qual_cd val_qual_cd1
                   on substring(r.val_qual_tx from 1 for 1) = val_qual_cd1.val_qual_cd
         left join nwis.val_qual_cd val_qual_cd2
                   on substring(r.val_qual_tx from 2 for 1) = val_qual_cd2.val_qual_cd
         left join nwis.val_qual_cd val_qual_cd3
                   on substring(r.val_qual_tx from 3 for 1) = val_qual_cd3.val_qual_cd
         left join nwis.val_qual_cd val_qual_cd4
                   on substring(r.val_qual_tx from 4 for 1) = val_qual_cd4.val_qual_cd
         left join nwis.val_qual_cd val_qual_cd5
                   on substring(r.val_qual_tx from 5 for 1) = val_qual_cd5.val_qual_cd
         left join nwis.wqp_nemi_nwis_crosswalk nemi
                   on coalesce(r.meth_cd, 'USGS') = nemi.analytical_procedure_source and
                      trim(r.meth_cd) = nemi.analytical_procedure_id
         left join nwis.qw_sample
                   on r.sample_id = qw_sample.sample_id and
                      parm.parm_medium_tx = 'Biological Tissue'
where
    r.result_web_cd = 'Y' and
    (r.result_va is not null or r.rpt_lev_va is not null or r.remark_cd is not null);
