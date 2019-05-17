select
    activity_swap_nwis.station_id,
 	activity_swap_nwis.site_id,
   	activity_swap_nwis.event_date,
   	nemi.nemi_url,
   	qw_result.parameter_cd,
   	activity_swap_nwis.activity,
   	parm.wqpcrosswalk,
   	parm.srsname,
   	parm.parm_seq_grp_nm,
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
   	row_number () over (order by station_id) result_id,
   	qw_result.remark_cd,
   	parm.parm_frac_tx,
   	fxd.fxd_tx,
   	qw_result.result_va,
   	parm.parm_unt_tx,
   	qw_result.dqi_cd,
   	parm.parm_stat_tx,
   	qw_result.result_md,
   	parm.parm_wt_tx,
   	parm.parm_tm_tx,
   	parm.parm_temp_tx,
   	parm.parm_size_tx,
   	qw_result.lab_std_va,
   	qw_result.result_lab_cm_tx,
   	parm.parm_medium_tx,
   	case
    	when parm.parm_medium_tx = 'Biological Tissue'
           then (select tu.tu_1_nm ||
                        case when tu.tu_2_cd is not null then ' ' || tu.tu_2_cd end ||
                        case when tu.tu_2_nm is not null then ' ' || tu.tu_2_nm end ||
                        case when tu.tu_3_cd is not null then ' ' || tu.tu_3_cd end ||
                        case when tu.tu_3_nm is not null then ' ' || tu.tu_3_nm end ||
                        case when tu.tu_4_cd is not null then ' ' || tu.tu_4_cd end ||
                        case when tu.tu_4_nm is not null then ' ' || tu.tu_4_nm end
                   from tu
                  where qw_sample.tu_id = cast(tu.tu_id as varchar))
        else null
    end sample_tissue_taxonomic_name,
   	case
     	when parm.parm_medium_tx = 'Biological Tissue'
           then (select body_part_nm
                   from body_part
                  where qw_sample.body_part_id = cast(body_part.body_part_id as varchar))
        else null
    end sample_tissue_anatomy_name,
   	qw_result.meth_cd,
   	meth.meth_nm,
   	meth.cit_nm,
   	proto_org.proto_org_nm,
   	qw_result.anl_dt,
   	val_qual_cd1.val_qual_nm val_qual_cd1_nm,
   	val_qual_cd2.val_qual_nm val_qual_cd2_nm,
   	val_qual_cd3.val_qual_nm val_qual_cd3_nm,
   	val_qual_cd4.val_qual_nm val_qual_cd4_nm,
   	val_qual_cd5.val_qual_nm val_qual_cd5_nm,
   	qw_result.rpt_lev_va,
	qw_result.rpt_lev_cd,
	qw_result.result_unrnd_va,
	nwis_wqx_rpt_lev_cd.rpt_lev_cd,
	nwis_wqx_rpt_lev_cd.rpt_lev_nm,
	parm_meth.multiplier parm_meth_multiplier,
	parm.multiplier parm_multiplier,
	qw_result.prep_dt
from qw_result
       join activity_swap_nwis
         on qw_result.sample_id = activity_swap_nwis.activity_id
       join parm
         on qw_result.parameter_cd = parm.parm_cd
       left join fxd
         on qw_result.parameter_cd = fxd.parm_cd and
            case when qw_result.result_va = '0.0' then '0' else qw_result.result_va end  = cast (fxd.fxd_va as varchar)
       left join proto_org
         on qw_result.anl_ent_cd = proto_org.proto_org_cd
       left join meth_with_cit meth
         on qw_result.meth_cd = meth.meth_cd
       left join parm_meth
         on qw_result.parameter_cd = parm_meth.parm_cd and
            qw_result.meth_cd = parm_meth.meth_cd
       left join nwis_wqx_rpt_lev_cd
         on qw_result.rpt_lev_cd = nwis_wqx_rpt_lev_cd.rpt_lev_cd
       left join val_qual_cd val_qual_cd1
         on substr(qw_result.val_qual_tx, 1, 1) = val_qual_cd1.val_qual_cd
       left join val_qual_cd val_qual_cd2
         on substr(qw_result.val_qual_tx, 2, 1) = val_qual_cd2.val_qual_cd
       left join val_qual_cd val_qual_cd3
         on substr(qw_result.val_qual_tx, 3, 1) = val_qual_cd3.val_qual_cd
       left join val_qual_cd val_qual_cd4
         on substr(qw_result.val_qual_tx, 4, 1) = val_qual_cd4.val_qual_cd
       left join val_qual_cd val_qual_cd5
         on substr(qw_result.val_qual_tx, 5, 1) = val_qual_cd5.val_qual_cd
       left join wqp_nemi_nwis_crosswalk nemi
         on case when qw_result.meth_cd is null then null else 'USGS' end = nemi.analytical_procedure_source and
            trim(qw_result.meth_cd) = nemi.analytical_procedure_id
       left join qw_sample
         on qw_result.sample_id = qw_sample.sample_id and
            parm.parm_medium_tx = 'Biological Tissue'
 where qw_result.result_web_cd and
       (qw_result.result_va is not null or
        qw_result.rpt_lev_va is not null or
        qw_result.remark_cd is not null);	