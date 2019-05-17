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