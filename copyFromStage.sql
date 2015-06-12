show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'copy from stage start time: ' || systimestamp from dual;

prompt aqfr
truncate table aqfr;
insert /*+ append parallel(4) */ into aqfr
select state_cd, trim(aqfr_cd), trim(aqfr_nm), aqfr_md from aqfr@nwis_ws_stg.er.usgs.gov;
commit;

prompt body_part
truncate table body_part;
insert /*+ append parallel(4) */ into body_part
select trim(body_part_id),
       trim(body_part_nm)
  from body_part@nwis_ws_stg.er.usgs.gov;
commit;

prompt country
truncate table country;
insert /*+ append parallel(4) */ into country
select * from country@nwis_ws_stg.er.usgs.gov;
commit;

prompt county
truncate table county;
insert /*+ append parallel(4) */ into county
select * from county@nwis_ws_stg.er.usgs.gov;
commit;

prompt fxd
truncate table fxd;
insert /*+ append parallel(4) */ into fxd
select * from fxd@nwis_ws_stg.er.usgs.gov;
commit;

prompt hyd_cond_cd
truncate table hyd_cond_cd;
insert /*+ append parallel(4) */ into hyd_cond_cd
select trim(hyd_cond_cd),
       hyd_cond_srt_nu,
       hyd_cond_vld_fg,
       trim(hyd_cond_nm),
       hyd_cond_ds
  from hyd_cond_cd@nwis_ws_stg.er.usgs.gov;
commit;

prompt hyd_event_cd
truncate table hyd_event_cd;
insert /*+ append parallel(4) */ into hyd_event_cd
select trim(hyd_event_cd),
       hyd_event_srt_nu,
       hyd_event_vld_fg,
       trim(hyd_event_nm),
       hyd_event_ds
  from hyd_event_cd@nwis_ws_stg.er.usgs.gov;
commit;

prompt lu_parm
truncate table lu_parm;
insert /*+ append parallel(4) */ into lu_parm
select * from lu_parm@nwq_stg.er.usgs.gov;
commit;

prompt lu_parm_alias
truncate table lu_parm_alias;
insert /*+ append parallel(4) */ into lu_parm_alias
select * from lu_parm_alias@nwq_stg.er.usgs.gov;
commit;

prompt lu_parm_meth
truncate table lu_parm_meth;
insert /*+ append parallel(4) */ into lu_parm_meth
select * from lu_parm_meth@nwq_stg.er.usgs.gov;
commit;

prompt lu_parm_seq_grp_cd
truncate table lu_parm_seq_grp_cd;
insert /*+ append parallel(4) */ into lu_parm_seq_grp_cd
select * from lu_parm_seq_grp_cd@nwq_stg.er.usgs.gov;
commit;

prompt lu_tz
truncate table lu_tz;
insert /*+ append parallel(4) */ into lu_tz (tz_cd, tz_nm, tz_ds, tz_utc_offset_tm)
select tz_cd, tz_nm, tz_ds, tz_utc_offset_tm
  from lu_tz@nwq_stg.er.usgs.gov
 where tz_cd is not null
union 
select tz_dst_cd, tz_dst_nm, tz_ds, tz_dst_utc_offset_tm
  from lu_tz@nwq_stg.er.usgs.gov
 where tz_dst_cd is not null;
commit;

prompt meth
truncate table meth;
insert /*+ append parallel(4) */ into meth
select * from meth@nwis_ws_stg.er.usgs.gov;
commit;

prompt nat_aqfr
truncate table nat_aqfr;
insert /*+ append parallel(4) */ into nat_aqfr
select * from nat_aqfr@nwis_ws_stg.er.usgs.gov;
commit;

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

prompt nwis_district_cds_by_host
truncate table nwis_district_cds_by_host;
insert /*+ append parallel(4) */ into nwis_district_cds_by_host
select * from nwis_district_cds_by_host@nwis_ws_stg.er.usgs.gov;
commit;

prompt nwis_misc_lookups
truncate table nwis_misc_lookups;
insert /*+ append parallel(4) */ into nwis_misc_lookups
select * from nwis_misc_lookups@nwis_ws_stg.er.usgs.gov;
commit;

prompt nwis_wqx_medium_cd
truncate table nwis_wqx_medium_cd;
insert /*+ append parallel(4) */ into nwis_wqx_medium_cd
select legacy_nwis_medium_cd,
       trim(nwis_medium_cd),
       trim(nwis_medium_nm), 
       trim(wqx_act_med_nm),
       trim(wqx_act_med_sub),
       load_date
  from nwis_wqx_medium_cd@nwis_ws_stg.er.usgs.gov;
commit;

prompt nwis_wqx_rpt_lev_cd
truncate table nwis_wqx_rpt_lev_cd;
insert /*+ append parallel(4) */ into nwis_wqx_rpt_lev_cd
select * from nwis_wqx_rpt_lev_cd@nwis_ws_stg.er.usgs.gov;
commit;

prompt proto_org
truncate table proto_org;
insert /*+ append parallel(4) */ into proto_org
select * from proto_org@nwis_ws_stg.er.usgs.gov;
commit;

prompt site_tp
truncate table site_tp;
insert /*+ append parallel(4) */ into site_tp
select * from site_tp@nwis_ws_stg.er.usgs.gov;
commit;

prompt state
truncate table state;
insert /*+ append parallel(4) */ into state
select * from state@nwis_ws_stg.er.usgs.gov;
commit;

prompt state_fips
truncate table state_fips;
insert /*+ append parallel(4) */ into state_fips
select * from state_fips@nwis_ws_stg.er.usgs.gov;
commit;

prompt tu
truncate table tu;
insert /*+ append parallel(4) */ into tu
select tu_id,
       trim(tu_1_cd),
       trim(tu_1_nm),
       trim(tu_2_cd),
       trim(tu_2_nm),
       trim(tu_3_cd),
       trim(tu_3_nm),
       trim(tu_4_cd),
       trim(tu_4_nm),
       tu_unnm_cd,
       tu_use_cd,
       tu_unaccp_rsn_tx,
       tu_cred_rat_tx,
       tu_cmplt_rat_cd,
       tu_curr_rat_cd,
       tu_phyl_srt_nu,
       tu_cr,
       tu_par_id,
       tu_tax_auth_id,
       tu_hybr_auth_id,
       tu_king_id,
       tu_rnk_id,
       tu_md
  from tu@nwis_ws_stg.er.usgs.gov;
commit;

prompt val_qual_cd
truncate table val_qual_cd;
insert /*+ append parallel(4) */ into val_qual_cd
select val_qual_cd,
       val_qual_srt_nu,
       val_qual_vld_fg,
       val_qual_nm || '. ',
       val_qual_ds
  from val_qual_cd@nwis_ws_stg.er.usgs.gov;
commit;

prompt z_cit_meth
truncate table z_cit_meth;
insert /*+ append parallel(4) */ into z_cit_meth
select * from z_cit_meth@nwis_ws_stg.er.usgs.gov;
commit;

--reactivate when nawqa_sites@nwq_data_checks.er.usgs.gov is pointing at a production database!!
--prompt nawqa_sites
--truncate table nawqa_sites;
--insert /*+ append parallel(4) */ into nawqa_sites
--select * from nawqa_sites@nwq_data_checks.er.usgs.gov;
--commit;

prompt z_parm_meth
truncate table z_parm_meth;
insert /*+ append parallel(4) */ into z_parm_meth
select parm_cd,
       meth_cd,
       decode(regexp_instr(parm_meth_rnd_tx, '[1-9]', 1, 1),
              1, '0.001',
              2, '0.01',
              3, '0.1',
              4, '1.',
              5, '10',
              6, '100',
              7, '1000',
              8, '10000',
              9, '100000') multiplier
  from lu_parm_meth;
commit;

prompt parm
truncate table parm;
insert /*+ append parallel(4) */ into parm
select a.parm_unt_tx,
       a.parm_frac_tx,
       a.parm_medium_tx,
       a.parm_stat_tx,
       a.parm_wt_tx,
       a.parm_temp_tx,
       a.parm_tm_tx,
       a.parm_cd,
       trim(a.parm_size_tx),
       b.parm_seq_grp_nm,
       z_parm_alias.srsname,
       z_parm_alias.srsid,
       z_parm_alias.casrn,
       z_parm_meth.multiplier
  from lu_parm a
       left join lu_parm_seq_grp_cd b
         on a.parm_seq_grp_cd = b.parm_seq_grp_cd
       join (select *
               from (select parm_cd, parm_alias_cd, parm_alias_nm
                       from lu_parm_alias
                      where parm_alias_cd in ('SRSNAME', 'SRSID', 'CASRN'))
                     pivot (max(parm_alias_nm)
                            for parm_alias_cd in ('SRSNAME' srsname, 'SRSID' srsid, 'CASRN' casrn))
               where srsname is not null) z_parm_alias
         on a.parm_cd = z_parm_alias.parm_cd
       left join z_parm_meth
         on a.parm_cd = z_parm_meth.parm_cd
 where a.parm_public_fg = 'Y' and
       z_parm_meth.meth_cd is null;
commit;
       
prompt meth_with_cit
truncate table meth_with_cit;
insert /*+ append parallel(4) */ into meth_with_cit
select meth.meth_cd,
       meth.meth_nm,
       min(z_cit_meth.cit_nm)
  from meth
       left join z_cit_meth
         on meth.meth_cd = z_cit_meth.meth_cd
    group by meth.meth_cd, meth.meth_nm;
commit;

prompt sample_parameter
truncate table sample_parameter;
insert /*+ append parallel(4) */ into sample_parameter
select sample_id,
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
  from (select * from (select sample_id, parameter_cd, result_unrnd_va
                         from qw_result
                        where result_web_cd = 'Y' and
                              parameter_cd in ('71999', '50280', '72015', '82047', '72016', '82048', '00003', '00098', '78890', '78891', '82398', '84164'))
                       pivot (max(result_unrnd_va)
                              for parameter_cd in ('71999' V71999, '50280' V50280, '72015' V72015, '82047' V82047, '72016' V72016, '82048' V82048,
                                                   '00003' V00003, '00098' V00098, '78890' V78890, '78891' V78891, '82398' V82398, '84164' V84164))
       ) p2
       left join (select fxd_nm v71999_fxd_nm, fxd_va from fxd where parm_cd = '71999') fxd_71999
         on p2.v71999 = fxd_71999.fxd_va
       left join (select fxd_tx v82398_fxd_tx, fxd_va from fxd where parm_cd = '82398') fxd_82398
         on p2.v82398 = fxd_82398.fxd_va
       left join (select fxd_tx v84164_fxd_tx, fxd_va from fxd where parm_cd = '84164') fxd_84164
         on p2.v84164 = fxd_84164.fxd_va;
commit;

select 'copy from stage end time: ' || systimestamp from dual;
