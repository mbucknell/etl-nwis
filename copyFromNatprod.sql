show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'copy from natprod start time: ' || systimestamp from dual;

prompt altitude_datum
truncate table altitude_datum;
insert /*+ append parallel(4) */ into altitude_datum
select trim(gw_ref_cd),
       trim(gw_ref_nm),
       gw_sort_nu,
       trim(gw_ref_ds),
       trim(gw_valid_fg)
  from natdb.gw_reflist@natdb.er.usgs.gov
 where gw_ed_tbl_nm = 'saltdm';
commit;

prompt altitude_method
truncate table altitude_method;
insert /*+ append parallel(4) */ into altitude_method
select trim(gw_ref_cd),
       trim(gw_ref_nm),
       gw_sort_nu,
       trim(gw_ref_ds),
       trim(gw_valid_fg)
  from natdb.gw_reflist@natdb.er.usgs.gov
 where gw_ed_tbl_nm = 'saltmt';
commit;

prompt aqfr
truncate table aqfr;
insert /*+ append parallel(4) */ into aqfr
select trim(aqfr_state.country_cd),
       trim(aqfr_state.state_cd),
       trim(aqfr.aqfr_cd),
       trim(aqfr.aqfr_nm),
       trim(aqfr.aqfr_dt)
  from natdb.aqfr@natdb.er.usgs.gov
       join natdb.aqfr_state@natdb.er.usgs.gov
         on aqfr.aqfr_cd = aqfr_state.aqfr_cd;
commit;

prompt aquifer_type
truncate table aquifer_type;
insert /*+ append parallel(4) */ into aquifer_type
select trim(gw_ref_cd),
       trim(gw_ref_nm),
       gw_sort_nu,
       trim(gw_ref_ds),
       trim(gw_valid_fg)
  from natdb.gw_reflist@natdb.er.usgs.gov
 where gw_ed_tbl_nm = 'saqtyp';
commit;

prompt body_part
truncate table body_part;
insert /*+ append parallel(4) */ into body_part
select trim(body_part_id),
       trim(body_part_nm)
  from natdb.body_part@natdb.er.usgs.gov;
commit;

prompt cit_meth
truncate table cit_meth;
insert /*+ append parallel(4) */ into cit_meth
select cit_meth_id,
       trim(meth_cd),
       trim(cit_nm),
       trim(cit_meth_no),
       trim(meth_src_cd),
       trim(cit_meth_init_nm),
       cit_meth_init_dt,
       trim(cit_meth_rev_nm),
       cit_meth_rev_dt
  from natdb.cit_meth@natdb.er.usgs.gov;
commit;

prompt country
truncate table country;
insert /*+ append parallel(4) */ into country
select trim(country_cd),
       trim(country_nm)
  from natdb.country@natdb.er.usgs.gov;
commit;

prompt county
truncate table county;
insert /*+ append parallel(4) */ into county
select trim(country_cd),
       trim(state_cd),
       trim(county_cd),
       trim(county_nm),
       trim(county_max_lat_va),
       trim(county_min_lat_va),
       trim(county_max_long_va),
       trim(county_min_long_va),
       trim(county_max_alt_va),
       trim(county_min_alt_va),
       trim(county_md)
  from natdb.county@natdb.er.usgs.gov;
commit;

prompt fxd
truncate table fxd;
insert /*+ append parallel(4) */ into fxd
select trim(parm_cd),
       fxd_va,
       trim(fxd_nm),
       trim(fxd_tx),
       fxd_init_dt,
       trim(fxd_init_nm),
       fxd_rev_dt,
       trim(fxd_rev_nm)
  from natdb.fxd@natdb.er.usgs.gov;
commit;

prompt hyd_cond_cd
truncate table hyd_cond_cd;
insert /*+ append parallel(4) */ into hyd_cond_cd
select trim(hyd_cond_cd),
       hyd_cond_srt_nu,
       trim(hyd_cond_vld_fg),
       trim(hyd_cond_nm),
       trim(hyd_cond_ds)
  from natdb.hyd_cond_cd@natdb.er.usgs.gov;
commit;

prompt hyd_event_cd
truncate table hyd_event_cd;
insert /*+ append parallel(4) */ into hyd_event_cd
select trim(hyd_event_cd),
       hyd_event_srt_nu,
       trim(hyd_event_vld_fg),
       trim(hyd_event_nm),
       trim(hyd_event_ds)
  from natdb.hyd_event_cd@natdb.er.usgs.gov;
commit;

prompt lat_long_datum
truncate table lat_long_datum;
insert /*+ append parallel(4) */ into lat_long_datum
select trim(gw_ref_cd),
       trim(gw_ref_nm),
       gw_sort_nu,
       trim(gw_ref_ds),
       trim(gw_valid_fg)
  from natdb.gw_reflist@natdb.er.usgs.gov
 where gw_ed_tbl_nm = 'scordm';
commit;

prompt lat_long_method
truncate table lat_long_method;
insert /*+ append parallel(4) */ into lat_long_method
select trim(gw_ref_cd),
       trim(gw_ref_nm),
       gw_sort_nu,
       trim(gw_ref_ds),
       trim(gw_valid_fg)
  from natdb.gw_reflist@natdb.er.usgs.gov
 where gw_ed_tbl_nm = 'scormt';
commit;

prompt meth
truncate table meth;
insert /*+ append parallel(4) */ into meth
select trim(meth_cd),
       trim(meth_tp),
       trim(meth_nm),
       trim(meth_ds),
       trim(meth_rnd_owner_cd),
       trim(discipline_cd),
       trim(meth_init_nm),
       meth_init_dt,
       trim(meth_rev_nm),
       meth_rev_dt
  from natdb.meth@natdb.er.usgs.gov;
commit;

prompt meth_with_cit
truncate table meth_with_cit;
insert /*+ append parallel(4) */ into meth_with_cit
select meth.meth_cd,
       meth.meth_nm,
       min(cit_meth.cit_nm)
  from meth
       left join cit_meth
         on meth.meth_cd = cit_meth.meth_cd
    group by meth.meth_cd, meth.meth_nm;
commit;

prompt nat_aqfr
truncate table nat_aqfr;
insert /*+ append parallel(4) */ into nat_aqfr
select trim(nat_aqfr_state.country_cd),
       trim(nat_aqfr_state.state_cd),
       trim(nat_aqfr.nat_aqfr_cd),
       trim(nat_aqfr.nat_aqfr_nm)
  from natdb.nat_aqfr@natdb.er.usgs.gov
       join natdb.nat_aqfr_state@natdb.er.usgs.gov
         on nat_aqfr.nat_aqfr_cd = nat_aqfr_state.nat_aqfr_cd;
commit;

prompt parm_meth
truncate table parm_meth;
insert /*+ append parallel(4) */ into parm_meth
select trim(parm_cd),
       trim(meth_cd),
       trim(parm_meth_lgcy_cd),
       trim(parm_meth_rnd_tx),
       trim(parm_meth_init_nm),
       parm_meth_init_dt,
       trim(parm_meth_rev_nm),
       parm_meth_rev_dt,
       trim(parm_meth_vld_fg),
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
  from natdb.parm_meth@natdb.er.usgs.gov;
commit;

prompt parm
truncate table parm;
insert /*+ append parallel(4) */ into parm
select trim(parm.parm_cd),
       trim(parm.parm_nm),
       trim(parm.parm_rmk_tx),
       trim(parm.parm_unt_tx),
       trim(parm.parm_seq_nu),
       trim(parm.parm_seq_grp_cd),
       trim(parm.parm_ds),
       trim(parm.parm_medium_tx),
       trim(parm.parm_frac_tx),
       trim(parm.parm_temp_tx),
       trim(parm.parm_stat_tx),
       trim(parm.parm_tm_tx),
       trim(parm.parm_wt_tx),
       trim(parm.parm_size_tx),
       trim(parm.parm_entry_fg),
       trim(parm.parm_public_fg),
       trim(parm.parm_neg_fg),
       trim(parm.parm_zero_fg),
       trim(parm.parm_null_fg),
       trim(parm.parm_ts_fg),
       trim(parm.parm_init_dt),
       trim(parm.parm_init_nm),
       trim(parm.parm_rev_dt),
       trim(parm.parm_rev_nm),
       parm_seq_grp_cd.parm_seq_grp_nm,
       parm_alias.srsname,
       parm_alias.srsid,
       parm_alias.casrn,
       parm_meth.multiplier
  from natdb.parm@natdb.er.usgs.gov
       left join natdb.parm_seq_grp_cd@natdb.er.usgs.gov
         on trim(parm.parm_seq_grp_cd) = trim(parm_seq_grp_cd.parm_seq_grp_cd)
       join (select *
               from (select parm_cd, parm_alias_cd, parm_alias_nm
                       from natdb.parm_alias@natdb.er.usgs.gov
                      where parm_alias_cd in ('SRSNAME', 'SRSID', 'CASRN'))
                     pivot (max(parm_alias_nm)
                            for parm_alias_cd in ('SRSNAME' srsname, 'SRSID' srsid, 'CASRN' casrn))
               where srsname is not null) parm_alias
         on trim(parm.parm_cd) = trim(parm_alias.parm_cd)
       left join parm_meth
         on trim(parm.parm_cd) = parm_meth.parm_cd and
            parm_meth.meth_cd is null
 where parm.parm_public_fg = 'Y';
commit;

prompt parm_alias
truncate table parm_alias;
insert /*+ append parallel(4) */ into parm_alias
select trim(parm_cd),
       trim(parm_alias_cd),
       trim(parm_alias_nm),
       parm_alias_init_dt,
       trim(parm_alias_init_nm),
       parm_alias_rev_dt,
       trim(parm_alias_rev_nm)
  from natdb.parm_alias@natdb.er.usgs.gov;
commit;

prompt proto_org
truncate table proto_org;
insert /*+ append parallel(4) */ into proto_org
select trim(proto_org_cd),
       trim(proto_org_nm),
       trim(proto_org_fv_cd),
       trim(proto_org_vld_fg),
       trim(proto_org_init_nm),
       proto_org_init_dt,
       trim(proto_org_rev_nm),
       proto_org_rev_dt
  from natdb.proto_org@natdb.er.usgs.gov;
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

prompt site_tp
truncate table site_tp;
insert /*+ append parallel(4) */ into site_tp
select trim(site_tp_cd),
       site_tp_srt_nu,
       trim(site_tp_vld_fg),
       trim(site_tp_prim_fg),
       trim(site_tp_nm),
       trim(site_tp_ln),
       trim(site_tp_ds)
  from natdb.site_tp@natdb.er.usgs.gov;
commit;

prompt state
truncate table state;
insert /*+ append parallel(4) */ into state
select trim(country_cd),
       trim(state_cd),
       trim(state_nm),
       trim(state_post_cd),
       trim(state_max_lat_va),
       trim(state_min_lat_va),
       trim(state_max_long_va),
       trim(state_min_long_va),
       trim(state_max_alt_va),
       trim(state_min_alt_va),
       trim(state_md)
 from natdb.state@natdb.er.usgs.gov;
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
       trim(tu_unnm_cd),
       trim(tu_use_cd),
       trim(tu_unaccp_rsn_tx),
       trim(tu_cred_rat_tx),
       trim(tu_cmplt_rat_cd),
       trim(tu_curr_rat_cd),
       tu_phyl_srt_nu,
       tu_cr,
       tu_par_id,
       tu_tax_auth_id,
       tu_hybr_auth_id,
       tu_king_id,
       tu_rnk_id,
       tu_md
  from natdb.tu@natdb.er.usgs.gov;
commit;

prompt val_qual_cd
truncate table val_qual_cd;
insert /*+ append parallel(4) */ into val_qual_cd
select trim(val_qual_cd),
       trim(val_qual_tp),
       val_qual_srt_nu,
       trim(val_qual_vld_fg),
       trim(val_qual_nm),
       trim(val_qual_ds)
  from natdb.val_qual_cd@natdb.er.usgs.gov;
commit;

select 'copy from natprod end time: ' || systimestamp from dual;
