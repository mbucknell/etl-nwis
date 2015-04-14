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
select * from aqfr@nwis_ws_stg.er.usgs.gov;
commit;

prompt body_part
truncate table body_part;
insert /*+ append parallel(4) */ into body_part
select * from body_part@nwis_ws_stg.er.usgs.gov;
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
select * from hyd_cond_cd@nwis_ws_stg.er.usgs.gov;
commit;

prompt hyd_event_cd
truncate table hyd_event_cd;
insert /*+ append parallel(4) */ into hyd_event_cd
select * from hyd_event_cd@nwis_ws_stg.er.usgs.gov;
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
insert /*+ append parallel(4) */ into lu_tz
select * from lu_tz@nwq_stg.er.usgs.gov;
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
       method_type
  from (select nemi_crosswalk.*,
               count(*) over (partition by analytical_procedure_source, analytical_procedure_id) cnt
          from nemi_crosswalk)
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
select * from nwis_wqx_medium_cd@nwis_ws_stg.er.usgs.gov;
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
select * from tu@nwis_ws_stg.er.usgs.gov;
commit;

prompt val_qual_cd
truncate table val_qual_cd;
insert /*+ append parallel(4) */ into val_qual_cd
select * from val_qual_cd@nwis_ws_stg.er.usgs.gov;
commit;

prompt z_cit_meth
truncate table z_cit_meth;
insert /*+ append parallel(4) */ into z_cit_meth
select * from z_cit_meth@nwis_ws_stg.er.usgs.gov;
commit;

select 'copy from stage end time: ' || systimestamp from dual;
