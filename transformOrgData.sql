show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform org_data start time: ' || systimestamp from dual;

prompt building org_data_swap_nwis 

prompt dropping nwis org_data indexes
exec etl_helper_org_data.drop_indexes('nwis');

set define off;
prompt populating org_data_swap_nwis
truncate table org_data_swap_nwis;
insert /*+ append parallel(4) */
  into org_data_swap_nwis (data_source_id, data_source, organization, organization_name)
select DISTINCT /* parallel(4) */
       2 data_source_id,
	   'NWIS' data_source,
	   cast('USGS-' || state_postal_cd as varchar2(7)) organization,
       'USGS ' || state_name || ' Water Science Center' organization_name
  from nwis_ws_star.nwis_district_cds_by_host