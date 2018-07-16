show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform org start time: ' || systimestamp from dual;

prompt building org_swap_nwis 

prompt dropping nwis org indexes
exec etl_helper_org.drop_indexes('org');

set define off;
prompt populating org_data_swap_nwis
truncate table org_swap_nwis;
insert /*+ append parallel(4) */
  into org_data_swap_nwis (data_source_id, data_source, organization_id, organization, organization_name,
                             organization_description, organization_type, tribal_code, electronic_address, telephonic, address_type_1,
                             address_text_1, supplemental_address_text_1, locality_name_1, postal_code_1,
                             country_code_1, state_code_1, county_code_1, address_type_2, address_text_2,
                             supplemental_address_text_2, locality_name_2, postal_code_2, country_code_2,
                             state_code_2, county_code_2, address_type_3, address_text_3,
                             supplemental_address_text_3, locality_name_3, postal_code_3, country_code_3,
                             state_code_3, county_code_3)
select DISTINCT /* parallel(4) */
       2 data_source_id,
	   'NWIS' data_source,
	   cast('USGS-' || state_postal_cd as varchar2(7)) organization_id,
       'USGS ' || state_name || ' Water Science Center' organization_name,
	   null organization_description,
       null organization_type,
       null tribal_code,
       null electronic_address,
       null telephonic,
       null address_type_1,
       null address_text_1,
       null supplemental_address_text_1,
       null locality_name_1,
       null postal_code_1,
       null country_code_1,
       null state_code_1,
       null county_code_1,
       null address_type_2,
       null address_text_2,
       null supplemental_address_text_2,
       null locality_name_2,
       null postal_code_2,
       null country_code_2,
       null state_code_2,
       null county_code_2,
       null address_type_3,
       null address_text_3,
       null supplemental_address_text_3,
       null locality_name_3,
       null postal_code_3,
       null country_code_3,
       null state_code_3,
       null county_code_3
  from nwis_ws_star.nwis_district_cds_by_host
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			