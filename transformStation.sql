show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform station start time: ' || systimestamp from dual;

prompt dropping nwis station indexes
exec etl_helper.drop_indexes('station_swap_nwis');
        
prompt populating nwis station
truncate table station_swap_nwis;

insert /*+ append parallel(4) */
  into station_swap_nwis (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,
                          geom, station_name, organization_name, description_text, station_type_name, latitude, longitude, map_scale,
                          geopositioning_method, hdatum_id_code, elevation_value, elevation_unit, elevation_method, vdatum_id_code,
                          drain_area_value, drain_area_unit, contrib_drain_area_value, contrib_drain_area_unit,
                          geoposition_accy_value, geoposition_accy_unit, vertical_accuracy_value, vertical_accuracy_unit,
                          nat_aqfr_name, aqfr_name, aqfr_type_name, construction_date, well_depth_value, well_depth_unit,
                          hole_depth_value, hole_depth_unit)
select 2 data_source_id,
       'NWIS' data_source,
       sitefile.site_id station_id,
       sitefile.agency_cd || '-' || sitefile.site_no site_id,
       ndcbh.organization_id organization,
       site_tp.primary_site_type site_type,
       sitefile.huc_cd huc,
       country.country_cd || ':' || state.state_cd || ':' || county.county_cd governmental_unit_code,
       mdsys.sdo_geometry(2001,4269,mdsys.sdo_point_type(round(sitefile.dec_long_va, 7),round(sitefile.dec_lat_va, 7), null), null, null) geom,
       trim(sitefile.station_nm) station_name,
       ndcbh.organization_name,
       trim(sitefile.site_rmks_tx) description_text,
       site_tp.station_type_name,
       round(sitefile.dec_lat_va , 7) latitude,
       round(sitefile.dec_long_va, 7) longitude,
       trim(sitefile.map_scale_fc) map_scale,
       nvl(geo_meth.geopositioning_method, 'Unknown') geopositioning_method,
       nvl(sitefile.dec_coord_datum_cd, 'Unknown') hdatum_id_code,
       case when sitefile.alt_datum_cd is not null then case when sitefile.alt_va = '.' then '0' else trim(sitefile.alt_va) end else null end elevation_value,
       case when sitefile.alt_va is not null and sitefile.alt_datum_cd is not null then 'feet' else null end elevation_unit,
       vert.vertical_method_name elevation_method,
       case when sitefile.alt_va is not null then sitefile.alt_datum_cd else null end vdatum_id_code,
       to_number(sitefile.drain_area_va) drain_area_value,
       nvl2(sitefile.drain_area_va, 'sq mi', null) drain_area_unit,
       case when sitefile.contrib_drain_area_va = '.' then 0 else to_number(sitefile.contrib_drain_area_va) end contrib_drain_area_value,
       nvl2(sitefile.contrib_drain_area_va, 'sq mi', null) contrib_drain_area_unit,
       geo_accuracy.geopositioning_accuracy_value geoposition_accy_value,
       geo_accuracy.geopositioning_accuracy_units geoposition_accy_unit,
       case when sitefile.alt_va is not null and sitefile.alt_datum_cd is not null then trim(sitefile.alt_acy_va) else null end vertical_accuracy_value,
       case when sitefile.alt_va is not null and sitefile.alt_datum_cd is not null and sitefile.alt_acy_va is not null then 'feet' else null end vertical_accuracy_unit,
       nat_aqfr.nat_aqfr_name,
       aqfr.aqfr_name,
       aqfr_type.aqfr_type_name,
       sitefile.construction_dt construction_date,
       to_number(case when sitefile.well_depth_va in ('.', '-') then '0' else sitefile.well_depth_va end) well_depth_value,
       nvl2(sitefile.well_depth_va, 'ft', null) well_depth_unit,
       to_number(case when sitefile.hole_depth_va in ('.', '-') then '0' else sitefile.hole_depth_va end) hole_depth_value,
       nvl2(sitefile.hole_depth_va, 'ft', null) hole_depth_unit
  from nwis_ws_star.sitefile
       join (select cast('USGS-' || state_postal_cd as varchar2(7)) organization_id,
                    'USGS ' || STATE_NAME || ' Water Science Center' organization_name,
                    host_name,
                    district_cd
               from nwis_ws_star.nwis_district_cds_by_host) ndcbh
         on sitefile.nwis_host = ndcbh.host_name and    /* host name must exist - no outer join */
            sitefile.district_cd  = ndcbh.district_cd
       left join (select cast(country_cd as varchar2(2)) as country_cd, country_nm as country_name from nwis_ws_star.country) country
         on sitefile.country_cd = country.country_cd
       left join (select cast(state_cd as varchar2(2)) as state_cd, state_nm as state_name, country_cd from nwis_ws_star.state) state
         on sitefile.country_cd = state.country_cd and
            sitefile.state_cd = state.state_cd
       left join (select cast(county_cd as varchar2(3)) as county_cd, state_cd, country_cd, county_nm as county_name from nwis_ws_star.county) county
         on sitefile.country_cd = county.country_cd and
            sitefile.state_cd = county.state_cd and
            sitefile.county_cd = county.county_cd
       left join (select cast(state_post_cd as varchar2(2)) as state_postal_cd, state_cd, country_cd from nwis_ws_star.state) postal
         on sitefile.country_cd = postal.country_cd and
            sitefile.state_cd = postal.state_cd
       left join (select a.site_tp_cd,
                         case when a.site_tp_prim_fg = 'Y' then a.site_tp_ln
                           else b.site_tp_ln || ': ' || a.site_tp_ln
                         end as station_type_name,
                         case when a.site_tp_prim_fg = 'Y' then a.site_tp_ln
                           else b.site_tp_ln
                         end as primary_site_type
                    from nwis_ws_star.site_tp a,
                         nwis_ws_star.site_tp b
                   where substr(a.site_tp_cd, 1, 2) = b.site_tp_cd and
                         b.site_tp_prim_fg = 'Y') site_tp
         on sitefile.site_tp_cd = site_tp.site_tp_cd
       left join (select nwis_name as vertical_method_name , nwis_code from nwis_ws_star.nwis_misc_lookups where category = 'Altitude Method') vert
         on sitefile.alt_meth_cd = vert.nwis_code
       left join (select nwis_name as geopositioning_method, nwis_code from nwis_ws_star.nwis_misc_lookups where category = 'Lat/Long Method') geo_meth
         on sitefile.coord_meth_cd= geo_meth.nwis_code
       left join (select inferred_value as geopositioning_accuracy_value,
                         inferred_units as geopositioning_accuracy_units,
                         nwis_code
                    from nwis_ws_star.nwis_misc_lookups
                   where category = 'Lat-Long Coordinate Accuracy') geo_accuracy
         on sitefile.coord_acy_cd = geo_accuracy.nwis_code
       left join (select nat_aqfr_nm as nat_aqfr_name, nat_aqfr_cd from nwis_ws_star.nat_aqfr group by nat_aqfr_nm, nat_aqfr_cd) nat_aqfr
         on sitefile.nat_aqfr_cd  = nat_aqfr.nat_aqfr_cd
       left join (select nwis_name as aqfr_type_name, nwis_code from nwis_ws_star.nwis_misc_lookups where CATEGORY='Aquifer Type Code') aqfr_type
         on sitefile.aqfr_type_cd = aqfr_type.nwis_code
       left join (select aqfr_nm as aqfr_name, trim(state_cd) state_cd, aqfr_cd from nwis_ws_star.aqfr) aqfr
         on sitefile.aqfr_cd = aqfr.aqfr_cd and
            sitefile.state_cd     = aqfr.state_cd
 where sitefile.DEC_LAT_VA   <> 0   and
       sitefile.DEC_LONG_VA  <> 0   and
       sitefile.site_web_cd  = 'Y'  and
       sitefile.db_no        = '01' and
       sitefile.site_tp_cd not in ('FA-WTP', 'FA-WWTP', 'FA-TEP', 'FA-HP')   and
       sitefile.nwis_host  not in ('fltlhsr001', 'fltpasr001', 'flalssr003')
    order by ndcbh.organization_id;

commit;

select 'transform station end time: ' || systimestamp from dual;
