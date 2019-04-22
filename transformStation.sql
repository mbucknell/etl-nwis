show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'transform station start time: ' || systimestamp from dual;

merge into nwis_station_local o 
      using (select /*+ parallel(4) */ 
                    sitefile.site_id station_id,
                    sitefile.agency_cd || '-' || sitefile.site_no site_id,
                    round(sitefile.dec_lat_va , 7) latitude,
                    round(sitefile.dec_long_va, 7) longitude,
                    sitefile.huc_cd huc,
                    case when sitefile.dec_long_va is not null and sitefile.dec_lat_va is not null
                      then mdsys.sdo_geometry(2001,
                                              4269,
                                              mdsys.sdo_point_type(round(sitefile.dec_long_va, 7),
                                                                   round(sitefile.dec_lat_va, 7),
                                                                   null),
                                              null, null)
                    else
                      null
                    end geom
               from nwis_ws_star.sitefile
            ) n
  on (o.station_id = n.station_id)
when matched then update
                     set o.site_id = n.site_id,
                         o.latitude = n.latitude,
                         o.longitude = n.longitude,
                         o.huc = n.huc,
                         o.calculated_huc_12 = null,
                         o.geom = n.geom
                   where lnnvl(o.latitude = n.latitude) or
                         lnnvl(o.longitude = n.longitude) or
                         lnnvl(o.huc = n.huc)
when not matched then insert (station_id, site_id, latitude, longitude, huc, geom)
                      values (n.station_id, n.site_id, n.latitude, n.longitude, n.huc, n.geom);
commit;
select 'Merge into nwis_station_local complete: ' || systimestamp from dual;

prompt calculating huc
merge into nwis_station_local o 
      using (select /*+ parallel(4) */ 
                    station_id,
                    huc12
               from huc12nometa,
                    nwis_station_local
              where nwis_station_local.geom is not null and
                    calculated_huc_12 is null and
                    sdo_contains(huc12nometa.geometry, nwis_station_local.geom) = 'TRUE') n
   on (o.station_id = n.station_id)
when matched then update set calculated_huc_12 = huc12;
commit;
select 'Calculating HUC complete: ' || systimestamp from dual;

prompt dropping nwis station indexes
exec etl_helper_station.drop_indexes('nwis');

prompt populating nwis station
truncate table station_swap_nwis;

insert /*+ append parallel(4) */
  into station_swap_nwis (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,
                          geom, station_name, organization_name, station_type_name, latitude, longitude, map_scale,
                          geopositioning_method, hdatum_id_code, elevation_value, elevation_unit, elevation_method, vdatum_id_code,
                          drain_area_value, drain_area_unit, contrib_drain_area_value, contrib_drain_area_unit,
                          geoposition_accy_value, geoposition_accy_unit, vertical_accuracy_value, vertical_accuracy_unit,
                          nat_aqfr_name, aqfr_name, aqfr_type_name, construction_date, well_depth_value, well_depth_unit,
                          hole_depth_value, hole_depth_unit)
select /*+ parallel(4) */ 
       2 data_source_id,
       'NWIS' data_source,
       sitefile.site_id station_id,
       sitefile.agency_cd || '-' || sitefile.site_no site_id,
       ndcbh.organization_id organization,
       site_tp.primary_site_type site_type,
       nvl(nwis_station_local.calculated_huc_12, sitefile.huc_cd) huc,
       sitefile.country_cd || ':' || sitefile.state_cd || ':' || sitefile.county_cd governmental_unit_code,
       nwis_station_local.geom,
       trim(sitefile.station_nm) station_name,
       ndcbh.organization_name,
       site_tp.station_type_name,
       round(sitefile.dec_lat_va , 7) latitude,
       round(sitefile.dec_long_va, 7) longitude,
       trim(sitefile.map_scale_fc) map_scale,
       nvl(lat_long_method.description, 'Unknown') geopositioning_method,
       nvl(sitefile.dec_coord_datum_cd, 'Unknown') hdatum_id_code,
       case when sitefile.alt_datum_cd is not null then case when sitefile.alt_va = '.' then '0' else trim(sitefile.alt_va) end else null end elevation_value,
       case when sitefile.alt_va is not null and sitefile.alt_datum_cd is not null then 'feet' else null end elevation_unit,
       altitude_method.description elevation_method,
       case when sitefile.alt_va is not null then sitefile.alt_datum_cd else null end vdatum_id_code,
       to_number(sitefile.drain_area_va) drain_area_value,
       nvl2(sitefile.drain_area_va, 'sq mi', null) drain_area_unit,
       case when sitefile.contrib_drain_area_va = '.' then 0 else to_number(sitefile.contrib_drain_area_va) end contrib_drain_area_value,
       nvl2(sitefile.contrib_drain_area_va, 'sq mi', null) contrib_drain_area_unit,
       lat_long_accuracy.accuracy geoposition_accy_value,
       lat_long_accuracy.unit geoposition_accy_unit,
       case when sitefile.alt_va is not null and sitefile.alt_datum_cd is not null then trim(sitefile.alt_acy_va) else null end vertical_accuracy_value,
       case when sitefile.alt_va is not null and sitefile.alt_datum_cd is not null and sitefile.alt_acy_va is not null then 'feet' else null end vertical_accuracy_unit,
       nat_aqfr.nat_aqfr_name,
       aqfr.aqfr_nm,
       aquifer_type.description,
       sitefile.construction_dt construction_date,
       to_number(case when sitefile.well_depth_va in ('.', '-') then '0' else sitefile.well_depth_va end) well_depth_value,
       nvl2(sitefile.well_depth_va, 'ft', null) well_depth_unit,
       to_number(case when sitefile.hole_depth_va in ('.', '-') then '0' else sitefile.hole_depth_va end) hole_depth_value,
       nvl2(sitefile.hole_depth_va, 'ft', null) hole_depth_unit
  from nwis_ws_star.sitefile
       join nwis_station_local
         on sitefile.site_id = nwis_station_local.station_id
       join (select cast('USGS-' || state_postal_cd as varchar2(7)) organization_id,
                    'USGS ' || state_name || ' Water Science Center' organization_name,
                    host_name,
                    district_cd
               from nwis_ws_star.nwis_district_cds_by_host) ndcbh
         on sitefile.nwis_host = ndcbh.host_name and    /* host name must exist - no outer join */
            sitefile.district_cd  = ndcbh.district_cd
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
       left join nwis_ws_star.altitude_method
         on sitefile.alt_meth_cd = altitude_method.code
       left join nwis_ws_star.lat_long_method
         on sitefile.coord_meth_cd= lat_long_method.code
       left join nwis_ws_star.lat_long_accuracy
         on sitefile.coord_acy_cd = lat_long_accuracy.code
       left join (select nat_aqfr_nm as nat_aqfr_name, nat_aqfr_cd from nwis_ws_star.nat_aqfr group by nat_aqfr_nm, nat_aqfr_cd) nat_aqfr
         on sitefile.nat_aqfr_cd  = nat_aqfr.nat_aqfr_cd
       left join nwis_ws_star.aquifer_type
         on sitefile.aqfr_type_cd = aquifer_type.code
       left join nwis_ws_star.aqfr
         on sitefile.aqfr_cd = aqfr.aqfr_cd and
            sitefile.state_cd = aqfr.state_cd
 where sitefile.DEC_LAT_VA   <> 0   and
       sitefile.DEC_LONG_VA  <> 0   and
       sitefile.site_web_cd  = 'Y'  and
       sitefile.db_no        = '01' and
       sitefile.site_tp_cd not in ('FA-WTP', 'FA-WWTP', 'FA-TEP', 'FA-HP') and
       sitefile.nwis_host  not in ('fltlhsr001', 'fltpasr001', 'flalssr003') and
       sitefile.country_cd != 'CN'
    order by ndcbh.organization_id;

commit;

prompt building nwis station indexes
exec etl_helper_station.create_indexes('nwis');

select 'transform station end time: ' || systimestamp from dual;