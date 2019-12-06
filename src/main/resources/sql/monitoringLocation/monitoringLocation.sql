insert
  into station_swap_nwis (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,
                          geom, station_name, organization_name, station_type_name, latitude, longitude, map_scale,
                          geopositioning_method, hdatum_id_code, elevation_value, elevation_unit, elevation_method, vdatum_id_code,
                          drain_area_value, drain_area_unit, contrib_drain_area_value, contrib_drain_area_unit,
                          geoposition_accy_value, geoposition_accy_unit, vertical_accuracy_value, vertical_accuracy_unit,
                          nat_aqfr_name, aqfr_name, aqfr_type_name, construction_date, well_depth_value, well_depth_unit,
                          hole_depth_value, hole_depth_unit)
select 2 data_source_id,
       'NWIS' data_source,
       monitoring_location.site_id station_id,
       monitoring_location.monitoring_location_identifier site_id,
       ndcbh.organization_id organization,
       site_tp.primary_site_type site_type,
       coalesce(monitoring_location.calculated_huc_12, monitoring_location.huc_cd) huc,
       coalesce(monitoring_location.country_cd, '') || ':' ||
           coalesce(monitoring_location.state_cd, '') || ':' ||
           coalesce(monitoring_location.county_cd, '') governmental_unit_code,
       monitoring_location.geom,
       monitoring_location.site_name station_name,
       ndcbh.organization_name,
       site_tp.station_type_name,
       monitoring_location.decimal_latitude latitude,
       monitoring_location.decimal_longitude longitude,
       monitoring_location.scale_of_location_map map_scale,
       monitoring_location.latitude_longitude_method geopositioning_method,
       monitoring_location.dec_coord_datum_cd hdatum_id_code,
       monitoring_location.altitude_of_guage_land_surface elevation_value,
       case
           when monitoring_location.altitude_of_guage_land_surface is not null and 
                monitoring_location.alt_datum_cd is not null then 'feet'
           else null
       end elevation_unit,
       monitoring_location.method_altitude_determined elevation_method,
       monitoring_location.alt_datum_cd vdatum_id_code,
       cast(monitoring_location.drainage_area as numeric) drain_area_value,
       case
           when monitoring_location.drainage_area is not null then 'sq mi'
           else null
       end drain_area_unit,
       cast(monitoring_location.contributing_drainage_area as numeric) contrib_drain_area_value,
       case
           when monitoring_location.contributing_drainage_area is not null then 'sq mi'
           else null
       end contrib_drain_area_unit,
       monitoring_location.latitude_longitude_accuracy_value geoposition_accy_value,
       monitoring_location.latitude_longitude_accuracy_unit geoposition_accy_unit,
       monitoring_location.altitude_accuracy vertical_accuracy_value,
       case
           when monitoring_location.altitude_of_guage_land_surface is not null and
                monitoring_location.alt_datum_cd is not null and
                monitoring_location.altitude_accuracy is not null then 'feet'
           else null
       end vertical_accuracy_unit,
       monitoring_location.national_aquifer nat_aqfr_name,
       monitoring_location.local_aquifer aqfr_nm,
       monitoring_location.local_aquifer_type aqfr_type_name,
       monitoring_location.date_of_first_construction construction_date,
       cast(monitoring_location.well_depth as numeric) well_depth_value,
       case
           when monitoring_location.well_depth is not null then 'ft'
           else null
       end well_depth_unit,
       cast(monitoring_location.hole_depth as numeric) hole_depth_value,
       case
           when monitoring_location.hole_depth is not null then 'ft'
           else null
       end hole_depth_unit
  from nwis.monitoring_location
       join (select 'USGS-' || coalesce(state_postal_cd, '') organization_id,
                    'USGS ' || coalesce(state_name, '') || ' Water Science Center' organization_name,
                    host_name,
                    district_cd
               from nwis.nwis_district_cds_by_host) ndcbh
                 on monitoring_location.nwis_host = ndcbh.host_name and
                    monitoring_location.district_cd  = ndcbh.district_cd
       left join (select a.site_tp_cd,
                         case
                             when a.site_tp_prim_fg then a.site_tp_ln
                             else b.site_tp_ln || ': ' || a.site_tp_ln
                         end as station_type_name,
                         case
                             when a.site_tp_prim_fg then a.site_tp_ln
                             else b.site_tp_ln
                         end as primary_site_type
                    from nwis.site_tp a,
                         nwis.site_tp b
                   where substr(a.site_tp_cd, 1, 2) = b.site_tp_cd and
                         b.site_tp_prim_fg) site_tp
         on monitoring_location.site_tp_cd = site_tp.site_tp_cd
    order by ndcbh.organization_id
