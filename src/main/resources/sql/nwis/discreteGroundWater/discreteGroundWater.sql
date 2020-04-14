insert
  into nwis.discrete_ground_water(monitoring_location_id, agency_code, agency, site_identification_number, monitoring_location_identifier,
                                  site_type_code, site_type, geom, decimal_latitude, decimal_longitude, decimal_latitude_longitude_datum,
                                  well_depth, hole_depth, local_aquifer, local_aquifer_type, date_measured_raw,
                                  timezone_code, parameter_code, date_measured,
                                  time_measured_utc, level_feet_below_land_surface, level_feet_above_vertical_datum,
                                  vertical_datum_code, vertical_datum, site_status_code, site_status,
                                  measuring_agency_code, measuring_agency, date_time_accuracy_code, date_time_accuracy,
                                  level_accuracy_code, level_accuracy, measurement_source_code, measurement_source,
                                  measurement_method_code, measurement_method, approval_status_code, approval_status)
select monitoring_location.monitoring_location_id,
       monitoring_location.agency_cd agency_code,
       monitoring_location.agency,
       monitoring_location.site_identification_number,
       monitoring_location.monitoring_location_identifier,
       monitoring_location.site_tp_cd site_type_code,
       monitoring_location.site_type,
       monitoring_location.geom,
       monitoring_location.decimal_latitude,
       monitoring_location.decimal_longitude,
       monitoring_location.decimal_latitude_longitude_datum,
       monitoring_location.well_depth,
       monitoring_location.hole_depth,
       monitoring_location.local_aquifer,
       monitoring_location.local_aquifer_type,
       gw_levels.lev_dtm date_measured_raw,
       gw_levels.lev_tz_cd timezone_code,
       gw_levels.parameter_cd parameter_code,
       case lev_dt_acy_cd
         when 'Y' then to_char(lev_dtm at time zone lev_tz_cd, 'YYYY')
         when 'M' then to_char(lev_dtm at time zone lev_tz_cd, 'YYYY-MM')
         else to_char(lev_dtm at time zone lev_tz_cd, 'YYYY-MM-DD')
       end date_measured,
       case lev_dt_acy_cd
         when 'h' then to_char(lev_dtm at time zone lev_tz_cd, 'HH24')
         when 'm' then to_char(lev_dtm at time zone lev_tz_cd, 'HH24:MI')
       end time_measured_utc,
       case
         when gw_levels.parameter_cd = '72019'
           then gw_levels.lev_va
       end level_feet_below_land_surface,
       case
         when gw_levels.parameter_cd != '72019'
           then gw_levels.lev_va
       end level_feet_above_vertical_datum,
       case
         when gw_levels.parameter_cd != '72019'
           then gw_levels.lev_datum_cd
       end vertical_datum_code,
       case
         when gw_levels.parameter_cd != '72019'
           then altitude_datum.description
       end vertical_datum,
       gw_levels.lev_status_cd site_status_code,
       coalesce(gw_level_site_status.description, 'The reported water-level measurement represents a static level') site_status,
       gw_levels.lev_agency_cd measuring_agency_code,
       coalesce(agency.name, 'Not Determined') measuring_agency,
       gw_levels.lev_dt_acy_cd date_time_accuracy_code,
       gw_level_date_time_accuracy.description date_time_accuracy,
       gw_levels.lev_acy_cd level_accuracy_code,
       gw_level_accuracy.description level_accuracy,
       gw_levels.lev_src_cd measurement_source_code,
       gw_level_source.description measurement_source,
       gw_levels.lev_meth_cd measurement_method_code,
       gw_level_method.description measurement_method,
       gw_levels.lev_age_cd approval_status_code,
       gw_level_approval_status.description approval_status
  from nwis.gw_levels
       join nwis.monitoring_location
         on gw_levels.site_id = monitoring_location.site_id
       left join nwis.altitude_datum
         on gw_levels.lev_datum_cd = altitude_datum.code
       left join nwis.gw_level_site_status
         on gw_levels.lev_status_cd = gw_level_site_status.code
       left join nwis.agency
         on gw_levels.lev_agency_cd = agency.code
       left join nwis.gw_level_accuracy
         on gw_levels.lev_acy_cd = gw_level_accuracy.code
       left join nwis.gw_level_date_time_accuracy
         on gw_levels.lev_dt_acy_cd = gw_level_date_time_accuracy.code
       left join nwis.gw_level_source
         on gw_levels.lev_src_cd = gw_level_source.code
       left join nwis.gw_level_method
         on gw_levels.lev_meth_cd = gw_level_method.code
       left join nwis.gw_level_approval_status
         on gw_levels.lev_age_cd = gw_level_approval_status.code;
