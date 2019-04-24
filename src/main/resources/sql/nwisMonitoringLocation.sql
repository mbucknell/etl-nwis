select sitefile.site_id site_id,
	sitefile.agency_cd,
	sitefile.site_no,
	ndcbh.organization_id organization_id,
	site_tp.primary_site_type primary_site_type,
	nwis_station_local.calculated_huc_12 calculated_huc_12,
	sitefile.huc_cd huc_cd,
	sitefile.country_cd country_cd,
	sitefile.state_cd state_cd,
	sitefile.county_cd county_cd,
	nwis_station_local.geom geom,
	sitefile.station_nm station_nm,
	ndcbh.organization_name organization_name,
	site_tp.station_type_name station_type_name,
	sitefile.dec_lat_va dec_lat_va,
	sitefile.dec_long_va,
	sitefile.map_scale_fc map_scale_fc,
	lat_long_method.description lat_long_method_description,
	sitefile.dec_coord_datum_cd dec_coord_datum_cd,
	sitefile.alt_datum_cd alt_datum_cd,
	sitefile.alt_va alt_va,
	sitefile.alt_acy_va alt_acy_va,
	altitude_method.description altitude_method_description,
	sitefile.drain_area_va drain_area_va,
	sitefile.contrib_drain_area_va contrib_drain_area_va,
	lat_long_accuracy.accuracy lat_long_accuracy,
	lat_long_accuracy.unit lat_long_accuracy_unit,
	nat_aqfr.nat_aqfr_name nat_aqfr_name,
	aqfr.aqfr_nm aqfr_nm,
	aquifer_type.description aquifer_type_description,
 	sitefile.construction_dt construction_dt,
 	sitefile.well_depth_va well_depth_va,
 	sitefile.hole_depth_va hole_depth_va
from sitefile
	join nwis_station_local on sitefile.site_id = nwis_station_local.station_id
	join (select'USGS-' || state_postal_cd organization_id,
                'USGS ' || state_name || ' Water Science Center' organization_name,
                host_name,
                district_cd
          from nwis_district_cds_by_host) ndcbh
      on sitefile.nwis_host = ndcbh.host_name and    /* host name must exist - no outer join */
         sitefile.district_cd  = ndcbh.district_cd
    left join (select a.site_tp_cd,
                      case when a.site_tp_prim_fg then a.site_tp_ln
                        else b.site_tp_ln || ': ' || a.site_tp_ln
                      end as station_type_name,
                      case when a.site_tp_prim_fg then a.site_tp_ln
                        else b.site_tp_ln
                      end as primary_site_type
               from site_tp a,
                    site_tp b
               where substr(a.site_tp_cd, 1, 2) = b.site_tp_cd and
                     b.site_tp_prim_fg) site_tp
	  on sitefile.site_tp_cd = site_tp.site_tp_cd
left join altitude_method
         on sitefile.alt_meth_cd = altitude_method.code
       left join lat_long_method
         on sitefile.coord_meth_cd= lat_long_method.code
       left join lat_long_accuracy
         on sitefile.coord_acy_cd = lat_long_accuracy.code
       left join (select nat_aqfr_nm as nat_aqfr_name, nat_aqfr_cd from nat_aqfr group by nat_aqfr_nm, nat_aqfr_cd) nat_aqfr
         on sitefile.nat_aqfr_cd  = nat_aqfr.nat_aqfr_cd
       left join aquifer_type
         on sitefile.aqfr_type_cd = aquifer_type.code
       left join aqfr
         on sitefile.aqfr_cd = aqfr.aqfr_cd and
            sitefile.state_cd = aqfr.state_cd
 where sitefile.dec_lat_va   <> 0   and
       sitefile.dec_long_va  <> 0   and
       sitefile.site_web_cd  = 'Y'  and
       sitefile.db_no        = '01' and
       sitefile.site_tp_cd not in ('FA-WTP', 'FA-WWTP', 'FA-TEP', 'FA-HP') and
       sitefile.nwis_host  not in ('fltlhsr001', 'fltpasr001', 'flalssr003') and
       sitefile.country_cd != 'CN'
    order by ndcbh.organization_id;
	
	