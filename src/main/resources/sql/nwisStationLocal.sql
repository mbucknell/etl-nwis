insert into nwis_station_local (station_id, site_id, latitude, longitude, huc, geom)
    select
        sitefile.site_id station_id,
        sitefile.agency_cd || '-' || sitefile.site_no site_id,
		sitefile.dec_lat_va latitude,
		sitefile.dec_long_va longitude,
		sitefile.huc_cd huc,
		case
		    when sitefile.dec_long_va is not null and sitefile.dec_lat_va is not null
		        then st_SetSrid(st_MakePoint(dec_long_va, dec_lat_va), 4269)
		    else null
		end geom
	from sitefile
on conflict (station_id) do
    update set
        latitude = excluded.latitude,
        longitude = excluded.longitude,
        huc = excluded.huc,
        calculated_huc_12 = null,
        geom = excluded.geom
    where excluded.latitude is distinct from nwis_station_local.latitude or
        excluded.longitude is distinct from nwis_station_local.longitude or
        excluded.huc is distinct from nwis_station_local.huc