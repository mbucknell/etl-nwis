update nwis_station_local
set calculated_huc_12 = huc12nometa.huc12
from wqp.huc12nometa
where
      nwis_station_local.geom is not null and
      nwis_station_local.calculated_huc_12 is null and
      ST_Covers(huc12nometa.geometry, st_transform(nwis_station_local.geom, 4326))
