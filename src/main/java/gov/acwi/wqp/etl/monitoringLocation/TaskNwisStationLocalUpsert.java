package gov.acwi.wqp.etl.monitoringLocation;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component 
public class TaskNwisStationLocalUpsert implements Tasklet {
	
	@Autowired
	@Qualifier("dataSourceNwis")
	private DataSource dataSourceNwis;
	
	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSourceNwis);
		
		String sql = "insert into nwis_station_local (station_id, site_id, latitude, longitude, huc, geom) " 
				+ "select sitefile.site_id station_id, sitefile.agency_cd || '-' || sitefile.site_no site_id, "
				+ "sitefile.dec_lat_va latitude, sitefile.dec_long_va longitude, "
				+ "sitefile.huc_cd huc, "
				+ "case when sitefile.dec_long_va is not null and sitefile.dec_lat_va is not null then "
				+ "st_SetSrid(st_MakePoint(dec_long_va, dec_lat_va), 4269) else null end geom "
				+ "from sitefile on conflict (station_id) do "
				+ "update set latitude = excluded.latitude, "
				+ "longitude = excluded.longitude, "
				+ "huc = excluded.huc,"
				+ "calculated_huc_12 = null, "
				+ "geom = excluded.geom";
		jdbcTemplate.update(sql);
		
		return RepeatStatus.FINISHED;
	}

}
