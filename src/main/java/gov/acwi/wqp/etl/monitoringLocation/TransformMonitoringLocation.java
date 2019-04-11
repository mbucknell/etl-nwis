package gov.acwi.wqp.etl.monitoringLocation;

import javax.sql.DataSource;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.ItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.util.FileCopyUtils;

import gov.acwi.wqp.etl.nwis.NwisMonitoringLocation;
import gov.acwi.wqp.etl.nwis.NwisMonitoringLocationRowMapper;
 

@Configuration
public class TransformMonitoringLocation {
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dataSourceWqp")
	private DataSource dataSourceWqp;

	@Autowired
	@Qualifier("dataSourceNwis")
	private DataSource dataSourceNwis;

	@Autowired
	@Qualifier("setupMonitoringLocationSwapTableFlow")
	private Flow setupMonitoringLocationSwapTableFlow;

	@Autowired
	@Qualifier("buildMonitoringLocationIndexesFlow")
	private Flow buildMonitoringLocationIndexesFlow;
	
	@Value("classpath:sql/nwisMonitoringLocation.sql")
	private Resource sqlResource;

	@Bean
	public JdbcCursorItemReader<NwisMonitoringLocation> monitoringLocationReader() throws Exception {
		return new JdbcCursorItemReaderBuilder<NwisMonitoringLocation>()
		.dataSource(dataSourceNwis)
		.name("monitoringLocationReader")
		.sql(new String(FileCopyUtils.copyToByteArray(sqlResource.getInputStream())))
		.rowMapper(new NwisMonitoringLocationRowMapper())
		.build();
	}
	
	@Bean
	public ItemWriter<MonitoringLocation> monitoringLocationWriter() {
		JdbcBatchItemWriter<MonitoringLocation> itemWriter = new JdbcBatchItemWriter<MonitoringLocation>();
		itemWriter.setDataSource(dataSourceWqp);
		itemWriter.setSql("insert"
				+ " into station_swap_nwis (data_source_id, data_source, station_id, site_id, organization, site_type, huc, governmental_unit_code,"
				+ " geom, station_name, organization_name, station_type_name, latitude, longitude, map_scale,"
				+ " geopositioning_method, hdatum_id_code, elevation_value, elevation_unit, elevation_method, vdatum_id_code,"
				+ " drain_area_value, drain_area_unit, contrib_drain_area_value, contrib_drain_area_unit,"
				+ " geoposition_accy_value, geoposition_accy_unit, vertical_accuracy_value, vertical_accuracy_unit,"
				+ " nat_aqfr_name, aqfr_name, aqfr_type_name, construction_date, well_depth_value, well_depth_unit,"
				+ " hole_depth_value, hole_depth_unit)"
			+ " values (:dataSourceId, :dataSource, :stationId, :siteId, :organization, :siteType, :huc, :governmentalUnitCode,"
				+ " :geom, :stationName, :organizationName, :stationTypeName, :latitude, :longitude, :mapScale,"
				+ " :geopositioningMethod, :hdatumIdCode, :elevationValue, :elevationUnit, :elevationMethod, :vdatumIdCode,"
				+ " :drainAreaValue, :drainAreaUnit, :contribDrainAreaValue, :contribDrainAreaUnit,"
				+ " :geopositionAccyValue, :geopositionAccyUnit, :verticalAccuracyValue, :verticalAccuracyUnit,"
				+ " :natAqfrName, :aqfrName, :aqfrTypeName, :constructionDate, :wellDepthValue, :wellDepthUnit,"
				+ " :holeDepthValue, :holeDepthUnit)");
		
		ItemSqlParameterSourceProvider<MonitoringLocation> paramProvider = new BeanPropertyItemSqlParameterSourceProvider<>();
		itemWriter.setItemSqlParameterSourceProvider(paramProvider);
		return itemWriter;
	}
	
	@Bean
	public Step transformMonitoringLocationStep() throws Exception{
		return stepBuilderFactory
				.get("transformMonitoringLocationStep")
				.<NwisMonitoringLocation, MonitoringLocation>chunk(10)
				.reader(monitoringLocationReader())
				.processor(new MonitoringLocationProcessor())
				.writer(monitoringLocationWriter())
				.build();
	}

	@Bean
	public Flow monitoringLocationFlow() throws Exception {
		return new FlowBuilder<SimpleFlow>("monitoringLocationFlow")
				.start(setupMonitoringLocationSwapTableFlow)
				.next(transformMonitoringLocationStep())
				.next(buildMonitoringLocationIndexesFlow)
				.build();
	}
}
