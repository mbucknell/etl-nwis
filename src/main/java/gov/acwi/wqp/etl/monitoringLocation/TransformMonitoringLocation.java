package gov.acwi.wqp.etl.monitoringLocation;

import javax.sql.DataSource;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.item.ItemProcessor;
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

import gov.acwi.wqp.etl.Application;
import gov.acwi.wqp.etl.EtlConstantUtils;
import gov.acwi.wqp.etl.nwis.monitoringLocation.NwisMonitoringLocation;
import gov.acwi.wqp.etl.nwis.monitoringLocation.NwisMonitoringLocationRowMapper;
 

@Configuration
public class TransformMonitoringLocation {

	@Autowired
	@Qualifier("monitoringLocationProcessor")
	private ItemProcessor<NwisMonitoringLocation, MonitoringLocation> processor;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSourceWqp;

	@Autowired
	@Qualifier(Application.DATASOURCE_NWIS_QUALIFIER)
	private DataSource dataSourceNwis;

	@Autowired
	@Qualifier("upsertNwisStationLocalFlow")
	private Flow upsertNwisStationLocalFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_MONITORING_LOCATION_SWAP_TABLE_FLOW)
	private Flow setupMonitoringLocationSwapTableFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.BUILD_MONITORING_LOCATION_INDEXES_FLOW)
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
		JdbcBatchItemWriter<MonitoringLocation> itemWriter = new JdbcBatchItemWriter<>();
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
				.processor(processor)
				.writer(monitoringLocationWriter())
				.build();
	}

	@Bean
	public Flow monitoringLocationFlow() throws Exception {
		return new FlowBuilder<SimpleFlow>("monitoringLocationFlow")
				.start(upsertNwisStationLocalFlow)
				.next(setupMonitoringLocationSwapTableFlow)
				.next(transformMonitoringLocationStep())
				.next(buildMonitoringLocationIndexesFlow)
				.build();
	}
}
