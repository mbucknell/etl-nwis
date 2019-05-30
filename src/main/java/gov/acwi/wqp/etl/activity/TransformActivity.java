package gov.acwi.wqp.etl.activity;

import javax.sql.DataSource;

import java.util.HashMap;
import java.util.Map;

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
import org.springframework.batch.item.database.Order;
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
import gov.acwi.wqp.etl.NwisJdbcPagingItemReader;
import gov.acwi.wqp.etl.NwisPostgresPagingQueryProvider;
import gov.acwi.wqp.etl.nwis.activity.NwisActivity;
import gov.acwi.wqp.etl.nwis.activity.NwisActivityRowMapper;

@Configuration
public class TransformActivity {

	@Autowired
	@Qualifier("activityProcessor")
	private ItemProcessor<NwisActivity, Activity> processor;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSourceWqp;

	@Autowired
	@Qualifier(Application.DATASOURCE_NWIS_QUALIFIER)
	private DataSource dataSourceNwis;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_ACTIVITY_SWAP_TABLE_FLOW)
	private Flow setupActivitySwapTableFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.BUILD_ACTIVITY_INDEXES_FLOW)
	private Flow buildActivityIndexesFlow;

	@Value("classpath:sql/activity/selectNwisActivity.sql")
	private Resource selectClause;
	@Value("classpath:sql/activity/fromNwisActivity.sql")
	private Resource fromClause;
	@Value("classpath:sql/activity/whereNwisActivity.sql")
	private Resource whereClause;
	
	@Bean
	public NwisJdbcPagingItemReader<NwisActivity> activityReader() throws Exception{
		NwisPostgresPagingQueryProvider queryProvider = new NwisPostgresPagingQueryProvider();
		Map<String, Order> sortKeys = new HashMap<>();

		queryProvider.setSelectClause(new String(FileCopyUtils.copyToByteArray(selectClause.getInputStream())));
		queryProvider.setFromClause(new String(FileCopyUtils.copyToByteArray(fromClause.getInputStream())));
		queryProvider.setWhereClause(new String(FileCopyUtils.copyToByteArray(whereClause.getInputStream())));
		sortKeys.put("qw_sample.sample_id", Order.ASCENDING);
		queryProvider.setSortKeys(sortKeys);

		NwisJdbcPagingItemReader reader = new NwisJdbcPagingItemReader();
		reader.setDataSource(dataSourceNwis);
		reader.setName("activityReader");
		reader.setPageSize(5000);
		reader.setRowMapper(new NwisActivityRowMapper());
		reader.setQueryProvider(queryProvider);

		return reader;
	}
	
	@Bean
	public ItemWriter<Activity> activityWriter() {
		JdbcBatchItemWriter<Activity> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setDataSource(dataSourceWqp);
		itemWriter.setSql("insert"
				+ " into activity_swap_nwis ("
				+ "data_source_id, data_source, station_id, site_id, event_date, activity, sample_media, "
				+ "organization, site_type, huc, governmental_unit_code, geom, organization_name, activity_id, "
				+ "activity_type_code, activity_media_subdiv_name, activity_start_time, act_start_time_zone, " 
				+ "activity_stop_date, activity_stop_time, act_stop_time_zone, " 
				+ "activity_depth, activity_depth_unit, activity_depth_ref_point, activity_upper_depth, "
				+ "activity_upper_depth_unit, activity_lower_depth, activity_lower_depth_unit, project_id, " 
				+ "activity_conducting_org, activity_comment, sample_aqfr_name, hydrologic_condition_name, "
				+ "hydrologic_event_name, sample_collect_method_id, "
				+ "sample_collect_method_ctx, sample_collect_method_name, sample_collect_equip_name) "
				+ "values (:dataSourceId, :dataSource, :stationId, :siteId, :eventDate, :activity, :sampleMedia, " 
				+ ":organization, :siteType, :huc, :governmentalUnitCode, :geom, :organizationName, :activityId, "
				+ ":activityTypeCode, :activityMediaSubdivName, :activityStartTime, :actStartTimeZone, "
				+ ":activityStopDate, :activityStopTime, :actStopTimeZone, "
				+ ":activityDepth, :activityDepthUnit, :activityDepthRefPoint, :activityUpperDepth, "
				+ ":activityUpperDepthUnit, :activityLowerDepth, :activityLowerDepthUnit, :projectId, "
				+ ":activityConductingOrg, :activityComment, :sampleAqfrName, :hydrologicConditionName, "
				+ ":hydrologicEventName, :sampleCollectMethodId, "
				+ ":sampleCollectMethodCtx, :sampleCollectMethodName, :sampleCollectEquipName)");
		ItemSqlParameterSourceProvider<Activity> paramProvider = new BeanPropertyItemSqlParameterSourceProvider<>();
		itemWriter.setItemSqlParameterSourceProvider(paramProvider);
		return itemWriter;
	}
	
	@Bean
	public Step transformActivityStep() throws Exception{
		return stepBuilderFactory
				.get("transformActivityStep")
				.<NwisActivity, Activity>chunk(10000)
				.reader(activityReader())
				.processor(processor)
				.writer(activityWriter())
				.build();
	}

	@Bean
	public Flow activityFlow() throws Exception {
		return new FlowBuilder<SimpleFlow>("activityFlow")
				.start(setupActivitySwapTableFlow)
				.next(transformActivityStep())
				.next(buildActivityIndexesFlow)
				.build();
	}

}
