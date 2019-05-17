package gov.acwi.wqp.etl.result;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
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
import gov.acwi.wqp.etl.nwis.result.NwisResult;
import gov.acwi.wqp.etl.nwis.result.NwisResultRowMapper;

@Configuration
public class TransformResult {

	@Autowired
	@Qualifier("resultProcessor")
	private ItemProcessor<NwisResult, Result> processor;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dataSourceWqp")
	private DataSource dataSourceWqp;

	@Autowired
	@Qualifier(Application.DATASOURCE_NWIS_QUALIFIER)
	private DataSource dataSourceNwis;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_RESULT_SWAP_TABLE_FLOW)
	private Flow setupResultSwapTableFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.BUILD_RESULT_INDEXES_FLOW)
	private Flow buildResultIndexesFlow;

	@Value("classpath:sql/result/selectNwisResult.sql")
	private Resource selectClause;
	@Value("classpath:sql/result/fromNwisResult.sql")
	private Resource fromClause;
	@Value("classpath:sql/result/whereNwisResult.sql")
	private Resource whereClause;
	
	@Value("classpath:sql/nwisResult.sql")
	private Resource sqlResource;
	
	//@Bean
	//public JdbcCursorItemReader<NwisResult> resultReader() throws Exception {
	//	return new JdbcCursorItemReaderBuilder<NwisResult>()
	//	.dataSource(dataSourceNwis)
	//	.name("resultReader")
	//	.sql(new String(FileCopyUtils.copyToByteArray(sqlResource.getInputStream())))
	//	.rowMapper(new NwisResultRowMapper())
	//	.build();
	//}

	@Bean
	public NwisJdbcPagingItemReader<NwisResult> resultReader() throws Exception{
		NwisPostgresPagingQueryProvider queryProvider = new NwisPostgresPagingQueryProvider();
		Map<String, Order> sortKeys = new LinkedHashMap<>();

		queryProvider.setSelectClause(new String(FileCopyUtils.copyToByteArray(selectClause.getInputStream())));
		queryProvider.setFromClause(new String(FileCopyUtils.copyToByteArray(fromClause.getInputStream())));
		queryProvider.setWhereClause(new String(FileCopyUtils.copyToByteArray(whereClause.getInputStream())));
		sortKeys.put("activity_swap_nwis.activity_id", Order.ASCENDING);
		sortKeys.put("qw_result.parameter_cd", Order.ASCENDING);
		queryProvider.setSortKeys(sortKeys);

		NwisJdbcPagingItemReader reader = new NwisJdbcPagingItemReader();
		reader.setDataSource(dataSourceNwis);
		reader.setName("activityReader");
		reader.setPageSize(50000);
		reader.setRowMapper(new NwisResultRowMapper());
		reader.setQueryProvider(queryProvider);

		return reader;
	}
	
	@Bean
	public ItemWriter<Result> resultWriter() {
		JdbcBatchItemWriter<Result> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setDataSource(dataSourceWqp);
		itemWriter.setSql("insert into result_swap_nwis ("
				+ "data_source_id, data_source, station_id, site_id, event_date, "
				+ "analytical_method, p_code, activity, characteristic_name, characteristic_type, "
				+ "sample_media, organization, site_type, huc, governmental_unit_code, geom, "
				+ "organization_name, activity_id, activity_type_code, activity_media_subdiv_name, "
				+ "activity_start_time, act_start_time_zone, activity_stop_date, "
				+ "activity_stop_time, act_stop_time_zone, activity_depth, activity_depth_unit, "
				+ "activity_depth_ref_point, activity_upper_depth, activity_upper_depth_unit, " 
				+ "activity_lower_depth, activity_lower_depth_unit, project_id, activity_conducting_org, "
				+ "activity_comment, sample_aqfr_name, hydrologic_condition_name, "
				+ "hydrologic_event_name, sample_collect_method_id, sample_collect_method_ctx, "
				+ "sample_collect_method_name, sample_collect_equip_name, result_id, "
				+ "result_detection_condition_tx, sample_fraction_type, result_measure_value, "
				+ "result_unit, result_value_status, statistic_type, result_value_type, "
				+ "weight_basis_type, duration_basis, temperature_basis_level, particle_size, "
				+ "precision, result_comment, sample_tissue_taxonomic_name, sample_tissue_anatomy_name, "
				+ "analytical_procedure_id, analytical_procedure_source, analytical_method_name, "
				+ "analytical_method_citation, lab_name, analysis_start_date, lab_remark, "
				+ "detection_limit, detection_limit_unit, detection_limit_desc, analysis_prep_date_tx) "
				+ "values (:dataSourceId, :dataSource, :stationId, :siteId, :eventDate, "
				+ ":analyticalMethod, :pCode, :activity, :characteristicName, :characteristicType, "
				+ ":sampleMedia, :organization, :siteType, :huc, :governmentalUnitCode, :geom, "
				+ ":organizationName, :activityId, :activityTypeCode, :activityMediaSubdivName, "
				+ ":activityStartTime, :actStartTimeZone, :activityStopDate, "
				+ ":activityStopTime, :actStopTimeZone, :activityDepth, :activityDepthUnit, "
				+ ":activityDepthRefPoint, :activityUpperDepth, :activityUpperDepthUnit, "
				+ ":activityLowerDepth, :activityLowerDepthUnit, :projectId, :activityConductingOrg, "
				+ ":activityComment, :sampleAqfrName, :hydrologicConditionName, "
				+ ":hydrologicEventName, :sampleCollectMethodId, :sampleCollectMethodCtx, "
				+ ":sampleCollectMethodName, :sampleCollectEquipName, :resultId, "
				+ ":resultDetectionConditionTx, :sampleFractionType, :resultMeasureValue, "
				+ ":resultUnit, :resultValueStatus, :statisticType, :resultValueType, "
				+ ":weightBasisType, :durationBasis, :temperatureBasisLevel, :particleSize, "
				+ ":precision, :resultComment, :sampleTissueTaxonomicName, :sampleTissueAnatomyName, "
				+ ":analyticalProcedureId, :analyticalProcedureSource, :analyticalMethodName, "
				+ ":analyticalMethodCitation, :labName, :analysisStartDate, :labRemark, "
				+ ":detectionLimit, :detectionLimitUnit, :detectionLimitDesc, :analysisPrepDateTx)"
				);
		ItemSqlParameterSourceProvider<Result> paramProvider = new BeanPropertyItemSqlParameterSourceProvider<>();
		itemWriter.setItemSqlParameterSourceProvider(paramProvider);
		return itemWriter;
	}
	
	@Bean
	public Step transformResultStep() throws Exception{
		return stepBuilderFactory
				.get("transformResultStep")
				.<NwisResult, Result>chunk(100000)
				.reader(resultReader())
				.processor(processor)
				.writer(resultWriter())
				.build();
	}

	@Bean
	public Flow resultFlow() throws Exception {
		return new FlowBuilder<SimpleFlow>("resultFlow")
				.start(setupResultSwapTableFlow)
				.next(transformResultStep())
				.next(buildResultIndexesFlow)
				.build();
	}
}
