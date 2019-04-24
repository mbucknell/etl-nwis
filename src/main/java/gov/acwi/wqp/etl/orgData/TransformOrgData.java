package gov.acwi.wqp.etl.orgData;

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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import gov.acwi.wqp.etl.Application;
import gov.acwi.wqp.etl.EtlConstantUtils;
import gov.acwi.wqp.etl.nwis.NwisDistrictCdsByHost;
import gov.acwi.wqp.etl.nwis.NwisDistrictCdsByHostRowMapper;

@Configuration
public class TransformOrgData {

	@Autowired
	@Qualifier("orgDataProcessor")
	private ItemProcessor<NwisDistrictCdsByHost, OrgData> processor;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSourceWqp;
	
	@Autowired
	@Qualifier(Application.DATASOURCE_NWIS_QUALIFIER)
	private DataSource dataSourceNwis;
	
	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_ORG_DATA_SWAP_TABLE_FLOW)
	private Flow setupOrgDataSwapTableFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.BUILD_ORG_DATA_INDEXES_FLOW)
	private Flow buildOrgDataIndexesFlow;

	@Bean
	public JdbcCursorItemReader<NwisDistrictCdsByHost> nwisOrgReader() {
		return new JdbcCursorItemReaderBuilder<NwisDistrictCdsByHost>()
				.dataSource(dataSourceNwis)
				.name("organizationReader")
				.sql("select distinct * from nwis_district_cds_by_host")
				.rowMapper(new NwisDistrictCdsByHostRowMapper())
				.build();
	}

	@Bean
	public ItemWriter<OrgData> orgDataWriter() {
		JdbcBatchItemWriter<OrgData> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setDataSource(dataSourceWqp);
		itemWriter.setSql("insert into org_data_swap_nwis (data_source_id, data_source, organization_id, organization, organization_name)"
				+ " values (:dataSourceId, :dataSource, :organizationId, :organization, :organizationName)");

		ItemSqlParameterSourceProvider<OrgData> paramProvider = new BeanPropertyItemSqlParameterSourceProvider<>();

		itemWriter.setItemSqlParameterSourceProvider(paramProvider);
		return itemWriter;
	}

	@Bean
	public Step transformOrgDataStep() {
		return stepBuilderFactory
				.get("transformOrgDataStep")
				.<NwisDistrictCdsByHost, OrgData>chunk(10)
				.reader(nwisOrgReader())
				.processor(processor)
				.writer(orgDataWriter())
				.build();
	}

	@Bean
	public Flow orgDataFlow() {
		return new FlowBuilder<SimpleFlow>("orgDataFlow")
				.start(setupOrgDataSwapTableFlow)
				.next(transformOrgDataStep())
				.next(buildOrgDataIndexesFlow)
				.build();
	}
}
