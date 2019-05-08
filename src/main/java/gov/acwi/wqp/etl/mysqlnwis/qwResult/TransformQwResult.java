package gov.acwi.wqp.etl.mysqlnwis.qwResult;

import javax.sql.DataSource;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.database.support.SqlServerPagingQueryProvider;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import gov.acwi.wqp.etl.Application;

@Configuration
public class TransformQwResult {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(Application.DATASOURCE_MYSQLNWIS_QUALIFER)
	private DataSource dataSourceMysqlnwis;

	@Autowired
	@Qualifier(Application.DATASOURCE_NWIS_QUALIFIER)
	private DataSource dataSourceNwis;

	@Bean
	public JdbcPagingItemReader<QwResult> qwResultReader() {
		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		Map<String, Order> sortKeys = new HashMap<>(1);

		sortKeys.put("sample_id", Order.ASCENDING);
		sortKeys.put("parameter_cd", Order.ASCENDING);
		queryProvider.setSelectClause("select *");
		queryProvider.setFromClause("from QW_RESULT");
		queryProvider.setSortKeys(sortKeys);

		return new JdbcPagingItemReaderBuilder<QwResult>()
				.dataSource(dataSourceMysqlnwis)
				.name("mysqlQwResultReader")
				.pageSize(50000)
				.queryProvider(queryProvider)
				.rowMapper(new QwResultRowMapper())
				.build();
	}
	
	@Bean
	public PassThroughItemProcessor<QwResult> qwResultProcessor() {
		return new PassThroughItemProcessor<>();
	}
	
	@Bean
	public JdbcBatchItemWriter<QwResult> qwResultWriter() {
		return new JdbcBatchItemWriterBuilder<QwResult>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("insert into qw_result ("
						+ "sample_id, site_id, record_no, result_web_cd, parameter_cd, "
						+ "meth_cd, result_va, result_unrnd_va, result_rd, rpt_lev_va, "
						+ "rpt_lev_cd, lab_std_va, remark_cd, val_qual_tx, null_val_qual_cd, "
						+ "qa_cd, dqi_cd, anl_ent_cd, anl_set_no, anl_dt, prep_set_no, "
						+ "prep_dt, result_field_cm_tx, result_lab_cm_tx, result_md, "
						+ "qw_result_md) VALUES (" 
						+ ":sampleId, :siteId, :recordNo, :resultWebCd, :parameterCd, "
						+ ":methCd, :resultVa, :resultUnrndVa, :resultRd, :rptLevVa, "
						+ ":rptLevCd, :labStdVa, :remarkCd, :valQualTx, :nullValQualCd, "
						+ ":qaCd, :dqiCd, :anlEntCd, :anlSetNo, :anlDt, :prepSetNo, "
						+ ":prepDt, :resultFieldCmTx, :resultLabCmTx, :resultMd, "
						+ ":qwResultMd)")
				.dataSource(dataSourceNwis)
				.build();
	}
						
}
