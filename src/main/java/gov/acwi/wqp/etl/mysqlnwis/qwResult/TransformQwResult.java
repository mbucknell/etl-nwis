package gov.acwi.wqp.etl.mysqlnwis.qwResult;

import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class TransformQwResult {

	@Autowired
	@Qualifier("dataSourceMysqlnwis")
	private DataSource dataSourceMysqlnwis;

	@Autowired
	@Qualifier("dataSourceNwis")
	private DataSource dataSourceNwis;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public JdbcCursorItemReader<QwResult> qwResultReader() {
		return new JdbcCursorItemReaderBuilder<QwResult>()
				.dataSource(dataSourceMysqlnwis)
				.name("mysqlQwResultReader")
				.sql("select * from QW_RESULT limit 2000") //TODO remove limit
				.rowMapper(new QwResultRowMapper())
				.build();
	}
	
	@Bean
	public PassThroughItemProcessor<QwResult> qwResultProcessor() {
		return new PassThroughItemProcessor<QwResult>();
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
