package gov.acwi.wqp.etl.mysqlnwis.qwSample;

import javax.sql.DataSource;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.database.support.SqlServerPagingQueryProvider;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import gov.acwi.wqp.etl.Application;

@Configuration
public class TransformQwSample {

	@Autowired
	@Qualifier(Application.DATASOURCE_MYSQLNWIS_QUALIFER)
	private DataSource dataSourceMysqlnwis;

	@Autowired
	@Qualifier(Application.DATASOURCE_NWIS_QUALIFIER)
	private DataSource dataSourceNwis;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public JdbcPagingItemReader<QwSample> qwSampleReader() {
		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		Map<String, Order> sortKeys = new HashMap<>(1);

		sortKeys.put("sample_id", Order.ASCENDING);
		queryProvider.setSelectClause("select *");
		queryProvider.setFromClause("from QW_SAMPLE");
		queryProvider.setSortKeys(sortKeys);

		return new JdbcPagingItemReaderBuilder<QwSample>()
				.dataSource(dataSourceMysqlnwis)
				.name("mysqlQwSampleReader")
				.queryProvider(queryProvider)
				.maxItemCount(5000)
				.pageSize(5000)
				.rowMapper(new QwSampleRowMapper())
				.build();
	}
	
	@Bean
	public PassThroughItemProcessor<QwSample> qwSampleProcessor() {
		return new PassThroughItemProcessor<>();
	}
	
	@Bean
	public JdbcBatchItemWriter<QwSample> qwSampleWriter() {
		return new JdbcBatchItemWriterBuilder<QwSample>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("insert into qw_sample "
						+ "(sample_id, site_id, record_no, nwis_host, db_no, qw_db_no, "
						+ "sample_web_cd, sample_start_dt, sample_start_display_dt, sample_start_sg, "
						+ "sample_end_dt, sample_end_display_dt, sample_end_sg, " 
						+ "sample_utc_start_dt, sample_utc_start_display_dt, sample_utc_end_dt, sample_utc_end_display_dt, "
						+ "sample_start_time_datum_cd, medium_cd, tu_id, body_part_id, "
						+ "nwis_sample_id, lab_no, project_cd, aqfr_cd, samp_type_cd, sample_lab_cm_tx, "
						+ "sample_field_cm_tx, coll_ent_cd, anl_stat_cd, anl_src_cd, "
						+ "anl_type_tx, hyd_cond_cd, hyd_event_cd, tm_datum_rlbty_cd, "
						+ "sample_md, qw_sample_md) VALUES ("
						+ ":sampleId, :siteId, :recordNo, :nwisHost, :dbNo, :qwDbNo, "
						+ ":sampleWebCd, :sampleStartDt, :sampleStartDisplayDt, :sampleStartSg, "
						+ ":sampleEndDt, :sampleEndDisplayDt, :sampleEndSg, "
						+ ":sampleUtcStartDt, :sampleUtcStartDisplayDt, :sampleUtcEndDt, :sampleUtcEndDisplayDt, "
						+ ":sampleStartTimeDatumCd, :mediumCd, :tuId, :bodyPartId, "
						+ ":nwisSampleId, :labNo, :projectCd, :aqfrCd, :sampTypeCd, :sampleLabCmTx, "
						+ ":sampleFieldCmTx, :collEntCd, :anlStatCd, :anlSrcCd, "
						+ ":anlTypeTx, :hydCondCd, :hydEventCd, :tmDatumRlbtyCd, "
						+ ":sampleMd, :qwSampleMd)")
				.dataSource(dataSourceNwis)
				.build();
						
	}
	
}
