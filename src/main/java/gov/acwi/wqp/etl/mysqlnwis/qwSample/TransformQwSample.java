package gov.acwi.wqp.etl.mysqlnwis.qwSample;

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
public class TransformQwSample {

	@Autowired
	@Qualifier("mysqlnwisDataSource")
	private DataSource mysqlnwisDataSource;

	@Autowired
	@Qualifier("nwisDataSource")
	private DataSource nwisDataSource;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public JdbcCursorItemReader<QwSample> qwSampleReader() {
		return new JdbcCursorItemReaderBuilder<QwSample>()
				.dataSource(mysqlnwisDataSource)
				.name("mysqlQwSampleReader")
				.sql("select * from QW_SAMPLE limit 2000") //TODO remove limit
				.rowMapper(new QwSampleRowMapper())
				.build();
	}
	
	@Bean
	public PassThroughItemProcessor<QwSample> qwSampleProcessor() {
		return new PassThroughItemProcessor<QwSample>();
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
				.dataSource(nwisDataSource)
				.build();
						
	}
	
}
