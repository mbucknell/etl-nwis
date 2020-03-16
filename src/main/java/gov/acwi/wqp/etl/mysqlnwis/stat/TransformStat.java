package gov.acwi.wqp.etl.mysqlnwis.stat;

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
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import gov.acwi.wqp.etl.Application;

@Configuration
public class TransformStat {

	@Autowired
	@Qualifier(Application.DATASOURCE_MYSQLNWIS_QUALIFER)
	private DataSource dataSourceMysqlnwis;

	@Autowired
	@Qualifier(Application.DATASOURCE_NWIS_QUALIFIER)
	private DataSource dataSourceNwis;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	public JdbcPagingItemReader<Stat> statReader() {
		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		Map<String, Order> sortKeys = new HashMap<>(1);

		sortKeys.put("stat_cd", Order.ASCENDING);
		queryProvider.setSelectClause("select *");
		queryProvider.setFromClause("from stat");
		queryProvider.setSortKeys(sortKeys);

		return new JdbcPagingItemReaderBuilder<Stat>()
				.dataSource(dataSourceMysqlnwis)
				.name("mysqlStatReader")
				.queryProvider(queryProvider)
				.pageSize(5000)
				.rowMapper(new StatRowMapper())
				.build();
	}

	@Bean
	public PassThroughItemProcessor<Stat> statProcessor() {
		return new PassThroughItemProcessor<>();
	}

	@Bean
	public JdbcBatchItemWriter<Stat> statWriter() {
		return new JdbcBatchItemWriterBuilder<Stat>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("insert into stat "
						+ "(stat_cd, dv_valid_fg, uv_valid_fg, stat_nm, stat_ds)"
						+ "values"
						+ "(:statCd, :dvValidFg, :uvValidFg, :statNm, :statDs)")
				.dataSource(dataSourceNwis)
				.build();
	}

}
