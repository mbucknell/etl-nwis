package gov.acwi.wqp.etl.mysqlnwis.gwlevels;

import java.util.LinkedHashMap;

import javax.sql.DataSource;

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
public class TransformGwLevels {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(Application.DATASOURCE_MYSQLNWIS_QUALIFER)
	private DataSource dataSourceMysqlnwis;

	@Autowired
	@Qualifier(Application.DATASOURCE_NWIS_QUALIFIER)
	private DataSource dataSourceNwis;

	@Bean
	public JdbcPagingItemReader<GwLevels> gwLevelsReader() {
		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		LinkedHashMap<String, Order> sortKeys = new LinkedHashMap<>(1);

		sortKeys.put(GwLevelsRowMapper.SITE_ID_COLUMN_NAME, Order.ASCENDING);
		sortKeys.put(GwLevelsRowMapper.LEV_DTM, Order.ASCENDING);
		sortKeys.put(GwLevelsRowMapper.LEV_DT_ACY_CD, Order.ASCENDING);
		sortKeys.put(GwLevelsRowMapper.LEV_TZ_CD, Order.ASCENDING);
		queryProvider.setSelectClause("select *");
		queryProvider.setFromClause("from GW_LEVELS");
		queryProvider.setSortKeys(sortKeys);

		return new JdbcPagingItemReaderBuilder<GwLevels>()
				.dataSource(dataSourceMysqlnwis)
				.name("mysqlGwLevelsReader")
				.pageSize(50000)
				.queryProvider(queryProvider)
				.rowMapper(new GwLevelsRowMapper())
				.build();
	}

	@Bean
	public PassThroughItemProcessor<GwLevels> gwLevelsProcessor() {
		return new PassThroughItemProcessor<>();
	}

	@Bean
	public JdbcBatchItemWriter<GwLevels> gwLevelsWriter() {
		return new JdbcBatchItemWriterBuilder<GwLevels>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("insert into gw_levels ("
						+ "site_id, lev_str_dt, lev_dtm, lev_dt_acy_cd, lev_tz_cd, lev_tz_offset, "
						+ "lev_va, lev_acy_cd, parameter_cd, lev_datum_cd, lev_src_cd, "
						+ "lev_status_cd, lev_meth_cd, lev_agency_cd, lev_age_cd, gw_levels_md"
						+ ") VALUES (" 
						+ ":siteId, :levStrDt, :levDtm, :levDtAcyCd, :levTzCd, :levTzOffset, "
						+ ":levVa, :levAcyCd, :parameterCd, :levDatumCd, :levSrcCd, "
						+ ":levStatusCd, :levMethCd, :levAgencyCd, :levAgeCd, :gwLevelsMd"
						+ ")"
						)
				.dataSource(dataSourceNwis)
				.build();
	}
}
