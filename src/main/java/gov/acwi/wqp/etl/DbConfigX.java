package gov.acwi.wqp.etl;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class DbConfigX {

	@Bean
	@Primary
	@ConfigurationProperties("spring.datasource-wqp")
	public DataSourceProperties wqpDataSourceProperties() {
		return new DataSourceProperties();
	}
	
	@Bean
	@Primary
	@ConfigurationProperties("spring.datasource-wqp")
	public DataSource wqpDataSource() {
		return wqpDataSourceProperties().initializeDataSourceBuilder().build();
	}
	
	@Bean
	@Primary
	public JdbcTemplate jdbcTemplateWqp() {
		return new JdbcTemplate(wqpDataSource());
	}
	
	@Bean
	@ConfigurationProperties("spring.datasource-nwis")
	public DataSourceProperties nwisDataSourceProperties() {
		return new DataSourceProperties();
	}
	
	@Bean
	@ConfigurationProperties("spring.datasource-nwis")
	public DataSource nwisDataSource() {
		return nwisDataSourceProperties().initializeDataSourceBuilder().build();
	}
	
	@Bean
	public JdbcTemplate jdbcTemplateNwis() {
		return new JdbcTemplate(nwisDataSource());
	}

	@Bean
	@ConfigurationProperties("spring.datasource-mysqlnwis")
	public DataSourceProperties mysqlnwisDataSourceProperties() {
		return new DataSourceProperties();
	}
	
	@Bean
	@ConfigurationProperties("spring.datasource-mysqlnwis")
	public DataSource mysqlnwisDataSource() {
		return mysqlnwisDataSourceProperties().initializeDataSourceBuilder().build();
	}
}
