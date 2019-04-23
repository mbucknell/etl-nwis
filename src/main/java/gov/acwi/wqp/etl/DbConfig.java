package gov.acwi.wqp.etl;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

@Configuration
@Profile("default")
public class DbConfig {

	@Bean
	@Primary
	@ConfigurationProperties(prefix=EtlConstantUtils.SPRING_DATASOURCE_WQP)
	public DataSourceProperties dataSourcePropertiesWqp() {
		return new DataSourceProperties();
	}
	
	@Bean
	@Primary
	public DataSource dataSourceWqp() {
		return dataSourcePropertiesWqp().initializeDataSourceBuilder().build();
	}
	
	@Bean
	@Primary
	public JdbcTemplate jdbcTemplateWqp() {
		return new JdbcTemplate(dataSourceWqp());
	}
	
	@Bean
	@ConfigurationProperties(prefix="spring.datasource-nwis")
	public DataSourceProperties dataSourcePropertiesNwis() {
		return new DataSourceProperties();
	}
	
	@Bean	
	public DataSource dataSourceNwis() {
		return dataSourcePropertiesNwis().initializeDataSourceBuilder().build();
	}
	
	@Bean
	@ConfigurationProperties(prefix="spring.datasource-mysqlnwis")
	public DataSourceProperties dataSourcePropertiesMysqlnwis() {
		return new DataSourceProperties();
	}
	
	@Bean
	public DataSource dataSourceMysqlnwis() {
		return dataSourcePropertiesMysqlnwis().initializeDataSourceBuilder().build();
	}
}
