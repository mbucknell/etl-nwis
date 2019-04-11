package gov.acwi.wqp.etl;

import javax.sql.DataSource;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class NwisDBTestConfig {
	
	@Bean
	@ConfigurationProperties("spring.datasource-mysqlnwis")
	public DataSourceProperties dataSourcePropertiesMysqlnwis() {
		return new DataSourceProperties();
	}
	
	@Bean
	@ConfigurationProperties("spring.datasource-mysqlnwis")
	public DataSource dataSourceMysqlnwis() {
		return dataSourcePropertiesMysqlnwis().initializeDataSourceBuilder().build();
	}
}
