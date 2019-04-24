package gov.acwi.wqp.etl;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import com.github.springtestdbunit.bean.DatabaseConfigBean;

@TestConfiguration
public class NwisDBTestConfig {

	@Autowired
	private DatabaseConfigBean dbUnitDatabaseConfig;

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
