package gov.acwi.wqp.etl;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.autoconfigure.liquibase.LiquibaseProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import com.github.springtestdbunit.bean.DatabaseConfigBean;
import com.github.springtestdbunit.bean.DatabaseDataSourceConnectionFactoryBean;

import liquibase.integration.spring.SpringLiquibase;

@TestConfiguration
public class NwisDBTestConfig {

	@Autowired
	private DatabaseConfigBean dbUnitDatabaseConfig;

	@Autowired
	@Qualifier("dataSourceWqp")
	private DataSource dataSourceWqp;

	@Autowired
	@Qualifier(Application.DATASOURCE_NWIS_QUALIFIER)
	private DataSource dataSourceNwis;

	@Bean
	@Primary
	public JdbcTemplate jdbcTemplate() {
		return new JdbcTemplate(dataSourceWqp);
	}

	@Bean
	public JdbcTemplate jdbcTemplateNwis() {
		return new JdbcTemplate(dataSourceNwis);
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
	@Bean
	@ConfigurationProperties(prefix = "spring.liquibase")
	public LiquibaseProperties primaryLiquibaseProperties() {
		return new LiquibaseProperties();
	}

	@Bean
	public SpringLiquibase liquibase() {
		return instantiateSpringLiquibase(dataSourceWqp, primaryLiquibaseProperties());
	}

	@Bean
	@ConfigurationProperties(prefix = "spring.datasource-mysqlnwis.liquibase")
	public LiquibaseProperties liquibasePropertiesMysqlnwis() {
		return new LiquibaseProperties();
	}

	@Bean
	public SpringLiquibase liquibaseMysqlnwis() {
		return instantiateSpringLiquibase(dataSourceMysqlnwis(), liquibasePropertiesMysqlnwis());
	}

	private SpringLiquibase instantiateSpringLiquibase(DataSource dataSource, LiquibaseProperties liquibaseProperties) {
		SpringLiquibase springLiquibase = new SpringLiquibase();
		springLiquibase.setDataSource(dataSource);
		springLiquibase.setChangeLog(liquibaseProperties.getChangeLog());
		springLiquibase.setChangeLogParameters(liquibaseProperties.getParameters());
		springLiquibase.setLiquibaseSchema(liquibaseProperties.getLiquibaseSchema());
		return springLiquibase;
	}

	@Bean
	public DatabaseDataSourceConnectionFactoryBean mysqlnwis() throws SQLException {
		DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection = new DatabaseDataSourceConnectionFactoryBean();
		dbUnitDatabaseConnection.setDatabaseConfig(dbUnitDatabaseConfig);
		dbUnitDatabaseConnection.setDataSource(dataSourceMysqlnwis());
		return dbUnitDatabaseConnection;
	}
}
