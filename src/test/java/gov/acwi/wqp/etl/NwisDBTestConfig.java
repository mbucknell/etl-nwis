package gov.acwi.wqp.etl;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import com.github.springtestdbunit.bean.DatabaseConfigBean;
import com.github.springtestdbunit.bean.DatabaseDataSourceConnectionFactoryBean;

@TestConfiguration
public class NwisDBTestConfig {
	
	
	@Autowired
	@Qualifier("nwisDataSource")
	private DataSource nwisDataSource;
	
	@Autowired
	private DatabaseConfigBean dbUnitDatabaseConfig;

	@Bean
	public DatabaseDataSourceConnectionFactoryBean nwis() throws SQLException {
		DatabaseDataSourceConnectionFactoryBean dbUnitDatabaseConnection = new DatabaseDataSourceConnectionFactoryBean();
		dbUnitDatabaseConnection.setDatabaseConfig(dbUnitDatabaseConfig);
		dbUnitDatabaseConnection.setDataSource(nwisDataSource);
		dbUnitDatabaseConnection.setSchema("nwis_ws_star");
		return dbUnitDatabaseConnection;
	}

}
