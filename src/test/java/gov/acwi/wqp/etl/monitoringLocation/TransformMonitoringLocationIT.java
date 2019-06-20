package gov.acwi.wqp.etl.monitoringLocation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.NwisBaseFlowIT;


public class TransformMonitoringLocationIT extends NwisBaseFlowIT {

	public static final String TABLE_NAME = "'station_swap_nwis'";
	public static final String EXPECTED_DATABASE_QUERY_ANALYZE = BASE_EXPECTED_DATABASE_QUERY_ANALYZE + TABLE_NAME;
	public static final String EXPECTED_DATABASE_QUERY_PRIMARY_KEY = BASE_EXPECTED_DATABASE_QUERY_PRIMARY_KEY
			+ EQUALS_QUERY + TABLE_NAME;
	public static final String EXPECTED_DATABASE_QUERY_FOREIGN_KEY = BASE_EXPECTED_DATABASE_QUERY_FOREIGN_KEY
			+ EQUALS_QUERY + TABLE_NAME;

	@Autowired
	@Qualifier("monitoringLocationFlow")
	private Flow monitoringLocationFlow;

	@Test
	@DatabaseSetup(value = "classpath:/testResult/wqp/monitoringLocation/empty.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/nwisDistrictCdsByHost/nwisDistrictCdsByHost.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/altitudeMethod/altitudeMethod.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/aqfr/aqfr.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/aquiferType/aquiferType.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/latLongAccuracy/latLongAccuracy.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/latLongMethod/latLongMethod.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/natAqfr/natAqfr.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testResult/nwis/nwisStationLocal/nwisStationLocal.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/sitefile/sitefile.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/siteTp/siteTp.xml")
	@ExpectedDatabase(
			value = "classpath:/testResult/wqp/monitoringLocation/monitoringLocation.xml",
			assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void transformNwisMonitoringLocationStepTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformMonitoringLocationStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
	
	@Test
	@DatabaseSetup(value = "classpath:/testData/wqp/monitoringLocation/monitoringLocationOld.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/altitudeMethod/altitudeMethod.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/aqfr/aqfr.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/aquiferType/aquiferType.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/latLongAccuracy/latLongAccuracy.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/latLongMethod/latLongMethod.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/natAqfr/natAqfr.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testResult/nwis/nwisStationLocal/nwisStationLocal.xml")
	@DatabaseSetup(value="classpath:/testData/wqp/huc12nometa/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/sitefile/sitefile.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/siteTp/siteTp.xml")
	@ExpectedDatabase(value = "classpath:/testResult/wqp/monitoringLocation/indexes/all.xml",
		assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
		table = EXPECTED_DATABASE_TABLE_CHECK_INDEX,
		query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX + TABLE_NAME)
	@ExpectedDatabase(connection = "pg", 
		value = "classpath:/testResult/wqp/monitoringLocation/create.xml", 
		assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
		table = EXPECTED_DATABASE_TABLE_CHECK_TABLE, 
		query = BASE_EXPECTED_DATABASE_QUERY_CHECK_TABLE + TABLE_NAME)
	@ExpectedDatabase(
			value = "classpath:/testResult/wqp/monitoringLocation/monitoringLocation.xml",
			assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/analyze/monitoringLocation.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_ANALYZE,
			query=EXPECTED_DATABASE_QUERY_ANALYZE)
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/monitoringLocation/primaryKey.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_PRIMARY_KEY,
			query=EXPECTED_DATABASE_QUERY_PRIMARY_KEY)
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/monitoringLocation/foreignKey.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_FOREIGN_KEY,
			query=EXPECTED_DATABASE_QUERY_FOREIGN_KEY)
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/monitoringLocation/indexes/pk.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_INDEX,
			query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX_PK + TABLE_NAME)
	public void monitoringLocationFlowTest() {
		Job monitoringLocationFlowTest = jobBuilderFactory.get("monitoringLocationFlowTest")
				.start(monitoringLocationFlow)
				.build()
				.build();
		jobLauncherTestUtils.setJob(monitoringLocationFlowTest);
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
