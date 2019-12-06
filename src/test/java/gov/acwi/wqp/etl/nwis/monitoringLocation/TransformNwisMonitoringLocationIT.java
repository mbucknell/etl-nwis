package gov.acwi.wqp.etl.nwis.monitoringLocation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.NwisBaseFlowIT;

public class TransformNwisMonitoringLocationIT extends NwisBaseFlowIT {

	public static final String EXPECTED_DATABASE_TABLE_CHECK_TABLE = "monitoring_location";
	public static final String EXPECTED_DATABASE_QUERY_MERGES = "select * from monitoring_location where monitoring_location_id < 5";

	@Autowired
	@Qualifier("nwisMonitoringLocationFlow")
	private Flow nwisMonitoringLocationFlow;

	@Autowired
	@Qualifier("jdbcTemplateNwis")
	private JdbcTemplate jdbcTemplateNwis;

	@Test
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/agency/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/altitudeDatum/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/altitudeMethod/altitudeMethod.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/aqfr/aqfr.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/aquiferType/aquiferType.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/county/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/dataReliability/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/huc/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/latLongAccuracy/latLongAccuracy.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/latLongDatum/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/latLongMethod/latLongMethod.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/monitoringLocation/setA/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/natAqfr/natAqfr.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/sitefile/csv/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/siteTp/siteTp.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/state/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/topographicSetting/")
	@DatabaseSetup(value = "classpath:/testData/wqp/huc12nometa/")

	@ExpectedDatabase(
			connection=CONNECTION_NWIS,
			value="classpath:/testResult/nwis/monitoringLocation/merges/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_TABLE,
			query=EXPECTED_DATABASE_QUERY_MERGES
			)
	@ExpectedDatabase(
			connection=CONNECTION_NWIS,
			value="classpath:/testResult/nwis/monitoringLocation/new/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_TABLE,
			query=MLNewQueryHelper.EXPECTED_DATABASE_QUERY_MERGES_NEW
			)
	public void monitoringLocationFlowTest() {
		Job nwisMonitoringLocationFlowTest = jobBuilderFactory.get("nwisMonitoringLocationFlowTest")
				.start(nwisMonitoringLocationFlow)
				.build()
				.build();
		jobLauncherTestUtils.setJob(nwisMonitoringLocationFlowTest);
		try {
			//Manually bump up identity past loaded test data. DBUnit @DatabaseSetup will not accomplish this.
			jdbcTemplateNwis.execute("alter table nwis.monitoring_location alter monitoring_location_id restart with 100");
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
