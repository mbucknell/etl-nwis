package gov.acwi.wqp.etl.monitoringLocation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.test.StepScopeTestExecutionListener;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.DBTestConfig;
import gov.acwi.wqp.etl.NwisBaseFlowIT;
import gov.acwi.wqp.etl.NwisDBTestConfig;
import gov.acwi.wqp.etl.monitoringLocation.index.BuildMonitoringLocationIndexesFlowIT;
import gov.acwi.wqp.etl.monitoringLocation.table.SetupMonitoringLocationSwapTableFlowIT;

@SpringBatchTest
@SpringBootTest
@RunWith(SpringRunner.class)
@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class, StepScopeTestExecutionListener.class,
		DbUnitTestExecutionListener.class })
@AutoConfigureTestDatabase(replace = Replace.NONE)
@Import({ DBTestConfig.class, NwisDBTestConfig.class })
@DbUnitConfiguration(databaseConnection = { "wqp", "pg", "nwis" })
@DirtiesContext
public class TransformMonitoringLocationIT extends NwisBaseFlowIT {

	@Autowired
	@Qualifier("monitoringLocationFlow")
	private Flow monitoringLocationFlow;

	@Before
	public void setup() {
		testJob = jobBuilderFactory
				.get("monitoringLocationFlowTest")
				.start(monitoringLocationFlow)
				.build().build();
		jobLauncherTestUtils.setJob(testJob);
	}

	@Test
	@DatabaseSetup(value = "classpath:/testResult/wqp/monitoringLocation/empty.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/nwisDistrictCdsByHost/nwisDistrictCdsByHost.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/altitudeMethod/altitudeMethod.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/aqfr/aqfr.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/aquiferType/aquiferType.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/latLongAccuracy/latLongAccuracy.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/latLongMethod/latLongMethod.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/natAqfr/natAqfr.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/sitefile/sitefile.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/siteTp/siteTp.xml")
	@ExpectedDatabase(value = "classpath:/testResult/wqp/monitoringLocation/monitoringLocation.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
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
	@DatabaseSetup(connection = "nwis", value = "classpath:/testResult/nwis/nwisStationLocal/empty.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/sitefile/sitefile.xml")
	@ExpectedDatabase(connection = "nwis", value="classpath:/testResult/nwis/nwisStationLocal/nwisStationLocal.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void insertNwisStationLocalStepTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("upsertNwisStationLocalStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
	
	@Test
	@DatabaseSetup(connection = "nwis", value = "classpath:/testResult/nwis/nwisStationLocal/nwisStationLocal.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/sitefile/sitefileUpdate.xml")
	@ExpectedDatabase(connection = "nwis", value="classpath:/testResult/nwis/nwisStationLocal/nwisStationLocalUpdate.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void updateNwisStationLocalStepTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("upsertNwisStationLocalStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

	@Test
	@DatabaseSetup(value = "classpath:/testResult/wqp/monitoringLocation/empty.xml")
	@DatabaseSetup(value = "classpath:/testData/wqp/monitoringLocation/monitoringLocationOld.xml")
	@DatabaseSetup(connection = "nwis", 
		value = "classpath:/testData/nwis/nwisDistrictCdsByHost/nwisDistrictCdsByHost.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/altitudeMethod/altitudeMethod.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/aqfr/aqfr.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/aquiferType/aquiferType.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/latLongAccuracy/latLongAccuracy.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/latLongMethod/latLongMethod.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/natAqfr/natAqfr.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/sitefile/sitefile.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/siteTp/siteTp.xml")
	//@ExpectedDatabase(value = "classpath:/testResult/wqp/monitoringLocation/indexes/all.xml",
	//	assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
	//	table = BuildMonitoringLocationIndexesFlowIT.EXPECTED_DATABASE_TABLE,
	//	query = BuildMonitoringLocationIndexesFlowIT.EXPECTED_DATABASE_QUERY)
	//@ExpectedDatabase(connection = "pg", 
	//	value = "classpath:/testResult/wqp/monitoringLocation/create.xml", 
	//	assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
	//	table = SetupMonitoringLocationSwapTableFlowIT.EXPECTED_DATABASE_TABLE, 
	//	query = SetupMonitoringLocationSwapTableFlowIT.EXPECTED_DATABASE_QUERY)
	@ExpectedDatabase(value = "classpath:/testResult/wqp/monitoringLocation/monitoringLocation.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void monitoringLocationFlowTest() {
		Job monitoringLocationFlowTest = jobBuilderFactory.get("monitoringLocationFlowTest")
				.start(monitoringLocationFlow)
				.build()
				.build();
		jobLauncherTestUtils.setJob(monitoringLocationFlowTest);
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
