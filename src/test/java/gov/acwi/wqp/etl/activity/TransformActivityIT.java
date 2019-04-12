package gov.acwi.wqp.etl.activity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
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
import gov.acwi.wqp.etl.activity.index.BuildActivityIndexesFlowIT;
import gov.acwi.wqp.etl.activity.table.SetupActivitySwapTableFlowIT;

public class TransformActivityIT extends NwisBaseFlowIT {
	
	@Autowired
	@Qualifier("activityFlow")
	private Flow activityFlow;

	@Before
	public void setup() {
		testJob = jobBuilderFactory
				.get("activityFlowTest")
				.start(activityFlow)
				.build().build();
		jobLauncherTestUtils.setJob(testJob);
	}
	
	@Test
	@DatabaseSetup(value = "classpath:/testResult/wqp/activity/empty.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/qwSample/qwSample.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/sitefile/sitefile.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/nwisWqxMediumCd/nwisWqxMediumCd.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/bodyPart/bodyPart.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/hydEventCd/hydEventCd.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/hydCondCd/hydCondCd.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/nwisDistrictCdsByHost/nwisDistrictCdsByHost.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/aqfr/aqfr.xml")
	@DatabaseSetup(value = "classpath:/testResult/wqp/monitoringLocation/monitoringLocation.xml")
	@ExpectedDatabase(value = "classpath:/testResult/wqp/activity/activity.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void transformNwisActivityStepTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformActivityStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
	
	@Test
	@DatabaseSetup(value = "classpath:/testResult/wqp/activity/empty.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/qwSample/qwSample.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/sitefile/sitefile.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/nwisWqxMediumCd/nwisWqxMediumCd.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/bodyPart/bodyPart.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/hydEventCd/hydEventCd.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/hydCondCd/hydCondCd.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/nwisDistrictCdsByHost/nwisDistrictCdsByHost.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/aqfr/aqfr.xml")
	@DatabaseSetup(value = "classpath:/testResult/wqp/monitoringLocation/monitoringLocation.xml")
	//@ExpectedDatabase(value = "classpath:/testResult/wqp/activity/indexes/all.xml",
	//		assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
	//		table = BuildActivityIndexesFlowIT.EXPECTED_DATABASE_TABLE,
	//		query = BuildActivityIndexesFlowIT.EXPECTED_DATABASE_QUERY)
	//@ExpectedDatabase(connection = "pg", 
	//		value = "classpath:/testResult/wqp/activity/create.xml", 
	//		assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
	//		table = SetupActivitySwapTableFlowIT.EXPECTED_DATABASE_TABLE, 
	//		query = SetupActivitySwapTableFlowIT.EXPECTED_DATABASE_QUERY)
	@ExpectedDatabase(value = "classpath:/testResult/wqp/activity/activity.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void activityFlowTest() {
		Job activityFlowTest = jobBuilderFactory.get("activityFlowTest")
				.start(activityFlow)
				.build()
				.build();
		jobLauncherTestUtils.setJob(activityFlowTest);
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
