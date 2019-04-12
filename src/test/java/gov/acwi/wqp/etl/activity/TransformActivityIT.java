package gov.acwi.wqp.etl.activity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
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

@SpringBatchTest
@SpringBootTest
@RunWith(SpringRunner.class)
@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class, StepScopeTestExecutionListener.class,
		DbUnitTestExecutionListener.class })
@AutoConfigureTestDatabase(replace = Replace.NONE)
@Import({ DBTestConfig.class })
@DbUnitConfiguration(databaseConnection = { "wqp", "pg", "nwis" })
@DirtiesContext
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
	@DatabaseSetup(value = "classpath:/testResult/wqp/monitoringLocation.monitoringLocation.xml")
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
}
