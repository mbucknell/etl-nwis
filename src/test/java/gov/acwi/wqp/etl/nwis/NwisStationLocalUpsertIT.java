package gov.acwi.wqp.etl.nwis;

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
import gov.acwi.wqp.etl.NwisDBTestConfig;

@SpringBatchTest
@SpringBootTest
@RunWith(SpringRunner.class)
@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class, StepScopeTestExecutionListener.class,
		DbUnitTestExecutionListener.class })
@AutoConfigureTestDatabase(replace = Replace.NONE)
@Import({ DBTestConfig.class, NwisDBTestConfig.class })
@DbUnitConfiguration(databaseConnection = { "nwis" })
@DirtiesContext
public class NwisStationLocalUpsertIT extends NwisBaseFlowIT {
	
	@Autowired
	@Qualifier("upsertNwisStationLocalFlow")
	private Flow upsertNwisStationLocalFlow;
	
	@Before
	public void setup() {
		testJob = jobBuilderFactory
				.get("upsertNwisStationLocalFlowTest")
				.start(upsertNwisStationLocalFlow)
				.build().build();
		jobLauncherTestUtils.setJob(testJob);
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

}
