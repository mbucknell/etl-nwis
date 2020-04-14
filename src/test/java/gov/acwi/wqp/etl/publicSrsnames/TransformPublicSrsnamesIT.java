package gov.acwi.wqp.etl.publicSrsnames;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
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

public class TransformPublicSrsnamesIT extends NwisBaseFlowIT {

	@Autowired
	@Qualifier("publicSrsnamesFlow")
	private Flow publicSrsnamesFlow;

	@Test
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/parm/parm.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/parmAlias/parmAlias.xml")
	@ExpectedDatabase(
			value = "classpath:/testResult/wqp/publicSrsnames/publicSrsnames.xml",
			assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void transformPublicSrsnamesStepTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformPublicSrsnamesStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

	@Test
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/parm/parm.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/parmAlias/parmAlias.xml")
	@ExpectedDatabase(
			value = "classpath:/testResult/wqp/publicSrsnames/publicSrsnames.xml",
			assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void publicSrsnamesFlowTest() {
		Job publicSrsnamesFlowTest = jobBuilderFactory.get("nwisStationLocalFlowTest")
				.start(publicSrsnamesFlow)
				.build()
				.build();
		jobLauncherTestUtils.setJob(publicSrsnamesFlowTest);
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
