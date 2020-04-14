package gov.acwi.wqp.etl.mysqlnwis.gwlevels;

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

public class TransformGwLevelsIT extends NwisBaseFlowIT {

	@Autowired
	@Qualifier("gwLevelsFlow")
	private Flow gwLevelsFlow;

	private Job setupFlowTestJob() {
		return jobBuilderFactory.get("gwLevelsFlowTest").start(gwLevelsFlow).build().build();
	}

	@Test
	@DatabaseSetup(
			connection=CONNECTION_MYSQLNWIS,
			value="classpath:/testData/mysqlnwis/gwLevels/"
			)
	@DatabaseSetup(
			connection=CONNECTION_NWIS,
			value="classpath:/testData/nwis/gwLevels/"
			)
	@ExpectedDatabase(
			connection=CONNECTION_NWIS,
			value="classpath:/testResult/nwis/gwLevels/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	public void gwLevelsFlowTest() {
		try {
			jobLauncherTestUtils.setJob(setupFlowTestJob());
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

}
