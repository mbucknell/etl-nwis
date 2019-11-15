package gov.acwi.wqp.etl.mysqlnwis.sitefile;

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

public class TransformSitefileIT extends NwisBaseFlowIT {

	@Autowired
	@Qualifier("sitefileFlow")
	private Flow sitefileFlow;

	private Job setupFlowTestJob() {
		return jobBuilderFactory.get("sitefileFlowTest").start(sitefileFlow).build().build();
	}

	@Test
	@DatabaseSetup(
			connection=CONNECTION_MYSQLNWIS,
			value="classpath:/testData/mysqlnwis/sitefile/"
			)
	@ExpectedDatabase(
			connection=CONNECTION_NWIS,
			value="classpath:/testData/nwis/sitefile/csv/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	public void sitefileFlowTest() {
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
