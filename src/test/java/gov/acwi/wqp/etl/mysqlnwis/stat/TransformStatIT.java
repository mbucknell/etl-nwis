package gov.acwi.wqp.etl.mysqlnwis.stat;

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

public class TransformStatIT extends NwisBaseFlowIT {

	@Autowired
	@Qualifier("statFlow")
	private Flow statFlow;

	@Test
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testResult/nwis/stat/empty.xml")
	@DatabaseSetup(connection = CONNECTION_MYSQLNWIS, value = "classpath:/testData/mysqlnwis/stat/")
	@ExpectedDatabase(connection = CONNECTION_NWIS, value = "classpath:/testResult/nwis/stat/", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void statFlowTest() {
		Job statFlowTest = jobBuilderFactory.get("statFlowTest")
				.start(statFlow)
				.build()
				.build();
		jobLauncherTestUtils.setJob(statFlowTest);
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
