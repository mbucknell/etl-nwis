package gov.acwi.wqp.etl.nwis.monitoringLocation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.NwisBaseFlowIT;

public class PurgeNwisMonitoringLocationIT extends NwisBaseFlowIT {

	@Test
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/sitefile/csv/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/monitoringLocation/purge/")
	@ExpectedDatabase(
			connection=CONNECTION_NWIS,
			value="classpath:/testResult/nwis/monitoringLocation/merges/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	public void transformTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("purgeNwisMonitoringLocationStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
