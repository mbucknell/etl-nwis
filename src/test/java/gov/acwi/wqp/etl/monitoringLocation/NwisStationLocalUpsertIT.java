package gov.acwi.wqp.etl.monitoringLocation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.NwisBaseFlowIT;


public class NwisStationLocalUpsertIT extends NwisBaseFlowIT {
	
	@Test
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testResult/nwis/nwisStationLocal/empty.xml")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/sitefile/sitefile.xml")
	@ExpectedDatabase(connection = CONNECTION_NWIS, value="classpath:/testResult/nwis/nwisStationLocal/nwisStationLocal.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
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
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/nwisStationLocal/updateTest/csv/")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/sitefile/updateTest/csv/")
	@ExpectedDatabase(connection = "nwis", value="classpath:/testResult/nwis/nwisStationLocal/updateTest/csv/", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
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
