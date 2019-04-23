package gov.acwi.wqp.etl.monitoringLocation;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;
import gov.acwi.wqp.etl.NwisBaseFlowIT;
import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class NwisStationLocalUpsertIT extends NwisBaseFlowIT {
	
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
