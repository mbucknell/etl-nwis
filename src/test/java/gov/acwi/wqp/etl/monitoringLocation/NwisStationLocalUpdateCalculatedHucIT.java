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

public class NwisStationLocalUpdateCalculatedHucIT extends NwisBaseFlowIT {

    @Test
    @DatabaseSetup(connection = "nwis", value="classpath:/testResult/nwis/nwisStationLocal/nwisStationLocal.xml")
    @ExpectedDatabase(
            connection = CONNECTION_NWIS,
            value = "classpath:/testResult/nwis/nwisStationLocal/calculatedHuc/nwisStationLocal.xml")
    public void updateNwisStationLocalCalculatedHucStepTest() {
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchStep("updateNwisStationLocalCalculatedHucStep", testJobParameters);
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getLocalizedMessage());
        }
    }
}
