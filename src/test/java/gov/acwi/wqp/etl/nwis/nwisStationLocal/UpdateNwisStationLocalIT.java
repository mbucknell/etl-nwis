package gov.acwi.wqp.etl.nwis.nwisStationLocal;

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

public class UpdateNwisStationLocalIT extends NwisBaseFlowIT {

    @Autowired
    @Qualifier("nwisStationLocalFlow")
    private Flow nwisStationLocalFlow;

    @Test
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testResult/nwis/nwisStationLocal/empty.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/sitefile/sitefile.xml")
    @ExpectedDatabase(
            connection = CONNECTION_NWIS,
            value="classpath:/testResult/nwis/nwisStationLocal/nwisStationLocal.xml",
            assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
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
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testResult/nwis/nwisStationLocal/empty.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/nwisStationLocal/updateTest/csv/")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/sitefile/updateTest/csv/")
    @ExpectedDatabase(
            connection = CONNECTION_NWIS,
            value="classpath:/testResult/nwis/nwisStationLocal/updateTest/csv/",
            assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    public void updateNwisStationLocalStepTest() {
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchStep("upsertNwisStationLocalStep", testJobParameters);
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getLocalizedMessage());
        }
    }

    @Test
    @DatabaseSetup(connection = CONNECTION_NWIS, value="classpath:/testResult/nwis/nwisStationLocal/nwisStationLocal.xml")
    @DatabaseSetup(value="classpath:/testData/wqp/huc12nometa/")
    @ExpectedDatabase(
            connection = CONNECTION_NWIS,
            value = "classpath:/testResult/nwis/nwisStationLocal/calculatedHuc/nwisStationLocal.xml",
            assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    public void updateNwisStationLocalCalculatedHucStepTest() {
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchStep("updateNwisStationLocalCalculatedHucStep", testJobParameters);
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getLocalizedMessage());
        }
    }

    @Test
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testResult/nwis/nwisStationLocal/empty.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/sitefile/sitefile.xml")
    @DatabaseSetup(connection = CONNECTION_NWIS, value="classpath:/testResult/nwis/nwisStationLocal/nwisStationLocal.xml")
    @DatabaseSetup(value="classpath:/testData/wqp/huc12nometa/")
    @ExpectedDatabase(
            connection = CONNECTION_NWIS,
            value = "classpath:/testResult/nwis/nwisStationLocal/calculatedHuc/nwisStationLocal.xml",
            assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    public void nwisStationLocalFlowTest() {
        Job nwisStationLocalFlowTest = jobBuilderFactory.get("nwisStationLocalFlowTest")
                .start(nwisStationLocalFlow)
                .build()
                .build();
        jobLauncherTestUtils.setJob(nwisStationLocalFlowTest);
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
