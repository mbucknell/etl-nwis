package gov.acwi.wqp.etl.nwis.sampleParameter;

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

public class TransformSampleParameterIT extends NwisBaseFlowIT {

    @Autowired
    @Qualifier("sampleParameterFlow")
    private Flow sampleParameterFlow;

    @Test
    @DatabaseSetup(connection = "nwis", value = "classpath:/testResult/nwis/sampleParameter/empty.xml")
    @DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/sampleParameter/sampleParameterOld.xml")
    @DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/qwResult/qwResultForSampleParameterTest.xml")
    @DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/fxd/sampleParameterTest/")
    @ExpectedDatabase(connection = "nwis", value = "classpath:/testResult/nwis/sampleParameter/sampleParameter.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
    public void sampleParameterFlowTest() {
        Job sampleParameterFlowTest = jobBuilderFactory.get("sampleParameterFlowTest")
                .start(sampleParameterFlow)
                .build()
                .build();
        jobLauncherTestUtils.setJob(sampleParameterFlowTest);
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getLocalizedMessage());
        }
    }
}
