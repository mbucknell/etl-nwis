package gov.acwi.wqp.etl.projectMLWeighting;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.NwisBaseFlowIT;

public class TransformProjectMLWeightingIT extends NwisBaseFlowIT {

    @Autowired
    @Qualifier("projectMLWeightingFlow")
    private Flow projectMLWeightingFlow;

    @Test
    @ExpectedDatabase(value="classpath:/testResult/wqp/projectMLWeighting/indexes/all.xml",
            assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
            table=EXPECTED_DATABASE_TABLE_CHECK_INDEX,
            query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX + "'prj_ml_weighting_swap_nwis'")
    @ExpectedDatabase(connection=CONNECTION_INFORMATION_SCHEMA, value="classpath:/testResult/wqp/projectMLWeighting/create.xml",
            assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
            table=EXPECTED_DATABASE_TABLE_CHECK_TABLE,
            query=BASE_EXPECTED_DATABASE_QUERY_CHECK_TABLE + "'prj_ml_weighting_swap_nwis'")
    @ExpectedDatabase(value="classpath:/testResult/wqp/projectMLWeighting/projectMLWeighting.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
    public void projectMLWeightingFlowTest() {
        Job projectMLWeightingFlowTest = jobBuilderFactory.get("projectMLWeightingFlowTest")
                .start(projectMLWeightingFlow)
                .build()
                .build();
        jobLauncherTestUtils.setJob(projectMLWeightingFlowTest);
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getLocalizedMessage());
        }
    }

}