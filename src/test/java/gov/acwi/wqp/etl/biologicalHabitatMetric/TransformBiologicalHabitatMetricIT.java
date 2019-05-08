package gov.acwi.wqp.etl.biologicalHabitatMetric;

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

public class TransformBiologicalHabitatMetricIT extends NwisBaseFlowIT {

    @Autowired
    @Qualifier("biologicalHabitatMetricFlow")
    private Flow biologicalHabitatMetricFlow;

    @Test
    @ExpectedDatabase(value="classpath:/testResult/wqp/biologicalHabitatMetric/indexes/all.xml",
            assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
            table=EXPECTED_DATABASE_TABLE_CHECK_INDEX,
            query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX + "'bio_hab_metric_swap_nwis'")
    @ExpectedDatabase(connection=CONNECTION_INFORMATION_SCHEMA, value="classpath:/testResult/wqp/biologicalHabitatMetric/create.xml",
            assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
            table=EXPECTED_DATABASE_TABLE_CHECK_TABLE,
            query=BASE_EXPECTED_DATABASE_QUERY_CHECK_TABLE + "'bio_hab_metric_swap_nwis'")
    @ExpectedDatabase(value="classpath:/testResult/wqp/biologicalHabitatMetric/biologicalHabitatMetric.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
    public void biologicalHabitatMetricFlowTest() {
        Job biologicalHabitatMetricFlowTest = jobBuilderFactory.get("biologicalHabitatMetricFlowTest")
                .start(biologicalHabitatMetricFlow)
                .build()
                .build();
        jobLauncherTestUtils.setJob(biologicalHabitatMetricFlowTest);
        try {
            JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
            assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getLocalizedMessage());
        }
    }

}

