package gov.acwi.wqp.etl.biologicalHabitatMetric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
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

    public static final String TABLE_NAME = "'bio_hab_metric_swap_nwis'";
    public static final String EXPECTED_DATABASE_QUERY_ANALYZE = BASE_EXPECTED_DATABASE_QUERY_ANALYZE + TABLE_NAME;

    @Autowired
    @Qualifier("biologicalHabitatMetricFlow")
    private Flow biologicalHabitatMetricFlow;

    @Test
    @ExpectedDatabase(
            value="classpath:/testResult/wqp/biologicalHabitatMetric/indexes/all.xml",
            assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
            table=EXPECTED_DATABASE_TABLE_CHECK_INDEX,
            query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX + TABLE_NAME)
    @ExpectedDatabase(
            connection=CONNECTION_INFORMATION_SCHEMA, value="classpath:/testResult/wqp/biologicalHabitatMetric/create.xml",
            assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
            table=EXPECTED_DATABASE_TABLE_CHECK_TABLE,
            query=BASE_EXPECTED_DATABASE_QUERY_CHECK_TABLE + TABLE_NAME)
    @ExpectedDatabase(
            value="classpath:/testResult/wqp/biologicalHabitatMetric/biologicalHabitatMetric.xml",
            assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
    @ExpectedDatabase(
            value="classpath:/testResult/wqp/analyze/biologicalHabitatMetric.xml",
            assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
            table=EXPECTED_DATABASE_TABLE_CHECK_ANALYZE,
            query=EXPECTED_DATABASE_QUERY_ANALYZE)
    public void biologicalHabitatMetricFlowTest() {
        Job biologicalHabitatMetricFlowTest = jobBuilderFactory.get("biologicalHabitatMetricFlowTest")
                .start(biologicalHabitatMetricFlow)
                .build()
                .build();
        jobLauncherTestUtils.setJob(biologicalHabitatMetricFlowTest);
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

