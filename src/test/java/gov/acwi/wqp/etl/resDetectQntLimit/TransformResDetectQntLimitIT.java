package gov.acwi.wqp.etl.resDetectQntLimit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
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

public class TransformResDetectQntLimitIT extends NwisBaseFlowIT {

	public static final String TABLE_NAME = "'r_detect_qnt_lmt_swap_nwis'";
	public static final String EXPECTED_DATABASE_QUERY_ANALYZE = BASE_EXPECTED_DATABASE_QUERY_ANALYZE + TABLE_NAME;

	@Autowired
	@Qualifier("resDetectQntLimitFlow")
	private Flow resDetectQntLimitFlow;
	
	@Test
	@DatabaseSetup(value="classpath:/testResult/wqp/resDetectQntLimit/empty.xml")
	@DatabaseSetup(value="classpath:/testData/wqp/result/result.xml")
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/resDetectQntLimit/resDetectQntLimit.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void transformResDetectQntLimitStepTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformResDetectQntLimitStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
	
	@Test
	@DatabaseSetup(value="classpath:/testResult/wqp/resDetectQntLimit/empty.xml")
	@DatabaseSetup(value="classpath:/testData/wqp/result/result.xml")
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/resDetectQntLimit/resDetectQntLimit.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	@ExpectedDatabase(
			value = "classpath:/testResult/wqp/resDetectQntLimit/indexes/all.xml",
			assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table = EXPECTED_DATABASE_TABLE_CHECK_INDEX,
			query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX + TABLE_NAME)
	@ExpectedDatabase(connection = "pg", 
		value = "classpath:/testResult/wqp/resDetectQntLimit/create.xml", 
		assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
		table = EXPECTED_DATABASE_TABLE_CHECK_TABLE, 
		query = BASE_EXPECTED_DATABASE_QUERY_CHECK_TABLE + TABLE_NAME)
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/analyze/resDetectQntLimit.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_ANALYZE,
			query=EXPECTED_DATABASE_QUERY_ANALYZE)
	public void resDetectQntLimitFlowTest() {
		Job resDetectQntLimitFlowTest = jobBuilderFactory.get("resDetectQntLimitFlowTest")
					.start(resDetectQntLimitFlow)
					.build()
					.build();
		jobLauncherTestUtils.setJob(resDetectQntLimitFlowTest);
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
