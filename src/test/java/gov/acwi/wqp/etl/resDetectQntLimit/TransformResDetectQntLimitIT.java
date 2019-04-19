package gov.acwi.wqp.etl.resDetectQntLimit;

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

public class TransformResDetectQntLimitIT extends NwisBaseFlowIT {

	@Autowired
	@Qualifier("resDetectQntLimitFlow")
	private Flow resDetectQntLimitFlow;
	
	@Test
	@DatabaseSetup(value="classpath:/testResult/wqp/resDetectQntLimit/empty.xml")
	@DatabaseSetup(value="classpath:/testData/wqp/result/result.xml")
	@ExpectedDatabase(value="classpath:/testResult/wqp/resDetectQntLimit/resDetectQntLimit.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
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
	@ExpectedDatabase(value="classpath:/testResult/wqp/resDetectQntLimit/resDetectQntLimit.xml", assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED)
	@ExpectedDatabase(value = "classpath:/testResult/wqp/resDetectQntLimit/indexes/all.xml",
		assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
		table = EXPECTED_DATABASE_TABLE_CHECK_INDEX,
		query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX + "'r_detect_qnt_lmt_swap_nwis'")
	@ExpectedDatabase(connection = "pg", 
		value = "classpath:/testResult/wqp/resDetectQntLimit/create.xml", 
		assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
		table = EXPECTED_DATABASE_TABLE_CHECK_TABLE, 
		query = BASE_EXPECTED_DATABASE_QUERY_CHECK_TABLE + "'r_detect_qnt_lmt_swap_nwis'")
	public void resDetectQntLimitFlowTest() {
		Job resDetectQntLimitFlowTest = jobBuilderFactory.get("resDetectQntLimitFlowTest")
					.start(resDetectQntLimitFlow)
					.build()
					.build();
		jobLauncherTestUtils.setJob(resDetectQntLimitFlowTest);
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
