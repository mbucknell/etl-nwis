package gov.acwi.wqp.etl.result;

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

public class TransformResultIT extends NwisBaseFlowIT {
	
	@Autowired
	@Qualifier("resultFlow")
	private Flow resultFlow;
	
	@Test
	@DatabaseSetup(value = "classpath:/testResult/wqp/result/empty.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/qwResult/qwResult.xml")
	@DatabaseSetup(value = "classpath:/testResult/wqp/activity/activity.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/parm/parm.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/fxd/fxd.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/methWithCit/methWithCit.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/parmMeth/parmMeth.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/nwisWqxRptLevCd/nwisWqxRptLevCd.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/qwSample/qwSample.xml")
	@ExpectedDatabase(value = "classpath:/testResult/wqp/result/result.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void transformNwisResultStepTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformResultStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
	
	@Test
	@DatabaseSetup(value = "classpath:/testResult/wqp/result/empty.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/qwResult/qwResult.xml")
	@DatabaseSetup(value = "classpath:/testResult/wqp/activity/activity.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/parm/parm.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/fxd/fxd.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/methWithCit/methWithCit.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/parmMeth/parmMeth.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/nwisWqxRptLevCd/nwisWqxRptLevCd.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/qwSample/qwSample.xml")
	@ExpectedDatabase(value = "classpath:/testResult/wqp/result/result.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	@ExpectedDatabase(value = "classpath:/testResult/wqp/result/indexes/all.xml",
		assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
		table = EXPECTED_DATABASE_TABLE_CHECK_INDEX,
		query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX + "'result_swap_nwis'")
	@ExpectedDatabase(connection = "pg", 
		value = "classpath:/testResult/wqp/result/create.xml", 
		assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
		table = EXPECTED_DATABASE_TABLE_CHECK_TABLE, 
		query = BASE_EXPECTED_DATABASE_QUERY_CHECK_TABLE + "'result_swap_nwis'")
	public void resultFlowTest() {
		Job resultFlowTest = jobBuilderFactory.get("resultFlowTest")
				.start(resultFlow)
				.build()
				.build();
		jobLauncherTestUtils.setJob(resultFlowTest);
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}