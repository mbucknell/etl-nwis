package gov.acwi.wqp.etl.orgData;

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

public class TransformOrgDataIT extends NwisBaseFlowIT {

	public static final String TABLE_NAME = "'org_data_swap_nwis'";
	public static final String EXPECTED_DATABASE_QUERY_ANALYZE = BASE_EXPECTED_DATABASE_QUERY_ANALYZE + TABLE_NAME;

	@Autowired
	@Qualifier("orgDataFlow")
	private Flow orgDataFlow;

	@Test
	@DatabaseSetup(value = "classpath:/testResult/wqp/orgData/empty.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/nwisDistrictCdsByHost/nwisDistrictCdsByHost.xml")
	@ExpectedDatabase(
			value = "classpath:/testResult/wqp/orgData/orgData.xml",
			assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void transformNwisOrgDataStepTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchStep("transformOrgDataStep", testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}

	@Test
	@DatabaseSetup(value = "classpath:/testData/wqp/orgData/orgDataOld.xml")
	@DatabaseSetup(
			connection = CONNECTION_NWIS,
			value = "classpath:/testData/nwis/nwisDistrictCdsByHost/nwisDistrictCdsByHost.xml")
	@ExpectedDatabase(value = "classpath:/testResult/wqp/orgData/indexes/all.xml",
		assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
		table = EXPECTED_DATABASE_TABLE_CHECK_INDEX,
		query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX + TABLE_NAME)
	@ExpectedDatabase(connection = "pg", 
		value = "classpath:/testResult/wqp/orgData/create.xml", 
		assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
		table = EXPECTED_DATABASE_TABLE_CHECK_TABLE, 
		query = BASE_EXPECTED_DATABASE_QUERY_CHECK_TABLE + TABLE_NAME)
	@ExpectedDatabase(
			value = "classpath:/testResult/wqp/orgData/orgData.xml",
			assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/analyze/orgData.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_ANALYZE,
			query=EXPECTED_DATABASE_QUERY_ANALYZE)
	@ExpectedDatabase(
			value="classpath:/testResult/wqp/orgData/indexes/pk.xml",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED,
			table=EXPECTED_DATABASE_TABLE_CHECK_INDEX,
			query=BASE_EXPECTED_DATABASE_QUERY_CHECK_INDEX_PK + TABLE_NAME)
	public void orgDataFlowTest() {
		Job orgDataFlowTest = jobBuilderFactory.get("orgDataFlowTest")
				.start(orgDataFlow)
				.build()
				.build();
		jobLauncherTestUtils.setJob(orgDataFlowTest);
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
