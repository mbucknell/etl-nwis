package gov.acwi.wqp.etl.orgData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.test.StepScopeTestExecutionListener;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import com.github.springtestdbunit.DbUnitTestExecutionListener;
import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.DBTestConfig;
import gov.acwi.wqp.etl.NwisBaseFlowIT;
import gov.acwi.wqp.etl.NwisDBTestConfig;
import gov.acwi.wqp.etl.orgData.index.BuildOrgDataIndexesFlowIT;
import gov.acwi.wqp.etl.orgData.table.SetupOrgDataSwapTableFlowIT;

@SpringBatchTest
@SpringBootTest
@RunWith(SpringRunner.class)
@TestExecutionListeners({ DependencyInjectionTestExecutionListener.class, StepScopeTestExecutionListener.class,
		DbUnitTestExecutionListener.class })
@AutoConfigureTestDatabase(replace = Replace.NONE)
@Import({ DBTestConfig.class, NwisDBTestConfig.class })
@DbUnitConfiguration(databaseConnection = { "wqp", "pg", "nwis" })
@DirtiesContext
public class TransformOrgDataIT extends NwisBaseFlowIT {

	@Autowired
	@Qualifier("orgDataFlow")
	private Flow orgDataFlow;

	@Before
	public void setup() {
		testJob = jobBuilderFactory
				.get("orgDataFlowTest")
				.start(orgDataFlow)
				.build().build();
		jobLauncherTestUtils.setJob(testJob);
	}

	@Test
	@DatabaseSetup(value = "classpath:/testResult/wqp/orgData/empty.xml")
	@DatabaseSetup(connection = "nwis", value = "classpath:/testData/nwis/nwisDistrictCdsByHost/nwisDistrictCdsByHost.xml")
	@ExpectedDatabase(value = "classpath:/testResult/wqp/orgData/orgData.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
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
	@DatabaseSetup(value = "classpath:/testResult/wqp/orgData/empty.xml")
	@DatabaseSetup(value = "classpath:/testData/wqp/orgData/orgDataOld.xml")
	@DatabaseSetup(connection = "nwis", 
		value = "classpath:/testData/nwis/nwisDistrictCdsByHost/nwisDistrictCdsByHost.xml")
	//@ExpectedDatabase(value = "classpath:/testResult/wqp/orgData/indexes/all.xml",
	//	assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
	//	table = BuildOrgDataIndexesFlowIT.EXPECTED_DATABASE_TABLE,
	//	query = BuildOrgDataIndexesFlowIT.EXPECTED_DATABASE_QUERY)
	//@ExpectedDatabase(connection = "pg", 
	//	value = "classpath:/testResult/wqp/orgData/create.xml", 
	//	assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED, 
	//	table = SetupOrgDataSwapTableFlowIT.EXPECTED_DATABASE_TABLE, 
	//	query = SetupOrgDataSwapTableFlowIT.EXPECTED_DATABASE_QUERY)
	@ExpectedDatabase(value = "classpath:/testResult/wqp/orgData/orgData.xml", assertionMode = DatabaseAssertionMode.NON_STRICT_UNORDERED)
	public void orgDataFlowTest() {
		try {
			JobExecution jobExecution = jobLauncherTestUtils.launchJob(testJobParameters);
			assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getLocalizedMessage());
		}
	}
}
