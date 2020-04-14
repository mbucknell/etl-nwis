package gov.acwi.wqp.etl.nwis.discreteGroundWater;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.assertion.DatabaseAssertionMode;

import gov.acwi.wqp.etl.NwisBaseFlowIT;

public class TransformNwisDiscreteGroundWaterIT extends NwisBaseFlowIT {

	@Autowired
	@Qualifier("nwisDiscreteGroundWaterFlow")
	private Flow nwisDiscreteGroundWaterFlow;

	@Autowired
	@Qualifier("jdbcTemplateNwis")
	private JdbcTemplate jdbcTemplateNwis;

	@Test
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/agency/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/discreteGroundWater/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/gwLevelDateTimeAccuracy/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/gwLevelMethod/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testResult/nwis/gwLevels/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/gwLevelSitestatus/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/gwLevelSource/")
	@DatabaseSetup(connection = CONNECTION_NWIS, value = "classpath:/testData/nwis/monitoringLocation/purge/")

	@ExpectedDatabase(
			connection=CONNECTION_NWIS,
			value="classpath:/testResult/nwis/discreteGroundWater/",
			assertionMode=DatabaseAssertionMode.NON_STRICT_UNORDERED
			)
	public void discreteGroundWaterFlowTest() {
		Job nwisDiscreteGroundWaterFlowTest = jobBuilderFactory.get("nwisDiscreteGroundWaterFlowTest")
				.start(nwisDiscreteGroundWaterFlow)
				.build()
				.build();
		jobLauncherTestUtils.setJob(nwisDiscreteGroundWaterFlowTest);
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
