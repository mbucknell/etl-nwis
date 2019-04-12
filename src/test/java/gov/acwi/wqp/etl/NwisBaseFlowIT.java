package gov.acwi.wqp.etl;

import org.junit.Before;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.context.annotation.Import;

@Import({DBTestConfig.class, NwisDBTestConfig.class})
public abstract class NwisBaseFlowIT extends BaseFlowIT{
		
	@Before
	@Override
	public void baseSetup() {
		testJobParameters= new JobParametersBuilder()
				.addJobParameters(jobLauncherTestUtils.getUniqueJobParameters())
				.addString("wqpDataSourceId", "2", true)
				.addString("wqpDataSource", "nwis", true)
				.addString("schemaName", "wqp", false)
				.toJobParameters();
	}
}
