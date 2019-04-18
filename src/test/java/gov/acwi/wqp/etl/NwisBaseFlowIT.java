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
				.addString("wqpDataSourceId", Application.DATA_SOURCE_ID.toString(), true)
				.addString("wqpDataSource", Application.DATA_SOURCE.toLowerCase(), true)
				.addString("schemaName", "wqp", false)
				.toJobParameters();
	}
}
