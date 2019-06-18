package gov.acwi.wqp.etl.activity;

import javax.sql.DataSource;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import gov.acwi.wqp.etl.Application;
import gov.acwi.wqp.etl.EtlConstantUtils;

@Configuration
public class TransformActivity {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_ACTIVITY_SWAP_TABLE_FLOW)
	private Flow setupActivitySwapTableFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_ACTIVITY_FLOW)
	private Flow afterLoadActivityFlow;

	@Autowired
	@Qualifier("transformActivityTasklet")
	private TransformActivityTasklet transformActivityTasklet;

	@Bean
	public Step transformActivityStep() throws Exception{
		return stepBuilderFactory
				.get("transformActivityStep")
				.tasklet(transformActivityTasklet)
				.build();
	}

	@Bean
	public Flow activityFlow() throws Exception {
		return new FlowBuilder<SimpleFlow>("activityFlow")
				.start(setupActivitySwapTableFlow)
				.next(transformActivityStep())
				.next(afterLoadActivityFlow)
				.build();
	}

}
