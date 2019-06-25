package gov.acwi.wqp.etl.publicSrsnames;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TransformPublicSrsnames {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("transformPublicSrsnamesTask")
	private Tasklet transformPublicSrsnamesTask;

	@Bean
	public Step transformPublicSrsnamesStep() {
		return stepBuilderFactory
				.get("transformPublicSrsnamesStep")
				.tasklet(transformPublicSrsnamesTask)
				.build();
	}

	@Bean
	public Flow publicSrsnamesFlow() {
		return new FlowBuilder<SimpleFlow>("publicSrsnamesFlow")
				.start(transformPublicSrsnamesStep())
				.build();
	}
}
