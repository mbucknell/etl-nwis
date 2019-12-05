package gov.acwi.wqp.etl.nwis.monitoringLocation;

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
public class TransformNwisMonitoringLocationFlowDefinition {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;


	@Autowired
	@Qualifier("purgeNwisMonitoringLocation")
	private Tasklet purgeNwisMonitoringLocation;

	@Autowired
	@Qualifier("transformNwisMonitoringLocation")
	private Tasklet transformNwisMonitoringLocation;

	@Bean
	public Step purgeNwisMonitoringLocationStep() {
		return stepBuilderFactory.get("purgeNwisMonitoringLocationStep")
				.tasklet(purgeNwisMonitoringLocation)
				.build();
	}

	@Bean
	public Step transformNwisMonitoringLocationStep() {
		return stepBuilderFactory.get("transformNwisMonitoringLocationStep")
				.tasklet(transformNwisMonitoringLocation)
				.build();
	}

	@Bean
	public Flow nwisMonitoringLocationFlow() {
		return new FlowBuilder<SimpleFlow>("nwisMonitoringLocationFlow")
				.start(purgeNwisMonitoringLocationStep())
				.next(transformNwisMonitoringLocationStep())
				.build();
	}
}
