package gov.acwi.wqp.etl.monitoringLocation;

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

import gov.acwi.wqp.etl.EtlConstantUtils;

@Configuration
public class TransformMonitoringLocationFlowDefinition {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_MONITORING_LOCATION_SWAP_TABLE_FLOW)
	private Flow setupMonitoringLocationSwapTableFlow;


	@Autowired
	@Qualifier("transformMonitoringLocation")
	private Tasklet transformMonitoringLocation;

	@Autowired
	@Qualifier(EtlConstantUtils.AFTER_LOAD_MONITORING_LOCATION_FLOW)
	private Flow afterLoadMonitoringLocationFlow;


	@Bean
	public Step transformMonitoringLocationStep() {
		return stepBuilderFactory.get("transformMonitoringLocationStep")
				.tasklet(transformMonitoringLocation)
				.build();
	}

	@Bean
	public Flow monitoringLocationFlow() throws Exception {
		return new FlowBuilder<SimpleFlow>("monitoringLocationFlow")
				.start(setupMonitoringLocationSwapTableFlow)
				.next(transformMonitoringLocationStep())
				.next(afterLoadMonitoringLocationFlow)
				.build();
	}

}
