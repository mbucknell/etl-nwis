package gov.acwi.wqp.etl.nwis.discreteGroundWater;

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
public class TransformNwisDiscreteGroundWaterFlowDefinition {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;


	@Autowired
	@Qualifier("deleteDiscreteGroundWaterTasklet")
	private Tasklet deleteDiscreteGroundWaterTasklet;

	@Autowired
	@Qualifier("transformNwisDiscreteGroundWater")
	private Tasklet transformNwisDiscreteGroundWater;

	@Bean
	public Step deleteDiscreteGroundWaterStep() {
		return stepBuilderFactory.get("deleteDiscreteGroundWaterStep")
				.tasklet(deleteDiscreteGroundWaterTasklet)
				.build();
	}

	@Bean
	public Step transformNwisDiscreteGroundWaterStep() {
		return stepBuilderFactory.get("transformNwisDiscreteGroundWaterStep")
				.tasklet(transformNwisDiscreteGroundWater)
				.build();
	}

	@Bean
	public Flow nwisDiscreteGroundWaterFlow() {
		return new FlowBuilder<SimpleFlow>("nwisDiscreteGroundWaterFlow")
				.start(deleteDiscreteGroundWaterStep())
				.next(transformNwisDiscreteGroundWaterStep())
				.build();
	}
}
