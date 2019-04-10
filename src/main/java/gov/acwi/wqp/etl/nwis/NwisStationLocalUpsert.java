package gov.acwi.wqp.etl.nwis;

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
public class NwisStationLocalUpsert {
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	@Qualifier("taskNwisStationLocalUpsert")
	private Tasklet taskNwisStationLocalUpsert;
	
	@Bean
	public Step upsertNwisStationLocalStep() {
		return stepBuilderFactory
				.get("upsertNwisStationLocalStep")
				.tasklet(taskNwisStationLocalUpsert)
				.build();
	}
	
	@Bean
	public Flow upsertNwisStationLocalFlow() {
		return new FlowBuilder<SimpleFlow>("upsertNwisStationLocalFlow")
				.start(upsertNwisStationLocalStep())
				.build();
	}


}
