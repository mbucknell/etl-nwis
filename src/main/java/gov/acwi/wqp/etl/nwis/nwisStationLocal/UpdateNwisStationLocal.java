package gov.acwi.wqp.etl.nwis.nwisStationLocal;

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
public class UpdateNwisStationLocal {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("upsertNwisStationLocal")
	private Tasklet upsertNwisStationLocal;

	@Autowired
	@Qualifier("updateNwisStationLocalCalculatedHuc")
	private Tasklet updateNwisStationLocalCalculatedHuc;

	@Bean
	public Step upsertNwisStationLocalStep() {
		return stepBuilderFactory
				.get("upsertNwisStationLocalStep")
				.tasklet(upsertNwisStationLocal)
				.build();
	}

	@Bean
	public Step updateNwisStationLocalCalculatedHucStep() {
		return stepBuilderFactory
				.get("updateNwisStationLocalCalculatedHucStep")
				.tasklet(updateNwisStationLocalCalculatedHuc)
				.build();
	}

	@Bean
	public Flow nwisStationLocalFlow() {
		return new FlowBuilder<SimpleFlow>("nwisStationLocalFlow")
				.start(upsertNwisStationLocalStep())
				.next(updateNwisStationLocalCalculatedHucStep())
				.build();
	}
}
