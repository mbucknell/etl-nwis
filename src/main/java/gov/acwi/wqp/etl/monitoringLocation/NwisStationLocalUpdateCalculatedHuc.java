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

@Configuration
public class NwisStationLocalUpdateCalculatedHuc {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    @Qualifier("taskNwisStationLocalUpdateCalculatedHuc")
    private Tasklet taskNwisStationLocalUpdateCalculatedHuc;

    @Bean
    public Step updateNwisStationLocalCalculatedHucStep() {
        return stepBuilderFactory
                .get("updateNwisStationLocalCalculatedHucStep")
                .tasklet(taskNwisStationLocalUpdateCalculatedHuc)
                .build();
    }

    @Bean
    public Flow updateNwisStationLocalCalculatedHucFlow() {
        return new FlowBuilder<SimpleFlow>("updatedNwisStationLocalCalculatedHucFlow")
                .start(updateNwisStationLocalCalculatedHucStep())
                .build();
    }
}
