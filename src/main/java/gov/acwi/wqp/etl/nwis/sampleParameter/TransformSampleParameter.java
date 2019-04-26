package gov.acwi.wqp.etl.nwis.sampleParameter;

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
public class TransformSampleParameter {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    @Qualifier("transformSampleParameterTasklet")
    private Tasklet transformSampleParameterTasklet;

    @Bean
    public Step transformSampleParameterStep() {
        return stepBuilderFactory
                .get("transformSampleParameter")
                .tasklet(transformSampleParameterTasklet)
                .build();
    }

    @Bean
    public Flow sampleParameterFlow() {
        return new FlowBuilder<SimpleFlow>("sampleParameterFlow")
                .start(transformSampleParameterStep())
                .build();
    }
}
