package gov.acwi.wqp.etl.nwqDataChecks.nawqaSites;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NawqaSitesTransformation {

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    @Qualifier("deleteNawqaSites")
    private Tasklet deleteNawqaSites;

    @Autowired
    @Qualifier("nawqaSitesReader")
    private JdbcCursorItemReader<NawqaSites> nawqaSitesReader;

    @Autowired
    @Qualifier("nawqaSitesProcessor")
    private PassThroughItemProcessor<NawqaSites> nawqaSitesProcessor;

    @Autowired
    @Qualifier("nawqaSitesWriter")
    private JdbcBatchItemWriter<NawqaSites> nawqaSitesWriter;

    @Bean
    public Step deleteNawqaSitesStep() {
        return stepBuilderFactory.get("deleteNawqaSites")
                .tasklet(deleteNawqaSites)
                .build();
    }

    @Bean
    public Step transformNawqaSitesStep() {
        return stepBuilderFactory
                .get("transformNawqaSitesStep")
                .<NawqaSites, NawqaSites> chunk(100)
                .reader(nawqaSitesReader)
                .processor(nawqaSitesProcessor)
                .writer(nawqaSitesWriter)
                .build();
    }

    @Bean
    public Flow nawqaSitesFlow() {
        return new FlowBuilder<SimpleFlow>("nawqaSitesFlow")
                .start(deleteNawqaSitesStep())
                .next(transformNawqaSitesStep())
                .build();
    }
}
