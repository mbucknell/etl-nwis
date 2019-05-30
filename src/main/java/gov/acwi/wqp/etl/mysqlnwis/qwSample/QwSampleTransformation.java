package gov.acwi.wqp.etl.mysqlnwis.qwSample;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QwSampleTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	@Qualifier("deleteQwSample")
	private Tasklet deleteQwSample;
	
	@Autowired
	@Qualifier("qwSampleReader")
	private JdbcPagingItemReader<QwSample> qwSampleReader;
	
	@Autowired
	@Qualifier("qwSampleProcessor")
	private PassThroughItemProcessor<QwSample> qwSampleProcessor;
	
	@Autowired
	@Qualifier("qwSampleWriter")
	private JdbcBatchItemWriter<QwSample> qwSampleWriter;
	
	@Bean
	public Step deleteQwSampleStep() {
		return stepBuilderFactory.get("deleteQwSample")
				.tasklet(deleteQwSample)
				.build();
	}
	
	@Bean
	public Step transformQwSampleStep() {
		return stepBuilderFactory
				.get("transformQwSampleStep")
				.<QwSample, QwSample> chunk(10000)
				.reader(qwSampleReader)
				.processor(qwSampleProcessor)
				.writer(qwSampleWriter)
				.build();
	}
	
	@Bean
	public Flow qwSampleFlow() {
		return new FlowBuilder<SimpleFlow>("qwSampleFlow")
				.start(deleteQwSampleStep())
				.next(transformQwSampleStep())
				.build();
	}

}
