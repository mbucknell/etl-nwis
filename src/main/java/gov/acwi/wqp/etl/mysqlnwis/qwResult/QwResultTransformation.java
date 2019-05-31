package gov.acwi.wqp.etl.mysqlnwis.qwResult;

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
public class QwResultTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	@Qualifier("truncateQwResult")
	private Tasklet truncateQwResult;

	@Autowired
	@Qualifier("buildQwResultResultIdIndex")
	private Tasklet buildQwResultResultIdIndex;

	@Autowired
	@Qualifier("dropQwResultResultIdIndex")
	private Tasklet dropQwResultResultIdIndex;

	@Autowired
	@Qualifier("qwResultReader")
	private JdbcPagingItemReader<QwResult> qwResultReader;
	
	@Autowired
	@Qualifier("qwResultProcessor")
	private PassThroughItemProcessor<QwResult> qwResultProcessor;
	
	@Autowired
	@Qualifier("qwResultWriter")
	private JdbcBatchItemWriter<QwResult> qwResultWriter;
	
	@Bean
	public Step truncateQwResultStep() {
		return stepBuilderFactory.get("truncateQwResult")
				.tasklet(truncateQwResult)
				.build();
	}
	
	@Bean
	public Step transformQwResultStep() {
		return stepBuilderFactory
				.get("transformQwResultStep")
				.<QwResult, QwResult> chunk(100000)
				.reader(qwResultReader)
				.processor(qwResultProcessor)
				.writer(qwResultWriter)
				.build();
	}

	@Bean
	public Step dropQwResultResultIdIndexStep() {
		return stepBuilderFactory.get("dropQwResultResultIdIndexStep")
				.tasklet(dropQwResultResultIdIndex)
				.build();
	}

	@Bean
	public Step buildQwResultResultIdIndexStep() {
		return stepBuilderFactory.get("buildQwResultResultIdIndexStep")
				.tasklet(buildQwResultResultIdIndex)
				.build();
	}

	@Bean
	public Flow qwResultFlow() {
		return new FlowBuilder<SimpleFlow>("qwResultFlow")
				.start(truncateQwResultStep())
				.next(dropQwResultResultIdIndexStep())
				.next(transformQwResultStep())
				.next(buildQwResultResultIdIndexStep())
				.build();
	}
}
