package gov.acwi.wqp.etl.mysqlnwis.stat;

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
public class StatTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("deleteStat")
	private Tasklet deleteStat;

	@Autowired
	@Qualifier("statReader")
	private JdbcPagingItemReader<Stat> statReader;

	@Autowired
	@Qualifier("statProcessor")
	private PassThroughItemProcessor<Stat> statProcessor;

	@Autowired
	@Qualifier("statWriter")
	private JdbcBatchItemWriter<Stat> statWriter;

	@Bean
	public Step deleteStatStep() {
		return stepBuilderFactory.get("deleteStat")
				.tasklet(deleteStat)
				.build();
	}

	@Bean
	public Step transformStatStep() {
		return stepBuilderFactory
				.get("transformStatStep")
				.<Stat, Stat> chunk(10000)
				.reader(statReader)
				.processor(statProcessor)
				.writer(statWriter)
				.build();
	}

	@Bean
	public Flow statFlow() {
		return new FlowBuilder<SimpleFlow>("statFlow")
				.start(deleteStatStep())
				.next(transformStatStep())
				.build();
	}

}
