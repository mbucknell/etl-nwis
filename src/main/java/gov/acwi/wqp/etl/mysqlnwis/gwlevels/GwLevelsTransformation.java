package gov.acwi.wqp.etl.mysqlnwis.gwlevels;

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
public class GwLevelsTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	@Qualifier("deleteGwLevels")
	private Tasklet deleteGwLevels;

	@Autowired
	@Qualifier("gwLevelsReader")
	private JdbcPagingItemReader<GwLevels> gwLevelsReader;
	
	@Autowired
	@Qualifier("gwLevelsProcessor")
	private PassThroughItemProcessor<GwLevels> gwLevelsProcessor;
	
	@Autowired
	@Qualifier("gwLevelsWriter")
	private JdbcBatchItemWriter<GwLevels> gwLevelsWriter;
	
	@Bean
	public Step deleteGwLevelsStep() {
		return stepBuilderFactory.get("deleteGwLevels")
				.tasklet(deleteGwLevels)
				.build();
	}
	
	@Bean
	public Step transformGwLevelsStep() {
		return stepBuilderFactory
				.get("transformGwLevelsStep")
				.<GwLevels, GwLevels> chunk(100000)
				.reader(gwLevelsReader)
				.processor(gwLevelsProcessor)
				.writer(gwLevelsWriter)
				.build();
	}

	@Bean
	public Flow gwLevelsFlow() {
		return new FlowBuilder<SimpleFlow>("gwLevelsFlow")
				.start(deleteGwLevelsStep())
				.next(transformGwLevelsStep())
				.build();
	}
}
