package gov.acwi.wqp.etl.mysqlnwis.sitefile;

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
public class SitefileTransformation {

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("deleteSitefile")
	private Tasklet deleteSitefile;

	@Autowired
	@Qualifier("sitefileReader")
	private JdbcPagingItemReader<Sitefile> sitefileReader;

	@Autowired
	@Qualifier("sitefileProcessor")
	private PassThroughItemProcessor<Sitefile> sitefileProcessor;

	@Autowired
	@Qualifier("sitefileWriter")
	private JdbcBatchItemWriter<Sitefile> sitefileWriter;

	@Bean
	public Step deleteSitefileStep() {
		return stepBuilderFactory.get("deleteSitefile")
				.tasklet(deleteSitefile)
				.build();
	}

	@Bean
	public Step transformSitefileStep() {
		return stepBuilderFactory
				.get("transformSitefileStep")
				.<Sitefile, Sitefile> chunk(10000)
				.reader(sitefileReader)
				.processor(sitefileProcessor)
				.writer(sitefileWriter)
				.build();
	}

	@Bean
	public Flow sitefileFlow() {
		return new FlowBuilder<SimpleFlow>("sitefileFlow")
				.start(deleteSitefileStep())
				.next(transformSitefileStep())
				.build();
	}

}
