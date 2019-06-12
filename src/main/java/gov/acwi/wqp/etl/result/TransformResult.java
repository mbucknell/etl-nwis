package gov.acwi.wqp.etl.result;

import javax.sql.DataSource;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import gov.acwi.wqp.etl.Application;
import gov.acwi.wqp.etl.EtlConstantUtils;
import gov.acwi.wqp.etl.nwis.result.NwisResult;

@Configuration
public class TransformResult {

	@Autowired
	@Qualifier("resultProcessor")
	private ItemProcessor<NwisResult, Result> processor;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("dataSourceWqp")
	private DataSource dataSourceWqp;

	@Autowired
	@Qualifier(Application.DATASOURCE_NWIS_QUALIFIER)
	private DataSource dataSourceNwis;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_RESULT_SWAP_TABLE_FLOW)
	private Flow setupResultSwapTableFlow;

	@Autowired
	@Qualifier(EtlConstantUtils.BUILD_RESULT_INDEXES_FLOW)
	private Flow buildResultIndexesFlow;

	@Autowired
	@Qualifier("transformResultTasklet")
	private Tasklet transformResultTasklet;
	
	@Bean
	public Step transformResultStep() throws Exception{
		return stepBuilderFactory
				.get("transformResultStep")
				.tasklet(transformResultTasklet)
				.build();
	}

	@Bean
	public Flow resultFlow() throws Exception {
		return new FlowBuilder<SimpleFlow>("resultFlow")
				.start(setupResultSwapTableFlow)
				.next(transformResultStep())
				.next(buildResultIndexesFlow)
				.build();
	}
}
