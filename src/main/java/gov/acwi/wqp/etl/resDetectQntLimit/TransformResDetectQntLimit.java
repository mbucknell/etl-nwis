package gov.acwi.wqp.etl.resDetectQntLimit;

import gov.acwi.wqp.etl.EtlConstantUtils;
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
public class TransformResDetectQntLimit {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier(EtlConstantUtils.SETUP_RES_DETECT_QNT_LIMIT_SWAP_TABLE_FLOW)
	private Flow setupResDetectQntLimitSwapTableFlow;

	@Autowired
	@Qualifier("transformResDetectQntLimitTasklet")
	private Tasklet transformResDetectQntLimitTasklet;

	@Autowired
	@Qualifier(EtlConstantUtils.BUILD_RES_DETECT_QNT_LIMIT_INDEXES_FLOW)
	private Flow buildResDetectQntLimitIndexesFlow;

	@Bean
	public Step transformResDetectQntLimitStep() {
		return stepBuilderFactory
				.get("transformResDetectQntLimitStep")
				.tasklet(transformResDetectQntLimitTasklet)
				.build();
	}

	@Bean
	public Flow resDetectQntLimitFlow() {
		return new FlowBuilder<SimpleFlow>("resDetectQntLimitFlow")
				.start(setupResDetectQntLimitSwapTableFlow)
				.next(transformResDetectQntLimitStep())
				.next(buildResDetectQntLimitIndexesFlow)
				.build();
	}

}
